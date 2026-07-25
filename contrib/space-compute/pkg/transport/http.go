package transport

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"math"
	mrand "math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
)

type PeerKeys interface {
	PublicKey(spacev1.DomainReference) (ed25519.PublicKey, error)
}

type Handler func(context.Context, *Envelope) error

type Receiver struct {
	Local       spacev1.DomainReference
	TrustDomain string
	Limits      Limits
	Keys        PeerKeys
	Dedupe      *DedupeStore
	Handler     Handler
	Now         func() time.Time
	mu          sync.Mutex
}

func (r *Receiver) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if r.Keys == nil || r.Dedupe == nil || r.Handler == nil {
		http.Error(w, "receiver unavailable", http.StatusServiceUnavailable)
		return
	}
	if err := r.Limits.Validate(); err != nil {
		http.Error(w, "receiver limits invalid", http.StatusServiceUnavailable)
		return
	}
	req.Body = http.MaxBytesReader(w, req.Body, r.Limits.MaxMessageBytes+64*1024)
	raw, err := io.ReadAll(req.Body)
	if err != nil {
		http.Error(w, "bounded envelope read failed", http.StatusRequestEntityTooLarge)
		return
	}
	var envelope Envelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		http.Error(w, "invalid envelope JSON", http.StatusBadRequest)
		return
	}
	if envelope.Destination != r.Local {
		http.Error(w, "destination domain mismatch", http.StatusForbidden)
		return
	}
	if req.TLS == nil || len(req.TLS.PeerCertificates) == 0 {
		http.Error(w, "mutual TLS client certificate required", http.StatusUnauthorized)
		return
	}
	expected := SPIFFEID(envelope.Source, r.TrustDomain)
	matched := false
	for _, uri := range req.TLS.PeerCertificates[0].URIs {
		if uri.String() == expected {
			matched = true
			break
		}
	}
	if !matched {
		http.Error(w, "client certificate SPIFFE identity does not match envelope source", http.StatusForbidden)
		return
	}
	publicKey, err := r.Keys.PublicKey(envelope.Source)
	if err != nil {
		http.Error(w, "unknown source domain", http.StatusForbidden)
		return
	}
	now := r.now()
	if err := envelope.Verify(publicKey, now, r.Limits); err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}

	// Serialize the check/handle/commit boundary so concurrent duplicate
	// deliveries cannot execute the durable handler twice. The dedupe record is
	// written only after the handler succeeds; a failed handler remains retryable.
	r.mu.Lock()
	defer r.mu.Unlock()
	seen, err := r.Dedupe.Seen(envelope.ID, envelope.Sequence, now)
	if err != nil {
		http.Error(w, "dedupe read failed", http.StatusServiceUnavailable)
		return
	}
	if seen {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if err := r.Handler(req.Context(), &envelope); err != nil {
		http.Error(w, "durable handler rejected envelope", http.StatusServiceUnavailable)
		return
	}
	if err := r.Dedupe.Record(envelope.ID, envelope.Sequence, now); err != nil {
		http.Error(w, "dedupe persistence failed", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (r *Receiver) now() time.Time {
	if r.Now != nil {
		return r.Now().UTC()
	}
	return time.Now().UTC()
}

func ServerTLSConfig(cert tls.Certificate, clientCAs *x509.CertPool) *tls.Config {
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientCAs,
	}
}

// ServerOnlyTLSConfig protects the local execution report endpoint. The fence
// token authenticates the workload report; cross-domain transport still always
// uses ServerTLSConfig and mutual certificate authentication.
func ServerOnlyTLSConfig(cert tls.Certificate) *tls.Config {
	return &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}}
}

type EndpointResolver interface {
	Endpoint(spacev1.DomainReference) (string, error)
}

type HTTPClientResolver interface {
	Client(spacev1.DomainReference) (*http.Client, error)
}

type circuitState struct {
	failures  int
	openUntil time.Time
}

type Sender struct {
	Queue     *DiskQueue
	Client    *http.Client
	Clients   HTTPClientResolver
	Endpoints EndpointResolver
	Limits    Limits
	Now       func() time.Time

	mu       sync.Mutex
	circuits map[string]circuitState
	random   *mrand.Rand
}

func (s *Sender) Run(ctx context.Context) error {
	if s.Queue == nil || (s.Client == nil && s.Clients == nil) || s.Endpoints == nil {
		return fmt.Errorf("sender queue, HTTP client resolver and endpoints are required")
	}
	if err := s.Limits.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	if s.circuits == nil {
		s.circuits = map[string]circuitState{}
	}
	if s.random == nil {
		s.random = mrand.New(mrand.NewSource(1))
	}
	s.mu.Unlock()

	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	semaphore := make(chan struct{}, s.Limits.MaxConcurrent)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			now := s.now()
			items, err := s.Queue.Due(now, s.Limits.MaxConcurrent)
			if err != nil {
				return err
			}
			for _, candidate := range items {
				item := candidate
				if s.circuitOpen(item.Envelope.Destination, now) {
					continue
				}
				select {
				case semaphore <- struct{}{}:
					go func() {
						defer func() { <-semaphore }()
						s.deliver(ctx, item)
					}()
				default:
				}
			}
		}
	}
}

func (s *Sender) deliver(ctx context.Context, item queuedEnvelope) {
	envelope := item.Envelope
	endpoint, err := s.Endpoints.Endpoint(envelope.Destination)
	client := s.Client
	if err == nil && s.Clients != nil {
		client, err = s.Clients.Client(envelope.Destination)
	}
	if err == nil && client == nil {
		err = fmt.Errorf("no HTTP client for destination")
	}
	if err == nil {
		var raw []byte
		raw, err = json.Marshal(&envelope)
		if err == nil {
			var request *http.Request
			request, err = http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(endpoint, "/")+"/v1/envelopes", strings.NewReader(string(raw)))
			if err == nil {
				request.Header.Set("Content-Type", "application/json")
				var response *http.Response
				response, err = client.Do(request)
				if err == nil {
					_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
					response.Body.Close()
					if response.StatusCode >= 200 && response.StatusCode < 300 {
						_ = s.Queue.Ack(envelope.ID, envelope.Sequence)
						s.recordSuccess(envelope.Destination)
						return
					}
					err = fmt.Errorf("remote status %s", response.Status)
				}
			}
		}
	}
	s.recordFailure(envelope.Destination)
	_ = s.Queue.Fail(item, err, s.now().Add(s.backoff(item.Attempts+1)))
}

func (s *Sender) now() time.Time {
	if s.Now != nil {
		return s.Now().UTC()
	}
	return time.Now().UTC()
}

func (s *Sender) backoff(attempt int) time.Duration {
	factor := math.Pow(2, float64(attempt-1))
	delay := time.Duration(float64(s.Limits.BackoffBase) * factor)
	if delay > s.Limits.BackoffMax {
		delay = s.Limits.BackoffMax
	}
	s.mu.Lock()
	jitter := (s.random.Float64()*2 - 1) * s.Limits.JitterFraction
	s.mu.Unlock()
	return time.Duration(float64(delay) * (1 + jitter))
}

func (s *Sender) circuitKey(domain spacev1.DomainReference) string {
	return strings.ToLower(string(domain.OrbitClass) + "/" + domain.ClusterID + "/" + domain.Name)
}

func (s *Sender) circuitOpen(domain spacev1.DomainReference, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.circuits[s.circuitKey(domain)].openUntil.After(now)
}

func (s *Sender) recordSuccess(domain spacev1.DomainReference) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.circuits, s.circuitKey(domain))
}

func (s *Sender) recordFailure(domain spacev1.DomainReference) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := s.circuitKey(domain)
	state := s.circuits[key]
	state.failures++
	if state.failures >= s.Limits.CircuitFailures {
		state.openUntil = s.nowUnlocked().Add(s.Limits.CircuitOpen)
		state.failures = 0
	}
	s.circuits[key] = state
}

func (s *Sender) nowUnlocked() time.Time {
	if s.Now != nil {
		return s.Now().UTC()
	}
	return time.Now().UTC()
}
