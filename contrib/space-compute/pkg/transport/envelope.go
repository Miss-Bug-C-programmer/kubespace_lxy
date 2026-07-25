// Package transport provides bounded, mutually-authenticated, persistent
// at-least-once cross-domain message transport. It is independent of scheduler
// framework callbacks and contains no scheduler dependency.
package transport

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
)

const EnvelopeVersion = "spacecompute-envelope-v1"

type Envelope struct {
	ID            string                  `json:"id"`
	Kind          string                  `json:"kind"`
	Source        spacev1.DomainReference `json:"source"`
	Destination   spacev1.DomainReference `json:"destination"`
	MissionUID    string                  `json:"missionUID"`
	PlanID        string                  `json:"planID"`
	Attempt       int32                   `json:"attempt"`
	Sequence      int64                   `json:"sequence"`
	Timestamp     time.Time               `json:"timestamp"`
	Expiry        time.Time               `json:"expiry"`
	PayloadDigest string                  `json:"payloadDigest"`
	Payload       []byte                  `json:"payload"`
	Signature     string                  `json:"signature"`
}

type Limits struct {
	MaxMessageBytes  int64
	MaxQueueItems    int
	MaxQueueBytes    int64
	MaxConcurrent    int
	MaxRetries       int
	DiskRetention    time.Duration
	BackoffBase      time.Duration
	BackoffMax       time.Duration
	JitterFraction   float64
	CircuitFailures  int
	CircuitOpen      time.Duration
	MaximumClockSkew time.Duration
}

func DefaultLimits() Limits {
	return Limits{MaxMessageBytes: 1 << 20, MaxQueueItems: 4096, MaxQueueBytes: 256 << 20, MaxConcurrent: 8, MaxRetries: 15, DiskRetention: 24 * time.Hour, BackoffBase: time.Second, BackoffMax: 2 * time.Minute, JitterFraction: .2, CircuitFailures: 5, CircuitOpen: 2 * time.Minute, MaximumClockSkew: 2 * time.Minute}
}

func (l Limits) Validate() error {
	if l.MaxMessageBytes < 1024 || l.MaxMessageBytes > 64<<20 {
		return fmt.Errorf("max message bytes out of bounds")
	}
	if l.MaxQueueItems < 1 || l.MaxQueueItems > 100000 || l.MaxQueueBytes < l.MaxMessageBytes {
		return fmt.Errorf("queue bounds are invalid")
	}
	if l.MaxConcurrent < 1 || l.MaxConcurrent > 128 || l.MaxRetries < 1 || l.MaxRetries > 100 {
		return fmt.Errorf("concurrency/retry bounds are invalid")
	}
	if l.DiskRetention <= 0 || l.BackoffBase <= 0 || l.BackoffMax < l.BackoffBase || l.JitterFraction < 0 || l.JitterFraction > .5 || l.CircuitFailures < 1 || l.CircuitOpen <= 0 || l.MaximumClockSkew < 0 {
		return fmt.Errorf("transport timing bounds are invalid")
	}
	return nil
}

func NewEnvelope(id, kind string, source, destination spacev1.DomainReference, missionUID, planID string, attempt int32, sequence int64, timestamp, expiry time.Time, payload []byte) *Envelope {
	sum := sha256.Sum256(payload)
	return &Envelope{ID: id, Kind: kind, Source: source, Destination: destination, MissionUID: missionUID, PlanID: planID, Attempt: attempt, Sequence: sequence, Timestamp: timestamp.UTC(), Expiry: expiry.UTC(), PayloadDigest: hex.EncodeToString(sum[:]), Payload: append([]byte(nil), payload...)}
}

func (e *Envelope) canonicalBytes() ([]byte, error) {
	if e == nil {
		return nil, fmt.Errorf("envelope is required")
	}
	var b bytes.Buffer
	field := func(k, v string) { fmt.Fprintf(&b, "%s=%s\n", k, strconv.Quote(v)) }
	integer := func(k string, v int64) { fmt.Fprintf(&b, "%s=%d\n", k, v) }
	domain := func(prefix string, d spacev1.DomainReference) {
		field(prefix+".name", strings.ToLower(strings.TrimSpace(d.Name)))
		field(prefix+".clusterID", strings.ToLower(strings.TrimSpace(d.ClusterID)))
		field(prefix+".orbitClass", strings.ToLower(strings.TrimSpace(string(d.OrbitClass))))
	}
	field("version", EnvelopeVersion)
	field("id", e.ID)
	field("kind", e.Kind)
	domain("source", e.Source)
	domain("destination", e.Destination)
	field("missionUID", e.MissionUID)
	field("planID", e.PlanID)
	integer("attempt", int64(e.Attempt))
	integer("sequence", e.Sequence)
	field("timestamp", e.Timestamp.UTC().Format(time.RFC3339Nano))
	field("expiry", e.Expiry.UTC().Format(time.RFC3339Nano))
	field("payloadDigest", e.PayloadDigest)
	return b.Bytes(), nil
}

func (e *Envelope) Sign(private ed25519.PrivateKey) error {
	raw, err := e.canonicalBytes()
	if err != nil {
		return err
	}
	if len(private) != ed25519.PrivateKeySize {
		return fmt.Errorf("Ed25519 private key is required")
	}
	e.Signature = base64.StdEncoding.EncodeToString(ed25519.Sign(private, raw))
	return nil
}

func (e *Envelope) Verify(public ed25519.PublicKey, now time.Time, limits Limits) error {
	if err := limits.Validate(); err != nil {
		return err
	}
	if e.ID == "" || len(e.ID) > 128 || e.Kind == "" || len(e.Kind) > 64 || e.Sequence < 1 || e.Attempt < 1 {
		return fmt.Errorf("envelope stable identity/sequence is invalid")
	}
	if e.Timestamp.IsZero() || e.Expiry.IsZero() || !e.Expiry.After(e.Timestamp) {
		return fmt.Errorf("envelope time interval is invalid")
	}
	now = now.UTC()
	if e.Timestamp.After(now.Add(limits.MaximumClockSkew)) {
		return fmt.Errorf("envelope timestamp exceeds clock skew")
	}
	if !e.Expiry.After(now.Add(-limits.MaximumClockSkew)) {
		return fmt.Errorf("envelope expired")
	}
	if int64(len(e.Payload)) > limits.MaxMessageBytes {
		return fmt.Errorf("envelope payload exceeds %d bytes", limits.MaxMessageBytes)
	}
	sum := sha256.Sum256(e.Payload)
	if hex.EncodeToString(sum[:]) != e.PayloadDigest {
		return fmt.Errorf("payload digest mismatch")
	}
	raw, err := e.canonicalBytes()
	if err != nil {
		return err
	}
	sig, err := base64.StdEncoding.DecodeString(e.Signature)
	if err != nil || len(sig) != ed25519.SignatureSize {
		return fmt.Errorf("invalid envelope signature encoding")
	}
	if len(public) != ed25519.PublicKeySize || !ed25519.Verify(public, raw, sig) {
		return fmt.Errorf("envelope signature verification failed")
	}
	return nil
}

func SPIFFEID(domain spacev1.DomainReference, trustDomain string) string {
	trustDomain = strings.TrimSpace(trustDomain)
	if trustDomain == "" {
		trustDomain = "spacecompute.k3s.io"
	}
	return fmt.Sprintf("spiffe://%s/domain/%s/%s/%s", trustDomain, url.PathEscape(strings.ToLower(string(domain.OrbitClass))), url.PathEscape(strings.ToLower(domain.ClusterID)), url.PathEscape(strings.ToLower(domain.Name)))
}
