package main

import (
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
	spacetransport "github.com/k3s-io/k3s/contrib/space-compute/pkg/transport"
)

type peerRecord struct {
	domain   spacev1.DomainReference
	endpoint string
	key      ed25519.PublicKey
	client   *http.Client
}
type peerRegistry struct{ records map[string]peerRecord }

func peerKey(d spacev1.DomainReference) string {
	return strings.ToLower(string(d.OrbitClass) + "/" + d.ClusterID + "/" + d.Name)
}
func newPeerRegistry(cfg agentConfig, cert tls.Certificate, roots *x509.CertPool) (*peerRegistry, error) {
	r := &peerRegistry{records: map[string]peerRecord{}}
	for _, p := range cfg.Peers {
		if p.Domain.Name == "" || p.Domain.ClusterID == "" || p.URL == "" || p.PublicKeyFile == "" {
			return nil, fmt.Errorf("peer domain/url/publicKeyFile are required")
		}
		parsed, err := url.Parse(p.URL)
		if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
			return nil, fmt.Errorf("peer %s URL must be https", p.Domain.Name)
		}
		key, err := loadPublicKey(p.PublicKeyFile)
		if err != nil {
			return nil, fmt.Errorf("peer %s public key: %w", p.Domain.Name, err)
		}
		expected := spacetransport.SPIFFEID(p.Domain, cfg.TrustDomain)
		tlsConfig := &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}, RootCAs: roots, InsecureSkipVerify: true, VerifyConnection: func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return fmt.Errorf("peer certificate missing")
			}
			intermediates := x509.NewCertPool()
			for _, c := range cs.PeerCertificates[1:] {
				intermediates.AddCert(c)
			}
			if _, err := cs.PeerCertificates[0].Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, CurrentTime: time.Now()}); err != nil {
				return fmt.Errorf("peer certificate chain: %w", err)
			}
			for _, uri := range cs.PeerCertificates[0].URIs {
				if uri.String() == expected {
					return nil
				}
			}
			return fmt.Errorf("peer certificate SPIFFE identity does not match %s", expected)
		}}
		client := &http.Client{Transport: &http.Transport{TLSClientConfig: tlsConfig, MaxIdleConns: cfg.MaxConcurrent * 2, MaxIdleConnsPerHost: cfg.MaxConcurrent, IdleConnTimeout: 90 * time.Second}, Timeout: 30 * time.Second}
		record := peerRecord{domain: p.Domain, endpoint: strings.TrimRight(p.URL, "/"), key: key, client: client}
		k := peerKey(p.Domain)
		if _, exists := r.records[k]; exists {
			return nil, fmt.Errorf("duplicate peer domain %s", p.Domain.Name)
		}
		r.records[k] = record
	}
	return r, nil
}
func (r *peerRegistry) Endpoint(d spacev1.DomainReference) (string, error) {
	p, ok := r.records[peerKey(d)]
	if !ok {
		return "", fmt.Errorf("peer endpoint not configured")
	}
	return p.endpoint, nil
}
func (r *peerRegistry) PublicKey(d spacev1.DomainReference) (ed25519.PublicKey, error) {
	p, ok := r.records[peerKey(d)]
	if !ok {
		return nil, fmt.Errorf("peer public key not configured")
	}
	return p.key, nil
}
func (r *peerRegistry) Client(d spacev1.DomainReference) (*http.Client, error) {
	p, ok := r.records[peerKey(d)]
	if !ok {
		return nil, fmt.Errorf("peer HTTP client not configured")
	}
	return p.client, nil
}
