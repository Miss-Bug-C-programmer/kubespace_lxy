package main

import (
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"os"
	"strings"
	"time"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
	spacetransport "github.com/k3s-io/k3s/contrib/space-compute/pkg/transport"
)

type peerConfig struct {
	Domain        spacev1.DomainReference `json:"domain"`
	URL           string                  `json:"url"`
	PublicKeyFile string                  `json:"publicKeyFile"`
}

type agentConfig struct {
	LocalDomain             spacev1.DomainReference `json:"localDomain"`
	ReporterPrincipal       string                  `json:"reporterPrincipal"`
	TrustDomain             string                  `json:"trustDomain"`
	ListenAddress           string                  `json:"listenAddress"`
	ReportAddress           string                  `json:"reportAddress"`
	HealthAddress           string                  `json:"healthAddress"`
	StateDir                string                  `json:"stateDir"`
	DataRoot                string                  `json:"dataRoot"`
	TLSCertFile             string                  `json:"tlsCertFile"`
	TLSKeyFile              string                  `json:"tlsKeyFile"`
	ClientCAFile            string                  `json:"clientCAFile"`
	SigningKeyFile          string                  `json:"signingKeyFile"`
	LeaseTTLSeconds         int64                   `json:"leaseTTLSeconds"`
	LeaseClockSkewSeconds   int64                   `json:"leaseClockSkewSeconds"`
	MaxChunkBytes           int                     `json:"maxChunkBytes"`
	MaxMessageBytes         int64                   `json:"maxMessageBytes"`
	MaxQueueItems           int                     `json:"maxQueueItems"`
	MaxQueueBytes           int64                   `json:"maxQueueBytes"`
	MaxConcurrent           int                     `json:"maxConcurrent"`
	MaxRetries              int                     `json:"maxRetries"`
	DiskRetentionSeconds    int64                   `json:"diskRetentionSeconds"`
	BackoffBaseMillis       int64                   `json:"backoffBaseMillis"`
	BackoffMaxMillis        int64                   `json:"backoffMaxMillis"`
	JitterFraction          float64                 `json:"jitterFraction"`
	CircuitFailures         int                     `json:"circuitFailures"`
	CircuitOpenSeconds      int64                   `json:"circuitOpenSeconds"`
	MaximumClockSkewSeconds int64                   `json:"maximumClockSkewSeconds"`
	Peers                   []peerConfig            `json:"peers"`
}

func loadAgentConfig(path string) (agentConfig, error) {
	var cfg agentConfig
	raw, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return cfg, err
	}
	if cfg.TrustDomain == "" {
		cfg.TrustDomain = "spacecompute.k3s.io"
	}
	if cfg.ListenAddress == "" {
		cfg.ListenAddress = ":10443"
	}
	if cfg.ReportAddress == "" {
		cfg.ReportAddress = ":10445"
	}
	if cfg.HealthAddress == "" {
		cfg.HealthAddress = ":10446"
	}
	if cfg.LeaseTTLSeconds == 0 {
		cfg.LeaseTTLSeconds = 120
	}
	if cfg.LeaseClockSkewSeconds == 0 {
		cfg.LeaseClockSkewSeconds = 2
	}
	if cfg.MaxChunkBytes == 0 {
		cfg.MaxChunkBytes = 256 << 10
	}
	defaults := spacetransport.DefaultLimits()
	if cfg.MaxMessageBytes == 0 {
		cfg.MaxMessageBytes = defaults.MaxMessageBytes
	}
	if cfg.MaxQueueItems == 0 {
		cfg.MaxQueueItems = defaults.MaxQueueItems
	}
	if cfg.MaxQueueBytes == 0 {
		cfg.MaxQueueBytes = defaults.MaxQueueBytes
	}
	if cfg.MaxConcurrent == 0 {
		cfg.MaxConcurrent = defaults.MaxConcurrent
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = defaults.MaxRetries
	}
	if cfg.DiskRetentionSeconds == 0 {
		cfg.DiskRetentionSeconds = int64(defaults.DiskRetention / time.Second)
	}
	if cfg.BackoffBaseMillis == 0 {
		cfg.BackoffBaseMillis = int64(defaults.BackoffBase / time.Millisecond)
	}
	if cfg.BackoffMaxMillis == 0 {
		cfg.BackoffMaxMillis = int64(defaults.BackoffMax / time.Millisecond)
	}
	if cfg.JitterFraction == 0 {
		cfg.JitterFraction = defaults.JitterFraction
	}
	if cfg.CircuitFailures == 0 {
		cfg.CircuitFailures = defaults.CircuitFailures
	}
	if cfg.CircuitOpenSeconds == 0 {
		cfg.CircuitOpenSeconds = int64(defaults.CircuitOpen / time.Second)
	}
	if cfg.MaximumClockSkewSeconds == 0 {
		cfg.MaximumClockSkewSeconds = int64(defaults.MaximumClockSkew / time.Second)
	}
	if cfg.LocalDomain.Name == "" || cfg.LocalDomain.ClusterID == "" || cfg.ReporterPrincipal == "" || cfg.StateDir == "" || cfg.DataRoot == "" || cfg.TLSCertFile == "" || cfg.TLSKeyFile == "" || cfg.ClientCAFile == "" || cfg.SigningKeyFile == "" {
		return cfg, fmt.Errorf("localDomain, reporterPrincipal, state/data directories and TLS/signing files are required")
	}
	if len(cfg.Peers) > 64 {
		return cfg, fmt.Errorf("peer count exceeds 64")
	}
	if cfg.LeaseClockSkewSeconds < 0 || cfg.LeaseClockSkewSeconds > 30 || 4*cfg.LeaseClockSkewSeconds >= cfg.LeaseTTLSeconds {
		return cfg, fmt.Errorf("leaseClockSkewSeconds must be 0..30 and strictly less than one quarter of leaseTTLSeconds")
	}
	if err := cfg.limits().Validate(); err != nil {
		return cfg, err
	}
	return cfg, nil
}
func (c agentConfig) limits() spacetransport.Limits {
	return spacetransport.Limits{MaxMessageBytes: c.MaxMessageBytes, MaxQueueItems: c.MaxQueueItems, MaxQueueBytes: c.MaxQueueBytes, MaxConcurrent: c.MaxConcurrent, MaxRetries: c.MaxRetries, DiskRetention: time.Duration(c.DiskRetentionSeconds) * time.Second, BackoffBase: time.Duration(c.BackoffBaseMillis) * time.Millisecond, BackoffMax: time.Duration(c.BackoffMaxMillis) * time.Millisecond, JitterFraction: c.JitterFraction, CircuitFailures: c.CircuitFailures, CircuitOpen: time.Duration(c.CircuitOpenSeconds) * time.Second, MaximumClockSkew: time.Duration(c.MaximumClockSkewSeconds) * time.Second}
}

func loadPrivateKey(path string) (ed25519.PrivateKey, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if block, _ := pem.Decode(raw); block != nil {
		key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return nil, err
		}
		value, ok := key.(ed25519.PrivateKey)
		if !ok {
			return nil, fmt.Errorf("signing key is not Ed25519")
		}
		return value, nil
	}
	trimmed := strings.TrimSpace(string(raw))
	if decoded, err := base64.StdEncoding.DecodeString(trimmed); err == nil {
		raw = decoded
	}
	if len(raw) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("Ed25519 private key must be raw/base64 64 bytes or PKCS8 PEM")
	}
	return ed25519.PrivateKey(append([]byte(nil), raw...)), nil
}
func loadPublicKey(path string) (ed25519.PublicKey, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if block, _ := pem.Decode(raw); block != nil {
		key, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			return nil, err
		}
		value, ok := key.(ed25519.PublicKey)
		if !ok {
			return nil, fmt.Errorf("peer key is not Ed25519")
		}
		return value, nil
	}
	trimmed := strings.TrimSpace(string(raw))
	if decoded, err := base64.StdEncoding.DecodeString(trimmed); err == nil {
		raw = decoded
	}
	if len(raw) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("Ed25519 public key must be raw/base64 32 bytes or PKIX PEM")
	}
	return ed25519.PublicKey(append([]byte(nil), raw...)), nil
}
func loadTLS(cfg agentConfig) (tls.Certificate, *x509.CertPool, error) {
	cert, err := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
	if err != nil {
		return cert, nil, err
	}
	ca, err := os.ReadFile(cfg.ClientCAFile)
	if err != nil {
		return cert, nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return cert, nil, fmt.Errorf("client CA file contains no certificates")
	}
	return cert, pool, nil
}
