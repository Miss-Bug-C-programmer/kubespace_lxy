package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	clientcmd "k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"

	spaceexecution "github.com/k3s-io/k3s/contrib/space-compute/pkg/execution"
	spacetransport "github.com/k3s-io/k3s/contrib/space-compute/pkg/transport"
)

const componentName = "space-compute-domain-agent"

type reportRequest struct {
	Namespace string                `json:"namespace"`
	Report    spaceexecution.Report `json:"report"`
}

func main() {
	klog.InitFlags(nil)
	configPath := flag.String("config", "/etc/space-compute/domain-agent.json", "Path to strict domain-agent JSON config")
	kubeconfig := flag.String("kubeconfig", "", "Path to kubeconfig; empty uses in-cluster config")
	flag.Parse()
	if err := run(*configPath, *kubeconfig); err != nil {
		klog.Fatalf("%s: %v", componentName, err)
	}
}
func run(configPath, kubeconfig string) error {
	cfg, err := loadAgentConfig(configPath)
	if err != nil {
		return err
	}
	privateKey, err := loadPrivateKey(cfg.SigningKeyFile)
	if err != nil {
		return err
	}
	cert, roots, err := loadTLS(cfg)
	if err != nil {
		return err
	}
	peers, err := newPeerRegistry(cfg, cert, roots)
	if err != nil {
		return err
	}
	restConfig, err := kubeConfig(kubeconfig)
	if err != nil {
		return err
	}
	dynamicClient, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return err
	}
	client, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return err
	}
	limits := cfg.limits()
	queue, err := spacetransport.OpenDiskQueue(filepath.Join(cfg.StateDir, "outbox"), limits)
	if err != nil {
		return err
	}
	dedupe, err := spacetransport.OpenDedupeStore(filepath.Join(cfg.StateDir, "dedupe.json"), limits.DiskRetention)
	if err != nil {
		return err
	}
	assignments, err := spacetransport.OpenFileAssignmentStore(filepath.Join(cfg.StateDir, "assignments"), 4096)
	if err != nil {
		return err
	}
	store := &kubeAgentStore{dynamic: dynamicClient, client: client, remote: assignments}
	agent := &spacetransport.Agent{Local: cfg.LocalDomain, ReporterPrincipal: cfg.ReporterPrincipal, PrivateKey: privateKey, PeerKeys: peers, StateDir: cfg.StateDir, Queue: queue, Store: store, Executor: &kubeExecutor{client: client}, Assembler: &spacetransport.FileAssembler{Root: cfg.DataRoot, MaxBytes: 1 << 40}, DataRoot: cfg.DataRoot, LeaseTTL: time.Duration(cfg.LeaseTTLSeconds) * time.Second, LeaseClockSkew: time.Duration(cfg.LeaseClockSkewSeconds) * time.Second, MaxChunkBytes: cfg.MaxChunkBytes, Limits: limits}
	if err := agent.Validate(); err != nil {
		return err
	}
	receiver := &spacetransport.Receiver{Local: cfg.LocalDomain, TrustDomain: cfg.TrustDomain, Limits: limits, Keys: peers, Dedupe: dedupe, Handler: agent.HandleEnvelope}
	sender := &spacetransport.Sender{Queue: queue, Clients: peers, Endpoints: peers, Limits: limits}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	errCh := make(chan error, 4)
	go func() {
		errCh <- serveEnvelope(ctx, cfg.ListenAddress, spacetransport.ServerTLSConfig(cert, roots), receiver)
	}()
	go func() { errCh <- serveReport(ctx, cfg.ReportAddress, spacetransport.ServerOnlyTLSConfig(cert), agent) }()
	go func() { errCh <- serveHealth(ctx, cfg.HealthAddress) }()
	go func() { errCh <- sender.Run(ctx) }()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-errCh:
			if err != nil && ctx.Err() == nil {
				return err
			}
		case <-ticker.C:
			if err := agent.ReconcileOnce(ctx); err != nil {
				klog.Errorf("domain reconcile failed: %v", err)
			}
		}
	}
}
func kubeConfig(path string) (*rest.Config, error) {
	if path != "" {
		return clientcmd.BuildConfigFromFlags("", path)
	}
	return rest.InClusterConfig()
}
func serveEnvelope(ctx context.Context, address string, tlsConfig *tls.Config, handler http.Handler) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 90 * time.Second}
	tlsListener := tls.NewListener(listener, tlsConfig)
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	err = server.Serve(tlsListener)
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}
func serveReport(ctx context.Context, address string, tlsConfig *tls.Config, agent *spacetransport.Agent) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/report", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", 405)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "report too large", 413)
			return
		}
		var request reportRequest
		if err := json.Unmarshal(raw, &request); err != nil {
			http.Error(w, "invalid report", 400)
			return
		}
		if request.Namespace == "" {
			http.Error(w, "namespace required", 400)
			return
		}
		if err := agent.ReportExecution(r.Context(), request.Namespace, request.Report); err != nil {
			http.Error(w, err.Error(), 403)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	return serveEnvelope(ctx, address, tlsConfig, mux)
}
func serveHealth(ctx context.Context, address string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/livez", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	return servePlain(ctx, address, mux)
}
func servePlain(ctx context.Context, address string, handler http.Handler) error {
	server := &http.Server{Addr: address, Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	err := server.ListenAndServe()
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}
