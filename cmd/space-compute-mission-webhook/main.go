package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	clientcmd "k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"

	spaceadmission "github.com/k3s-io/k3s/contrib/space-compute/pkg/admission"
)

const componentName = "space-compute-mission-webhook"

type options struct {
	kubeconfig, master, bindAddress, tlsCertFile, tlsKeyFile, policyFile string
	maxBodyBytes                                                         int64
}

func main() {
	klog.InitFlags(nil)
	opt := options{}
	flag.StringVar(&opt.kubeconfig, "kubeconfig", "", "Path to kubeconfig; empty uses in-cluster configuration")
	flag.StringVar(&opt.master, "master", "", "Optional API server address")
	flag.StringVar(&opt.bindAddress, "bind-address", ":9444", "HTTPS admission and health listen address")
	flag.StringVar(&opt.tlsCertFile, "tls-cert-file", "/tls/tls.crt", "Webhook serving certificate")
	flag.StringVar(&opt.tlsKeyFile, "tls-private-key-file", "/tls/tls.key", "Webhook serving private key")
	flag.StringVar(&opt.policyFile, "policy-file", "/etc/space-compute-mission-security/policy.json", "Strict administrator-owned Mission security policy")
	flag.Int64Var(&opt.maxBodyBytes, "max-admission-body-bytes", spaceadmission.DefaultMaxAdmissionBodyBytes, "Maximum AdmissionReview request body")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, opt); err != nil {
		klog.Fatalf("%s failed: %v", componentName, err)
	}
}

func run(ctx context.Context, opt options) error {
	if opt.bindAddress == "" || opt.tlsCertFile == "" || opt.tlsKeyFile == "" || opt.policyFile == "" {
		return fmt.Errorf("bind address, TLS certificate/key files and policy file are required")
	}
	if opt.maxBodyBytes < 4096 || opt.maxBodyBytes > 8<<20 {
		return fmt.Errorf("max-admission-body-bytes must be between 4096 and %d", 8<<20)
	}
	policy, err := spaceadmission.LoadMissionSecurityPolicy(opt.policyFile)
	if err != nil {
		return err
	}
	config, err := kubeConfig(opt.master, opt.kubeconfig)
	if err != nil {
		return err
	}
	config.UserAgent = componentName
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("create Kubernetes client: %w", err)
	}
	reviewer, err := spaceadmission.NewKubernetesSubjectAccessReviewer(client)
	if err != nil {
		return err
	}
	validator, err := spaceadmission.NewMissionValidator(policy, reviewer)
	if err != nil {
		return err
	}
	handler, err := spaceadmission.NewHandler(validator, opt.maxBodyBytes)
	if err != nil {
		return err
	}

	mux := http.NewServeMux()
	mux.Handle("/validate", handler)
	mux.HandleFunc("/livez", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	server := &http.Server{
		Addr:              opt.bindAddress,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
		TLSConfig:         &tls.Config{MinVersion: tls.VersionTLS12},
	}
	errCh := make(chan error, 1)
	go func() {
		err := server.ListenAndServeTLS(opt.tlsCertFile, opt.tlsKeyFile)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}

func kubeConfig(master, kubeconfig string) (*rest.Config, error) {
	if master != "" || kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags(master, kubeconfig)
	}
	return rest.InClusterConfig()
}
