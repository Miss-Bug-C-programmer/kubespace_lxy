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

	"k8s.io/klog/v2"

	spaceconversion "github.com/k3s-io/k3s/contrib/space-compute/pkg/conversion"
)

const componentName = "space-compute-conversion-webhook"

type options struct {
	bindAddress, tlsCertFile, tlsKeyFile string
	maxBodyBytes                         int64
}

func main() {
	klog.InitFlags(nil)
	opt := options{}
	flag.StringVar(&opt.bindAddress, "bind-address", ":9445", "HTTPS conversion and health listen address")
	flag.StringVar(&opt.tlsCertFile, "tls-cert-file", "/tls/tls.crt", "Webhook serving certificate")
	flag.StringVar(&opt.tlsKeyFile, "tls-private-key-file", "/tls/tls.key", "Webhook serving private key")
	flag.Int64Var(&opt.maxBodyBytes, "max-conversion-body-bytes", spaceconversion.DefaultMaxConversionBodyBytes, "Maximum ConversionReview request body")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, opt); err != nil {
		klog.Fatalf("%s failed: %v", componentName, err)
	}
}

func run(ctx context.Context, opt options) error {
	if opt.bindAddress == "" || opt.tlsCertFile == "" || opt.tlsKeyFile == "" {
		return fmt.Errorf("bind address and TLS certificate/key files are required")
	}
	if opt.maxBodyBytes < 4096 || opt.maxBodyBytes > 16<<20 {
		return fmt.Errorf("max-conversion-body-bytes must be between 4096 and %d", 16<<20)
	}
	mux := http.NewServeMux()
	mux.Handle("/convert", spaceconversion.Handler{MaxBodyBytes: opt.maxBodyBytes})
	mux.HandleFunc("/livez", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	server := &http.Server{Addr: opt.bindAddress, Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second, TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12}}
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
