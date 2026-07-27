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
	"sync/atomic"
	"syscall"
	"time"

	"k8s.io/client-go/dynamic"
	dynamicinformer "k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	clientcmd "k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"

	spaceadmission "github.com/k3s-io/k3s/contrib/space-compute/pkg/admission"
	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
	spacekube "github.com/k3s-io/k3s/contrib/space-compute/pkg/kube"
)

const componentName = "space-compute-reporter-webhook"

type options struct {
	kubeconfig, master, bindAddress, tlsCertFile, tlsKeyFile string
	publicKeySecretNamespace, publicKeySecretName            string
	maxBodyBytes                                             int64
	maxLinkSnapshots, maxResourceSummaries                   int
	reporterQPS                                              float64
	reporterBurst, maxTrackedPrincipals                      int
}

func main() {
	klog.InitFlags(nil)
	opt := options{}
	flag.StringVar(&opt.kubeconfig, "kubeconfig", "", "Path to kubeconfig; empty uses in-cluster configuration")
	flag.StringVar(&opt.master, "master", "", "Optional API server address")
	flag.StringVar(&opt.bindAddress, "bind-address", ":9443", "HTTPS admission and health listen address")
	flag.StringVar(&opt.tlsCertFile, "tls-cert-file", "/tls/tls.crt", "Webhook serving certificate")
	flag.StringVar(&opt.tlsKeyFile, "tls-private-key-file", "/tls/tls.key", "Webhook serving private key")
	flag.StringVar(&opt.publicKeySecretNamespace, "reporter-public-key-secret-namespace", "kube-system", "Namespace of the single reporter public-key Secret")
	flag.StringVar(&opt.publicKeySecretName, "reporter-public-key-secret-name", "space-compute-reporter-public-keys", "Name of the single reporter public-key Secret")
	flag.Int64Var(&opt.maxBodyBytes, "max-admission-body-bytes", spaceadmission.DefaultMaxAdmissionBodyBytes, "Maximum AdmissionReview request body")
	flag.IntVar(&opt.maxLinkSnapshots, "max-link-snapshots", 10000, "Cluster-wide admission quota for SpaceLinkSnapshot objects")
	flag.IntVar(&opt.maxResourceSummaries, "max-resource-summaries", 10000, "Cluster-wide admission quota for SpaceDomainResourceSummary objects")
	flag.Float64Var(&opt.reporterQPS, "reporter-qps", 20, "Per authenticated reporter/gateway admission QPS")
	flag.IntVar(&opt.reporterBurst, "reporter-burst", 40, "Per authenticated reporter/gateway admission burst")
	flag.IntVar(&opt.maxTrackedPrincipals, "max-rate-limit-principals", 4096, "Maximum reporter principals tracked by the in-memory admission limiter")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, opt); err != nil {
		klog.Fatalf("%s failed: %v", componentName, err)
	}
}

type informerReporterCounter struct {
	links     atomic.Int64
	resources atomic.Int64
}

func (c *informerReporterCounter) Count(resource string) int {
	switch resource {
	case "spacelinksnapshots":
		return int(c.links.Load())
	case "spacedomainresourcesummaries":
		return int(c.resources.Load())
	default:
		return 0
	}
}

func incrementCounter(counter *atomic.Int64) func(interface{}) {
	return func(interface{}) { counter.Add(1) }
}

func decrementCounter(counter *atomic.Int64) func(interface{}) {
	return func(interface{}) {
		for {
			current := counter.Load()
			if current <= 0 || counter.CompareAndSwap(current, current-1) {
				return
			}
		}
	}
}

func run(ctx context.Context, opt options) error {
	if opt.bindAddress == "" || opt.tlsCertFile == "" || opt.tlsKeyFile == "" {
		return fmt.Errorf("bind address and TLS certificate/key files are required")
	}
	if opt.maxBodyBytes < 4096 || opt.maxBodyBytes > 8<<20 {
		return fmt.Errorf("max-admission-body-bytes must be between 4096 and %d", 8<<20)
	}
	config, err := kubeConfig(opt.master, opt.kubeconfig)
	if err != nil {
		return err
	}
	config.UserAgent = componentName
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("create dynamic client: %w", err)
	}
	coreClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("create Kubernetes client: %w", err)
	}
	factory := dynamicinformer.NewDynamicSharedInformerFactory(dynamicClient, 10*time.Minute)
	links := factory.ForResource(spacekube.LinkGVR).Informer()
	resources := factory.ForResource(spacekube.ResourceSummaryGVR).Informer()
	counter := &informerReporterCounter{}
	_, _ = links.AddEventHandler(cache.ResourceEventHandlerFuncs{AddFunc: incrementCounter(&counter.links), DeleteFunc: decrementCounter(&counter.links)})
	_, _ = resources.AddEventHandler(cache.ResourceEventHandlerFuncs{AddFunc: incrementCounter(&counter.resources), DeleteFunc: decrementCounter(&counter.resources)})
	factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), links.HasSynced, resources.HasSynced) {
		return fmt.Errorf("reporter quota informer cache synchronization failed")
	}

	trust, err := spaceadmission.NewKubernetesTrustSource(dynamicClient, coreClient, opt.publicKeySecretNamespace, opt.publicKeySecretName)
	if err != nil {
		return err
	}
	validator, err := spaceadmission.NewValidator(trust, spacev1.RealClock{})
	if err != nil {
		return err
	}
	limitedValidator, err := spaceadmission.NewReporterLimitValidator(validator, spaceadmission.ReporterLimits{
		MaxLinkSnapshots: opt.maxLinkSnapshots, MaxResourceSummaries: opt.maxResourceSummaries,
		QPS: opt.reporterQPS, Burst: opt.reporterBurst, MaxTrackedPrincipals: opt.maxTrackedPrincipals,
	}, counter)
	if err != nil {
		return err
	}
	handler, err := spaceadmission.NewHandler(limitedValidator, opt.maxBodyBytes)
	if err != nil {
		return err
	}

	mux := http.NewServeMux()
	mux.Handle("/validate", handler)
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

func kubeConfig(master, kubeconfig string) (*rest.Config, error) {
	if master != "" || kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags(master, kubeconfig)
	}
	return rest.InClusterConfig()
}
