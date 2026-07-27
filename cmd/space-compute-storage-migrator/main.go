package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	apiextclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	clientcmd "k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"

	spacemigration "github.com/k3s-io/k3s/contrib/space-compute/pkg/migration"
)

const componentName = "space-compute-storage-migrator"

type options struct{ kubeconfig, master, targetVersion string }

func main() {
	klog.InitFlags(nil)
	opt := options{}
	flag.StringVar(&opt.kubeconfig, "kubeconfig", "", "Path to kubeconfig; empty uses in-cluster configuration")
	flag.StringVar(&opt.master, "master", "", "Optional API server address")
	flag.StringVar(&opt.targetVersion, "target-version", "v1beta1", "Storage version to migrate to: v1alpha1 or v1beta1")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, opt); err != nil {
		klog.Fatalf("%s failed: %v", componentName, err)
	}
}

func run(ctx context.Context, opt options) error {
	config, err := kubeConfig(opt.master, opt.kubeconfig)
	if err != nil {
		return err
	}
	config.UserAgent = componentName
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("create dynamic client: %w", err)
	}
	extensions, err := apiextclient.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("create apiextensions client: %w", err)
	}
	return (&spacemigration.Migrator{Dynamic: dynamicClient, APIExtensions: extensions}).Migrate(ctx, opt.targetVersion)
}

func kubeConfig(master, kubeconfig string) (*rest.Config, error) {
	if master != "" || kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags(master, kubeconfig)
	}
	return rest.InClusterConfig()
}
