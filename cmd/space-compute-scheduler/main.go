// space-compute-scheduler is the separately deployable Kubernetes scheduler
// profile for opt-in heterogeneous accelerator workloads.
package main

import (
	"os"

	"github.com/spf13/cobra"
	"k8s.io/apiserver/pkg/server"
	"k8s.io/component-base/cli"
	_ "k8s.io/component-base/logs/json/register"
	_ "k8s.io/component-base/metrics/prometheus/clientgo"
	_ "k8s.io/component-base/metrics/prometheus/version"
	"k8s.io/kubernetes/cmd/kube-scheduler/app"

	"github.com/k3s-io/k3s/pkg/scheduler/plugins/gpustability"
)

const componentName = "space-compute-scheduler"

func newSchedulerCommand(stopCh <-chan struct{}) *cobra.Command {
	command := app.NewSchedulerCommand(stopCh, app.WithPlugin(gpustability.Name, gpustability.New))
	command.Use = componentName
	command.Short = "Schedule opt-in space-compute workloads from validated exporter snapshots"
	command.Long = `space-compute-scheduler is a Kubernetes kube-scheduler component with the
K3SGPUStability framework plugin registered. Its configuration must expose only
the space-compute-scheduler profile. Pods opt in through spec.schedulerName;
ordinary Pods remain the responsibility of the independently running default
scheduler.`
	return command
}

func main() {
	os.Exit(cli.Run(newSchedulerCommand(server.SetupSignalHandler())))
}
