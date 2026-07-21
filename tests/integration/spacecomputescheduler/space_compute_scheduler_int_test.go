package spacecomputescheduler

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/retry"
)

const (
	testNamespace = "space-compute-phase3-e2e"
	testNode      = "space-compute-fixture-node"
	leaseName     = "space-compute-scheduler-e2e"
)

// TestIndependentSchedulerAgainstK3s validates the second-scheduler process
// against a real, externally managed K3s API server. The K3s server is kept
// outside this test so operators can choose a privileged or isolated rootless
// runner without embedding host mutation or credentials in the test binary.
// Exporter telemetry is still served by a Go HTTP test server and consumed by
// the production collector, registry, parser, snapshot store and framework
// callbacks.
func TestIndependentSchedulerAgainstK3s(t *testing.T) {
	kubeconfig := os.Getenv("SPACE_COMPUTE_E2E_KUBECONFIG")
	schedulerBinary := os.Getenv("SPACE_COMPUTE_E2E_SCHEDULER_BINARY")
	if kubeconfig == "" || schedulerBinary == "" {
		t.Skip("set SPACE_COMPUTE_E2E_KUBECONFIG and SPACE_COMPUTE_E2E_SCHEDULER_BINARY to run the real K3s Phase 3 e2e")
	}
	for label, path := range map[string]string{"kubeconfig": kubeconfig, "scheduler binary": schedulerBinary} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("%s %q: %v", label, path, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	client := clientForKubeconfig(t, kubeconfig)
	cleanupObjects(t, context.Background(), client)
	defer cleanupObjects(t, context.Background(), client)

	fixture, err := os.ReadFile(filepath.Join("..", "..", "..", "pkg", "scheduler", "plugins", "gpustability", "testdata", "fixtures", "iluvatar.prom"))
	if err != nil {
		t.Fatal(err)
	}
	var exporterRequests atomic.Int64
	exporter := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/metrics" {
			http.NotFound(w, r)
			return
		}
		exporterRequests.Add(1)
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write(fixture)
	}))
	defer exporter.Close()

	createFixtureNode(t, ctx, client, exporter.URL)
	createPod(t, ctx, client, "ordinary-before-space", corev1.DefaultSchedulerName, nil)
	waitForNodeBinding(t, ctx, client, "ordinary-before-space", testNode)

	accelerator := corev1.ResourceList{"iluvatar.com/gpu": resource.MustParse("1")}
	createPod(t, ctx, client, "space-waits-for-scheduler", "space-compute-scheduler", accelerator)
	assertUnboundFor(t, ctx, client, "space-waits-for-scheduler", 2*time.Second)
	if exporterRequests.Load() != 0 {
		t.Fatalf("exporter received %d requests before the space scheduler started", exporterRequests.Load())
	}

	configPath := writeSchedulerConfig(t, kubeconfig, exporter.URL)
	first := startScheduler(t, schedulerBinary, configPath, kubeconfig, "11359")
	defer first.stop(t)
	firstHolder := waitForLeaseHolder(t, ctx, client, "", 20*time.Second)
	waitForHTTPSProbe(t, ctx, "https://127.0.0.1:11359/livez")
	waitForHTTPSProbe(t, ctx, "https://127.0.0.1:11359/readyz")

	second := startScheduler(t, schedulerBinary, configPath, kubeconfig, "11360")
	defer second.stop(t)
	waitForHTTPSProbe(t, ctx, "https://127.0.0.1:11360/livez")
	waitForNodeBinding(t, ctx, client, "space-waits-for-scheduler", testNode)
	if exporterRequests.Load() == 0 {
		t.Fatal("space scheduler never collected the exporter fixture")
	}
	if holder := currentLeaseHolder(t, ctx, client); holder != firstHolder {
		t.Fatalf("standby changed active holder from %q to %q without a failure", firstHolder, holder)
	}

	first.stop(t)
	secondHolder := waitForLeaseHolder(t, ctx, client, firstHolder, 25*time.Second)
	if secondHolder == firstHolder {
		t.Fatalf("leader holder did not change from %q", firstHolder)
	}
	waitForHTTPSProbe(t, ctx, "https://127.0.0.1:11360/readyz")

	deletePod(t, ctx, client, "space-waits-for-scheduler")
	createPod(t, ctx, client, "space-after-failover", "space-compute-scheduler", accelerator)
	waitForNodeBinding(t, ctx, client, "space-after-failover", testNode)
	createPod(t, ctx, client, "space-no-overcommit", "space-compute-scheduler", accelerator)
	assertUnboundFor(t, ctx, client, "space-no-overcommit", 3*time.Second)

	second.stop(t)
	createPod(t, ctx, client, "ordinary-after-space-down", corev1.DefaultSchedulerName, nil)
	waitForNodeBinding(t, ctx, client, "ordinary-after-space-down", testNode)
}

type schedulerProcess struct {
	cmd  *exec.Cmd
	logs bytes.Buffer
	done chan error
}

func startScheduler(t *testing.T, binary, config, kubeconfig, port string) *schedulerProcess {
	t.Helper()
	process := &schedulerProcess{done: make(chan error, 1)}
	process.cmd = exec.Command(binary,
		"--config="+config,
		"--kubeconfig="+kubeconfig,
		"--authentication-kubeconfig="+kubeconfig,
		"--authorization-kubeconfig="+kubeconfig,
		"--bind-address=127.0.0.1",
		"--secure-port="+port,
		"--leader-elect=true",
	)
	process.cmd.Stdout = &process.logs
	process.cmd.Stderr = &process.logs
	if err := process.cmd.Start(); err != nil {
		t.Fatalf("start scheduler on port %s: %v", port, err)
	}
	go func() { process.done <- process.cmd.Wait() }()
	return process
}

func (p *schedulerProcess) stop(t *testing.T) {
	t.Helper()
	if p == nil || p.cmd == nil || p.cmd.Process == nil {
		return
	}
	select {
	case <-p.done:
		p.cmd = nil
		return
	default:
	}
	_ = p.cmd.Process.Signal(os.Interrupt)
	select {
	case err := <-p.done:
		if err != nil && !strings.Contains(err.Error(), "signal: interrupt") {
			t.Logf("scheduler exit: %v\n%s", err, p.logs.String())
		}
	case <-time.After(10 * time.Second):
		t.Errorf("scheduler did not stop after interrupt; logs:\n%s", p.logs.String())
		_ = p.cmd.Process.Kill()
		<-p.done
	}
	p.cmd = nil
}

func writeSchedulerConfig(t *testing.T, kubeconfig, exporterURL string) string {
	t.Helper()
	parsed, err := url.Parse(exporterURL)
	if err != nil {
		t.Fatal(err)
	}
	_, port, err := net.SplitHostPort(parsed.Host)
	if err != nil {
		t.Fatal(err)
	}
	raw := fmt.Sprintf(`apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
clientConnection:
  kubeconfig: %q
leaderElection:
  leaderElect: true
  resourceName: %s
  resourceNamespace: %s
  leaseDuration: 8s
  renewDeadline: 6s
  retryPeriod: 1s
profiles:
- schedulerName: space-compute-scheduler
  plugins:
    preFilter: {enabled: [{name: K3SGPUStability}]}
    filter: {enabled: [{name: K3SGPUStability}]}
    preScore: {enabled: [{name: K3SGPUStability}]}
    score: {enabled: [{name: K3SGPUStability, weight: 5}]}
  pluginConfig:
  - name: K3SGPUStability
    args:
      apiVersion: gpustability.k3s.io/v1alpha1
      kind: K3SGPUStabilityArgs
      exporter:
        scheme: http
        port: %q
        profile: iluvatar
        timeout: 1s
        allowInsecureHTTP: true
      collector:
        snapshotTTL: 30s
        refreshInterval: 5s
        backoffBase: 100ms
        backoffMax: 2s
        circuitOpenDuration: 2s
        jitterFraction: 0
`, kubeconfig, leaseName, testNamespace, port)
	path := filepath.Join(t.TempDir(), "scheduler.yaml")
	if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func clientForKubeconfig(t *testing.T, path string) kubernetes.Interface {
	t.Helper()
	config, err := clientcmd.BuildConfigFromFlags("", path)
	if err != nil {
		t.Fatal(err)
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func createFixtureNode(t *testing.T, ctx context.Context, client kubernetes.Interface, exporterURL string) {
	t.Helper()
	parsed, err := url.Parse(exporterURL)
	if err != nil {
		t.Fatal(err)
	}
	host, port, err := net.SplitHostPort(parsed.Host)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: testNamespace}}, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatal(err)
	}
	node, err := client.CoreV1().Nodes().Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: testNode,
		Annotations: map[string]string{
			"gpustability.k3s.io/exporter-port":    port,
			"gpustability.k3s.io/exporter-path":    "/metrics",
			"gpustability.k3s.io/exporter-scheme":  "http",
			"gpustability.k3s.io/exporter-profile": "iluvatar",
		},
	}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	status := corev1.NodeStatus{
		Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: host}},
		Capacity: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("8"), corev1.ResourceMemory: resource.MustParse("16Gi"),
			corev1.ResourcePods: resource.MustParse("100"), "iluvatar.com/gpu": resource.MustParse("1"),
		},
		Allocatable: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("8"), corev1.ResourceMemory: resource.MustParse("16Gi"),
			corev1.ResourcePods: resource.MustParse("100"), "iluvatar.com/gpu": resource.MustParse("1"),
		},
		Conditions: []corev1.NodeCondition{{
			Type: corev1.NodeReady, Status: corev1.ConditionTrue, LastHeartbeatTime: metav1.Now(), LastTransitionTime: metav1.Now(), Reason: "FixtureReady",
		}},
	}
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest, err := client.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		latest.Status = status
		_, err = client.CoreV1().Nodes().UpdateStatus(ctx, latest, metav1.UpdateOptions{})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

func createPod(t *testing.T, ctx context.Context, client kubernetes.Interface, name, scheduler string, accelerator corev1.ResourceList) {
	t.Helper()
	requests := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("10m"), corev1.ResourceMemory: resource.MustParse("8Mi")}
	for resourceName, quantity := range accelerator {
		requests[resourceName] = quantity
	}
	_, err := client.CoreV1().Pods(testNamespace).Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
		Spec: corev1.PodSpec{
			SchedulerName: scheduler,
			Containers:    []corev1.Container{{Name: "workload", Image: "registry.invalid/phase3-fixture:never-pulled", Resources: corev1.ResourceRequirements{Requests: requests, Limits: accelerator}}},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
}

func deletePod(t *testing.T, ctx context.Context, client kubernetes.Interface, name string) {
	t.Helper()
	zero := int64(0)
	if err := client.CoreV1().Pods(testNamespace).Delete(ctx, name, metav1.DeleteOptions{GracePeriodSeconds: &zero}); err != nil && !apierrors.IsNotFound(err) {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := client.CoreV1().Pods(testNamespace).Get(ctx, name, metav1.GetOptions{}); apierrors.IsNotFound(err) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("pod %s was not deleted", name)
}

func waitForNodeBinding(t *testing.T, ctx context.Context, client kubernetes.Interface, podName, nodeName string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		pod, err := client.CoreV1().Pods(testNamespace).Get(ctx, podName, metav1.GetOptions{})
		if err == nil && pod.Spec.NodeName == nodeName {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	pod, _ := client.CoreV1().Pods(testNamespace).Get(ctx, podName, metav1.GetOptions{})
	t.Fatalf("pod %s did not bind to %s; pod=%+v", podName, nodeName, pod)
}

func assertUnboundFor(t *testing.T, ctx context.Context, client kubernetes.Interface, podName string, duration time.Duration) {
	t.Helper()
	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		pod, err := client.CoreV1().Pods(testNamespace).Get(ctx, podName, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if pod.Spec.NodeName != "" {
			t.Fatalf("pod %s unexpectedly bound to %s", podName, pod.Spec.NodeName)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func currentLeaseHolder(t *testing.T, ctx context.Context, client kubernetes.Interface) string {
	t.Helper()
	lease, err := client.CoordinationV1().Leases(testNamespace).Get(ctx, leaseName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if lease.Spec.HolderIdentity == nil {
		return ""
	}
	return *lease.Spec.HolderIdentity
}

func waitForLeaseHolder(t *testing.T, ctx context.Context, client kubernetes.Interface, previous string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		lease, err := client.CoordinationV1().Leases(testNamespace).Get(ctx, leaseName, metav1.GetOptions{})
		if err == nil && lease.Spec.HolderIdentity != nil && *lease.Spec.HolderIdentity != "" && *lease.Spec.HolderIdentity != previous {
			return *lease.Spec.HolderIdentity
		}
		if err != nil && !apierrors.IsNotFound(err) {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	lease, _ := client.CoordinationV1().Leases(testNamespace).Get(ctx, leaseName, metav1.GetOptions{})
	t.Fatalf("lease holder did not change from %q; lease=%+v", previous, lease)
	return ""
}

func waitForHTTPSProbe(t *testing.T, ctx context.Context, endpoint string) {
	t.Helper()
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true} // #nosec G402 -- ephemeral self-signed scheduler serving cert
	client := &http.Client{Transport: transport, Timeout: time.Second}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		request, _ := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		response, err := client.Do(request)
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("probe %s did not become healthy", endpoint)
}

func cleanupObjects(t *testing.T, ctx context.Context, client kubernetes.Interface) {
	t.Helper()
	// There is no kubelet in the agentless qualification cluster to complete a
	// normal Pod grace period. Remove only this test namespace's fixture Pods
	// with zero grace before namespace deletion.
	zero := int64(0)
	_ = client.CoreV1().Pods(testNamespace).DeleteCollection(ctx, metav1.DeleteOptions{GracePeriodSeconds: &zero}, metav1.ListOptions{})
	propagation := metav1.DeletePropagationForeground
	_ = client.CoreV1().Namespaces().Delete(ctx, testNamespace, metav1.DeleteOptions{PropagationPolicy: &propagation})
	_ = client.CoreV1().Nodes().Delete(ctx, testNode, metav1.DeleteOptions{})
	// Namespaced CRDs add discovery/finalization work to the Phase 4 e2e. Wait
	// for actual deletion so a following test never reuses a Terminating
	// namespace and mistakes an API lifecycle race for a scheduler failure.
	waitUntil := time.Now().Add(30 * time.Second)
	for time.Now().Before(waitUntil) {
		_, namespaceErr := client.CoreV1().Namespaces().Get(ctx, testNamespace, metav1.GetOptions{})
		_, nodeErr := client.CoreV1().Nodes().Get(ctx, testNode, metav1.GetOptions{})
		if apierrors.IsNotFound(namespaceErr) && apierrors.IsNotFound(nodeErr) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("fixture namespace or Node did not finish deletion")
}
