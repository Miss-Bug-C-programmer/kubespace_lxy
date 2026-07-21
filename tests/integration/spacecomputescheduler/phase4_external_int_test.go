package spacecomputescheduler

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
	spacekube "github.com/k3s-io/k3s/contrib/space-compute/pkg/kube"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/retry"
)

const (
	phase4MissionName = "phase4-live-flow"
	phase4LinkName    = "phase4-ground-leo"
	phase4SummaryName = "phase4-leo"
)

// TestPhase4PlannerToIndependentSchedulerAgainstK3s is the CPU-only external
// qualification path: production CRDs and controllers consume recorded
// exporter/link objects, create a durable plan and attempt Pod, and the real
// independent scheduler binds it. The default scheduler is also exercised.
func TestPhase4PlannerToIndependentSchedulerAgainstK3s(t *testing.T) {
	kubeconfig := os.Getenv("SPACE_COMPUTE_E2E_KUBECONFIG")
	schedulerBinary := os.Getenv("SPACE_COMPUTE_E2E_SCHEDULER_BINARY")
	plannerBinary := os.Getenv("SPACE_COMPUTE_PHASE4_E2E_PLANNER_BINARY")
	if kubeconfig == "" || schedulerBinary == "" || plannerBinary == "" {
		t.Skip("set Phase 4 K3s kubeconfig, scheduler binary and planner binary to run the real Phase 4 e2e")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	client := clientForKubeconfig(t, kubeconfig)
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	cleanupPhase4Objects(t, context.Background(), client, dynamicClient)
	defer cleanupPhase4Objects(t, context.Background(), client, dynamicClient)

	fixture, err := os.ReadFile(filepath.Join("..", "..", "..", "pkg", "scheduler", "plugins", "gpustability", "testdata", "fixtures", "iluvatar.prom"))
	if err != nil {
		t.Fatal(err)
	}
	var exporterRequests atomic.Int64
	exporter := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/metrics" {
			http.NotFound(w, request)
			return
		}
		exporterRequests.Add(1)
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write(fixture)
	}))
	defer exporter.Close()

	createFixtureNode(t, ctx, client, exporter.URL)
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		node, getErr := client.CoreV1().Nodes().Get(ctx, testNode, metav1.GetOptions{})
		if getErr != nil {
			return getErr
		}
		if node.Labels == nil {
			node.Labels = map[string]string{}
		}
		node.Labels[spacev1.LabelDomain] = phase4SummaryName
		node.Labels[spacev1.LabelOrbitClass] = string(spacev1.OrbitLEO)
		_, updateErr := client.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
		return updateErr
	})
	if err != nil {
		t.Fatal(err)
	}

	planner := startPlanner(t, plannerBinary, kubeconfig)
	defer planner.stop(t)
	now := time.Now().UTC().Truncate(time.Second)
	createPhase4Resource(t, ctx, dynamicClient, spacekube.LinkGVR, "", phase4Link(now))
	createPhase4Resource(t, ctx, dynamicClient, spacekube.ResourceSummaryGVR, "", phase4Summary(now))
	waitForNodeProjection(t, ctx, client)

	configPath := writeSchedulerConfig(t, kubeconfig, exporter.URL)
	schedulerProcess := startScheduler(t, schedulerBinary, configPath, kubeconfig, "12359")
	defer schedulerProcess.stop(t)
	waitForHTTPSProbe(t, ctx, "https://127.0.0.1:12359/readyz")
	createPhase4Resource(t, ctx, dynamicClient, spacekube.MissionGVR, testNamespace, phase4Mission(now))
	waitForPhase4Binding(t, ctx, client, dynamicClient)
	if exporterRequests.Load() == 0 {
		t.Fatal("independent scheduler did not collect the recorded exporter fixture")
	}

	createPod(t, ctx, client, "ordinary-phase4-control", corev1.DefaultSchedulerName, nil)
	waitForNodeBinding(t, ctx, client, "ordinary-phase4-control", testNode)
	completePhase4Pod(t, ctx, client)
	waitForPhase4Completion(t, ctx, dynamicClient)
}

type plannerProcess struct {
	cmd  *exec.Cmd
	logs bytes.Buffer
	done chan error
}

func startPlanner(t *testing.T, binary, kubeconfig string) *plannerProcess {
	t.Helper()
	process := &plannerProcess{done: make(chan error, 1)}
	process.cmd = exec.Command(binary, "--kubeconfig="+kubeconfig, "--leader-elect=false", "--workers=2", "--metrics-bind-address=127.0.0.1:13361")
	process.cmd.Stdout = &process.logs
	process.cmd.Stderr = &process.logs
	if err := process.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { process.done <- process.cmd.Wait() }()
	return process
}

func (p *plannerProcess) stop(t *testing.T) {
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
			t.Logf("planner exit: %v\n%s", err, p.logs.String())
		}
	case <-time.After(10 * time.Second):
		t.Errorf("planner did not stop after interrupt; logs:\n%s", p.logs.String())
		_ = p.cmd.Process.Kill()
		<-p.done
	}
	p.cmd = nil
}

func phase4Link(now time.Time) *spacev1.SpaceLinkSnapshot {
	return &spacev1.SpaceLinkSnapshot{TypeMeta: metav1.TypeMeta{APIVersion: spacev1.SchemeGroupVersion.String(), Kind: "SpaceLinkSnapshot"}, ObjectMeta: metav1.ObjectMeta{Name: phase4LinkName}, Spec: spacev1.SpaceLinkSnapshotSpec{
		Source: spacev1.DomainReference{Name: "ground-e2e", ClusterID: "ground-e2e", OrbitClass: spacev1.OrbitGround}, Destination: spacev1.DomainReference{Name: phase4SummaryName, ClusterID: "phase4-leo-cluster", OrbitClass: spacev1.OrbitLEO},
		ObservedAt: metav1.NewTime(now.Add(-time.Second)), ValidUntil: metav1.NewTime(now.Add(20 * time.Minute)), MaximumClockSkewSeconds: 0, MinimumUpdateSeconds: 1, HistoryLimit: 8,
		Provenance: spacev1.Provenance{ReporterID: "system:admin", Source: "recorded-e2e-contact", Digest: strings.Repeat("a", 64), Sequence: 1},
		Windows:    []spacev1.ContactWindow{{ID: "contact-one", Start: metav1.NewTime(now.Add(time.Second)), End: metav1.NewTime(now.Add(2 * time.Minute)), BandwidthBitsPerSec: 100_000_000, RTTMicroseconds: 20_000, LossPartsPerMillion: 100, ErrorPartsPerMillion: 10, StabilityMilli: 900, ConfidenceMilli: 900, Predicted: true}},
	}}
}

func phase4Summary(now time.Time) *spacev1.SpaceDomainResourceSummary {
	return &spacev1.SpaceDomainResourceSummary{TypeMeta: metav1.TypeMeta{APIVersion: spacev1.SchemeGroupVersion.String(), Kind: "SpaceDomainResourceSummary"}, ObjectMeta: metav1.ObjectMeta{Name: phase4SummaryName}, Spec: spacev1.SpaceDomainResourceSummarySpec{
		Domain: spacev1.DomainReference{Name: phase4SummaryName, ClusterID: "phase4-leo-cluster", OrbitClass: spacev1.OrbitLEO}, ObservedAt: metav1.NewTime(now.Add(-time.Second)), ValidUntil: metav1.NewTime(now.Add(20 * time.Minute)),
		Provenance: spacev1.Provenance{ReporterID: "system:admin", Source: "recorded-exporter-summary", Digest: strings.Repeat("b", 64), Sequence: 1},
		Devices:    []spacev1.DeviceCapacity{{Class: "gpu", Count: 1, Architectures: []string{"space-cuda"}, ComputeMilli: 1000, FragmentationMilli: 800}}, QueueDelaySeconds: 0, EnergyHeadroomMilli: 800, ThermalHeadroomMilli: 800, ResilienceMilli: 850, MinimumEnergyMilli: 300, MinimumThermalMilli: 300, MaximumSnapshotAgeSecs: 120, ExporterSnapshotDigest: strings.Repeat("b", 64),
	}}
}

func phase4Mission(now time.Time) *spacev1.SpaceMission {
	accelerator := corev1.ResourceList{"iluvatar.com/gpu": resource.MustParse("1")}
	return &spacev1.SpaceMission{TypeMeta: metav1.TypeMeta{APIVersion: spacev1.SchemeGroupVersion.String(), Kind: "SpaceMission"}, ObjectMeta: metav1.ObjectMeta{Name: phase4MissionName, Namespace: testNamespace}, Spec: spacev1.SpaceMissionSpec{
		MissionClass: "fixture", Priority: 900, StatePolicy: spacev1.PolicyStrict, RequiredCapabilities: []spacev1.CapabilityRequirement{{Class: "gpu", Quantity: 1, Architecture: "space-cuda"}}, Inputs: []spacev1.DataObject{{ID: "frame", SizeBytes: 1_000_000, Locations: []string{"ground-e2e"}}},
		OutputSizeBytes: 0, Deadline: metav1.NewTime(now.Add(10 * time.Minute)), ExpectedDurationSeconds: 10, MaximumDurationSeconds: 20, DurationUncertaintySecs: 10, SafetyMarginSeconds: 10, MaximumClockSkewSeconds: 0, ResultReturnRequired: false,
		Retry: spacev1.RetryPolicy{MaxAttempts: 3, AllowMigration: true, MaxConcurrentExecutions: 1}, Checkpoint: spacev1.CheckpointPolicy{Checkpointable: true, MinimumIntervalSecs: 5, MaximumStateBytes: 1_000_000},
		WorkloadTemplate: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "processor", Image: "registry.invalid/phase4-fixture:never-pulled", Resources: corev1.ResourceRequirements{Requests: accelerator, Limits: accelerator}}}}},
	}}
}

func createPhase4Resource(t *testing.T, ctx context.Context, client dynamic.Interface, gvr schema.GroupVersionResource, namespace string, value interface{}) {
	t.Helper()
	object, err := runtime.DefaultUnstructuredConverter.ToUnstructured(value)
	if err != nil {
		t.Fatal(err)
	}
	resource := client.Resource(gvr)
	var resourceClient dynamic.ResourceInterface = resource
	if namespace != "" {
		resourceClient = resource.Namespace(namespace)
	}
	if _, err := resourceClient.Create(ctx, &unstructured.Unstructured{Object: object}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func waitForNodeProjection(t *testing.T, ctx context.Context, client kubernetes.Interface) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		node, err := client.CoreV1().Nodes().Get(ctx, testNode, metav1.GetOptions{})
		if err == nil && node.Annotations[spacev1.AnnotationLinkProjection] != "" {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("resource controller did not project link/resource state to the Node")
}

func waitForPhase4Binding(t *testing.T, ctx context.Context, client kubernetes.Interface, dynamicClient dynamic.Interface) {
	t.Helper()
	deadline := time.Now().Add(40 * time.Second)
	for time.Now().Before(deadline) {
		placements, _ := dynamicClient.Resource(spacekube.PlacementGVR).Namespace(testNamespace).List(ctx, metav1.ListOptions{})
		pod, err := client.CoreV1().Pods(testNamespace).Get(ctx, phase4MissionName+"-attempt-1", metav1.GetOptions{})
		if placements != nil && len(placements.Items) == 1 && err == nil && pod.Spec.NodeName == testNode {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("production planner -> independent scheduler flow did not bind")
}

func completePhase4Pod(t *testing.T, ctx context.Context, client kubernetes.Interface) {
	t.Helper()
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		pod, getErr := client.CoreV1().Pods(testNamespace).Get(ctx, phase4MissionName+"-attempt-1", metav1.GetOptions{})
		if getErr != nil {
			return getErr
		}
		pod.Status.Phase = corev1.PodSucceeded
		_, updateErr := client.CoreV1().Pods(testNamespace).UpdateStatus(ctx, pod, metav1.UpdateOptions{})
		return updateErr
	})
	if err != nil {
		t.Fatal(err)
	}
}

func waitForPhase4Completion(t *testing.T, ctx context.Context, client dynamic.Interface) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		placement, err := client.Resource(spacekube.PlacementGVR).Namespace(testNamespace).Get(ctx, phase4MissionName+"-placement", metav1.GetOptions{})
		if err == nil {
			phase, _, _ := unstructured.NestedString(placement.Object, "status", "phase")
			if phase == string(spacev1.PlacementCompleted) {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("workload controller did not persist terminal completion")
}

func cleanupPhase4Objects(t *testing.T, ctx context.Context, client kubernetes.Interface, dynamicClient dynamic.Interface) {
	t.Helper()
	cleanupObjects(t, ctx, client)
	_ = dynamicClient.Resource(spacekube.LinkGVR).Delete(ctx, phase4LinkName, metav1.DeleteOptions{})
	_ = dynamicClient.Resource(spacekube.ResourceSummaryGVR).Delete(ctx, phase4SummaryName, metav1.DeleteOptions{})
	if _, err := dynamicClient.Resource(spacekube.LinkGVR).Get(ctx, phase4LinkName, metav1.GetOptions{}); err != nil && !apierrors.IsNotFound(err) {
		t.Logf("link cleanup check: %v", err)
	}
}
