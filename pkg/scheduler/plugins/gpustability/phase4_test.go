package gpustability

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
	spaceplanner "github.com/k3s-io/k3s/contrib/space-compute/pkg/planner"
	spacepolicy "github.com/k3s-io/k3s/contrib/space-compute/pkg/policy"
	spaceworkload "github.com/k3s-io/k3s/contrib/space-compute/pkg/workload"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

type phase4Clock struct{ now time.Time }

func (f phase4Clock) Now() time.Time { return f.now }

func TestPhase4FixtureFlowPlannerProjectionSchedulerAndStatus(t *testing.T) {
	now := time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)
	mission := phase4Mission(now)
	summary := phase4Summary(now)
	link := phase4ReturnLink(now)
	decision, err := spaceplanner.Plan(mission, []*spacev1.SpaceDomainResourceSummary{summary}, []*spacev1.SpaceLinkSnapshot{link}, phase4Clock{now})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Placement.Spec.Target.Name != "leo-a" {
		t.Fatalf("planned target = %s", decision.Placement.Spec.Target.Name)
	}

	node := readPhase4Node(t)
	projected, err := spacepolicy.ProjectNode(node, summary, []*spacev1.SpaceLinkSnapshot{link}, phase4Clock{now})
	if err != nil {
		t.Fatal(err)
	}
	template := v1.PodTemplateSpec{Spec: v1.PodSpec{Containers: []v1.Container{{Name: "processor", Resources: v1.ResourceRequirements{Requests: v1.ResourceList{"iluvatar.com/gpu": resource.MustParse("1")}, Limits: v1.ResourceList{"iluvatar.com/gpu": resource.MustParse("1")}}}}}}
	pod, err := spaceworkload.BuildAttemptPod(mission, decision.Placement, template)
	if err != nil {
		t.Fatal(err)
	}
	if pod.Spec.SchedulerName != "space-compute-scheduler" {
		t.Fatalf("schedulerName = %s", pod.Spec.SchedulerName)
	}

	var exporterCalls atomic.Int64
	cfg := testConfig(t)
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		exporterCalls.Add(1)
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader(readPhase2Fixture(t, "iluvatar.prom")))}, nil
	})}
	collector, err := newCollector(context.Background(), cfg, client)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(collector.Close)
	plugin := &Plugin{config: cfg, collector: collector, clock: phase4Clock{decision.Placement.Spec.ComputeStart.Time}, blocked: newBlockedPodIndex(100, time.Minute)}
	nodeInfo := framework.NewNodeInfo()
	nodeInfo.SetNode(projected)
	warmNode(t, plugin, nodeInfo)
	before := exporterCalls.Load()
	state := cycleStateForPod(t, plugin, pod)
	if status := plugin.Filter(context.Background(), state, pod, nodeInfo); !status.IsSuccess() {
		t.Fatalf("Filter: %v", status)
	}
	if status := plugin.PreScore(context.Background(), state, pod, []*framework.NodeInfo{nodeInfo}); !status.IsSuccess() {
		t.Fatalf("PreScore: %v", status)
	}
	score, status := plugin.Score(context.Background(), state, pod, nodeInfo)
	if !status.IsSuccess() || score <= 0 {
		t.Fatalf("Score=%d status=%v", score, status)
	}
	if exporterCalls.Load() != before {
		t.Fatalf("scheduler callbacks performed remote I/O: before=%d after=%d", before, exporterCalls.Load())
	}
	preScore, _ := scoreState(state)
	dimensions := preScore.Nodes[projected.Name].Evaluation.Dimensions
	if dimensions.PredictedCompletion <= 0 || dimensions.LinkRisk <= 0 || dimensions.Resilience <= 0 {
		t.Fatalf("Phase 4 dimensions = %+v", dimensions)
	}

	observation := spacev1.ExecutionObservation{Sequence: 1, Attempt: decision.Placement.Spec.Attempt, Phase: "dispatched", PodUID: "pod-uid", ObservedAt: metav1.NewTime(decision.Placement.Spec.ComputeStart.Time)}
	if changed, err := spaceplanner.ApplyExecutionObservation(decision.Placement, mission, observation, phase4Clock{decision.Placement.Spec.ComputeStart.Time}); err != nil || !changed || decision.Placement.Status.Phase != spacev1.PlacementDispatched {
		t.Fatalf("status changed=%v err=%v phase=%s", changed, err, decision.Placement.Status.Phase)
	}

	wrong := projected.DeepCopy()
	wrong.Labels[spacev1.LabelDomain] = "geo-a"
	wrongInfo := framework.NewNodeInfo()
	wrongInfo.SetNode(wrong)
	if status := plugin.Filter(context.Background(), state, pod, wrongInfo); status.IsSuccess() || !strings.Contains(status.Message(), "target_domain_mismatch") {
		t.Fatalf("wrong-domain status = %v", status)
	}

	ordinary := &v1.Pod{Spec: v1.PodSpec{Containers: []v1.Container{{Name: "ordinary"}}}}
	ordinaryState := cycleStateForPod(t, plugin, ordinary)
	if status := plugin.Filter(context.Background(), ordinaryState, ordinary, nodeInfo); !status.IsSuccess() {
		t.Fatalf("ordinary Pod affected: %v", status)
	}
}

func TestPhase4IntentAndProjectionMutationCannotChangeCycleDecision(t *testing.T) {
	now := time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)
	mission := phase4Mission(now)
	summary := phase4Summary(now)
	link := phase4ReturnLink(now)
	decision, err := spaceplanner.Plan(mission, []*spacev1.SpaceDomainResourceSummary{summary}, []*spacev1.SpaceLinkSnapshot{link}, phase4Clock{now})
	if err != nil {
		t.Fatal(err)
	}
	pod, err := spaceworkload.BuildAttemptPod(mission, decision.Placement, v1.PodTemplateSpec{Spec: v1.PodSpec{Containers: []v1.Container{{Name: "processor", Resources: v1.ResourceRequirements{Requests: v1.ResourceList{"iluvatar.com/gpu": resource.MustParse("1")}}}}}})
	if err != nil {
		t.Fatal(err)
	}
	plugin := newTestPlugin(t, iluvatarMetrics)
	plugin.clock = phase4Clock{decision.Placement.Spec.ComputeStart.Time}
	state := cycleStateForPod(t, plugin, pod)
	requirement, status := workloadFromState(state)
	if !status.IsSuccess() {
		t.Fatal(status)
	}
	pod.Annotations[spacev1.AnnotationMissionIntent] = `{"invalid":true}`
	pod.Annotations[spacev1.AnnotationPlacement] = `{"invalid":true}`
	if requirement.Space == nil || requirement.Space.Placement.Spec.PlanID != decision.Placement.Spec.PlanID {
		t.Fatal("immutable Phase 4 cycle state changed after Pod mutation")
	}
}

func readPhase4Node(t *testing.T) *v1.Node {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "contrib", "space-compute", "testdata", "golden", "leo-node.json"))
	if err != nil {
		t.Fatal(err)
	}
	node := &v1.Node{}
	if err := json.Unmarshal(raw, node); err != nil {
		t.Fatal(err)
	}
	return node
}

func phase4Mission(now time.Time) *spacev1.SpaceMission {
	quantity := resource.MustParse("1")
	return &spacev1.SpaceMission{ObjectMeta: metav1.ObjectMeta{Name: "local-flow", Namespace: "missions", UID: types.UID("mission-uid"), Generation: 1}, Spec: spacev1.SpaceMissionSpec{MissionClass: "science", Priority: 500, StatePolicy: spacev1.PolicyStrict, RequiredCapabilities: []spacev1.CapabilityRequirement{{Class: "gpu", Quantity: 1, Architecture: "space-cuda"}}, Inputs: []spacev1.DataObject{{ID: "local-frame", SizeBytes: 1000, Locations: []string{"leo-a"}}}, OutputSizeBytes: 1000, ResultDestinations: []string{"ground-a"}, ResultReturnRequired: true, Deadline: metav1.NewTime(now.Add(time.Hour)), ExpectedDurationSeconds: 60, MaximumDurationSeconds: 100, DurationUncertaintySecs: 20, SafetyMarginSeconds: 10, MaximumClockSkewSeconds: 2, Retry: spacev1.RetryPolicy{MaxAttempts: 2, AllowMigration: true, MaxConcurrentExecutions: 1}, Checkpoint: spacev1.CheckpointPolicy{Checkpointable: true}, WorkloadTemplate: v1.PodTemplateSpec{Spec: v1.PodSpec{Containers: []v1.Container{{Name: "processor", Image: "example.invalid/processor:v1", Resources: v1.ResourceRequirements{Requests: v1.ResourceList{"iluvatar.com/gpu": quantity}, Limits: v1.ResourceList{"iluvatar.com/gpu": quantity}}}}}}}}
}

func phase4Summary(now time.Time) *spacev1.SpaceDomainResourceSummary {
	return &spacev1.SpaceDomainResourceSummary{ObjectMeta: metav1.ObjectMeta{Name: "leo-a"}, Spec: spacev1.SpaceDomainResourceSummarySpec{Domain: spacev1.DomainReference{Name: "leo-a", ClusterID: "leo-a-cluster", OrbitClass: spacev1.OrbitLEO}, ObservedAt: metav1.NewTime(now), ValidUntil: metav1.NewTime(now.Add(time.Hour)), Provenance: phase4Provenance(1), Devices: []spacev1.DeviceCapacity{{Class: "gpu", Count: 1, Architectures: []string{"space-cuda"}, ComputeMilli: 1000, FragmentationMilli: 1000}}, DataLocations: []string{"local-frame"}, EnergyHeadroomMilli: 800, ThermalHeadroomMilli: 800, ResilienceMilli: 900, MinimumEnergyMilli: 300, MinimumThermalMilli: 300, MaximumSnapshotAgeSecs: 60, ExporterSnapshotDigest: strings.Repeat("b", 64)}}
}

func phase4ReturnLink(now time.Time) *spacev1.SpaceLinkSnapshot {
	return &spacev1.SpaceLinkSnapshot{ObjectMeta: metav1.ObjectMeta{Name: "leo-ground"}, Spec: spacev1.SpaceLinkSnapshotSpec{Source: spacev1.DomainReference{Name: "leo-a", ClusterID: "leo-a-cluster", OrbitClass: spacev1.OrbitLEO}, Destination: spacev1.DomainReference{Name: "ground-a", ClusterID: "ground-a-cluster", OrbitClass: spacev1.OrbitGround}, ObservedAt: metav1.NewTime(now), ValidUntil: metav1.NewTime(now.Add(time.Hour)), MaximumClockSkewSeconds: 2, MinimumUpdateSeconds: 10, HistoryLimit: 8, Provenance: phase4Provenance(7), Windows: []spacev1.ContactWindow{{ID: "return-window", Start: metav1.NewTime(now.Add(3 * time.Minute)), End: metav1.NewTime(now.Add(20 * time.Minute)), BandwidthBitsPerSec: 100_000_000, RTTMicroseconds: 1000, StabilityMilli: 900, ConfidenceMilli: 900, Predicted: true}}}}
}
func phase4Provenance(sequence int64) spacev1.Provenance {
	return spacev1.Provenance{ReporterID: "authenticated-reporter", Source: "recorded-contact-product", Digest: strings.Repeat("a", 64), Sequence: sequence}
}
