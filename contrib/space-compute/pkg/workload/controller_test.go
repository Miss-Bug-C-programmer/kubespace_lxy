package workload

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
	"github.com/k3s-io/k3s/contrib/space-compute/pkg/planner"
	spacepolicy "github.com/k3s-io/k3s/contrib/space-compute/pkg/policy"
)

type mutableClock struct{ now time.Time }

func (c *mutableClock) Now() time.Time { return c.now }

func TestDispatchRequiresExecutionLeaseAndComputeStart(t *testing.T) {
	now := time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)
	clock := &mutableClock{now: now}
	mission, placement := dispatchFixture(now)
	store := &memoryStore{}
	evidence := newMemoryEvidence()
	controller := &Controller{Store: store, Evidence: evidence, Clock: clock}
	delay, err := controller.ReconcileDispatch(context.Background(), mission, placement, mission.Spec.WorkloadTemplate)
	if err != nil || delay != time.Second || store.creates != 0 || placement.Status.Phase != spacev1.PlacementExecutionLeasePending {
		t.Fatalf("without lease delay=%v creates=%d phase=%s err=%v", delay, store.creates, placement.Status.Phase, err)
	}
	lease := validLease(mission, placement, 1, 1, now)
	evidence.leases = []*spacev1.SpaceExecutionLease{lease}
	delay, err = controller.ReconcileDispatch(context.Background(), mission, placement, mission.Spec.WorkloadTemplate)
	if err != nil || delay != time.Minute || store.creates != 0 {
		t.Fatalf("pre-compute delay=%v creates=%d err=%v", delay, store.creates, err)
	}
	clock.now = now.Add(time.Minute)
	delay, err = controller.ReconcileDispatch(context.Background(), mission, placement, mission.Spec.WorkloadTemplate)
	if err != nil || delay != 0 || store.creates != 1 {
		t.Fatalf("leased dispatch delay=%v creates=%d err=%v", delay, store.creates, err)
	}
	if store.pod.Spec.SchedulerName != "space-compute-scheduler" || store.pod.Annotations[spacev1.GroupName+"/token-hash"] != lease.Spec.Fence.TokenHash {
		t.Fatalf("created Pod fence=%v", store.pod.Annotations)
	}
	foundSecret := false
	for _, env := range store.pod.Spec.Containers[0].Env {
		if env.Name == "SPACE_COMPUTE_FENCE_TOKEN" && env.ValueFrom != nil && env.ValueFrom.SecretKeyRef != nil && env.ValueFrom.SecretKeyRef.Name == spacev1.ExecutionTokenSecretName(lease.Spec.Fence) {
			foundSecret = true
		}
	}
	if !foundSecret {
		t.Fatal("fence token was not injected from deterministic Secret reference")
	}
	if _, err := spacepolicy.ParsePod(store.pod, clock); err != nil {
		t.Fatalf("production annotations: %v", err)
	}
	if _, err = controller.ReconcileDispatch(context.Background(), mission, placement, mission.Spec.WorkloadTemplate); err != nil || store.creates != 1 {
		t.Fatalf("duplicate dispatch creates=%d err=%v", store.creates, err)
	}
}

func TestTransferReceiptThenLeaseAreBothRequired(t *testing.T) {
	now := time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)
	mission, placement := dispatchFixture(now)
	digest := strings.Repeat("a", 64)
	mission.Spec.Inputs = []spacev1.DataObject{{ID: "sensor", SizeBytes: 1024, Locations: []spacev1.DataLocation{{Domain: spacev1.DomainReference{Name: "ground-a", ClusterID: "ground-cluster", OrbitClass: spacev1.OrbitGround}}}, PayloadDigest: digest}}
	source := spacev1.DomainReference{Name: "ground-a", ClusterID: "ground-cluster", OrbitClass: spacev1.OrbitGround}
	placement.Spec.InputTransfers = []spacev1.TransferEpoch{{LinkSnapshotName: "ground-leo", WindowID: "w1", DataID: "sensor", Source: source, Destination: placement.Spec.Target, Start: metav1.NewTime(now.Add(-time.Minute)), End: metav1.NewTime(now), Bytes: 1024}}
	placement.Spec.NotBefore = metav1.NewTime(now)
	placement.Spec.ComputeStart = metav1.NewTime(now)
	store := &memoryStore{}
	evidence := newMemoryEvidence()
	coordinator := placement.Spec.Target
	controller := &Controller{Store: store, Evidence: evidence, Clock: &mutableClock{now: now}, LocalDomain: &coordinator}
	if _, err := controller.ReconcileDispatch(context.Background(), mission, placement, mission.Spec.WorkloadTemplate); err != nil || store.creates != 0 || placement.Status.Phase != spacev1.PlacementTransferPending {
		t.Fatalf("missing receipt phase=%s creates=%d err=%v", placement.Status.Phase, store.creates, err)
	}
	if len(evidence.intents) != 0 {
		t.Fatalf("dispatcher created %d transfer intents; transport-agent must own intent writes", len(evidence.intents))
	}
	intents, err := BuildInputTransferIntents(mission, placement, coordinator)
	if err != nil || len(intents) != 1 {
		t.Fatalf("transport intent build count=%d err=%v", len(intents), err)
	}
	intent := intents[0]
	evidence.intents = append(evidence.intents, intent.DeepCopy())
	evidence.receipts = []*spacev1.SpaceTransferReceipt{{Spec: spacev1.SpaceTransferReceiptSpec{TransferID: intent.Spec.TransferID, MissionUID: intent.Spec.MissionUID, PlanID: intent.Spec.PlanID, Attempt: intent.Spec.Attempt, Source: intent.Spec.Source, Destination: intent.Spec.Destination, DataID: intent.Spec.DataID, Bytes: intent.Spec.Bytes, PayloadDigest: intent.Spec.PayloadDigest, StartedAt: metav1.NewTime(now.Add(-time.Minute)), CompletedAt: metav1.NewTime(now), Provenance: testProvenance(1)}}}
	if _, err := controller.ReconcileDispatch(context.Background(), mission, placement, mission.Spec.WorkloadTemplate); err != nil || store.creates != 0 || placement.Status.Phase != spacev1.PlacementExecutionLeasePending {
		t.Fatalf("receipt without lease phase=%s creates=%d err=%v", placement.Status.Phase, store.creates, err)
	}
	evidence.leases = []*spacev1.SpaceExecutionLease{validLease(mission, placement, 1, 1, now)}
	if _, err := controller.ReconcileDispatch(context.Background(), mission, placement, mission.Spec.WorkloadTemplate); err != nil || store.creates != 1 {
		t.Fatalf("receipt+lease dispatch creates=%d err=%v", store.creates, err)
	}
}

func TestCrossDomainTransferWithoutDeclaredDigestFailsClosed(t *testing.T) {
	now := time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)
	mission, placement := dispatchFixture(now)
	mission.Spec.Inputs = []spacev1.DataObject{{ID: "sensor", SizeBytes: 1, Locations: []spacev1.DataLocation{{Domain: spacev1.DomainReference{Name: "ground-a", ClusterID: "g", OrbitClass: spacev1.OrbitGround}}}}}
	source := spacev1.DomainReference{Name: "ground-a", ClusterID: "g", OrbitClass: spacev1.OrbitGround}
	placement.Spec.InputTransfers = []spacev1.TransferEpoch{{DataID: "sensor", Source: source, Destination: placement.Spec.Target, Start: metav1.NewTime(now), End: metav1.NewTime(now.Add(time.Minute)), Bytes: 1}}
	coordinator := spacev1.DomainReference{Name: "ground-control", ClusterID: "ground-control", OrbitClass: spacev1.OrbitGround}
	_, err := (&Controller{Store: &memoryStore{}, Evidence: newMemoryEvidence(), Clock: &mutableClock{now: now}, LocalDomain: &coordinator}).ReconcileDispatch(context.Background(), mission, placement, mission.Spec.WorkloadTemplate)
	if err == nil {
		t.Fatal("missing payload digest was accepted")
	}
}

func TestMissingTransferCoordinatorStaysPendingWithoutPod(t *testing.T) {
	now := time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)
	mission, placement := dispatchFixture(now)
	digest := strings.Repeat("a", 64)
	source := spacev1.DomainReference{Name: "ground-a", ClusterID: "ground", OrbitClass: spacev1.OrbitGround}
	mission.Spec.Inputs = []spacev1.DataObject{{ID: "sensor", SizeBytes: 1, Locations: []spacev1.DataLocation{{Domain: source}}, PayloadDigest: digest}}
	placement.Spec.InputTransfers = []spacev1.TransferEpoch{{DataID: "sensor", Source: source, Destination: placement.Spec.Target, Start: metav1.NewTime(now), End: metav1.NewTime(now.Add(time.Minute)), Bytes: 1}}
	placement.Spec.NotBefore = metav1.NewTime(now)
	placement.Spec.ComputeStart = metav1.NewTime(now)
	store := &memoryStore{}
	evidence := newMemoryEvidence()
	c := &Controller{Store: store, Evidence: evidence, Clock: &mutableClock{now: now}}
	if _, err := c.ReconcileDispatch(context.Background(), mission, placement, mission.Spec.WorkloadTemplate); err != nil {
		t.Fatalf("missing coordinator should wait fail-closed: %v", err)
	}
	if placement.Status.Phase != spacev1.PlacementTransferPending || store.creates != 0 || len(evidence.intents) != 0 {
		t.Fatalf("phase=%s creates=%d intents=%d", placement.Status.Phase, store.creates, len(evidence.intents))
	}
}

func TestDeterministicAttemptFenceRejectsDifferentPlan(t *testing.T) {
	now := time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)
	mission, placement := dispatchFixture(now)
	clock := &mutableClock{now: now.Add(time.Minute)}
	lease := validLease(mission, placement, 1, 1, clock.now)
	store := &memoryStore{pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: mission.Namespace, Name: AttemptPodName(mission.Name, 1), Labels: map[string]string{spacev1.LabelPlacementID: "different-plan"}}}}
	e := newMemoryEvidence()
	e.leases = []*spacev1.SpaceExecutionLease{lease}
	if _, err := (&Controller{Store: store, Evidence: e, Clock: clock}).ReconcileDispatch(context.Background(), mission, placement, mission.Spec.WorkloadTemplate); err == nil {
		t.Fatal("different plan fence was accepted")
	}
}

func TestLegacyResultAndCheckpointAnnotationsAreUntrustedHints(t *testing.T) {
	now := time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)
	mission, placement := dispatchFixture(now)
	mission.Spec.ResultReturnRequired = true
	mission.Spec.ResultDestinations = []spacev1.DataLocation{{Domain: spacev1.DomainReference{Name: "ground-a", ClusterID: "ground", OrbitClass: spacev1.OrbitGround}}}
	mission.Spec.OutputSizeBytes = 1
	placement.Spec.ResultTransfer = &spacev1.TransferEpoch{WindowID: "result", DataID: "result", Source: placement.Spec.Target, Destination: spacev1.DomainReference{Name: "ground-a", ClusterID: "ground", OrbitClass: spacev1.OrbitGround}, Start: placement.Spec.ComputeEnd, End: metav1.NewTime(placement.Spec.ComputeEnd.Add(time.Minute)), Bytes: 1}
	clock := &mutableClock{now: now.Add(time.Minute)}
	store := &memoryStore{}
	e := newMemoryEvidence()
	lease := validLease(mission, placement, 1, 1, clock.now)
	e.leases = []*spacev1.SpaceExecutionLease{lease}
	c := &Controller{Store: store, Evidence: e, Clock: clock}
	if _, err := c.ReconcileDispatch(context.Background(), mission, placement, mission.Spec.WorkloadTemplate); err != nil {
		t.Fatal(err)
	}
	pod := store.pod.DeepCopy()
	pod.Status.Phase = corev1.PodSucceeded
	if changed, err := c.ReconcilePodStatus(context.Background(), mission, placement, pod); err != nil || !changed || placement.Status.Phase != spacev1.PlacementReturnPending {
		t.Fatalf("succeeded changed=%v phase=%s err=%v", changed, placement.Status.Phase, err)
	}
	pod.Annotations[spacev1.AnnotationResultReturned] = "true"
	pod.Annotations[spacev1.AnnotationCheckpointID] = "untrusted-checkpoint"
	if changed, err := c.ReconcilePodStatus(context.Background(), mission, placement, pod); err != nil || changed || placement.Status.Phase != spacev1.PlacementReturnPending {
		t.Fatalf("legacy hint changed=%v phase=%s err=%v", changed, placement.Status.Phase, err)
	}
	result := validResultReceipt(mission, placement, lease, clock.now)
	e.results = []*spacev1.SpaceResultReceipt{result}
	if changed, err := c.ReconcileTrustedEvidence(context.Background(), mission, placement); err != nil || !changed || placement.Status.Phase != spacev1.PlacementCompleted || !placement.Status.ResultReturned {
		t.Fatalf("trusted result changed=%v phase=%s returned=%v err=%v", changed, placement.Status.Phase, placement.Status.ResultReturned, err)
	}
}

func TestRetryRequiresRemoteCheckpointAndFenceBeforeLocalCleanup(t *testing.T) {
	now := time.Date(2026, 7, 21, 0, 2, 0, 0, time.UTC)
	mission, placement := dispatchFixture(now.Add(-time.Minute))
	placement.Spec.Attempt = 2
	placement.Spec.NotBefore = metav1.NewTime(now.Add(-time.Second))
	placement.Spec.ComputeStart = metav1.NewTime(now.Add(-time.Second))
	placement.Status.Phase = spacev1.PlacementCheckpointed
	placement.Status.ActivePod = &corev1.ObjectReference{Namespace: mission.Namespace, Name: AttemptPodName(mission.Name, 1), UID: types.UID("old")}
	store := &memoryStore{pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: AttemptPodName(mission.Name, 1), Namespace: mission.Namespace, UID: types.UID("old")}}}
	e := newMemoryEvidence()
	previous := validLease(mission, placement, 1, 1, now.Add(-2*time.Minute))
	previous.Spec.Fence.PlanID = "old-plan"
	previous.Name = spacev1.ExecutionLeaseName(previous.Spec.Fence.MissionUID, previous.Spec.Fence.PlanID, 1, 1)
	previous.Spec.Fence.ExpiresAt = metav1.NewTime(now.Add(time.Minute))
	current := validLease(mission, placement, 2, 2, now)
	e.leases = []*spacev1.SpaceExecutionLease{previous, current}
	c := &Controller{Store: store, Evidence: e, Clock: &mutableClock{now: now}}
	if _, err := c.ReconcileDispatch(context.Background(), mission, placement, mission.Spec.WorkloadTemplate); err != nil || store.pod == nil || store.creates != 0 || placement.Status.Phase != spacev1.PlacementExecutionLeasePending {
		t.Fatalf("unfenced prior attempt changed local pod: phase=%s pod=%v creates=%d err=%v", placement.Status.Phase, store.pod, store.creates, err)
	}
	checkpoint := validObservation(previous, "checkpoint-1", spacev1.ExecutionObservationCheckpointed, "checkpoint-1", now.Add(-30*time.Second))
	e.observations = []*spacev1.SpaceExecutionObservation{checkpoint}
	if _, err := c.ReconcileDispatch(context.Background(), mission, placement, mission.Spec.WorkloadTemplate); err != nil || store.pod == nil {
		t.Fatalf("checkpoint without stop/expiry fenced old pod err=%v pod=%v", err, store.pod)
	}
	stop := validObservation(previous, "stopped-1", spacev1.ExecutionObservationStopped, "", now.Add(-20*time.Second))
	e.observations = append(e.observations, stop)
	delay, err := c.ReconcileDispatch(context.Background(), mission, placement, mission.Spec.WorkloadTemplate)
	if err != nil || delay != time.Second || store.pod != nil || store.creates != 0 {
		t.Fatalf("trusted remote fence cleanup delay=%v pod=%v creates=%d err=%v", delay, store.pod, store.creates, err)
	}
	if _, err := c.ReconcileDispatch(context.Background(), mission, placement, mission.Spec.WorkloadTemplate); err != nil || store.creates != 1 || store.pod.Name != AttemptPodName(mission.Name, 2) {
		t.Fatalf("replacement pod=%v creates=%d err=%v", store.pod, store.creates, err)
	}
}

func dispatchFixture(now time.Time) (*spacev1.SpaceMission, *spacev1.SpacePlacementIntent) {
	mission := &spacev1.SpaceMission{ObjectMeta: metav1.ObjectMeta{Name: "dispatch", Namespace: "missions", UID: types.UID("mission-uid")}, Spec: spacev1.SpaceMissionSpec{MissionClass: "science", Priority: 1, StatePolicy: spacev1.PolicyStrict, RequiredCapabilities: []spacev1.CapabilityRequirement{{Class: "gpu", Quantity: 1}}, Deadline: metav1.NewTime(now.Add(time.Hour)), ExpectedDurationSeconds: 30, MaximumDurationSeconds: 60, DurationUncertaintySecs: 10, SafetyMarginSeconds: 5, MaximumClockSkewSeconds: 1, Retry: spacev1.RetryPolicy{MaxAttempts: 2, AllowMigration: true, MaxConcurrentExecutions: 1}, Checkpoint: spacev1.CheckpointPolicy{Checkpointable: true}, WorkloadTemplate: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "processor", Image: "example.invalid/processor:v1"}}}}}}
	placement := &spacev1.SpacePlacementIntent{ObjectMeta: metav1.ObjectMeta{Name: "dispatch-placement", Namespace: mission.Namespace}, Spec: spacev1.SpacePlacementIntentSpec{MissionRef: corev1.ObjectReference{Namespace: mission.Namespace, Name: mission.Name, UID: mission.UID}, PlanID: "plan-one", Attempt: 1, Target: spacev1.DomainReference{Name: "leo-a", ClusterID: "leo-cluster", OrbitClass: spacev1.OrbitLEO}, NotBefore: metav1.NewTime(now.Add(time.Minute)), ExpiresAt: metav1.NewTime(now.Add(30 * time.Minute)), ComputeStart: metav1.NewTime(now.Add(time.Minute)), ComputeEnd: metav1.NewTime(now.Add(2 * time.Minute)), MaterialInputDigest: "digest", SnapshotSequences: map[string]int64{}, Score: spacev1.DecisionScore{}, Explanations: []spacev1.ConstraintExplanation{}}}
	return mission, placement
}
func testProvenance(seq int64) spacev1.Provenance {
	return spacev1.Provenance{ReporterID: "system:serviceaccount:kube-system:domain-agent", Source: "domain-agent", Digest: strings.Repeat("c", 64), Sequence: seq}
}
func validLease(m *spacev1.SpaceMission, p *spacev1.SpacePlacementIntent, attempt int32, epoch int64, now time.Time) *spacev1.SpaceExecutionLease {
	f := spacev1.ExecutionFence{MissionUID: string(m.UID), PlanID: p.Spec.PlanID, Attempt: attempt, LeaseEpoch: epoch, TokenHash: strings.Repeat("b", 64), ExpiresAt: metav1.NewTime(now.Add(10 * time.Minute))}
	source := p.Spec.Target
	destination := spacev1.DomainReference{Name: "ground-a", ClusterID: "ground-cluster", OrbitClass: spacev1.OrbitGround}
	return &spacev1.SpaceExecutionLease{ObjectMeta: metav1.ObjectMeta{Name: spacev1.ExecutionLeaseName(f.MissionUID, f.PlanID, f.Attempt, f.LeaseEpoch)}, Spec: spacev1.SpaceExecutionLeaseSpec{Source: source, Destination: destination, Fence: f, HeartbeatAt: metav1.NewTime(now), MaximumClockSkewSeconds: 1, Provenance: testProvenance(1)}}
}
func validObservation(lease *spacev1.SpaceExecutionLease, id string, phase spacev1.ExecutionObservationPhase, checkpoint string, at time.Time) *spacev1.SpaceExecutionObservation {
	f := lease.Spec.Fence
	o := &spacev1.SpaceExecutionObservation{Spec: spacev1.SpaceExecutionObservationSpec{ObservationID: id, MissionUID: f.MissionUID, PlanID: f.PlanID, Attempt: f.Attempt, LeaseEpoch: f.LeaseEpoch, TokenHash: f.TokenHash, Source: lease.Spec.Source, Destination: lease.Spec.Destination, Phase: phase, CheckpointID: checkpoint, ObservedAt: metav1.NewTime(at), Provenance: testProvenance(1)}}
	o.Name = spacev1.ExecutionObservationName(o.Spec.Source, o.Spec.Destination, o.Spec.MissionUID, o.Spec.PlanID, id)
	return o
}
func validResultReceipt(m *spacev1.SpaceMission, p *spacev1.SpacePlacementIntent, lease *spacev1.SpaceExecutionLease, at time.Time) *spacev1.SpaceResultReceipt {
	destination := lease.Spec.Destination
	if p.Spec.ResultTransfer != nil {
		destination = p.Spec.ResultTransfer.Destination
	}
	r := &spacev1.SpaceResultReceipt{Spec: spacev1.SpaceResultReceiptSpec{ResultID: "result-one", MissionUID: string(m.UID), PlanID: p.Spec.PlanID, Attempt: p.Spec.Attempt, Source: lease.Spec.Source, Destination: destination, Bytes: 1, PayloadDigest: strings.Repeat("d", 64), LeaseEpoch: lease.Spec.Fence.LeaseEpoch, TokenHash: lease.Spec.Fence.TokenHash, CompletedAt: metav1.NewTime(at), Provenance: testProvenance(1)}}
	r.Name = spacev1.ResultReceiptName(r.Spec.Source, r.Spec.Destination, r.Spec.MissionUID, r.Spec.PlanID, r.Spec.ResultID)
	return r
}

type memoryStore struct {
	pod     *corev1.Pod
	creates int
	status  spacev1.SpacePlacementIntentStatus
}

func (s *memoryStore) GetPod(_ context.Context, _, name string) (*corev1.Pod, error) {
	if s.pod == nil || s.pod.Name != name {
		return nil, planner.ErrNotFound
	}
	return s.pod.DeepCopy(), nil
}
func (s *memoryStore) DeletePod(_ context.Context, _, name string) error {
	if s.pod == nil || s.pod.Name != name {
		return planner.ErrNotFound
	}
	s.pod = nil
	return nil
}
func (s *memoryStore) CreatePod(_ context.Context, p *corev1.Pod) (*corev1.Pod, error) {
	s.creates++
	s.pod = p.DeepCopy()
	s.pod.UID = types.UID("pod-uid")
	return s.pod.DeepCopy(), nil
}
func (s *memoryStore) UpdatePlacementStatus(_ context.Context, p *spacev1.SpacePlacementIntent) error {
	s.status = p.Status
	return nil
}
func (*memoryStore) Event(context.Context, string, string, string, string, string) {}

type memoryEvidence struct {
	intents      []*spacev1.SpaceTransferIntent
	receipts     []*spacev1.SpaceTransferReceipt
	leases       []*spacev1.SpaceExecutionLease
	observations []*spacev1.SpaceExecutionObservation
	results      []*spacev1.SpaceResultReceipt
}

func newMemoryEvidence() *memoryEvidence { return &memoryEvidence{} }
func (e *memoryEvidence) EnsureTransferIntent(_ context.Context, v *spacev1.SpaceTransferIntent) error {
	for _, x := range e.intents {
		if x.Name == v.Name {
			return nil
		}
	}
	e.intents = append(e.intents, v.DeepCopy())
	return nil
}
func (e *memoryEvidence) ListTransferReceipts(context.Context) ([]*spacev1.SpaceTransferReceipt, error) {
	return e.receipts, nil
}
func (e *memoryEvidence) ListExecutionLeases(context.Context) ([]*spacev1.SpaceExecutionLease, error) {
	return e.leases, nil
}
func (e *memoryEvidence) GetExecutionLease(_ context.Context, name string) (*spacev1.SpaceExecutionLease, error) {
	for _, v := range e.leases {
		if v.Name == name {
			return v, nil
		}
	}
	return nil, planner.ErrNotFound
}
func (e *memoryEvidence) ListExecutionObservations(context.Context) ([]*spacev1.SpaceExecutionObservation, error) {
	return e.observations, nil
}
func (e *memoryEvidence) ListResultReceipts(context.Context) ([]*spacev1.SpaceResultReceipt, error) {
	return e.results, nil
}
