package workload

import (
	"context"
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

func TestFutureWaitRestartAndDuplicateDispatchAreIdempotent(t *testing.T) {
	now := time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)
	clock := &mutableClock{now: now}
	mission, placement := dispatchFixture(now)
	store := &memoryStore{}
	controller := &Controller{Store: store, Clock: clock}
	delay, err := controller.ReconcileDispatch(context.Background(), mission, placement, mission.Spec.WorkloadTemplate)
	if err != nil || delay != time.Minute || store.creates != 0 {
		t.Fatalf("future wait delay=%v creates=%d err=%v", delay, store.creates, err)
	}
	clock.now = now.Add(time.Minute)
	if delay, err = (&Controller{Store: store, Clock: clock}).ReconcileDispatch(context.Background(), mission, placement, mission.Spec.WorkloadTemplate); err != nil || delay != 0 || store.creates != 1 {
		t.Fatalf("restart dispatch delay=%v creates=%d err=%v", delay, store.creates, err)
	}
	if _, err = controller.ReconcileDispatch(context.Background(), mission, placement, mission.Spec.WorkloadTemplate); err != nil || store.creates != 1 {
		t.Fatalf("duplicate dispatch creates=%d err=%v", store.creates, err)
	}
	if store.pod.Spec.SchedulerName != "space-compute-scheduler" || store.pod.Labels[spacev1.LabelPlacementID] != placement.Spec.PlanID {
		t.Fatalf("created Pod = %+v", store.pod)
	}
	if _, err := spacepolicy.ParsePod(store.pod, clock); err != nil {
		t.Fatalf("created production annotations: %v", err)
	}
}

func TestDeterministicAttemptFenceRejectsDifferentPlan(t *testing.T) {
	now := time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)
	mission, placement := dispatchFixture(now)
	store := &memoryStore{pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: mission.Namespace, Name: AttemptPodName(mission.Name, 1), Labels: map[string]string{spacev1.LabelPlacementID: "different-plan"}}}}
	if _, err := (&Controller{Store: store, Clock: &mutableClock{now: now.Add(time.Minute)}}).ReconcileDispatch(context.Background(), mission, placement, mission.Spec.WorkloadTemplate); err == nil {
		t.Fatal("different plan fence was accepted")
	}
}

func TestPodStatusRequiresExplicitResultReturnAndIgnoresDuplicates(t *testing.T) {
	now := time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)
	mission, placement := dispatchFixture(now)
	mission.Spec.ResultReturnRequired = true
	mission.Spec.ResultDestinations = []string{"ground-a"}
	mission.Spec.OutputSizeBytes = 1
	placement.Spec.ResultTransfer = &spacev1.TransferEpoch{WindowID: "local-result", Start: placement.Spec.ComputeEnd, End: placement.Spec.ComputeEnd, Bytes: 1}
	clock := &mutableClock{now: now.Add(time.Minute)}
	store := &memoryStore{}
	controller := &Controller{Store: store, Clock: clock}
	if _, err := controller.ReconcileDispatch(context.Background(), mission, placement, mission.Spec.WorkloadTemplate); err != nil {
		t.Fatal(err)
	}
	pod := store.pod.DeepCopy()
	pod.Status.Phase = corev1.PodRunning
	if changed, err := controller.ReconcilePodStatus(context.Background(), mission, placement, pod); err != nil || !changed || placement.Status.Phase != spacev1.PlacementRunning {
		t.Fatalf("running changed=%v err=%v phase=%s", changed, err, placement.Status.Phase)
	}
	if changed, err := controller.ReconcilePodStatus(context.Background(), mission, placement, pod); err != nil || changed {
		t.Fatalf("duplicate running changed=%v err=%v", changed, err)
	}
	pod.Status.Phase = corev1.PodSucceeded
	if changed, err := controller.ReconcilePodStatus(context.Background(), mission, placement, pod); err != nil || !changed || placement.Status.Phase != spacev1.PlacementReturnPending || placement.Status.ResultReturned {
		t.Fatalf("return pending changed=%v err=%v status=%+v", changed, err, placement.Status)
	}
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	pod.Annotations[spacev1.AnnotationResultReturned] = "true"
	if changed, err := controller.ReconcilePodStatus(context.Background(), mission, placement, pod); err != nil || !changed || placement.Status.Phase != spacev1.PlacementCompleted || !placement.Status.ResultReturned {
		t.Fatalf("completed changed=%v err=%v status=%+v", changed, err, placement.Status)
	}
}

func TestRetryDeletesFencedAttemptBeforeCreatingReplacement(t *testing.T) {
	now := time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)
	mission, placement := dispatchFixture(now)
	placement.Spec.Attempt = 2
	placement.Spec.NotBefore = metav1.NewTime(now.Add(-time.Second))
	placement.Status.Phase = spacev1.PlacementCheckpointed
	placement.Status.ActivePod = &corev1.ObjectReference{Namespace: mission.Namespace, Name: AttemptPodName(mission.Name, 1), UID: types.UID("old-pod-uid")}
	placement.Status.LastObservation = &spacev1.ExecutionObservation{Sequence: 3, Attempt: 1, PodUID: "old-pod-uid", Phase: "checkpointed", CheckpointID: "checkpoint-1", ObservedAt: metav1.NewTime(now)}
	store := &memoryStore{pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: AttemptPodName(mission.Name, 1), Namespace: mission.Namespace, UID: types.UID("old-pod-uid")}}}
	controller := &Controller{Store: store, Clock: &mutableClock{now: now}}

	delay, err := controller.ReconcileDispatch(context.Background(), mission, placement, mission.Spec.WorkloadTemplate)
	if err != nil || delay != time.Second || store.pod != nil || store.creates != 0 {
		t.Fatalf("fence pass delay=%s err=%v pod=%v creates=%d", delay, err, store.pod, store.creates)
	}
	delay, err = controller.ReconcileDispatch(context.Background(), mission, placement, mission.Spec.WorkloadTemplate)
	if err != nil || delay != 0 || store.creates != 1 || store.pod == nil || store.pod.Name != AttemptPodName(mission.Name, 2) {
		t.Fatalf("replacement pass delay=%s err=%v pod=%v creates=%d", delay, err, store.pod, store.creates)
	}
	if placement.Status.RetryCount != 1 || len(store.pod.OwnerReferences) != 1 || store.pod.OwnerReferences[0].UID != mission.UID {
		t.Fatalf("retry/ownership status=%+v owner=%+v", placement.Status, store.pod.OwnerReferences)
	}
}

func dispatchFixture(now time.Time) (*spacev1.SpaceMission, *spacev1.SpacePlacementIntent) {
	mission := &spacev1.SpaceMission{ObjectMeta: metav1.ObjectMeta{Name: "dispatch", Namespace: "missions", UID: types.UID("mission-uid")}, Spec: spacev1.SpaceMissionSpec{MissionClass: "science", Priority: 1, StatePolicy: spacev1.PolicyStrict, RequiredCapabilities: []spacev1.CapabilityRequirement{{Class: "gpu", Quantity: 1}}, Deadline: metav1.NewTime(now.Add(time.Hour)), ExpectedDurationSeconds: 30, MaximumDurationSeconds: 60, DurationUncertaintySecs: 10, SafetyMarginSeconds: 5, MaximumClockSkewSeconds: 1, Retry: spacev1.RetryPolicy{MaxAttempts: 2, MaxConcurrentExecutions: 1}, Checkpoint: spacev1.CheckpointPolicy{Checkpointable: true}, WorkloadTemplate: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "processor", Image: "example.invalid/processor:v1"}}}}}}
	placement := &spacev1.SpacePlacementIntent{ObjectMeta: metav1.ObjectMeta{Name: "dispatch-placement", Namespace: mission.Namespace}, Spec: spacev1.SpacePlacementIntentSpec{MissionRef: corev1.ObjectReference{Namespace: mission.Namespace, Name: mission.Name, UID: mission.UID}, PlanID: "plan-one", Attempt: 1, Target: spacev1.DomainReference{Name: "leo-a", ClusterID: "leo-cluster", OrbitClass: spacev1.OrbitLEO}, NotBefore: metav1.NewTime(now.Add(time.Minute)), ExpiresAt: metav1.NewTime(now.Add(30 * time.Minute)), ComputeStart: metav1.NewTime(now.Add(time.Minute)), ComputeEnd: metav1.NewTime(now.Add(2 * time.Minute)), MaterialInputDigest: "digest", SnapshotSequences: map[string]int64{}, Score: spacev1.DecisionScore{}, Explanations: []spacev1.ConstraintExplanation{}}}
	return mission, placement
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
func (s *memoryStore) CreatePod(_ context.Context, pod *corev1.Pod) (*corev1.Pod, error) {
	s.creates++
	s.pod = pod.DeepCopy()
	s.pod.UID = types.UID("pod-uid")
	return s.pod.DeepCopy(), nil
}
func (s *memoryStore) UpdatePlacementStatus(_ context.Context, placement *spacev1.SpacePlacementIntent) error {
	s.status = placement.Status
	return nil
}
func (*memoryStore) Event(context.Context, string, string, string, string, string) {}
