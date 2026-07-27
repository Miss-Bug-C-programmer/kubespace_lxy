package kube

import (
	"context"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
	"github.com/k3s-io/k3s/contrib/space-compute/pkg/planner"
)

func TestDynamicRepositoryPlacementIsDurableAndIdempotent(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := spacev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	mission := &spacev1.SpaceMission{TypeMeta: metav1.TypeMeta{APIVersion: spacev1.SchemeGroupVersion.String(), Kind: "SpaceMission"}, ObjectMeta: metav1.ObjectMeta{Name: "mission", Namespace: "missions", UID: types.UID("mission-uid")}}
	client := dynamicfake.NewSimpleDynamicClient(scheme, mission)
	repository := &Repository{Dynamic: client}
	ctx := context.Background()
	key := planner.MissionKey{Namespace: mission.Namespace, Name: mission.Name}
	if _, err := repository.GetPlacement(ctx, key); err != planner.ErrNotFound {
		t.Fatalf("missing placement error = %v", err)
	}
	placement := repositoryPlacement(mission)
	changed, err := repository.ApplyPlacement(ctx, placement, "")
	if err != nil || !changed {
		t.Fatalf("create changed=%v err=%v", changed, err)
	}
	changed, err = repository.ApplyPlacement(ctx, placement.DeepCopy(), placement.Spec.PlanID)
	if err != nil || changed {
		t.Fatalf("duplicate changed=%v err=%v", changed, err)
	}
	got, err := repository.GetPlacement(ctx, key)
	if err != nil || got.Spec.MaterialInputDigest != placement.Spec.MaterialInputDigest {
		t.Fatalf("durable placement=%+v err=%v", got, err)
	}
	got.Status.Phase = spacev1.PlacementRunning
	if err := repository.UpdatePlacementStatus(ctx, got); err != nil {
		t.Fatal(err)
	}
	got, _ = repository.GetPlacement(ctx, key)
	if got.Status.Phase != spacev1.PlacementRunning {
		t.Fatalf("status phase=%s", got.Status.Phase)
	}
}

func repositoryPlacement(mission *spacev1.SpaceMission) *spacev1.SpacePlacementIntent {
	now := time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)
	return &spacev1.SpacePlacementIntent{TypeMeta: metav1.TypeMeta{APIVersion: spacev1.SchemeGroupVersion.String(), Kind: "SpacePlacementIntent"}, ObjectMeta: metav1.ObjectMeta{Name: "mission-placement", Namespace: mission.Namespace, Labels: map[string]string{spacev1.LabelMissionUID: string(mission.UID)}}, Spec: spacev1.SpacePlacementIntentSpec{MissionRef: corev1.ObjectReference{Namespace: mission.Namespace, Name: mission.Name, UID: mission.UID}, PlanID: "plan-one", Attempt: 1, Target: spacev1.DomainReference{Name: "leo-a", ClusterID: "leo", OrbitClass: spacev1.OrbitLEO}, NotBefore: metav1.NewTime(now), ExpiresAt: metav1.NewTime(now.Add(time.Hour)), ComputeStart: metav1.NewTime(now), ComputeEnd: metav1.NewTime(now.Add(time.Minute)), MaterialInputDigest: "digest", SnapshotSequences: map[string]int64{}, Score: spacev1.DecisionScore{}, Explanations: []spacev1.ConstraintExplanation{}}}
}

func TestPlanningSnapshotUsesInformerStoresAndPinsVersions(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := spacev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	client := dynamicfake.NewSimpleDynamicClient(scheme)
	client.PrependReactor("list", "spacedomainresourcesummaries", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("unexpected live resource list")
	})
	client.PrependReactor("list", "spacelinksnapshots", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("unexpected live link list")
	})
	resourceStore := cache.NewStore(cache.MetaNamespaceKeyFunc)
	linkStore := cache.NewStore(cache.MetaNamespaceKeyFunc)
	now := time.Date(2026, 7, 27, 0, 0, 0, 0, time.UTC)
	summary := &spacev1.SpaceDomainResourceSummary{ObjectMeta: metav1.ObjectMeta{Name: "leo-a", ResourceVersion: "41"}, Spec: spacev1.SpaceDomainResourceSummarySpec{Domain: spacev1.DomainReference{Name: "leo-a", ClusterID: "leo", OrbitClass: spacev1.OrbitLEO}, ObservedAt: metav1.NewTime(now), ValidUntil: metav1.NewTime(now.Add(time.Hour))}}
	link := &spacev1.SpaceLinkSnapshot{ObjectMeta: metav1.ObjectMeta{Name: "ground-to-leo", ResourceVersion: "52"}, Spec: spacev1.SpaceLinkSnapshotSpec{Source: spacev1.DomainReference{Name: "ground", ClusterID: "ground", OrbitClass: spacev1.OrbitGround}, Destination: summary.Spec.Domain, ObservedAt: metav1.NewTime(now), ValidUntil: metav1.NewTime(now.Add(time.Hour))}}
	for store, value := range map[cache.Store]interface{}{resourceStore: summary, linkStore: link} {
		u, err := toUnstructured(value)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Add(u); err != nil {
			t.Fatal(err)
		}
	}
	repository := &Repository{Dynamic: client, ResourceSummaryStore: resourceStore, LinkSnapshotStore: linkStore, CacheResourceVersions: func() map[string]string { return map[string]string{"resourceSummaries": "41", "linkSnapshots": "52"} }}
	first, err := repository.PlanningSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	second, err := repository.PlanningSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if first.InputDigest == "" || first.InputDigest != second.InputDigest {
		t.Fatalf("input digests first=%q second=%q", first.InputDigest, second.InputDigest)
	}
	if first.CacheVersions["resourceSummaries"] != "41" || first.CacheVersions["linkSnapshots"] != "52" {
		t.Fatalf("cache versions=%v", first.CacheVersions)
	}
	if len(first.ResourceSummaries) != 1 || len(first.LinkSnapshots) != 1 {
		t.Fatalf("snapshot sizes resources=%d links=%d", len(first.ResourceSummaries), len(first.LinkSnapshots))
	}
}

func TestStatusMergePreservesConcurrentProgressAndConditions(t *testing.T) {
	now := metav1.Now()
	currentMission := spacev1.SpaceMissionStatus{ObservedGeneration: 5, Phase: spacev1.MissionExecuting, PlanID: "current", Conditions: []metav1.Condition{{Type: "Authorized", Status: metav1.ConditionTrue, Reason: "Concurrent", LastTransitionTime: now}}}
	staleMission := spacev1.SpaceMissionStatus{ObservedGeneration: 4, Phase: spacev1.MissionPlanned, PlanID: "stale", Conditions: []metav1.Condition{{Type: "Planned", Status: metav1.ConditionTrue, Reason: "Planner", LastTransitionTime: now}}}
	mergedMission := mergeMissionStatus(currentMission, staleMission)
	if mergedMission.Phase != spacev1.MissionExecuting || mergedMission.PlanID != "current" || len(mergedMission.Conditions) != 2 {
		t.Fatalf("mission merge regressed concurrent status: %+v", mergedMission)
	}

	currentPlacement := spacev1.SpacePlacementIntentStatus{Phase: spacev1.PlacementRunning, LastObservationSequence: 9, RetryCount: 2, ResultReturned: true, Conditions: []metav1.Condition{{Type: "Lease", Status: metav1.ConditionTrue, Reason: "Concurrent", LastTransitionTime: now}}}
	stalePlacement := spacev1.SpacePlacementIntentStatus{Phase: spacev1.PlacementDispatched, LastObservationSequence: 8, RetryCount: 1, Conditions: []metav1.Condition{{Type: "ExecutionSafe", Status: metav1.ConditionTrue, Reason: "Dispatcher", LastTransitionTime: now}}}
	mergedPlacement := mergePlacementStatus(currentPlacement, stalePlacement)
	if mergedPlacement.Phase != spacev1.PlacementRunning || mergedPlacement.LastObservationSequence != 9 || mergedPlacement.RetryCount != 2 || !mergedPlacement.ResultReturned || len(mergedPlacement.Conditions) != 2 {
		t.Fatalf("placement merge regressed concurrent status: %+v", mergedPlacement)
	}
}

func TestMissionStatusMergeDoesNotRegressTerminalOrExecutingState(t *testing.T) {
	terminal := spacev1.SpaceMissionStatus{ObservedGeneration: 3, Phase: spacev1.MissionSucceeded, PlanID: "current"}
	stale := spacev1.SpaceMissionStatus{ObservedGeneration: 3, Phase: spacev1.MissionBlocked, PlanID: "stale"}
	merged := mergeMissionStatus(terminal, stale)
	if merged.Phase != spacev1.MissionSucceeded || merged.PlanID != "current" {
		t.Fatalf("terminal status regressed: %#v", merged)
	}
	executing := spacev1.SpaceMissionStatus{ObservedGeneration: 3, Phase: spacev1.MissionExecuting, PlanID: "current"}
	planned := spacev1.SpaceMissionStatus{ObservedGeneration: 3, Phase: spacev1.MissionPlanned, PlanID: "stale"}
	merged = mergeMissionStatus(executing, planned)
	if merged.Phase != spacev1.MissionExecuting || merged.PlanID != "current" {
		t.Fatalf("executing status regressed: %#v", merged)
	}
}
