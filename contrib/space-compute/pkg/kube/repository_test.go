package kube

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
	spacev1beta1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1beta1"
	"github.com/k3s-io/k3s/contrib/space-compute/pkg/planner"
)

func TestDynamicRepositoryPlacementIsDurableAndIdempotent(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := spacev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := spacev1beta1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	mission := &spacev1.SpaceMission{TypeMeta: metav1.TypeMeta{APIVersion: spacev1.CanonicalAPIVersion, Kind: "SpaceMission"}, ObjectMeta: metav1.ObjectMeta{Name: "mission", Namespace: "missions", UID: types.UID("mission-uid")}}
	client := dynamicfake.NewSimpleDynamicClient(scheme)
	ctx := context.Background()
	missionObject, err := toUnstructured(mission)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Resource(MissionGVR).Namespace(mission.Namespace).Create(ctx, missionObject, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	repository := &Repository{Dynamic: client}
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
	return &spacev1.SpacePlacementIntent{TypeMeta: metav1.TypeMeta{APIVersion: spacev1.CanonicalAPIVersion, Kind: "SpacePlacementIntent"}, ObjectMeta: metav1.ObjectMeta{Name: "mission-placement", Namespace: mission.Namespace, Labels: map[string]string{spacev1.LabelMissionUID: string(mission.UID)}}, Spec: spacev1.SpacePlacementIntentSpec{MissionRef: corev1.ObjectReference{Namespace: mission.Namespace, Name: mission.Name, UID: mission.UID}, PlanID: "plan-one", Attempt: 1, Target: spacev1.DomainReference{Name: "leo-a", ClusterID: "leo", OrbitClass: spacev1.OrbitLEO}, NotBefore: metav1.NewTime(now), ExpiresAt: metav1.NewTime(now.Add(time.Hour)), ComputeStart: metav1.NewTime(now), ComputeEnd: metav1.NewTime(now.Add(time.Minute)), MaterialInputDigest: "digest", SnapshotSequences: map[string]int64{}, Score: spacev1.DecisionScore{}, Explanations: []spacev1.ConstraintExplanation{}}}
}

func TestPlanningSnapshotUsesInformerStoresAndPinsVersions(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := spacev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := spacev1beta1.AddToScheme(scheme); err != nil {
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

func TestPhase9PlacementStatusMergePreservesAuditEvidence(t *testing.T) {
	current := spacev1.SpacePlacementIntentStatus{
		Phase: spacev1.PlacementRunning, TransferState: spacev1.TransferStateCompleted,
		TransferReceiptReferences: []string{"receipt-a"}, ExecutionLeaseReference: "lease-a",
		FencingTokenHash: strings.Repeat("a", 64), CheckpointReceipt: "checkpoint-a",
		RemoteAcknowledgementSequence: 8,
	}
	desired := spacev1.SpacePlacementIntentStatus{
		Phase: spacev1.PlacementCompleted, TransferState: spacev1.TransferStatePending,
		TransferReceiptReferences: []string{"receipt-b", "receipt-a"}, ExecutionLeaseReference: "lease-b",
		FencingTokenHash: strings.Repeat("b", 64), ResultReceipt: "result-b",
		RemoteAcknowledgementSequence: 10,
	}
	merged := mergePlacementStatus(current, desired)
	if merged.Phase != spacev1.PlacementCompleted || merged.TransferState != spacev1.TransferStateCompleted || merged.RemoteAcknowledgementSequence != 10 {
		t.Fatalf("monotonic merge failed: %+v", merged)
	}
	if len(merged.TransferReceiptReferences) != 2 || merged.TransferReceiptReferences[0] != "receipt-a" || merged.TransferReceiptReferences[1] != "receipt-b" {
		t.Fatalf("receipt union=%v", merged.TransferReceiptReferences)
	}
	if merged.ExecutionLeaseReference != "lease-b" || merged.FencingTokenHash != strings.Repeat("b", 64) || merged.CheckpointReceipt != "checkpoint-a" || merged.ResultReceipt != "result-b" {
		t.Fatalf("audit evidence merge=%+v", merged)
	}
}

func TestPhase9RepositoryUsesCanonicalBetaGVRs(t *testing.T) {
	for name, gvr := range map[string]schema.GroupVersionResource{
		"mission": MissionGVR, "placement": PlacementGVR, "link": LinkGVR, "resource": ResourceSummaryGVR,
		"inventory": PhysicalDeviceInventoryGVR, "transferIntent": TransferIntentGVR, "transferReceipt": TransferReceiptGVR,
		"executionLease": ExecutionLeaseGVR, "executionObservation": ExecutionObservationGVR, "resultReceipt": ResultReceiptGVR,
	} {
		if gvr.Group != spacev1.GroupName || gvr.Version != spacev1.CanonicalVersion {
			t.Fatalf("%s GVR=%s, want canonical %s/%s", name, gvr.String(), spacev1.GroupName, spacev1.CanonicalVersion)
		}
	}
}

func TestPhase10PlanningIndexReusesImmutablePreparedSnapshotAt5000Domains(t *testing.T) {
	index := NewPlanningIndex()
	now := time.Date(2026, 7, 28, 0, 0, 0, 0, time.UTC)
	for n := 0; n < 5000; n++ {
		name := fmt.Sprintf("domain-%05d", n)
		summary := &spacev1.SpaceDomainResourceSummary{
			ObjectMeta: metav1.ObjectMeta{Name: name, ResourceVersion: fmt.Sprintf("%d", n+1)},
			Spec: spacev1.SpaceDomainResourceSummarySpec{
				Domain:     spacev1.DomainReference{Name: name, ClusterID: name + "-cluster", OrbitClass: spacev1.OrbitGround},
				ObservedAt: metav1.NewTime(now), ValidUntil: metav1.NewTime(now.Add(time.Hour)),
			},
		}
		if err := index.UpsertResource(summary); err != nil {
			t.Fatal(err)
		}
	}
	repository := &Repository{PlanningIndex: index, CacheResourceVersions: func() map[string]string {
		return map[string]string{"resourceSummaries": "5000", "linkSnapshots": "0"}
	}}
	first, err := repository.PlanningSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	second, err := repository.PlanningSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if first != second || first.Prepared != second.Prepared {
		t.Fatal("unchanged informer generation rebuilt the immutable planning snapshot")
	}
	if len(first.ResourceSummaries) != 5000 || first.InputDigest == "" {
		t.Fatalf("prepared snapshot resources=%d digest=%q", len(first.ResourceSummaries), first.InputDigest)
	}
	changed := first.ResourceSummaries[1234].DeepCopy()
	changed.ResourceVersion = "5001"
	changed.Spec.QueueDelaySeconds++
	if err := index.UpsertResource(changed); err != nil {
		t.Fatal(err)
	}
	third, err := repository.PlanningSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if third == first || third.Prepared == first.Prepared || third.InputDigest == first.InputDigest {
		t.Fatal("changed informer object did not replace prepared snapshot/digest")
	}
}

func TestPhase10MissionStatusNoopSkipsUpdateWrite(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := spacev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := spacev1beta1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	now := metav1.Now()
	mission := &spacev1.SpaceMission{
		TypeMeta:   metav1.TypeMeta{APIVersion: spacev1.CanonicalAPIVersion, Kind: "SpaceMission"},
		ObjectMeta: metav1.ObjectMeta{Name: "noop", Namespace: "missions"},
		Status:     spacev1.SpaceMissionStatus{ObservedGeneration: 3, Phase: spacev1.MissionPlanned, PlanID: "plan", Conditions: []metav1.Condition{{Type: "Planned", Status: metav1.ConditionTrue, Reason: "PlanCurrent", Message: "same", ObservedGeneration: 3, LastTransitionTime: now}}},
	}
	client := dynamicfake.NewSimpleDynamicClient(scheme)
	object, err := toUnstructured(mission)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Resource(MissionGVR).Namespace(mission.Namespace).Create(context.Background(), object, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	client.ClearActions()
	repository := &Repository{Dynamic: client}
	if err := repository.UpdateMissionStatus(context.Background(), mission.DeepCopy()); err != nil {
		t.Fatal(err)
	}
	updates := 0
	for _, action := range client.Actions() {
		if action.GetVerb() == "update" {
			updates++
		}
	}
	if updates != 0 {
		t.Fatalf("no-op status produced %d update writes", updates)
	}
}
