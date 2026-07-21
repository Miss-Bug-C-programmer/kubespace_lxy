package kube

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"

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
