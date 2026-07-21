package policy

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
)

type policyClock struct{ now time.Time }

func (f policyClock) Now() time.Time { return f.now }

func TestFreshStalePolicyAndClockSkewBoundaries(t *testing.T) {
	now := time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)
	requirement, node := localFixture(t, now)
	if evaluation := Evaluate(requirement, node, policyClock{now}); !evaluation.Feasible || evaluation.Dimensions.LinkRisk != 90 {
		t.Fatalf("fresh evaluation = %+v", evaluation)
	}

	projection := decodeProjection(t, node)
	projection.ObservedAt = metav1.NewTime(now.Add(-2 * time.Minute))
	projection.ValidUntil = metav1.NewTime(now.Add(-5 * time.Second))
	node.Annotations[spacev1.AnnotationLinkProjection] = mustJSON(t, projection)
	if evaluation := Evaluate(requirement, node, policyClock{now}); evaluation.Feasible || evaluation.ReasonCode != "link_projection_stale" {
		t.Fatalf("strict stale = %+v", evaluation)
	}
	requirement.Mission.Spec.StatePolicy = spacev1.PolicyDegraded
	if evaluation := Evaluate(requirement, node, policyClock{now}); !evaluation.Feasible || !evaluation.Degraded || evaluation.Dimensions.LinkRisk > 20 {
		t.Fatalf("degraded stale = %+v", evaluation)
	}
	requirement.Mission.Spec.StatePolicy = spacev1.PolicyBestEffort
	if evaluation := Evaluate(requirement, node, policyClock{now}); !evaluation.Feasible || evaluation.Dimensions.LinkRisk != 0 {
		t.Fatalf("best-effort stale = %+v", evaluation)
	}

	// Five seconds before notBefore is inside a ten-second declared skew bound.
	requirement.Mission.Spec.MaximumClockSkewSeconds = 10
	requirement.Placement.Spec.NotBefore = metav1.NewTime(now.Add(5 * time.Second))
	requirement.Mission.Spec.StatePolicy = spacev1.PolicyStrict
	projection.ValidUntil = metav1.NewTime(now.Add(time.Hour))
	node.Annotations[spacev1.AnnotationLinkProjection] = mustJSON(t, projection)
	if evaluation := Evaluate(requirement, node, policyClock{now}); !evaluation.Feasible {
		t.Fatalf("skew boundary rejected: %+v", evaluation)
	}
	requirement.Placement.Spec.NotBefore = metav1.NewTime(now.Add(11 * time.Second))
	if evaluation := Evaluate(requirement, node, policyClock{now}); evaluation.Feasible || evaluation.ReasonCode != "execution_epoch_not_open" {
		t.Fatalf("outside skew boundary = %+v", evaluation)
	}
}

func TestPodProjectionParsingRejectsContradictionAndOversize(t *testing.T) {
	now := time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)
	requirement, _ := localFixture(t, now)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{spacev1.AnnotationMissionIntent: mustJSON(t, requirement.Mission), spacev1.AnnotationPlacement: mustJSON(t, requirement.Placement)}}}
	if _, err := ParsePod(pod, policyClock{now}); err != nil {
		t.Fatalf("valid projection: %v", err)
	}
	pod.Annotations[spacev1.AnnotationMissionIntent] = strings.Repeat("x", maxProjectionBytes+1)
	if _, err := ParsePod(pod, policyClock{now}); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversize error = %v", err)
	}

	pod.Annotations[spacev1.AnnotationMissionIntent] = mustJSON(t, requirement.Mission)
	bad := requirement.Placement
	bad.Spec.MissionRef.UID = types.UID("different")
	pod.Annotations[spacev1.AnnotationPlacement] = mustJSON(t, bad)
	if _, err := ParsePod(pod, policyClock{now}); err == nil || !strings.Contains(err.Error(), "missionUID") {
		t.Fatalf("identity mismatch = %v", err)
	}
}

func FuzzPodMissionProjection(f *testing.F) {
	now := time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)
	requirement, _ := localFixture(f, now)
	// Seed directly without relying on test-only success behavior; all fuzzed
	// values still traverse the production strict JSON decoder and validators.
	missionRaw, _ := json.Marshal(requirement.Mission)
	placementRaw, _ := json.Marshal(requirement.Placement)
	f.Add(string(missionRaw), string(placementRaw))
	f.Fuzz(func(t *testing.T, mission, placement string) {
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{spacev1.AnnotationMissionIntent: mission, spacev1.AnnotationPlacement: placement}}}
		_, _ = ParsePod(pod, policyClock{now})
	})
}

func localFixture(t testing.TB, now time.Time) (*Requirement, *corev1.Node) {
	t.Helper()
	deadline := metav1.NewTime(now.Add(time.Hour))
	missionUID := types.UID("mission-uid")
	mission := PodMissionIntent{TypeMeta: metav1.TypeMeta{APIVersion: spacev1.SchemeGroupVersion.String(), Kind: "PodMissionIntent"}, MissionUID: string(missionUID), Spec: spacev1.SpaceMissionSpec{MissionClass: "science", Priority: 10, StatePolicy: spacev1.PolicyStrict, RequiredCapabilities: []spacev1.CapabilityRequirement{{Class: "gpu", Quantity: 1}}, Deadline: deadline, ExpectedDurationSeconds: 60, MaximumDurationSeconds: 100, DurationUncertaintySecs: 10, SafetyMarginSeconds: 10, MaximumClockSkewSeconds: 2, ResultReturnRequired: true, ResultDestinations: []string{"ground-a"}, OutputSizeBytes: 1000, Retry: spacev1.RetryPolicy{MaxAttempts: 2, MaxConcurrentExecutions: 1}, Checkpoint: spacev1.CheckpointPolicy{Checkpointable: true}, WorkloadTemplate: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "processor", Image: "example.invalid/processor:v1"}}}}}}
	linkSpec := spacev1.SpaceLinkSnapshotSpec{Source: spacev1.DomainReference{Name: "leo-a", ClusterID: "leo-cluster", OrbitClass: spacev1.OrbitLEO}, Destination: spacev1.DomainReference{Name: "ground-a", ClusterID: "ground-cluster", OrbitClass: spacev1.OrbitGround}, ObservedAt: metav1.NewTime(now), ValidUntil: metav1.NewTime(now.Add(time.Hour)), MaximumClockSkewSeconds: 2, MinimumUpdateSeconds: 10, HistoryLimit: 8, Provenance: spacev1.Provenance{ReporterID: "link-reporter", Source: "contact-product", Digest: strings.Repeat("a", 64), Sequence: 7}, Windows: []spacev1.ContactWindow{{ID: "return-window", Start: metav1.NewTime(now.Add(3 * time.Minute)), End: metav1.NewTime(now.Add(20 * time.Minute)), BandwidthBitsPerSec: 100_000_000, RTTMicroseconds: 1000, StabilityMilli: 900, ConfidenceMilli: 900, Predicted: true}}}
	resultTransfer := &spacev1.TransferEpoch{LinkSnapshotName: "leo-ground", WindowID: "return-window", Start: metav1.NewTime(now.Add(4 * time.Minute)), End: metav1.NewTime(now.Add(5 * time.Minute)), Bytes: 1000}
	placement := PodPlacement{TypeMeta: metav1.TypeMeta{APIVersion: spacev1.SchemeGroupVersion.String(), Kind: "PodPlacement"}, Spec: spacev1.SpacePlacementIntentSpec{MissionRef: corev1.ObjectReference{Namespace: "missions", Name: "mission", UID: missionUID}, PlanID: "plan-123", Attempt: 1, Target: spacev1.DomainReference{Name: "leo-a", ClusterID: "leo-cluster", OrbitClass: spacev1.OrbitLEO}, NotBefore: metav1.NewTime(now), ExpiresAt: deadline, ComputeStart: metav1.NewTime(now), ComputeEnd: metav1.NewTime(now.Add(10 * time.Minute)), ResultTransfer: resultTransfer, MaterialInputDigest: strings.Repeat("b", 64), SnapshotSequences: map[string]int64{"link/leo-ground": 7}}}
	projection := NodeProjection{TypeMeta: metav1.TypeMeta{APIVersion: spacev1.SchemeGroupVersion.String(), Kind: "NodeLinkProjection"}, Domain: placement.Spec.Target, ObservedAt: metav1.NewTime(now), ValidUntil: metav1.NewTime(now.Add(time.Hour)), ResourceDigest: strings.Repeat("c", 64), ResilienceMilli: 800, Links: []ProjectedLink{{Name: "leo-ground", Spec: linkSpec}}}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "leo-node", Labels: map[string]string{spacev1.LabelDomain: "leo-a", spacev1.LabelOrbitClass: "leo"}, Annotations: map[string]string{spacev1.AnnotationLinkProjection: mustJSON(t, projection)}}}
	return &Requirement{Mission: mission, Placement: placement}, node
}
func decodeProjection(t testing.TB, node *corev1.Node) NodeProjection {
	t.Helper()
	var value NodeProjection
	if err := json.Unmarshal([]byte(node.Annotations[spacev1.AnnotationLinkProjection]), &value); err != nil {
		t.Fatal(err)
	}
	return value
}
func mustJSON(t testing.TB, value interface{}) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}
