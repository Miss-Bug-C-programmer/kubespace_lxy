package v1alpha1

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type fakeClock struct{ now time.Time }

func (f fakeClock) Now() time.Time { return f.now }

func TestLinkValidationRejectsOverlapSkewStaleAndFastUnchangedUpdate(t *testing.T) {
	now := time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)
	clock := fakeClock{now}
	base := validLink(now)
	if err := ValidateLinkSnapshot(base, nil, clock); err != nil {
		t.Fatalf("valid link: %v", err)
	}

	overlap := base.DeepCopy()
	overlap.Spec.Windows = append(overlap.Spec.Windows, ContactWindow{ID: "overlap", Start: metav1.NewTime(now.Add(5 * time.Minute)), End: metav1.NewTime(now.Add(20 * time.Minute)), BandwidthBitsPerSec: 1_000_000, StabilityMilli: 900, ConfidenceMilli: 900})
	if err := ValidateLinkSnapshot(overlap, nil, clock); err == nil || !strings.Contains(err.Error(), "overlap") {
		t.Fatalf("overlap error = %v", err)
	}

	future := base.DeepCopy()
	future.Spec.ObservedAt = metav1.NewTime(now.Add(2 * time.Minute))
	future.Spec.ValidUntil = metav1.NewTime(now.Add(time.Hour))
	if err := ValidateLinkSnapshot(future, nil, clock); err == nil || !strings.Contains(err.Error(), "clock skew") {
		t.Fatalf("future error = %v", err)
	}

	stale := base.DeepCopy()
	stale.Spec.ObservedAt = metav1.NewTime(now.Add(-2 * time.Hour))
	stale.Spec.ValidUntil = metav1.NewTime(now.Add(-time.Minute))
	if err := ValidateLinkSnapshot(stale, nil, clock); err == nil || !strings.Contains(err.Error(), "stale") {
		t.Fatalf("stale error = %v", err)
	}

	fast := base.DeepCopy()
	fast.Generation = 2
	fast.Spec.Provenance.Sequence++
	fast.Spec.ObservedAt = metav1.NewTime(now.Add(time.Second))
	fast.Spec.ValidUntil = metav1.NewTime(now.Add(time.Hour + time.Second))
	if err := ValidateLinkSnapshot(fast, base, clock); err == nil || !strings.Contains(err.Error(), "minimumUpdateSeconds") {
		t.Fatalf("fast update error = %v", err)
	}
}

func FuzzLinkSnapshotValidation(f *testing.F) {
	now := time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)
	seed, _ := json.Marshal(validLink(now))
	f.Add(string(seed))
	f.Fuzz(func(t *testing.T, raw string) {
		var value SpaceLinkSnapshot
		if json.Unmarshal([]byte(raw), &value) == nil {
			_ = ValidateLinkSnapshot(&value, nil, fakeClock{now})
		}
	})
}

func FuzzMissionValidation(f *testing.F) {
	now := time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)
	seed, _ := json.Marshal(validMission(now))
	f.Add(string(seed))
	f.Fuzz(func(t *testing.T, raw string) {
		var value SpaceMission
		if json.Unmarshal([]byte(raw), &value) == nil {
			_ = ValidateMission(&value, fakeClock{now})
		}
	})
}

func FuzzResourceSummaryValidation(f *testing.F) {
	now := time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)
	seed := SpaceDomainResourceSummary{Spec: SpaceDomainResourceSummarySpec{
		Domain: DomainReference{Name: "leo-a", ClusterID: "leo-cluster", OrbitClass: OrbitLEO}, ObservedAt: metav1.NewTime(now), ValidUntil: metav1.NewTime(now.Add(time.Hour)),
		Provenance: Provenance{ReporterID: "reporter", Source: "exporter", Digest: strings.Repeat("a", 64), Sequence: 1}, Devices: []DeviceCapacity{{Class: "gpu", Count: 1, ComputeMilli: 1000}},
		EnergyHeadroomMilli: 800, ThermalHeadroomMilli: 800, ResilienceMilli: 800, MaximumSnapshotAgeSecs: 60, ExporterSnapshotDigest: strings.Repeat("b", 64),
	}}
	raw, _ := json.Marshal(seed)
	f.Add(string(raw))
	f.Fuzz(func(t *testing.T, raw string) {
		var value SpaceDomainResourceSummary
		if json.Unmarshal([]byte(raw), &value) == nil {
			_ = ValidateResourceSummary(&value, fakeClock{now})
		}
	})
}

func FuzzPlacementValidation(f *testing.F) {
	now := time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)
	mission := validMission(now)
	mission.UID = types.UID("mission-uid")
	seed := SpacePlacementIntent{Spec: SpacePlacementIntentSpec{
		MissionRef: corev1.ObjectReference{Name: mission.Name, Namespace: mission.Namespace, UID: mission.UID}, PlanID: "plan-123", Attempt: 1,
		Target: DomainReference{Name: "leo-a", ClusterID: "leo-cluster", OrbitClass: OrbitLEO}, NotBefore: metav1.NewTime(now.Add(time.Minute)),
		ExpiresAt: metav1.NewTime(now.Add(20 * time.Minute)), ComputeStart: metav1.NewTime(now.Add(2 * time.Minute)), ComputeEnd: metav1.NewTime(now.Add(4 * time.Minute)),
		MaterialInputDigest: strings.Repeat("c", 64), SnapshotSequences: map[string]int64{"resource/leo-a": 1},
	}}
	raw, _ := json.Marshal(seed)
	f.Add(string(raw))
	f.Fuzz(func(t *testing.T, raw string) {
		var value SpacePlacementIntent
		if json.Unmarshal([]byte(raw), &value) == nil {
			_ = ValidatePlacement(&value, mission)
		}
	})
}

func TestMissionValidationRejectsContradictions(t *testing.T) {
	now := time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)
	mission := validMission(now)
	if err := ValidateMission(mission, fakeClock{now}); err != nil {
		t.Fatalf("valid mission: %v", err)
	}
	mission.Spec.Retry.AllowMigration = true
	mission.Spec.Checkpoint.Checkpointable = false
	mission.Spec.ResultReturnRequired = true
	mission.Spec.ResultDestinations = nil
	mission.Spec.DurationUncertaintySecs = mission.Spec.MaximumDurationSeconds
	err := ValidateMission(mission, fakeClock{now})
	for _, fragment := range []string{"allowMigration", "resultDestinations", "durationUncertaintySeconds"} {
		if err == nil || !strings.Contains(err.Error(), fragment) {
			t.Fatalf("validation error %v does not contain %q", err, fragment)
		}
	}
}

func TestMissionAndResourceValidationBoundPayloadAndProvenance(t *testing.T) {
	now := time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)
	mission := validMission(now)
	mission.Spec.WorkloadTemplate.Annotations = map[string]string{"oversized": strings.Repeat("x", MaxWorkloadTemplateBytes)}
	if err := ValidateMission(mission, fakeClock{now}); err == nil || !strings.Contains(err.Error(), "serialized size") {
		t.Fatalf("oversized workload template error = %v", err)
	}

	summary := &SpaceDomainResourceSummary{Spec: SpaceDomainResourceSummarySpec{
		Domain: DomainReference{Name: "leo-a", ClusterID: "leo-cluster", OrbitClass: OrbitLEO}, ObservedAt: metav1.NewTime(now), ValidUntil: metav1.NewTime(now.Add(time.Hour)),
		Provenance: Provenance{ReporterID: "reporter", Source: "exporter\nforged", Digest: strings.Repeat("a", 64), Sequence: 1}, Devices: []DeviceCapacity{{Class: "gpu", Count: 1, ComputeMilli: 1000}},
		EnergyHeadroomMilli: 800, ThermalHeadroomMilli: 800, ResilienceMilli: 800, MaximumSnapshotAgeSecs: 60, ExporterSnapshotDigest: "not-a-digest",
	}}
	err := ValidateResourceSummary(summary, fakeClock{now})
	for _, fragment := range []string{"control separators", "exporterSnapshotDigest"} {
		if err == nil || !strings.Contains(err.Error(), fragment) {
			t.Fatalf("resource validation error %v does not contain %q", err, fragment)
		}
	}
}

func validLink(now time.Time) *SpaceLinkSnapshot {
	return &SpaceLinkSnapshot{ObjectMeta: metav1.ObjectMeta{Name: "ground-leo", Generation: 1}, Spec: SpaceLinkSnapshotSpec{
		Source: DomainReference{Name: "ground-a", ClusterID: "ground-cluster", OrbitClass: OrbitGround}, Destination: DomainReference{Name: "leo-a", ClusterID: "leo-cluster", OrbitClass: OrbitLEO},
		ObservedAt: metav1.NewTime(now), ValidUntil: metav1.NewTime(now.Add(time.Hour)), MaximumClockSkewSeconds: 5, MinimumUpdateSeconds: 10, HistoryLimit: 8,
		Provenance: Provenance{ReporterID: "ground-link-reporter", Source: "signed-contact-product", Digest: strings.Repeat("a", 64), Sequence: 1},
		Windows:    []ContactWindow{{ID: "contact-1", Start: metav1.NewTime(now), End: metav1.NewTime(now.Add(10 * time.Minute)), BandwidthBitsPerSec: 100_000_000, RTTMicroseconds: 20_000, LossPartsPerMillion: 100, ErrorPartsPerMillion: 10, StabilityMilli: 950, ConfidenceMilli: 900, Predicted: true}},
	}}
}

func validMission(now time.Time) *SpaceMission {
	return &SpaceMission{ObjectMeta: metav1.ObjectMeta{Name: "mission-a", Namespace: "missions"}, Spec: SpaceMissionSpec{MissionClass: "science", Priority: 500, StatePolicy: PolicyStrict,
		RequiredCapabilities: []CapabilityRequirement{{Class: "gpu", Quantity: 1}}, Inputs: []DataObject{{ID: "image-a", SizeBytes: 1000, Locations: []string{"ground-a"}}}, OutputSizeBytes: 100,
		Deadline: metav1.NewTime(now.Add(time.Hour)), ExpectedDurationSeconds: 60, MaximumDurationSeconds: 120, DurationUncertaintySecs: 30, SafetyMarginSeconds: 10, MaximumClockSkewSeconds: 5,
		Retry: RetryPolicy{MaxAttempts: 2, AllowMigration: true, MaxConcurrentExecutions: 1}, Checkpoint: CheckpointPolicy{Checkpointable: true, MinimumIntervalSecs: 30, MaximumStateBytes: 1024},
		WorkloadTemplate: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "processor", Image: "example.invalid/processor:v1"}}}}}}
}
