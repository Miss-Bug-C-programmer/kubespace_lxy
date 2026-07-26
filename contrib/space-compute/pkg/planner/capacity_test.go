package planner

import (
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
)

func TestIdenticalCapabilityRequirementsAggregateWithoutCapacityReuse(t *testing.T) {
	now := time.Date(2026, 7, 26, 0, 0, 0, 0, time.UTC)
	mission := capacityMission(now, []spacev1.CapabilityRequirement{
		{Class: "gpu", Quantity: 4, Architecture: "space-cuda", Precision: []string{"fp16"}},
		{Class: "gpu", Quantity: 4, Architecture: "space-cuda", Precision: []string{"fp16"}},
	}, nil)
	summary := capacitySummary(now, []spacev1.DeviceCapacity{{Class: "gpu", Count: 4, Architectures: []string{"space-cuda"}, Precision: []string{"fp16"}, ComputeMilli: 1000, FragmentationMilli: 500}})

	decision, err := Plan(mission, []*spacev1.SpaceDomainResourceSummary{summary}, nil, testClock{now})
	if err == nil || len(decision.Rejected) != 1 || len(decision.Rejected[0].Explanations) == 0 || !strings.Contains(decision.Rejected[0].Explanations[0].Observed, "requires 8") {
		t.Fatalf("duplicated requirement reused capacity: decision=%+v err=%v", decision, err)
	}
}

func TestDifferentModelBucketsAreAccountedSeparately(t *testing.T) {
	now := time.Date(2026, 7, 26, 0, 0, 0, 0, time.UTC)
	mission := capacityMission(now, []spacev1.CapabilityRequirement{
		{Class: "gpu", Quantity: 2, Model: "model-a"},
		{Class: "gpu", Quantity: 3, Model: "model-b"},
	}, nil)
	summary := capacitySummary(now, []spacev1.DeviceCapacity{
		{Class: "gpu", Count: 2, Models: []string{"model-a"}, ComputeMilli: 1200, FragmentationMilli: 300},
		{Class: "gpu", Count: 3, Models: []string{"model-b"}, ComputeMilli: 900, FragmentationMilli: 700},
	})
	if _, err := Plan(mission, []*spacev1.SpaceDomainResourceSummary{summary}, nil, testClock{now}); err != nil {
		t.Fatalf("separate model buckets rejected: %v", err)
	}

	insufficient := summary.DeepCopy()
	insufficient.Spec.Devices[1].Count = 2
	if decision, err := Plan(mission, []*spacev1.SpaceDomainResourceSummary{insufficient}, nil, testClock{now}); err == nil || len(decision.Rejected) != 1 || len(decision.Rejected[0].Explanations) == 0 || !strings.Contains(decision.Rejected[0].Explanations[0].Observed, "model=model-b") {
		t.Fatalf("model-b shortage was hidden by model-a capacity: decision=%+v err=%v", decision, err)
	}
}

func TestOverlappingConstraintsUseDeterministicMaximumMatching(t *testing.T) {
	now := time.Date(2026, 7, 26, 0, 0, 0, 0, time.UTC)
	mission := capacityMission(now,
		[]spacev1.CapabilityRequirement{{Class: "gpu", Quantity: 1}},
		[]spacev1.CapabilitySet{{Name: "specific", AllOf: []spacev1.CapabilityRequirement{{Class: "gpu", Quantity: 1, Model: "model-a"}}}},
	)
	summary := capacitySummary(now, []spacev1.DeviceCapacity{
		{Class: "gpu", Count: 1, Models: []string{"model-a"}, ComputeMilli: 1000, FragmentationMilli: 400},
		{Class: "gpu", Count: 1, Models: []string{"model-b"}, ComputeMilli: 1000, FragmentationMilli: 600},
	})
	decision, err := Plan(mission, []*spacev1.SpaceDomainResourceSummary{summary}, nil, testClock{now})
	if err != nil {
		t.Fatalf("bounded max-flow failed to reroute overlapping constraints: %v", err)
	}
	allocations := 0
	for _, explanation := range decision.Placement.Spec.Explanations {
		if explanation.Code == "capability_allocation" {
			allocations++
		}
	}
	if allocations != 2 {
		t.Fatalf("allocation explanation count=%d, want 2: %+v", allocations, decision.Placement.Spec.Explanations)
	}
}

func TestRequiredAndAlternativeCannotReuseSameCapacityBucket(t *testing.T) {
	now := time.Date(2026, 7, 26, 0, 0, 0, 0, time.UTC)
	mission := capacityMission(now,
		[]spacev1.CapabilityRequirement{{Class: "gpu", Quantity: 4, Model: "model-a"}},
		[]spacev1.CapabilitySet{{Name: "extra", AllOf: []spacev1.CapabilityRequirement{{Class: "gpu", Quantity: 1, Model: "model-a"}}}},
	)
	summary := capacitySummary(now, []spacev1.DeviceCapacity{{Class: "gpu", Count: 4, Models: []string{"model-a"}, ComputeMilli: 1000, FragmentationMilli: 500}})

	decision, err := Plan(mission, []*spacev1.SpaceDomainResourceSummary{summary}, nil, testClock{now})
	if err == nil || len(decision.Rejected) != 1 || len(decision.Rejected[0].Explanations) == 0 || !strings.Contains(decision.Rejected[0].Explanations[0].Observed, "requires 5") {
		t.Fatalf("required and alternative reused one capacity bucket: decision=%+v err=%v", decision, err)
	}
}

func TestCapabilityAndCapacityInputOrderDoesNotChangeDecision(t *testing.T) {
	now := time.Date(2026, 7, 26, 0, 0, 0, 0, time.UTC)
	requirements := []spacev1.CapabilityRequirement{
		{Class: "gpu", Quantity: 1, Model: "model-b", Precision: []string{"fp16", "fp32"}},
		{Class: "gpu", Quantity: 1, Model: "model-a", Precision: []string{"fp16"}},
	}
	devices := []spacev1.DeviceCapacity{
		{Class: "gpu", Count: 1, Models: []string{"model-b"}, Precision: []string{"fp32", "fp16"}, ComputeMilli: 1800, FragmentationMilli: 300},
		{Class: "gpu", Count: 1, Models: []string{"model-a"}, Precision: []string{"fp16"}, ComputeMilli: 1200, FragmentationMilli: 700},
	}
	first, err := Plan(capacityMission(now, requirements, nil), []*spacev1.SpaceDomainResourceSummary{capacitySummary(now, devices)}, nil, testClock{now})
	if err != nil {
		t.Fatal(err)
	}
	reversedRequirements := append([]spacev1.CapabilityRequirement(nil), requirements...)
	reversedRequirements[0], reversedRequirements[1] = reversedRequirements[1], reversedRequirements[0]
	reversedDevices := append([]spacev1.DeviceCapacity(nil), devices...)
	reversedDevices[0], reversedDevices[1] = reversedDevices[1], reversedDevices[0]
	second, err := Plan(capacityMission(now, reversedRequirements, nil), []*spacev1.SpaceDomainResourceSummary{capacitySummary(now, reversedDevices)}, nil, testClock{now})
	if err != nil {
		t.Fatal(err)
	}
	if first.Placement.Spec.PlanID != second.Placement.Spec.PlanID || first.Placement.Spec.Score != second.Placement.Spec.Score || !reflect.DeepEqual(first.Placement.Spec.Explanations, second.Placement.Spec.Explanations) {
		t.Fatalf("input order changed deterministic decision:\nfirst=%+v\nsecond=%+v", first.Placement.Spec, second.Placement.Spec)
	}
}

func TestAlternativeOnlyCapabilityDrivesComputeEstimate(t *testing.T) {
	now := time.Date(2026, 7, 26, 0, 0, 0, 0, time.UTC)
	mission := capacityMission(now, nil, []spacev1.CapabilitySet{{Name: "fast", AllOf: []spacev1.CapabilityRequirement{{Class: "gpu", Quantity: 1, Model: "model-fast"}}}})
	summary := capacitySummary(now, []spacev1.DeviceCapacity{{Class: "gpu", Count: 1, Models: []string{"model-fast"}, ComputeMilli: 2000, FragmentationMilli: 500}})
	decision, err := Plan(mission, []*spacev1.SpaceDomainResourceSummary{summary}, nil, testClock{now})
	if err != nil {
		t.Fatal(err)
	}
	duration := decision.Placement.Spec.ComputeEnd.Time.Sub(decision.Placement.Spec.ComputeStart.Time)
	if duration != 300*time.Second || duration == time.Duration(mission.Spec.MaximumDurationSeconds)*time.Second {
		t.Fatalf("alternative-only compute duration=%s, want 300s and not maximum fallback", duration)
	}
	found := false
	for _, explanation := range decision.Placement.Spec.Explanations {
		if explanation.Code == "capability_set_selected" && explanation.Observed == "fast" {
			found = true
		}
	}
	if !found {
		t.Fatalf("selected alternative set missing from explanations: %+v", decision.Placement.Spec.Explanations)
	}
}

func TestTransferArithmeticRejectsOverflowWithoutWraparound(t *testing.T) {
	now := time.Date(2026, 7, 26, 0, 0, 0, 0, time.UTC)
	source := spacev1.DomainReference{Name: "ground-a", ClusterID: "cluster-a", OrbitClass: spacev1.OrbitGround}
	target := spacev1.DomainReference{Name: "leo-a", ClusterID: "leo-cluster", OrbitClass: spacev1.OrbitLEO}
	link := plannerLink("overflow", source, target, now, now, now.Add(time.Hour), 1)
	mission := capacityMission(now, []spacev1.CapabilityRequirement{{Class: "gpu", Quantity: 1}}, nil)
	_, _, err := fitTransfer(link, math.MaxInt64, now, mission.Spec, now)
	if err == nil || !strings.Contains(err.Error(), "bytes to bits") {
		t.Fatalf("transfer overflow was not rejected: %v", err)
	}
}

func TestFullDomainIdentityPreventsSameNameCrossClusterConfusion(t *testing.T) {
	now := time.Date(2026, 7, 26, 0, 0, 0, 0, time.UTC)
	wanted := spacev1.DomainReference{Name: "ground-a", ClusterID: "cluster-a", OrbitClass: spacev1.OrbitGround}
	wrong := spacev1.DomainReference{Name: "ground-a", ClusterID: "cluster-b", OrbitClass: spacev1.OrbitGround}
	target := spacev1.DomainReference{Name: "leo-a", ClusterID: "leo-cluster", OrbitClass: spacev1.OrbitLEO}
	mission := capacityMission(now, []spacev1.CapabilityRequirement{{Class: "gpu", Quantity: 1}}, nil)
	mission.Spec.Inputs = []spacev1.DataObject{{ID: "frame", SizeBytes: 8, Locations: []spacev1.DataLocation{{Domain: wanted, URI: "s3://cluster-a/frame"}}, PayloadDigest: strings.Repeat("a", 64)}}
	summary := capacitySummary(now, []spacev1.DeviceCapacity{{Class: "gpu", Count: 1, ComputeMilli: 1000, FragmentationMilli: 500}})
	summary.Spec.Domain = target
	wrongLink := plannerLink("wrong-cluster", wrong, target, now, now, now.Add(20*time.Minute), 1_000_000)
	wantedLink := plannerLink("wanted-cluster", wanted, target, now, now.Add(time.Minute), now.Add(20*time.Minute), 1_000_000)
	decision, err := Plan(mission, []*spacev1.SpaceDomainResourceSummary{summary}, []*spacev1.SpaceLinkSnapshot{wrongLink, wantedLink}, testClock{now})
	if err != nil {
		t.Fatal(err)
	}
	if len(decision.Placement.Spec.InputTransfers) != 1 || decision.Placement.Spec.InputTransfers[0].Source != wanted || decision.Placement.Spec.InputTransfers[0].SourceURI != "s3://cluster-a/frame" {
		t.Fatalf("planner confused same-name domains: %+v", decision.Placement.Spec.InputTransfers)
	}
}

func TestCheckedPlannerArithmeticRejectsBoundaryOverflow(t *testing.T) {
	if _, err := checkedAddInt64(math.MaxInt64, 1); err == nil {
		t.Fatal("checked addition wrapped")
	}
	if _, err := checkedMulInt64(math.MaxInt64, 2); err == nil {
		t.Fatal("checked multiplication wrapped")
	}
	if _, err := checkedDivInt64(math.MinInt64, -1); err == nil {
		t.Fatal("checked division overflow was accepted")
	}
	if _, err := checkedSecondsDuration(math.MaxInt64); err == nil {
		t.Fatal("seconds-to-duration overflow was accepted")
	}
	nearLimit := time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)
	if _, err := checkedTimeAdd(nearLimit, time.Second); err == nil {
		t.Fatal("timestamp outside RFC3339 range was accepted")
	}
}

func TestPlannerTopologyLimitRejectsOversizedInput(t *testing.T) {
	now := time.Date(2026, 7, 26, 0, 0, 0, 0, time.UTC)
	mission := capacityMission(now, []spacev1.CapabilityRequirement{{Class: "gpu", Quantity: 1}}, nil)
	summaries := make([]*spacev1.SpaceDomainResourceSummary, spacev1.MaxPlannerTopologyEntries+1)
	if _, err := Plan(mission, summaries, nil, testClock{now}); err == nil || !strings.Contains(err.Error(), "topology") {
		t.Fatalf("oversized topology was not rejected: %v", err)
	}
}

func capacityMission(now time.Time, required []spacev1.CapabilityRequirement, alternatives []spacev1.CapabilitySet) *spacev1.SpaceMission {
	return &spacev1.SpaceMission{
		ObjectMeta: metav1.ObjectMeta{Name: "capacity-mission", Namespace: "missions", UID: types.UID("capacity-mission-uid"), Generation: 1},
		Spec: spacev1.SpaceMissionSpec{
			MissionClass: "capacity", Priority: 500, StatePolicy: spacev1.PolicyStrict,
			RequiredCapabilities: required, AlternativeCapabilities: alternatives,
			Deadline: metav1.NewTime(now.Add(2 * time.Hour)), ExpectedDurationSeconds: 60, MaximumDurationSeconds: 600, DurationUncertaintySecs: 30,
			SafetyMarginSeconds: 0, MaximumClockSkewSeconds: 0, ResultReturnRequired: false,
			Retry: spacev1.RetryPolicy{MaxAttempts: 2, MaxConcurrentExecutions: 1}, Checkpoint: spacev1.CheckpointPolicy{},
			WorkloadTemplate: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "worker", Image: "example.invalid/worker:v1"}}}},
		},
	}
}

func capacitySummary(now time.Time, devices []spacev1.DeviceCapacity) *spacev1.SpaceDomainResourceSummary {
	return &spacev1.SpaceDomainResourceSummary{
		ObjectMeta: metav1.ObjectMeta{Name: "compute-a"},
		Spec: spacev1.SpaceDomainResourceSummarySpec{
			Domain:     spacev1.DomainReference{Name: "compute-a", ClusterID: "compute-cluster", OrbitClass: spacev1.OrbitLEO},
			ObservedAt: metav1.NewTime(now), ValidUntil: metav1.NewTime(now.Add(time.Hour)),
			Provenance: spacev1.Provenance{ReporterID: "reporter", Source: "exporter", Digest: strings.Repeat("a", 64), Sequence: 1},
			Devices:    devices, QueueDelaySeconds: 0, EnergyHeadroomMilli: 800, ThermalHeadroomMilli: 800, ResilienceMilli: 800,
			MinimumEnergyMilli: 200, MinimumThermalMilli: 200, MaximumSnapshotAgeSecs: 60, ExporterSnapshotDigest: strings.Repeat("b", 64),
		},
	}
}

func plannerLink(name string, source, destination spacev1.DomainReference, now, start, end time.Time, bandwidth int64) *spacev1.SpaceLinkSnapshot {
	return &spacev1.SpaceLinkSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: spacev1.SpaceLinkSnapshotSpec{
			Source: source, Destination: destination, ObservedAt: metav1.NewTime(now), ValidUntil: metav1.NewTime(now.Add(time.Hour)),
			MaximumClockSkewSeconds: 0, MinimumUpdateSeconds: 1, HistoryLimit: 8,
			Provenance: spacev1.Provenance{ReporterID: "reporter", Source: "contact", Digest: strings.Repeat("c", 64), Sequence: 1},
			Windows:    []spacev1.ContactWindow{{ID: "window-" + name, Start: metav1.NewTime(start), End: metav1.NewTime(end), BandwidthBitsPerSec: bandwidth, StabilityMilli: 900, ConfidenceMilli: 900}},
		},
	}
}
