package planner

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
)

func TestPhase9WorkingMemoryAndStorageAreHardAcrossAllStatePolicies(t *testing.T) {
	now := time.Date(2026, 7, 27, 0, 0, 0, 0, time.UTC)
	for _, policy := range []spacev1.StatePolicy{spacev1.PolicyStrict, spacev1.PolicyDegraded, spacev1.PolicyBestEffort} {
		t.Run(string(policy)+"-memory", func(t *testing.T) {
			mission := phase9CapacityMission(now)
			mission.Spec.StatePolicy = policy
			mission.Spec.WorkingMemoryBytes = 8 << 30
			summary := phase9CapacitySummary(now)
			summary.Spec.SystemMemoryBytes = spacev1.ScalarCapacity{Capacity: 16 << 30, Available: 4 << 30}
			decision, err := Plan(mission, []*spacev1.SpaceDomainResourceSummary{summary}, nil, testClock{now})
			assertPhase9HardRejection(t, decision, err, "working_memory_insufficient", policy, "working-memory/storage/link limits")
		})
		t.Run(string(policy)+"-storage", func(t *testing.T) {
			mission := phase9CapacityMission(now)
			mission.Spec.StatePolicy = policy
			mission.Spec.WorkingStorageBytes = 20 << 30
			summary := phase9CapacitySummary(now)
			summary.Spec.EphemeralStorageBytes = spacev1.ScalarCapacity{Capacity: 10 << 30, Available: 4 << 30}
			summary.Spec.PersistentStorage = []spacev1.StorageCapacity{{Class: "nvme", CapacityBytes: 20 << 30, AvailableBytes: 8 << 30}}
			decision, err := Plan(mission, []*spacev1.SpaceDomainResourceSummary{summary}, nil, testClock{now})
			assertPhase9HardRejection(t, decision, err, "working_storage_insufficient", policy, "working-memory/storage/link limits")
		})
	}
}

func TestPhase9LinkQualityLimitsAreHardAcrossAllStatePolicies(t *testing.T) {
	now := time.Date(2026, 7, 27, 0, 0, 0, 0, time.UTC)
	source := spacev1.DomainReference{Name: "ground-a", ClusterID: "ground-cluster", OrbitClass: spacev1.OrbitGround}
	target := spacev1.DomainReference{Name: "leo-a", ClusterID: "compute-cluster", OrbitClass: spacev1.OrbitLEO}
	for _, policy := range []spacev1.StatePolicy{spacev1.PolicyStrict, spacev1.PolicyDegraded, spacev1.PolicyBestEffort} {
		mission := phase9CapacityMission(now)
		mission.Spec.StatePolicy = policy
		mission.Spec.Inputs = []spacev1.DataObject{{ID: "frame", SizeBytes: 1024, Locations: []spacev1.DataLocation{{Domain: source}}}}
		mission.Spec.MinimumBandwidthBitsPerSecond = 2_000_000
		mission.Spec.MaximumRTTMicroseconds = 20_000
		mission.Spec.MaximumLossPartsPerMillion = 100
		summary := phase9CapacitySummary(now)
		summary.Spec.Domain = target
		link := plannerLink("quality", source, target, now, now, now.Add(10*time.Minute), 1_000_000)
		link.Spec.Windows[0].RTTMicroseconds = 30_000
		link.Spec.Windows[0].LossPartsPerMillion = 200
		decision, err := Plan(mission, []*spacev1.SpaceDomainResourceSummary{summary}, []*spacev1.SpaceLinkSnapshot{link}, testClock{now})
		assertPhase9HardRejection(t, decision, err, "input_transfer_window_missing", policy, "bandwidth/RTT/loss hard constraints")
	}
}

func TestPhase9PlacementRecordsSelectedCapabilityAndPhysicalConstraints(t *testing.T) {
	now := time.Date(2026, 7, 27, 0, 0, 0, 0, time.UTC)
	mission := capacityMission(now, nil, []spacev1.CapabilitySet{{Name: "accelerated", AllOf: []spacev1.CapabilityRequirement{{Class: "gpu", Quantity: 2, Architecture: "sm90", Model: "h100", Precision: []string{"fp8"}}}}})
	summary := capacitySummary(now, []spacev1.DeviceCapacity{{Class: "gpu", Count: 2, Architectures: []string{"sm90"}, Models: []string{"h100"}, Precision: []string{"fp8"}, ComputeMilli: 2000, FragmentationMilli: 500}})
	decision, err := Plan(mission, []*spacev1.SpaceDomainResourceSummary{summary}, nil, testClock{now})
	if err != nil {
		t.Fatal(err)
	}
	placement := decision.Placement
	if placement.APIVersion != spacev1.CanonicalAPIVersion {
		t.Fatalf("placement apiVersion=%q want %q", placement.APIVersion, spacev1.CanonicalAPIVersion)
	}
	if placement.Spec.SelectedCapabilitySetName != "accelerated" || len(placement.Spec.SelectedCapabilities) != 1 || len(placement.Spec.SelectedPhysicalDeviceConstraints) != 1 {
		t.Fatalf("selected capability audit fields=%+v", placement.Spec)
	}
	constraint := placement.Spec.SelectedPhysicalDeviceConstraints[0]
	if constraint.Class != "gpu" || constraint.Quantity != 2 || constraint.Architecture != "sm90" || constraint.Model != "h100" {
		t.Fatalf("physical constraint=%+v", constraint)
	}
	foundPolicy := false
	for _, explanation := range placement.Spec.Explanations {
		if explanation.Code == "state_policy_hard_constraints" {
			foundPolicy = true
		}
	}
	if !foundPolicy {
		t.Fatalf("missing state policy semantics: %+v", placement.Spec.Explanations)
	}
}

func TestPhase9HardConstraintBoundaryCanSucceed(t *testing.T) {
	now := time.Date(2026, 7, 27, 0, 0, 0, 0, time.UTC)
	mission := phase9CapacityMission(now)
	mission.Spec.WorkingMemoryBytes = 4 << 30
	mission.Spec.WorkingStorageBytes = 8 << 30
	summary := phase9CapacitySummary(now)
	summary.Spec.SystemMemoryBytes = spacev1.ScalarCapacity{Capacity: 8 << 30, Available: 4 << 30}
	summary.Spec.EphemeralStorageBytes = spacev1.ScalarCapacity{Capacity: 16 << 30, Available: 8 << 30}
	decision, err := Plan(mission, []*spacev1.SpaceDomainResourceSummary{summary}, nil, testClock{now})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Placement.Spec.ComputeStart.Before(&metav1.Time{Time: now}) {
		t.Fatal("invalid compute start")
	}
}

func phase9CapacityMission(now time.Time) *spacev1.SpaceMission {
	return capacityMission(now, []spacev1.CapabilityRequirement{{Class: "gpu", Quantity: 1}}, nil)
}

func phase9CapacitySummary(now time.Time) *spacev1.SpaceDomainResourceSummary {
	return capacitySummary(now, []spacev1.DeviceCapacity{{Class: "gpu", Count: 1, ComputeMilli: 1000, FragmentationMilli: 500}})
}

func assertPhase9HardRejection(t *testing.T, decision Decision, err error, code string, policy spacev1.StatePolicy, messageFragment string) {
	t.Helper()
	if err == nil {
		t.Fatalf("policy=%s code=%s unexpectedly feasible", policy, code)
	}
	for _, rejected := range decision.Rejected {
		for _, explanation := range rejected.Explanations {
			if explanation.Code == code {
				if !strings.Contains(explanation.Message, "statePolicy="+string(policy)) || !strings.Contains(explanation.Message, messageFragment) {
					t.Fatalf("policy=%s code=%s explanation lacks hard-constraint semantics: %+v", policy, code, explanation)
				}
				return
			}
		}
	}
	t.Fatalf("policy=%s missing structured rejection code=%s: decision=%+v err=%v", policy, code, decision.Rejected, err)
}
