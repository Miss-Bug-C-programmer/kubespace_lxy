package v1alpha1

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPhase9MissionHardConstraintValidation(t *testing.T) {
	now := time.Date(2026, 7, 27, 0, 0, 0, 0, time.UTC)
	mission := validMission(now)
	mission.Spec.WorkingMemoryBytes = 8 << 30
	mission.Spec.WorkingStorageBytes = 64 << 30
	mission.Spec.MinimumBandwidthBitsPerSecond = 100_000_000
	mission.Spec.MaximumRTTMicroseconds = 50_000
	mission.Spec.MaximumLossPartsPerMillion = 1000
	if err := ValidateMission(mission, fakeClock{now}); err != nil {
		t.Fatalf("valid Phase 9 mission: %v", err)
	}
	cases := []struct {
		name   string
		mutate func(*SpaceMission)
		want   string
	}{
		{"memory", func(v *SpaceMission) { v.Spec.WorkingMemoryBytes = MaxCapacityBytes + 1 }, "workingMemoryBytes"},
		{"storage", func(v *SpaceMission) { v.Spec.WorkingStorageBytes = MaxCapacityBytes + 1 }, "workingStorageBytes"},
		{"bandwidth", func(v *SpaceMission) { v.Spec.MinimumBandwidthBitsPerSecond = MaxBandwidthBitsPerSecond + 1 }, "minimumBandwidthBitsPerSecond"},
		{"rtt", func(v *SpaceMission) { v.Spec.MaximumRTTMicroseconds = MaxRTTMicroseconds + 1 }, "maximumRTTMicroseconds"},
		{"loss", func(v *SpaceMission) { v.Spec.MaximumLossPartsPerMillion = 1_000_001 }, "maximumLossPartsPerMillion"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bad := mission.DeepCopy()
			tc.mutate(bad)
			if err := ValidateMission(bad, fakeClock{now}); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error=%v want %q", err, tc.want)
			}
		})
	}
}

func TestPhase9ResourceSummaryCanonicalCapacityValidation(t *testing.T) {
	now := time.Date(2026, 7, 27, 0, 0, 0, 0, time.UTC)
	summary := &SpaceDomainResourceSummary{ObjectMeta: metav1.ObjectMeta{Name: "leo-a"}, Spec: SpaceDomainResourceSummarySpec{
		Domain:     DomainReference{Name: "leo-a", ClusterID: "leo-cluster", OrbitClass: OrbitLEO},
		ObservedAt: metav1.NewTime(now), ValidUntil: metav1.NewTime(now.Add(time.Hour)),
		Provenance:        Provenance{ReporterID: "reporter", Source: "agent", Digest: strings.Repeat("a", 64), Sequence: 1},
		Devices:           []DeviceCapacity{{Class: "gpu", Count: 2, ComputeMilli: 1000, FragmentationMilli: 500}},
		QueueDelaySeconds: 1, EnergyHeadroomMilli: 800, ThermalHeadroomMilli: 800, ResilienceMilli: 900,
		MaximumSnapshotAgeSecs: 60, ExporterSnapshotDigest: strings.Repeat("b", 64),
		CPU:                        ScalarCapacity{Capacity: 16000, Available: 8000},
		SystemMemoryBytes:          ScalarCapacity{Capacity: 64 << 30, Available: 32 << 30},
		EphemeralStorageBytes:      ScalarCapacity{Capacity: 1 << 40, Available: 1 << 39},
		PersistentStorage:          []StorageCapacity{{Class: "nvme", CapacityBytes: 2 << 40, AvailableBytes: 1 << 40}},
		NUMATopology:               []NUMAResource{{ID: 0, CPUMilliCapacity: 8000, CPUMilliAvailable: 4000, MemoryCapacityBytes: 32 << 30, MemoryAvailableBytes: 16 << 30}},
		Trust:                      TrustAttestationState{State: "verified", Provider: "tpm", EvidenceDigest: strings.Repeat("c", 64), ObservedAt: metav1.NewTime(now), ValidUntil: metav1.NewTime(now.Add(time.Hour))},
		AutonomyDurationSeconds:    3600,
		Energy:                     EnergyBudget{Source: "solar+battery", CapacityMilliWattHours: 1_000_000, AvailableMilliWattHours: 700_000},
		PhysicalDeviceInventoryRef: &PhysicalDeviceInventoryReference{Name: "leo-a-node-a", Digest: strings.Repeat("d", 64), ResourceVersion: "123"},
	}}
	if err := ValidateResourceSummary(summary, fakeClock{now}); err != nil {
		t.Fatalf("valid canonical summary: %v", err)
	}
	bad := summary.DeepCopy()
	bad.Spec.SystemMemoryBytes.Available = bad.Spec.SystemMemoryBytes.Capacity + 1
	if err := ValidateResourceSummary(bad, fakeClock{now}); err == nil || !strings.Contains(err.Error(), "systemMemoryBytes") {
		t.Fatalf("invalid available memory accepted: %v", err)
	}
	bad = summary.DeepCopy()
	bad.Spec.Energy.AvailableMilliWattHours = bad.Spec.Energy.CapacityMilliWattHours + 1
	if err := ValidateResourceSummary(bad, fakeClock{now}); err == nil || !strings.Contains(err.Error(), "energy") {
		t.Fatalf("invalid energy budget accepted: %v", err)
	}
}

func TestPhase9PhysicalDeviceInventoryValidationAndCanonicalDigest(t *testing.T) {
	now := time.Date(2026, 7, 27, 0, 0, 0, 0, time.UTC)
	domain := DomainReference{Name: "leo-a", ClusterID: "leo-cluster", OrbitClass: OrbitLEO}
	inventory := &PhysicalDeviceInventory{ObjectMeta: metav1.ObjectMeta{Name: PhysicalDeviceInventoryName(domain, "node-a")}, Spec: PhysicalDeviceInventorySpec{
		Domain: domain, NodeName: "node-a", ObservedAt: metav1.NewTime(now), ValidUntil: metav1.NewTime(now.Add(time.Hour)), ConfidenceMilli: 950,
		Provenance: Provenance{ReporterID: "reporter", Source: "device-agent", Digest: strings.Repeat("a", 64), Sequence: 1},
		Devices: []PhysicalDevice{
			{StableDeviceID: "gpu-0001", KubernetesResourceName: "nvidia.com/gpu", AllocationID: "dra/claim/device-1", DRAAllocationID: "resource.k8s.io/claim/device-1", VendorAllocationID: "GPU-0001", Class: "gpu", Vendor: "nvidia", Model: "h100", Architecture: "sm90", Topology: DeviceTopology{NUMANode: 0, SocketID: "socket-0", PCIAddress: "0000:01:00.0"}, PeerInterconnects: []DevicePeerInterconnect{{PeerStableDeviceID: "gpu-0002", Type: "nvlink", BandwidthBitsPerSecond: 900_000_000_000}}, TotalMemoryBytes: 80 << 30, FreeMemoryBytes: 60 << 30, MemoryBandwidthBitsPerSecond: 3_000_000_000_000, InterconnectBandwidthBitsPerSecond: 900_000_000_000, SupportedPrecision: []string{"fp16", "fp8"}, Firmware: "1.2", Driver: "580", Runtime: "cuda-13", Libraries: map[string]string{"runtime.example/cudnn": "9"}, Health: "healthy", TemperatureMilliCelsius: 55000, PowerMilliwatts: 250000, ConfidenceMilli: 940},
			{StableDeviceID: "gpu-0002", KubernetesResourceName: "nvidia.com/gpu", VendorAllocationID: "GPU-0002", Class: "gpu", Vendor: "nvidia", Model: "h100", Architecture: "sm90", Topology: DeviceTopology{NUMANode: 0, SocketID: "socket-0", PCIAddress: "0000:02:00.0"}, PeerInterconnects: []DevicePeerInterconnect{{PeerStableDeviceID: "gpu-0001", Type: "nvlink", BandwidthBitsPerSecond: 900_000_000_000}}, TotalMemoryBytes: 80 << 30, FreeMemoryBytes: 70 << 30, MemoryBandwidthBitsPerSecond: 3_000_000_000_000, InterconnectBandwidthBitsPerSecond: 900_000_000_000, SupportedPrecision: []string{"fp8", "fp16"}, Firmware: "1.2", Driver: "580", Runtime: "cuda-13", Health: "healthy", TemperatureMilliCelsius: 52000, PowerMilliwatts: 240000, ConfidenceMilli: 945},
		},
	}}
	if err := ValidatePhysicalDeviceInventory(inventory, fakeClock{now}); err != nil {
		t.Fatalf("valid inventory: %v", err)
	}
	first, err := ReporterDigest(inventory)
	if err != nil {
		t.Fatal(err)
	}
	reordered := inventory.DeepCopy()
	reordered.Spec.Devices[0].SupportedPrecision = []string{"fp8", "fp16"}
	second, err := ReporterDigest(reordered)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("canonical inventory digest changed with unordered precision: %s != %s", first, second)
	}
	bad := inventory.DeepCopy()
	bad.Spec.Devices[0].FreeMemoryBytes = bad.Spec.Devices[0].TotalMemoryBytes + 1
	if err := ValidatePhysicalDeviceInventory(bad, fakeClock{now}); err == nil || !strings.Contains(err.Error(), "freeMemoryBytes") {
		t.Fatalf("invalid inventory memory accepted: %v", err)
	}
}

func FuzzPhysicalDeviceInventoryValidation(f *testing.F) {
	f.Add("gpu-a", int64(1024), int64(512), int64(900))
	f.Add("npu-a", int64(1), int64(0), int64(0))
	f.Fuzz(func(t *testing.T, stableID string, total, free int64, confidence int64) {
		now := time.Date(2026, 7, 27, 0, 0, 0, 0, time.UTC)
		if confidence < -10000 || confidence > 10000 {
			return
		}
		domain := DomainReference{Name: "leo-a", ClusterID: "leo-cluster", OrbitClass: OrbitLEO}
		inventory := &PhysicalDeviceInventory{ObjectMeta: metav1.ObjectMeta{Name: PhysicalDeviceInventoryName(domain, "node-a")}, Spec: PhysicalDeviceInventorySpec{
			Domain: domain, NodeName: "node-a", ObservedAt: metav1.NewTime(now), ValidUntil: metav1.NewTime(now.Add(time.Hour)), ConfidenceMilli: 900,
			Provenance: Provenance{ReporterID: "reporter", Source: "device-agent", Digest: strings.Repeat("a", 64), Sequence: 1},
			Devices:    []PhysicalDevice{{StableDeviceID: stableID, KubernetesResourceName: "example.com/device", Class: "gpu", Vendor: "vendor", Model: "model", Architecture: "arch", TotalMemoryBytes: total, FreeMemoryBytes: free, Health: "healthy", ConfidenceMilli: int32(confidence)}},
		}}
		_ = ValidatePhysicalDeviceInventory(inventory, fakeClock{now})
	})
}
