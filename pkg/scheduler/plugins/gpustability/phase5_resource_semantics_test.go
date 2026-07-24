package gpustability

import (
	"context"
	"fmt"
	"strings"
	"testing"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1beta1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

const (
	testDRADriver = "dra.iluvatar.example.com"
	testDRAPool   = "gpu-node-pool"
)

type staticAllocationIdentitySource struct {
	claims map[string]*resourceapi.ResourceClaim
	slices []*resourceapi.ResourceSlice
}

func (s *staticAllocationIdentitySource) GetResourceClaim(namespace, name string) (*resourceapi.ResourceClaim, error) {
	if claim := s.claims[namespace+"/"+name]; claim != nil {
		return claim, nil
	}
	return nil, fmt.Errorf("resourceclaim %s/%s not found", namespace, name)
}

func (s *staticAllocationIdentitySource) ListResourceSlices() ([]*resourceapi.ResourceSlice, error) {
	return append([]*resourceapi.ResourceSlice(nil), s.slices...), nil
}

func TestOverlappingResourceNamesCannotReusePhysicalDevice(t *testing.T) {
	plugin := newTestPlugin(t, iluvatarMetrics)
	resourceA := v1.ResourceName("vendor.example.com/gpu-a")
	resourceB := v1.ResourceName("vendor.example.com/gpu-b")
	mapping := resourceMapping{Class: DeviceClassGPU, Profiles: map[string]struct{}{"iluvatar": {}}, AllocationMode: allocationModeExclusive}
	plugin.config.ResourceMappings[resourceA] = mapping
	plugin.config.ResourceMappings[resourceB] = mapping
	plugin.collector.config.ResourceMappings[resourceA] = mapping
	plugin.collector.config.ResourceMappings[resourceB] = mapping

	one := resource.MustParse("1")
	pod := &v1.Pod{Spec: v1.PodSpec{Containers: []v1.Container{{Name: "workload", Resources: v1.ResourceRequirements{
		Requests: v1.ResourceList{resourceA: one, resourceB: one},
		Limits:   v1.ResourceList{resourceA: one, resourceB: one},
	}}}}}
	node := nodeInfoWithNamedEndpointAndCapacity("gpu-node", "http://gpu-node:32021/metrics", string(resourceA), 1)
	node.Node().Status.Allocatable[resourceB] = one
	warmNode(t, plugin, node)

	status := plugin.Filter(context.Background(), cycleStateForPod(t, plugin, pod), pod, node)
	if status.IsSuccess() || !strings.Contains(status.Message(), "may overlap") {
		t.Fatalf("Filter status=%v, want fail-closed overlapping inventory rejection", status)
	}
}

func TestDisjointSameClassInventorySelectorsDoNotMixDevices(t *testing.T) {
	plugin := newTestPlugin(t, readPhase2Fixture(t, "iluvatar-2gpu.prom"))
	resourceA := v1.ResourceName("vendor.example.com/gpu-a")
	resourceB := v1.ResourceName("vendor.example.com/gpu-b")
	selectorA, err := parseInventorySelector("uuid-prefix=GPU-node-a-0000")
	if err != nil {
		t.Fatal(err)
	}
	selectorB, err := parseInventorySelector("uuid-prefix=GPU-node-a-0001")
	if err != nil {
		t.Fatal(err)
	}
	mappingA := resourceMapping{Class: DeviceClassGPU, Profiles: map[string]struct{}{"iluvatar": {}}, Selector: selectorA, AllocationMode: allocationModeExclusive}
	mappingB := resourceMapping{Class: DeviceClassGPU, Profiles: map[string]struct{}{"iluvatar": {}}, Selector: selectorB, AllocationMode: allocationModeExclusive}
	plugin.config.ResourceMappings[resourceA] = mappingA
	plugin.config.ResourceMappings[resourceB] = mappingB
	plugin.collector.config.ResourceMappings[resourceA] = mappingA
	plugin.collector.config.ResourceMappings[resourceB] = mappingB

	one := resource.MustParse("1")
	pod := &v1.Pod{Spec: v1.PodSpec{Containers: []v1.Container{{Name: "workload", Resources: v1.ResourceRequirements{
		Requests: v1.ResourceList{resourceA: one, resourceB: one},
		Limits:   v1.ResourceList{resourceA: one, resourceB: one},
	}}}}}
	node := nodeInfoWithNamedEndpointAndCapacity("gpu-node", "http://gpu-node:32021/metrics", string(resourceA), 1)
	node.Node().Status.Allocatable[resourceB] = one
	warmNode(t, plugin, node)
	if status := plugin.Filter(context.Background(), cycleStateForPod(t, plugin, pod), pod, node); !status.IsSuccess() {
		t.Fatalf("Filter status=%v, want disjoint partitions to pass: %s", status, status.Message())
	}
}

func TestDRAAllocatedDeviceMatchesExporterStableID(t *testing.T) {
	plugin := newTestPlugin(t, readPhase2Fixture(t, "iluvatar-2gpu.prom"))
	plugin.config.MaxTemperatureC = 70
	configureDRALinkedMapping(t, plugin, "iluvatar.com/gpu", testDRADriver, testDRAPool)
	claimName := "gpu-claim"
	pod := gpuPod()
	pod.Namespace = "default"
	pod.Spec.ResourceClaims = []v1.PodResourceClaim{{Name: "accelerator", ResourceClaimName: &claimName}}
	plugin.allocationSource = &staticAllocationIdentitySource{
		claims: map[string]*resourceapi.ResourceClaim{"default/" + claimName: allocatedClaim("default", claimName, testDRADriver, testDRAPool, "gpu-node-a-0000")},
		slices: []*resourceapi.ResourceSlice{currentSlice(testDRADriver, testDRAPool, 2, 1, "gpu-node", "gpu-node-a-0000", "gpu-node-a-0001")},
	}
	node := nodeInfoWithEndpointAndGPUCapacity("http://gpu-node:32021/metrics", 2)
	warmNode(t, plugin, node)
	state := cycleStateForPod(t, plugin, pod)
	requirement, status := workloadFromState(state)
	if !status.IsSuccess() || len(requirement.PhysicalAllocations["iluvatar.com/gpu"]) != 1 {
		t.Fatalf("physical allocations=%+v status=%v", requirement.PhysicalAllocations, status)
	}
	if status := plugin.Filter(context.Background(), state, pod, node); !status.IsSuccess() {
		t.Fatalf("DRA-selected cool device should pass even though an unselected device is hot: %v", status)
	}
}

func TestInvalidDRAAllocationIdentitiesFailClosed(t *testing.T) {
	tests := []struct {
		name       string
		claim      *resourceapi.ResourceClaim
		slices     []*resourceapi.ResourceSlice
		profiles   map[string]struct{}
		wantReason string
	}{
		{
			name:       "missing-device",
			claim:      allocatedClaim("default", "gpu-claim", testDRADriver, testDRAPool, "gpu-node-a-9999"),
			slices:     []*resourceapi.ResourceSlice{currentSlice(testDRADriver, testDRAPool, 2, 1, "gpu-node", "gpu-node-a-0000")},
			wantReason: "does not exist",
		},
		{
			name:  "duplicate-device",
			claim: allocatedClaim("default", "gpu-claim", testDRADriver, testDRAPool, "gpu-node-a-0000", "gpu-node-a-0000"),
			slices: []*resourceapi.ResourceSlice{
				currentSlice(testDRADriver, testDRAPool, 2, 1, "gpu-node", "gpu-node-a-0000"),
			},
			wantReason: "duplicated",
		},
		{
			name:  "stale-device",
			claim: allocatedClaim("default", "gpu-claim", testDRADriver, testDRAPool, "gpu-node-a-0000"),
			slices: []*resourceapi.ResourceSlice{
				currentSlice(testDRADriver, testDRAPool, 1, 1, "gpu-node", "gpu-node-a-0000"),
				currentSlice(testDRADriver, testDRAPool, 2, 1, "gpu-node", "gpu-node-a-0001"),
			},
			wantReason: "stale",
		},
		{
			name:       "profile-mismatch",
			claim:      allocatedClaim("default", "gpu-claim", testDRADriver, testDRAPool, "gpu-node-a-0000"),
			slices:     []*resourceapi.ResourceSlice{currentSlice(testDRADriver, testDRAPool, 2, 1, "gpu-node", "gpu-node-a-0000")},
			profiles:   map[string]struct{}{"dcgm": {}},
			wantReason: "not compatible",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			plugin := newTestPlugin(t, readPhase2Fixture(t, "iluvatar-2gpu.prom"))
			configureDRALinkedMapping(t, plugin, "iluvatar.com/gpu", testDRADriver, testDRAPool)
			claimName := "gpu-claim"
			pod := gpuPod()
			pod.Namespace = "default"
			pod.Spec.ResourceClaims = []v1.PodResourceClaim{{Name: "accelerator", ResourceClaimName: &claimName}}
			plugin.allocationSource = &staticAllocationIdentitySource{claims: map[string]*resourceapi.ResourceClaim{"default/" + claimName: tc.claim}, slices: tc.slices}
			node := nodeInfoWithEndpointAndGPUCapacity("http://gpu-node:32021/metrics", 2)
			warmNode(t, plugin, node)
			if tc.profiles != nil {
				mapping := plugin.config.ResourceMappings["iluvatar.com/gpu"]
				mapping.Profiles = tc.profiles
				plugin.config.ResourceMappings["iluvatar.com/gpu"] = mapping
			}
			status := plugin.Filter(context.Background(), cycleStateForPod(t, plugin, pod), pod, node)
			if status.IsSuccess() || !strings.Contains(strings.ToLower(status.Message()), strings.ToLower(tc.wantReason)) {
				t.Fatalf("Filter status=%v, want strict reason containing %q", status, tc.wantReason)
			}
		})
	}
}

func TestExtendedResourceWithoutAllocationLinkageRemainsConservative(t *testing.T) {
	plugin := newTestPlugin(t, readPhase2Fixture(t, "iluvatar-2gpu.prom"))
	plugin.config.MaxTemperatureC = 70
	pod := gpuPod()
	node := nodeInfoWithEndpointAndGPUCapacity("http://gpu-node:32021/metrics", 2)
	warmNode(t, plugin, node)
	status := plugin.Filter(context.Background(), cycleStateForPod(t, plugin, pod), pod, node)
	if status.IsSuccess() || !strings.Contains(status.Message(), "temperature") {
		t.Fatalf("Filter status=%v, want conservative node-wide telemetry rejection", status)
	}
}

func TestResourceClaimOnlyPodStillBelongsToDynamicResources(t *testing.T) {
	plugin := newTestPlugin(t, iluvatarMetrics)
	claimName := "claim-only"
	pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "dra-only"}, Spec: v1.PodSpec{
		ResourceClaims: []v1.PodResourceClaim{{Name: "accelerator", ResourceClaimName: &claimName}},
		Containers:     []v1.Container{{Name: "workload"}},
	}}
	plugin.allocationSource = &staticAllocationIdentitySource{claims: map[string]*resourceapi.ResourceClaim{"default/" + claimName: allocatedClaim("default", claimName, testDRADriver, testDRAPool, "gpu-node-a-0000")}}
	state := framework.NewCycleState()
	if _, status := plugin.PreFilter(context.Background(), state, pod); status.Code() != framework.Skip {
		t.Fatalf("DRA-only pod status=%v, want Skip", status)
	}
}

func TestStructuredResourceMappingsEnvAndLegacyGate(t *testing.T) {
	t.Run("structured-env", func(t *testing.T) {
		t.Setenv("K3S_GPU_RESOURCE_MAPPINGS_JSON", `[{"name":"example.com/gpu","class":"gpu","profiles":["iluvatar"],"inventorySelector":"uuid-prefix=GPU-","allocationMode":"exclusive"}]`)
		cfg, err := configFromArgs(nil)
		if err != nil {
			t.Fatal(err)
		}
		mapping, ok := cfg.ResourceMappings["example.com/gpu"]
		if !ok || mapping.Class != DeviceClassGPU || mapping.Selector.UUIDPrefix != "GPU-" || mapping.AllocationMode != allocationModeExclusive {
			t.Fatalf("structured mapping=%+v ok=%v", mapping, ok)
		}
	})

	t.Run("legacy-disabled", func(t *testing.T) {
		t.Setenv("K3S_GPU_RESOURCE_NAMES", "iluvatar.com/gpu")
		if _, err := configFromArgs(nil); err == nil || !strings.Contains(err.Error(), "allowLegacyResourceNames") {
			t.Fatalf("config error=%v, want explicit legacy gate failure", err)
		}
	})

	t.Run("legacy-known-names-preserve-semantics", func(t *testing.T) {
		t.Setenv("K3S_GPU_RESOURCE_NAMES", "iluvatar.com/gpu,huawei.com/npu")
		raw := []byte(`{"apiVersion":"gpustability.k3s.io/v1alpha1","kind":"K3SGPUStabilityArgs","allowLegacyResourceNames":true}`)
		cfg, err := configFromArgs(&runtime.Unknown{Raw: raw, ContentType: runtime.ContentTypeJSON})
		if err != nil {
			t.Fatal(err)
		}
		if cfg.ResourceMappings["iluvatar.com/gpu"].Class != DeviceClassGPU || cfg.ResourceMappings["huawei.com/npu"].Class != DeviceClassNPU {
			t.Fatalf("legacy mappings were collapsed to generic accelerator: %+v", cfg.ResourceMappings)
		}
	})
}

func configureDRALinkedMapping(t *testing.T, plugin *Plugin, name, driver, pool string) {
	t.Helper()
	selector, err := parseInventorySelector("dra-driver=" + driver + ",dra-pool=" + pool)
	if err != nil {
		t.Fatal(err)
	}
	resourceName := v1.ResourceName(name)
	mapping := plugin.config.ResourceMappings[resourceName]
	mapping.AllocationMode = allocationModeDRALinked
	mapping.Selector = selector
	plugin.config.ResourceMappings[resourceName] = mapping
	plugin.collector.config.ResourceMappings[resourceName] = mapping
}

func allocatedClaim(namespace, name, driver, pool string, devices ...string) *resourceapi.ResourceClaim {
	results := make([]resourceapi.DeviceRequestAllocationResult, 0, len(devices))
	for i, device := range devices {
		results = append(results, resourceapi.DeviceRequestAllocationResult{Request: fmt.Sprintf("request-%d", i), Driver: driver, Pool: pool, Device: device})
	}
	return &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Status:     resourceapi.ResourceClaimStatus{Allocation: &resourceapi.AllocationResult{Devices: resourceapi.DeviceAllocationResult{Results: results}}},
	}
}

func currentSlice(driver, pool string, generation, count int64, nodeName string, devices ...string) *resourceapi.ResourceSlice {
	result := &resourceapi.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("slice-%d-%s", generation, strings.ReplaceAll(strings.Join(devices, "-"), "/", "-"))},
		Spec:       resourceapi.ResourceSliceSpec{Driver: driver, Pool: resourceapi.ResourcePool{Name: pool, Generation: generation, ResourceSliceCount: count}, NodeName: nodeName},
	}
	for _, device := range devices {
		result.Spec.Devices = append(result.Spec.Devices, resourceapi.Device{Name: device, Basic: &resourceapi.BasicDevice{}})
	}
	return result
}
