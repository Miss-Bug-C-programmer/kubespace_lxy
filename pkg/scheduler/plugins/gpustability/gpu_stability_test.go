package gpustability

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

const iluvatarMetrics = `
# HELP ix_gpu_utilization The utilization of iluvatar GPU (%).
# TYPE ix_gpu_utilization gauge
ix_gpu_utilization{gpu="0",name="MRC-V100-0x18",uuid="GPU-f2a72787"} 0
# HELP ix_mem_free The free physical memory of iluvatar GPU (MiB).
# TYPE ix_mem_free gauge
ix_mem_free{gpu="0",name="MRC-V100-0x18",uuid="GPU-f2a72787"} 32704
# HELP ix_mem_total The total physical memory of iluvatar GPU (MiB).
# TYPE ix_mem_total gauge
ix_mem_total{gpu="0",name="MRC-V100-0x18",uuid="GPU-f2a72787"} 32768
# HELP ix_mem_used The used physical memory of iluvatar GPU (MiB).
# TYPE ix_mem_used gauge
ix_mem_used{gpu="0",name="MRC-V100-0x18",uuid="GPU-f2a72787"} 64
# HELP ix_mem_utilization The memory utilization of iluvatar GPU (%).
# TYPE ix_mem_utilization gauge
ix_mem_utilization{gpu="0",name="MRC-V100-0x18",uuid="GPU-f2a72787"} 1
# HELP ix_sm_clock Sm clock of iluvatar GPU (MHz).
# TYPE ix_sm_clock gauge
ix_sm_clock{gpu="0",name="MRC-V100-0x18",uuid="GPU-f2a72787"} 1500
# HELP ix_mem_clock Mem clock of iluvatar GPU (MHz).
# TYPE ix_mem_clock gauge
ix_mem_clock{gpu="0",name="MRC-V100-0x18",uuid="GPU-f2a72787"} 1600
# HELP ix_temperature The temperature of iluvatar GPU (C).
# TYPE ix_temperature gauge
ix_temperature{gpu="0",name="MRC-V100-0x18",uuid="GPU-f2a72787"} 70
# HELP ix_power_usage The power usage of iluvatar GPU.
# TYPE ix_power_usage gauge
ix_power_usage{gpu="0",name="MRC-V100-0x18",uuid="GPU-f2a72787"} 28
`

const dcgmMetrics = `
# HELP DCGM_FI_DEV_GPU_UTIL GPU utilization.
# TYPE DCGM_FI_DEV_GPU_UTIL gauge
DCGM_FI_DEV_GPU_UTIL{gpu="0",UUID="GPU-0",device="nvidia0"} 12
DCGM_FI_DEV_GPU_UTIL{gpu="1",UUID="GPU-1",device="nvidia1"} 25
# HELP DCGM_FI_DEV_FB_FREE Framebuffer memory free.
# TYPE DCGM_FI_DEV_FB_FREE gauge
DCGM_FI_DEV_FB_FREE{gpu="0",UUID="GPU-0",device="nvidia0"} 20000
DCGM_FI_DEV_FB_FREE{gpu="1",UUID="GPU-1",device="nvidia1"} 12000
# HELP DCGM_FI_DEV_FB_TOTAL Framebuffer memory total.
# TYPE DCGM_FI_DEV_FB_TOTAL gauge
DCGM_FI_DEV_FB_TOTAL{gpu="0",UUID="GPU-0",device="nvidia0"} 24576
DCGM_FI_DEV_FB_TOTAL{gpu="1",UUID="GPU-1",device="nvidia1"} 24576
# HELP DCGM_FI_DEV_GPU_TEMP GPU temperature.
# TYPE DCGM_FI_DEV_GPU_TEMP gauge
DCGM_FI_DEV_GPU_TEMP{gpu="0",UUID="GPU-0",device="nvidia0"} 68
DCGM_FI_DEV_GPU_TEMP{gpu="1",UUID="GPU-1",device="nvidia1"} 72
# HELP DCGM_FI_DEV_SM_CLOCK SM clock.
# TYPE DCGM_FI_DEV_SM_CLOCK gauge
DCGM_FI_DEV_SM_CLOCK{gpu="0",UUID="GPU-0",device="nvidia0"} 1410
DCGM_FI_DEV_SM_CLOCK{gpu="1",UUID="GPU-1",device="nvidia1"} 1380
# HELP DCGM_FI_DEV_MEM_CLOCK Memory clock.
# TYPE DCGM_FI_DEV_MEM_CLOCK gauge
DCGM_FI_DEV_MEM_CLOCK{gpu="0",UUID="GPU-0",device="nvidia0"} 5001
DCGM_FI_DEV_MEM_CLOCK{gpu="1",UUID="GPU-1",device="nvidia1"} 5001
`

const genericNPUMetrics = `
# HELP k3s_npu_utilization_percent Accelerator utilization.
# TYPE k3s_npu_utilization_percent gauge
k3s_npu_utilization_percent{npu="0",npu_name="Ascend-0",npu_uuid="NPU-0"} 15
# HELP k3s_npu_memory_free_mib Accelerator memory free.
# TYPE k3s_npu_memory_free_mib gauge
k3s_npu_memory_free_mib{npu="0",npu_name="Ascend-0",npu_uuid="NPU-0"} 28672
# HELP k3s_npu_memory_total_mib Accelerator memory total.
# TYPE k3s_npu_memory_total_mib gauge
k3s_npu_memory_total_mib{npu="0",npu_name="Ascend-0",npu_uuid="NPU-0"} 32768
# HELP k3s_npu_temperature_celsius Accelerator temperature.
# TYPE k3s_npu_temperature_celsius gauge
k3s_npu_temperature_celsius{npu="0",npu_name="Ascend-0",npu_uuid="NPU-0"} 62
# HELP k3s_npu_compute_clock_mhz Accelerator compute clock.
# TYPE k3s_npu_compute_clock_mhz gauge
k3s_npu_compute_clock_mhz{npu="0",npu_name="Ascend-0",npu_uuid="NPU-0"} 1200
# HELP k3s_npu_health Accelerator health.
# TYPE k3s_npu_health gauge
k3s_npu_health{npu="0",npu_name="Ascend-0",npu_uuid="NPU-0"} 1
`

const richGenericAcceleratorMetrics = `
# HELP k3s_accelerator_utilization_percent Accelerator utilization.
# TYPE k3s_accelerator_utilization_percent gauge
k3s_accelerator_utilization_percent{accelerator="0",accelerator_name="accel-0",accelerator_uuid="ACCEL-0"} 3
k3s_accelerator_utilization_percent{accelerator="1",accelerator_name="accel-1",accelerator_uuid="ACCEL-1"} 4
k3s_accelerator_utilization_percent{accelerator="2",accelerator_name="accel-2",accelerator_uuid="ACCEL-2"} 5
# HELP k3s_accelerator_memory_free_mib Accelerator memory free.
# TYPE k3s_accelerator_memory_free_mib gauge
k3s_accelerator_memory_free_mib{accelerator="0",accelerator_name="accel-0",accelerator_uuid="ACCEL-0"} 60000
k3s_accelerator_memory_free_mib{accelerator="1",accelerator_name="accel-1",accelerator_uuid="ACCEL-1"} 60000
k3s_accelerator_memory_free_mib{accelerator="2",accelerator_name="accel-2",accelerator_uuid="ACCEL-2"} 60000
# HELP k3s_accelerator_memory_total_mib Accelerator memory total.
# TYPE k3s_accelerator_memory_total_mib gauge
k3s_accelerator_memory_total_mib{accelerator="0",accelerator_name="accel-0",accelerator_uuid="ACCEL-0"} 65536
k3s_accelerator_memory_total_mib{accelerator="1",accelerator_name="accel-1",accelerator_uuid="ACCEL-1"} 65536
k3s_accelerator_memory_total_mib{accelerator="2",accelerator_name="accel-2",accelerator_uuid="ACCEL-2"} 65536
# HELP k3s_accelerator_temperature_celsius Accelerator temperature.
# TYPE k3s_accelerator_temperature_celsius gauge
k3s_accelerator_temperature_celsius{accelerator="0",accelerator_name="accel-0",accelerator_uuid="ACCEL-0"} 55
k3s_accelerator_temperature_celsius{accelerator="1",accelerator_name="accel-1",accelerator_uuid="ACCEL-1"} 56
k3s_accelerator_temperature_celsius{accelerator="2",accelerator_name="accel-2",accelerator_uuid="ACCEL-2"} 57
`

const vendorXMetrics = `
# HELP vendorx_busy Accelerator busy percent.
# TYPE vendorx_busy gauge
vendorx_busy{chip="0",serial="VX-0"} 7
# HELP vendorx_mem_free_bytes Accelerator free memory.
# TYPE vendorx_mem_free_bytes gauge
vendorx_mem_free_bytes{chip="0",serial="VX-0"} 17179869184
# HELP vendorx_mem_total_bytes Accelerator total memory.
# TYPE vendorx_mem_total_bytes gauge
vendorx_mem_total_bytes{chip="0",serial="VX-0"} 34359738368
# HELP vendorx_temp_c Accelerator temperature.
# TYPE vendorx_temp_c gauge
vendorx_temp_c{chip="0",serial="VX-0"} 58
`

func TestParseMetrics(t *testing.T) {
	metrics, err := parseMetrics(strings.NewReader(iluvatarMetrics))
	if err != nil {
		t.Fatalf("parseMetrics returned error: %v", err)
	}

	if metrics.GPUCount != 1 {
		t.Fatalf("GPUCount = %d, want 1", metrics.GPUCount)
	}
	if metrics.MemoryTotalMiB != 32768 {
		t.Fatalf("MemoryTotalMiB = %.0f, want 32768", metrics.MemoryTotalMiB)
	}
	if metrics.MemoryFreeMiB != 32704 {
		t.Fatalf("MemoryFreeMiB = %.0f, want 32704", metrics.MemoryFreeMiB)
	}
	if metrics.TemperatureC != 70 {
		t.Fatalf("TemperatureC = %.0f, want 70", metrics.TemperatureC)
	}
	if metrics.Profile != "iluvatar" {
		t.Fatalf("Profile = %q, want iluvatar", metrics.Profile)
	}
	if len(metrics.Devices) != 1 {
		t.Fatalf("Devices = %d, want 1", len(metrics.Devices))
	}
}

func TestParseDCGMMetrics(t *testing.T) {
	metrics, err := parseMetrics(strings.NewReader(dcgmMetrics))
	if err != nil {
		t.Fatalf("parseMetrics returned error: %v", err)
	}

	if metrics.Profile != "dcgm" {
		t.Fatalf("Profile = %q, want dcgm", metrics.Profile)
	}
	if metrics.GPUCount != 2 {
		t.Fatalf("GPUCount = %d, want 2", metrics.GPUCount)
	}
	if metrics.MemoryTotalMiB != 49152 {
		t.Fatalf("MemoryTotalMiB = %.0f, want 49152", metrics.MemoryTotalMiB)
	}
	if metrics.TemperatureC != 72 {
		t.Fatalf("TemperatureC = %.0f, want 72", metrics.TemperatureC)
	}
}

func TestParseGenericNPUMetrics(t *testing.T) {
	metrics, err := parseMetrics(strings.NewReader(genericNPUMetrics))
	if err != nil {
		t.Fatalf("parseMetrics returned error: %v", err)
	}

	if metrics.Profile != "generic" {
		t.Fatalf("Profile = %q, want generic", metrics.Profile)
	}
	if metrics.GPUCount != 1 {
		t.Fatalf("GPUCount = %d, want 1", metrics.GPUCount)
	}
	if metrics.MemoryFreeMiB != 28672 {
		t.Fatalf("MemoryFreeMiB = %.0f, want 28672", metrics.MemoryFreeMiB)
	}
	if metrics.TemperatureC != 62 {
		t.Fatalf("TemperatureC = %.0f, want 62", metrics.TemperatureC)
	}
}

func TestCustomMetricProfileNormalizesUnknownExporter(t *testing.T) {
	profiles := appendMetricProfiles(registeredMetricProfiles(), []metricProfile{
		{
			Name:       "vendorx",
			MatchNames: []string{"vendorx_mem_total_bytes"},
			Fields: map[deviceMetricField]metricFieldSpec{
				fieldGPUUtilization: {Names: []string{"vendorx_busy"}, Rollup: rollupAvg},
				fieldMemoryFreeMiB:  {Names: []string{"vendorx_mem_free_bytes"}, Unit: unitBytes, Rollup: rollupMax},
				fieldMemoryTotalMiB: {Names: []string{"vendorx_mem_total_bytes"}, Unit: unitBytes, Rollup: rollupMax},
				fieldTemperatureC:   {Names: []string{"vendorx_temp_c"}, Rollup: rollupMax},
			},
		},
	})

	metrics, err := parseMetricsWithProfiles(strings.NewReader(vendorXMetrics), "vendorx", profiles)
	if err != nil {
		t.Fatalf("parseMetricsWithProfiles returned error: %v", err)
	}
	if metrics.Profile != "vendorx" {
		t.Fatalf("Profile = %q, want vendorx", metrics.Profile)
	}
	if metrics.GPUCount != 1 {
		t.Fatalf("GPUCount = %d, want 1", metrics.GPUCount)
	}
	if metrics.MemoryTotalMiB != 32768 {
		t.Fatalf("MemoryTotalMiB = %.0f, want 32768", metrics.MemoryTotalMiB)
	}
}

func TestMetricProfilesFromFile(t *testing.T) {
	path := t.TempDir() + "/profiles.json"
	raw := `{
  "profiles": [
    {
      "name": "vendorx",
      "matchNames": ["vendorx_mem_total_bytes"],
      "fields": {
        "gpu_utilization": {"names": ["vendorx_busy"], "rollup": "avg"},
        "memory_free_mib": {"names": ["vendorx_mem_free_bytes"], "unit": "bytes", "rollup": "max"},
        "memory_total_mib": {"names": ["vendorx_mem_total_bytes"], "unit": "bytes", "rollup": "max"},
        "temperature_celsius": {"names": ["vendorx_temp_c"], "rollup": "max"}
      }
    }
  ]
}`
	if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
		t.Fatalf("failed to write profile file: %v", err)
	}

	profiles, err := metricProfilesFromFile(path)
	if err != nil {
		t.Fatalf("metricProfilesFromFile returned error: %v", err)
	}
	metrics, err := parseMetricsWithProfiles(strings.NewReader(vendorXMetrics), "vendorx", appendMetricProfiles(registeredMetricProfiles(), profiles))
	if err != nil {
		t.Fatalf("parseMetricsWithProfiles returned error: %v", err)
	}
	if metrics.Profile != "vendorx" || metrics.GPUCount != 1 {
		t.Fatalf("Profile=%q GPUCount=%d, want vendorx/1", metrics.Profile, metrics.GPUCount)
	}
}

func TestParseMetricsFromFile(t *testing.T) {
	path := os.Getenv("K3S_GPU_TEST_METRICS_FILE")
	if path == "" {
		t.Skip("K3S_GPU_TEST_METRICS_FILE is not set")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read metrics file: %v", err)
	}
	metrics, err := parseMetrics(strings.NewReader(string(raw)))
	if err != nil {
		t.Fatalf("parseMetrics returned error: %v", err)
	}
	if metrics.GPUCount == 0 {
		t.Fatalf("GPUCount = 0, want at least 1")
	}

	plugin := newTestPlugin(t, string(raw))
	nodeInfo := nodeInfoWithEndpointAndGPUCapacity("http://gpu-node:32021/metrics", int64(metrics.GPUCount))
	pod := gpuAwarePod()
	warmNode(t, plugin, nodeInfo)
	state := cycleStateForPod(t, plugin, pod)
	if status := plugin.Filter(context.Background(), state, pod, nodeInfo); !status.IsSuccess() {
		t.Fatalf("Filter status = %v, want success", status)
	}
	if status := plugin.PreScore(context.Background(), state, pod, []*framework.NodeInfo{nodeInfo}); !status.IsSuccess() {
		t.Fatalf("PreScore status = %v, want success", status)
	}
	if score, status := plugin.Score(context.Background(), state, pod, nodeInfo); !status.IsSuccess() || score <= framework.MinNodeScore {
		t.Fatalf("Score = %d, status = %v, want positive successful score", score, status)
	}
}

func TestFilterAndScoreGPUNode(t *testing.T) {
	plugin := newTestPlugin(t, iluvatarMetrics)
	pod := gpuPod()
	nodeInfo := nodeInfoWithEndpoint("http://gpu-node:32021/metrics")
	warmNode(t, plugin, nodeInfo)
	state := cycleStateForPod(t, plugin, pod)

	status := plugin.Filter(context.Background(), state, pod, nodeInfo)
	if !status.IsSuccess() {
		t.Fatalf("Filter status = %v, want success", status)
	}

	if status := plugin.PreScore(context.Background(), state, pod, []*framework.NodeInfo{nodeInfo}); !status.IsSuccess() {
		t.Fatalf("PreScore status = %v, want success", status)
	}
	score, status := plugin.Score(context.Background(), state, pod, nodeInfo)
	if !status.IsSuccess() {
		t.Fatalf("Score status = %v, want success", status)
	}
	if score <= framework.MinNodeScore {
		t.Fatalf("Score = %d, want a positive score", score)
	}
}

func TestFilterRejectsHotGPU(t *testing.T) {
	hotMetrics := strings.ReplaceAll(iluvatarMetrics, "} 70\n# HELP ix_power_usage", "} 95\n# HELP ix_power_usage")

	plugin := newTestPlugin(t, hotMetrics)
	pod := gpuPod()
	node := nodeInfoWithEndpoint("http://gpu-node:32021/metrics")
	warmNode(t, plugin, node)
	status := plugin.Filter(context.Background(), cycleStateForPod(t, plugin, pod), pod, node)
	if status.IsSuccess() {
		t.Fatalf("Filter status = success, want rejection")
	}
}

func TestFilterDoesNotDoubleCountRequestsAndLimits(t *testing.T) {
	plugin := newTestPlugin(t, iluvatarMetrics)
	pod := gpuPodWithRequestAndLimit(1)
	node := nodeInfoWithEndpoint("http://gpu-node:32021/metrics")
	warmNode(t, plugin, node)
	status := plugin.Filter(context.Background(), cycleStateForPod(t, plugin, pod), pod, node)
	if !status.IsSuccess() {
		t.Fatalf("Filter status = %v, want success", status)
	}
}

func TestFilterRejectsInsufficientGPUDevices(t *testing.T) {
	plugin := newTestPlugin(t, iluvatarMetrics)
	pod := gpuPodWithRequestAndLimit(2)
	node := nodeInfoWithEndpoint("http://gpu-node:32021/metrics")
	warmNode(t, plugin, node)
	status := plugin.Filter(context.Background(), cycleStateForPod(t, plugin, pod), pod, node)
	if status.IsSuccess() {
		t.Fatalf("Filter status = success, want rejection")
	}
}

func TestFilterLeavesAllocatableAccountingToNodeResourcesFit(t *testing.T) {
	nodeInfo := nodeInfoWithEndpointAndGPUCapacity("http://gpu-node:32021/metrics", 1)
	nodeInfo.AddPod(gpuPodWithRequestAndLimit(1))
	plugin := newTestPlugin(t, iluvatarMetrics)
	pod := gpuPodWithRequestAndLimit(1)
	warmNode(t, plugin, nodeInfo)
	status := plugin.Filter(context.Background(), cycleStateForPod(t, plugin, pod), pod, nodeInfo)
	if !status.IsSuccess() {
		t.Fatalf("Filter status = %v, want telemetry compatibility success; NodeResourcesFit owns allocatable accounting", status)
	}
}

func TestPreScoreAndScorePreferTightFitGPUNode(t *testing.T) {
	plugin := newTestPluginWithMetrics(t, map[string]string{
		"single-node:32021": genericAcceleratorMetricsForDevices(1),
		"rich-node:32021":   genericAcceleratorMetricsForDevices(3),
	})
	pod := podWithAcceleratorRequest("intel.com/xpu", 1)
	singleNode := nodeInfoWithNamedEndpointAndCapacity("single-node", "http://single-node:32021/metrics", "intel.com/xpu", 1)
	richNode := nodeInfoWithNamedEndpointAndCapacity("rich-node", "http://rich-node:32021/metrics", "intel.com/xpu", 3)
	warmNode(t, plugin, singleNode)
	warmNode(t, plugin, richNode)
	state := cycleStateForPod(t, plugin, pod)

	status := plugin.PreScore(context.Background(), state, pod, []*framework.NodeInfo{singleNode, richNode})
	if !status.IsSuccess() {
		t.Fatalf("PreScore status = %v, want success", status)
	}

	singleScore, status := plugin.Score(context.Background(), state, pod, singleNode)
	if !status.IsSuccess() {
		t.Fatalf("single node Score status = %v, want success", status)
	}
	richScore, status := plugin.Score(context.Background(), state, pod, richNode)
	if !status.IsSuccess() {
		t.Fatalf("rich node Score status = %v, want success", status)
	}
	if singleScore <= richScore {
		t.Fatalf("single node score = %d, rich node score = %d, want tighter-fit single node higher", singleScore, richScore)
	}
}

func TestPreScoreAggregatesThreeExporterNodes(t *testing.T) {
	plugin := newTestPluginWithMetrics(t, map[string]string{
		"single-node:32021": genericAcceleratorMetricsForDevices(1),
		"dual-node:32021":   genericAcceleratorMetricsForDevices(2),
		"rich-node:32021":   genericAcceleratorMetricsForDevices(3),
	})
	pod := podWithAcceleratorRequest("intel.com/xpu", 1)
	singleNode := nodeInfoWithNamedEndpointAndCapacity("single-node", "http://single-node:32021/metrics", "intel.com/xpu", 1)
	dualNode := nodeInfoWithNamedEndpointAndCapacity("dual-node", "http://dual-node:32021/metrics", "intel.com/xpu", 2)
	richNode := nodeInfoWithNamedEndpointAndCapacity("rich-node", "http://rich-node:32021/metrics", "intel.com/xpu", 3)
	warmNode(t, plugin, singleNode)
	warmNode(t, plugin, dualNode)
	warmNode(t, plugin, richNode)
	state := cycleStateForPod(t, plugin, pod)

	status := plugin.PreScore(context.Background(), state, pod, []*framework.NodeInfo{singleNode, dualNode, richNode})
	if !status.IsSuccess() {
		t.Fatalf("PreScore status = %v, want success", status)
	}

	singleScore, status := plugin.Score(context.Background(), state, pod, singleNode)
	if !status.IsSuccess() {
		t.Fatalf("single node Score status = %v, want success", status)
	}
	dualScore, status := plugin.Score(context.Background(), state, pod, dualNode)
	if !status.IsSuccess() {
		t.Fatalf("dual node Score status = %v, want success", status)
	}
	richScore, status := plugin.Score(context.Background(), state, pod, richNode)
	if !status.IsSuccess() {
		t.Fatalf("rich node Score status = %v, want success", status)
	}
	if singleScore <= dualScore || singleScore <= richScore {
		t.Fatalf("scores single=%d dual=%d rich=%d, want tight fit single highest", singleScore, dualScore, richScore)
	}
}

func TestGPUAwarePodDoesNotRequireDevicePluginResourceRequest(t *testing.T) {
	plugin := newTestPlugin(t, iluvatarMetrics)
	pod := gpuAwarePod()
	nodeInfo := nodeInfoWithEndpointAndGPUCapacity("http://gpu-node:32021/metrics", 0)

	warmNode(t, plugin, nodeInfo)
	status := plugin.Filter(context.Background(), cycleStateForPod(t, plugin, pod), pod, nodeInfo)
	if !status.IsSuccess() {
		t.Fatalf("Filter status = %v, want success without iluvatar.com/gpu resource request", status)
	}
}

func TestNPUResourceRequestTriggersAcceleratorScheduling(t *testing.T) {
	plugin := newTestPluginWithMetrics(t, map[string]string{
		"gpu-node:32021": genericNPUMetrics,
	})
	pod := podWithAcceleratorRequest("huawei.com/npu", 1)
	nodeInfo := nodeInfoWithAcceleratorCapacity("http://gpu-node:32021/metrics", "huawei.com/npu", 1)

	warmNode(t, plugin, nodeInfo)
	status := plugin.Filter(context.Background(), cycleStateForPod(t, plugin, pod), pod, nodeInfo)
	if !status.IsSuccess() {
		t.Fatalf("Filter status = %v, want success", status)
	}
}

func TestExporterEndpointAnnotationDoesNotOverrideNodeInternalIP(t *testing.T) {
	plugin := newTestPluginWithMetrics(t, map[string]string{
		"fresh-node:32021": iluvatarMetrics,
	})
	nodeInfo := nodeInfoWithNamedEndpointAndCapacity("fresh-node", "http://stale-node:32021/metrics", "iluvatar.com/gpu", 1)
	nodeInfo.Node().Status.Addresses[0].Address = "fresh-node"

	warmNode(t, plugin, nodeInfo)
	pod := gpuPod()
	status := plugin.Filter(context.Background(), cycleStateForPod(t, plugin, pod), pod, nodeInfo)
	if !status.IsSuccess() {
		t.Fatalf("Filter status = %v, want success through NodeInternalIP-derived endpoint", status)
	}
}

func TestCachePrunesExpiredMetrics(t *testing.T) {
	plugin := newTestPlugin(t, iluvatarMetrics)
	nodeInfo := nodeInfoWithNamedEndpointAndCapacity("old-node", "http://old-node:32021/metrics", "iluvatar.com/gpu", 1)
	target, err := plugin.collector.targetForNode(nodeInfo.Node())
	if err != nil {
		t.Fatalf("targetForNode: %v", err)
	}
	now := time.Now()
	observed := now.Add(-2 * plugin.config.SnapshotTTL)
	metrics := nodeMetrics{
		FetchedAt: observed, ValidUntil: observed.Add(plugin.config.SnapshotTTL),
	}
	if !plugin.collector.store.publish(target, metrics, observed, metrics.ValidUntil) {
		t.Fatal("publish stale snapshot = false")
	}
	result := plugin.collector.snapshotForNode(nodeInfo.Node())
	if result.State != snapshotStale {
		t.Fatalf("snapshot state = %q, want stale", result.State)
	}
	if !result.ObservedAt.Equal(observed) {
		t.Fatalf("ObservedAt = %v, want unchanged %v", result.ObservedAt, observed)
	}
}

func TestCacheMaxEntriesPrunesOldestMetrics(t *testing.T) {
	plugin := newTestPlugin(t, iluvatarMetrics)
	plugin.config.CacheMaxEntries = 2
	plugin.collector.config.CacheMaxEntries = 2
	plugin.collector.store.maxEntries = 2
	now := time.Now()
	plugin.collector.mu.Lock()
	for i, name := range []string{"oldest", "middle", "newest"} {
		target := scrapeTarget{
			NodeName: name, Identity: nodeIdentity{Name: name}, Key: name, Generation: uint64(i + 1),
			SeenAt: now.Add(time.Duration(i-3) * time.Minute),
		}
		plugin.collector.targets[name] = target
		plugin.collector.store.transition(target, nodeResourceContext{}, target.SeenAt)
	}
	plugin.collector.pruneLocked("")
	plugin.collector.mu.Unlock()
	plugin.collector.mu.RLock()
	if len(plugin.collector.targets) != 2 {
		t.Fatalf("target entries = %d, want 2", len(plugin.collector.targets))
	}
	if _, ok := plugin.collector.targets["oldest"]; ok {
		t.Fatalf("oldest cache entry was not pruned")
	}
	if _, ok := plugin.collector.targets["middle"]; !ok {
		t.Fatalf("middle cache entry was pruned")
	}
	if _, ok := plugin.collector.targets["newest"]; !ok {
		t.Fatalf("newest cache entry was pruned")
	}
	plugin.collector.mu.RUnlock()
	plugin.collector.store.mu.RLock()
	defer plugin.collector.store.mu.RUnlock()
	if len(plugin.collector.store.records) != 2 {
		t.Fatalf("snapshot entries = %d, want 2", len(plugin.collector.store.records))
	}
	if _, ok := plugin.collector.store.records["oldest"]; ok {
		t.Fatalf("oldest snapshot entry was not pruned")
	}
}

func TestNonGPUPodIsIgnored(t *testing.T) {
	plugin := newTestPlugin(t, iluvatarMetrics)
	pod := &v1.Pod{}
	status := plugin.Filter(context.Background(), cycleStateForPod(t, plugin, pod), pod, framework.NewNodeInfo())
	if !status.IsSuccess() {
		t.Fatalf("Filter status = %v, want success", status)
	}
}

func newTestPlugin(t *testing.T, metrics string) *Plugin {
	t.Helper()
	return newTestPluginWithMetrics(t, map[string]string{
		"gpu-node:32021": metrics,
	})
}

func newTestPluginWithMetrics(t *testing.T, metricsByHost map[string]string) *Plugin {
	t.Helper()
	cfg := testConfig(t)
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		metrics := metricsByHost[req.URL.Host]
		if metrics == "" {
			metrics = metricsByHost["gpu-node:32021"]
		}
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader(metrics))}, nil
	})}
	collector, err := newCollector(context.Background(), cfg, client)
	if err != nil {
		t.Fatalf("newCollector: %v", err)
	}
	t.Cleanup(collector.Close)
	return &Plugin{config: cfg, collector: collector}
}

func testConfig(t *testing.T) Config {
	t.Helper()
	args := defaultArgs()
	args.Exporter.Scheme = "http"
	args.Exporter.AllowInsecureHTTP = true
	args.Collector.SnapshotTTL = "1s"
	args.Collector.RefreshInterval = "500ms"
	cfg, err := validateAndConvertArgs(args)
	if err != nil {
		t.Fatalf("test config: %v", err)
	}
	return cfg
}

func warmNode(t *testing.T, plugin *Plugin, nodeInfo *framework.NodeInfo) {
	t.Helper()
	if err := plugin.collector.refreshNode(context.Background(), nodeInfo.Node()); err != nil {
		t.Fatalf("refreshNode(%s): %v", nodeInfo.Node().Name, err)
	}
}

func cycleStateForPod(t *testing.T, plugin *Plugin, pod *v1.Pod) *framework.CycleState {
	t.Helper()
	state := framework.NewCycleState()
	if _, status := plugin.PreFilter(context.Background(), state, pod); !status.IsSuccess() && !status.IsSkip() {
		t.Fatalf("PreFilter status = %v", status)
	}
	return state
}

func gpuPod() *v1.Pod {
	return gpuPodWithRequestAndLimit(1)
}

func gpuAwarePod() *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				AnnotationEnabled: "true",
			},
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{Name: "workload"},
			},
		},
	}
}

func gpuPodWithRequestAndLimit(count int64) *v1.Pod {
	return podWithAcceleratorRequest("iluvatar.com/gpu", count)
}

func podWithAcceleratorRequest(name string, count int64) *v1.Pod {
	quantity := resource.MustParse(strconv.FormatInt(count, 10))
	return &v1.Pod{
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name: "workload",
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceName(name): quantity,
						},
						Limits: v1.ResourceList{
							v1.ResourceName(name): quantity,
						},
					},
				},
			},
		},
	}
}

func nodeInfoWithEndpoint(endpoint string) *framework.NodeInfo {
	// The default Iluvatar fixture describes one physical device. Keep the
	// Kubernetes allocatable inventory consistent so strict Phase 3 telemetry
	// coverage represents every device that the vendor plugin may allocate.
	return nodeInfoWithEndpointAndGPUCapacity(endpoint, 1)
}

func nodeInfoWithEndpointAndGPUCapacity(endpoint string, gpuCapacity int64) *framework.NodeInfo {
	return nodeInfoWithAcceleratorCapacity(endpoint, "iluvatar.com/gpu", gpuCapacity)
}

func nodeInfoWithAcceleratorCapacity(endpoint, name string, capacity int64) *framework.NodeInfo {
	return nodeInfoWithNamedEndpointAndCapacity("gpu-node", endpoint, name, capacity)
}

func nodeInfoWithNamedEndpointAndCapacity(nodeName, endpoint, resourceName string, capacity int64) *framework.NodeInfo {
	internalIP := endpointHost(endpoint)
	if internalIP == "" {
		internalIP = nodeName
	}
	nodeInfo := framework.NewNodeInfo()
	nodeInfo.SetNode(&v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: nodeName,
			Annotations: map[string]string{
				AnnotationExporterEndpoint: endpoint,
			},
		},
		Status: v1.NodeStatus{
			Addresses: []v1.NodeAddress{
				{
					Type:    v1.NodeInternalIP,
					Address: internalIP,
				},
			},
			Allocatable: v1.ResourceList{
				v1.ResourceName(resourceName): resource.MustParse(strconv.FormatInt(capacity, 10)),
			},
		},
	})
	return nodeInfo
}

func endpointHost(endpoint string) string {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return ""
	}
	return parsed.Hostname()
}

func genericAcceleratorMetricsForDevices(count int) string {
	var result strings.Builder
	for _, metric := range []struct {
		name   string
		help   string
		values func(int) float64
	}{
		{name: "k3s_accelerator_utilization_percent", help: "Accelerator utilization.", values: func(i int) float64 { return float64(5 + i) }},
		{name: "k3s_accelerator_memory_free_mib", help: "Accelerator free memory.", values: func(int) float64 { return 60000 }},
		{name: "k3s_accelerator_memory_total_mib", help: "Accelerator total memory.", values: func(int) float64 { return 65536 }},
		{name: "k3s_accelerator_temperature_celsius", help: "Accelerator temperature.", values: func(i int) float64 { return float64(55 + i) }},
	} {
		fmt.Fprintf(&result, "# HELP %s %s\n# TYPE %s gauge\n", metric.name, metric.help, metric.name)
		for i := 0; i < count; i++ {
			fmt.Fprintf(&result, "%s{accelerator=\"%d\",accelerator_name=\"accel-%d\",accelerator_uuid=\"ACCEL-%d\"} %.0f\n", metric.name, i, i, i, metric.values(i))
		}
	}
	return result.String()
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
