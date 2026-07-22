package gpustability

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	resourcehelper "k8s.io/component-helpers/resource"
	schedulerconfig "k8s.io/kubernetes/pkg/scheduler/apis/config"
	schedulerconfigscheme "k8s.io/kubernetes/pkg/scheduler/apis/config/scheme"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

func TestPodDemandMatchesKubernetes133Accounting(t *testing.T) {
	plugin := newTestPlugin(t, iluvatarMetrics)
	restartAlways := v1.ContainerRestartPolicyAlways
	pod := &v1.Pod{
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				containerWithAcceleratorRequests("gpu-app", map[v1.ResourceName]int64{"hygon.com/dcu": 1}),
				containerWithAcceleratorRequests("npu-app", map[v1.ResourceName]int64{"huawei.com/npu": 2}),
			},
			InitContainers: []v1.Container{
				{
					Name:          "gpu-sidecar",
					RestartPolicy: &restartAlways,
					Resources: v1.ResourceRequirements{Requests: v1.ResourceList{
						"hygon.com/dcu": resource.MustParse("2"),
					}},
				},
				{
					Name: "npu-init",
					Resources: v1.ResourceRequirements{Requests: v1.ResourceList{
						"huawei.com/npu": resource.MustParse("5"),
					}},
				},
			},
			Overhead: v1.ResourceList{"hygon.com/dcu": resource.MustParse("1")},
		},
	}

	requirement, err := plugin.schedulingRequirement(pod)
	if err != nil {
		t.Fatalf("schedulingRequirement: %v", err)
	}
	upstream := resourcehelper.PodRequests(pod, resourcehelper.PodResourcesOptions{})
	for _, name := range []v1.ResourceName{"hygon.com/dcu", "huawei.com/npu"} {
		quantity := upstream[name]
		if got, want := requirement.Resources[name], quantity.Value(); got != want {
			t.Fatalf("resource %q demand = %d, upstream PodRequests = %d", name, got, want)
		}
	}
	gpuQuantity := upstream["hygon.com/dcu"]
	if got, want := requirement.Classes[DeviceClassGPU], gpuQuantity.Value(); got != want {
		t.Fatalf("GPU class demand = %d, want %d", got, want)
	}
	npuQuantity := upstream["huawei.com/npu"]
	if got, want := requirement.Classes[DeviceClassNPU], npuQuantity.Value(); got != want {
		t.Fatalf("NPU class demand = %d, want %d", got, want)
	}
}

func TestPodDemandUsesRequestsWithoutDoubleCountingLimits(t *testing.T) {
	plugin := newTestPlugin(t, iluvatarMetrics)
	pod := podWithAcceleratorRequest("iluvatar.com/gpu", 2)
	pod.Spec.Containers[0].Resources.Limits["iluvatar.com/gpu"] = resource.MustParse("8")
	requirement, err := plugin.schedulingRequirement(pod)
	if err != nil {
		t.Fatalf("schedulingRequirement: %v", err)
	}
	if got := requirement.Resources["iluvatar.com/gpu"]; got != 2 {
		t.Fatalf("GPU demand = %d, want request value 2", got)
	}

	limitOnly := podWithAcceleratorRequest("iluvatar.com/gpu", 1)
	limitOnly.Spec.Containers[0].Resources.Requests = nil
	requirement, err = plugin.schedulingRequirement(limitOnly)
	if err != nil {
		t.Fatalf("limit-only schedulingRequirement: %v", err)
	}
	if requirement.Required {
		t.Fatal("limit-only Pod unexpectedly requires telemetry; scheduler accounting uses requests")
	}
}

func TestPodDemandRejectsFractionalExtendedResources(t *testing.T) {
	plugin := newTestPlugin(t, iluvatarMetrics)
	pod := podWithAcceleratorRequest("iluvatar.com/gpu", 1)
	pod.Spec.Containers[0].Resources.Requests["iluvatar.com/gpu"] = resource.MustParse("500m")
	if _, err := plugin.schedulingRequirement(pod); err == nil || !strings.Contains(err.Error(), "whole number") {
		t.Fatalf("schedulingRequirement error = %v, want whole-number rejection", err)
	}
}

func TestExplicitResourceMappingsDoNotGuessBySubstring(t *testing.T) {
	plugin := newTestPlugin(t, iluvatarMetrics)
	pod := podWithAcceleratorRequest("example.com/supergpu", 1)
	requirement, err := plugin.schedulingRequirement(pod)
	if err != nil {
		t.Fatalf("schedulingRequirement: %v", err)
	}
	if requirement.Required || len(requirement.Resources) != 0 {
		t.Fatalf("unmapped resource was guessed as an accelerator: %+v", requirement)
	}
}

func TestMixedGPUAndNPUAreSatisfiedSeparately(t *testing.T) {
	metrics := genericGPUAndNPUMetrics(1, 1, 32000)
	plugin := newTestPlugin(t, metrics)
	node := nodeInfoWithResourceCapacities("mixed-node", "http://mixed-node:32021/metrics", map[v1.ResourceName]int64{
		"hygon.com/dcu": 1, "huawei.com/npu": 1,
	})
	pod := podWithAcceleratorRequests(map[v1.ResourceName]int64{"hygon.com/dcu": 1, "huawei.com/npu": 1})
	warmNode(t, plugin, node)
	if status := plugin.Filter(context.Background(), cycleStateForPod(t, plugin, pod), pod, node); !status.IsSuccess() {
		t.Fatalf("mixed Filter status = %v, want success", status)
	}

	wrongType := newTestPlugin(t, genericGPUAndNPUMetrics(2, 0, 32000))
	wrongNode := nodeInfoWithResourceCapacities("wrong-node", "http://wrong-node:32021/metrics", map[v1.ResourceName]int64{
		"hygon.com/dcu": 2, "huawei.com/npu": 1,
	})
	warmNode(t, wrongType, wrongNode)
	status := wrongType.Filter(context.Background(), cycleStateForPod(t, wrongType, pod), pod, wrongNode)
	if status.IsSuccess() || !strings.Contains(status.Message(), "npu") {
		t.Fatalf("wrong-type Filter status = %v, want NPU-class rejection", status)
	}
}

func TestMinimumFreeMemoryUsesSameCandidateSetAcrossStages(t *testing.T) {
	plugin := newTestPluginWithMetrics(t, map[string]string{
		"enough-node:32021": genericAcceleratorMetricsWithFree(1, 16000),
		"low-node:32021":    genericAcceleratorMetricsWithFree(1, 1024),
	})
	pod := podWithAcceleratorRequest("intel.com/xpu", 1)
	pod.Annotations = map[string]string{AnnotationMinFreeMemoryMiB: "8192"}
	enough := nodeInfoWithNamedEndpointAndCapacity("enough-node", "http://enough-node:32021/metrics", "intel.com/xpu", 1)
	low := nodeInfoWithNamedEndpointAndCapacity("low-node", "http://low-node:32021/metrics", "intel.com/xpu", 1)
	warmNode(t, plugin, enough)
	warmNode(t, plugin, low)
	state := cycleStateForPod(t, plugin, pod)
	if status := plugin.Filter(context.Background(), state, pod, enough); !status.IsSuccess() {
		t.Fatalf("enough-memory Filter status = %v", status)
	}
	if status := plugin.Filter(context.Background(), state, pod, low); status.IsSuccess() || !strings.Contains(status.Message(), "free memory") {
		t.Fatalf("low-memory Filter status = %v, want per-dimension free-memory rejection", status)
	}
	if status := plugin.PreScore(context.Background(), state, pod, []*framework.NodeInfo{enough, low}); !status.IsSuccess() {
		t.Fatalf("PreScore status = %v", status)
	}
	enoughScore, _ := plugin.Score(context.Background(), state, pod, enough)
	lowScore, _ := plugin.Score(context.Background(), state, pod, low)
	if enoughScore <= 0 || lowScore != 0 {
		t.Fatalf("scores enough=%d low=%d, want positive/zero from the same eligible set", enoughScore, lowScore)
	}
}

func TestSchedulerCallbacksNeverWaitForExporterIO(t *testing.T) {
	cfg := testConfig(t)
	cfg.Timeout = time.Second
	started := make(chan struct{}, 1)
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-req.Context().Done()
		return nil, req.Context().Err()
	})}
	collector, err := newCollector(context.Background(), cfg, client)
	if err != nil {
		t.Fatalf("newCollector: %v", err)
	}
	t.Cleanup(collector.Close)
	plugin := &Plugin{config: cfg, collector: collector}
	pod := gpuPod()
	node := nodeInfoWithEndpoint("http://slow-node:32021/metrics")
	node.Node().Status.Addresses[0].Address = "slow-node"
	state := cycleStateForPod(t, plugin, pod)

	start := time.Now()
	status := plugin.Filter(context.Background(), state, pod, node)
	duration := time.Since(start)
	if status.IsSuccess() {
		t.Fatal("strict Filter status = success without a snapshot")
	}
	if duration > 50*time.Millisecond {
		t.Fatalf("Filter blocked for %v waiting on exporter I/O", duration)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("non-blocking callback did not enqueue asynchronous refresh")
	}
}

func TestCollectorCoalescesConcurrentRefreshRequests(t *testing.T) {
	cfg := testConfig(t)
	cfg.Workers = 1
	cfg.SnapshotTTL = time.Hour
	cfg.RefreshInterval = 30 * time.Minute
	var requests atomic.Int64
	started := make(chan struct{})
	release := make(chan struct{})
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		if requests.Add(1) == 1 {
			close(started)
		}
		<-release
		return metricsResponse(iluvatarMetrics), nil
	})}
	collector, err := newCollector(context.Background(), cfg, client)
	if err != nil {
		t.Fatalf("newCollector: %v", err)
	}
	t.Cleanup(collector.Close)
	node := nodeInfoWithEndpoint("http://coalesce-node:32021/metrics").Node()
	node.Name = "coalesce-node"
	node.Status.Addresses[0].Address = "coalesce-node"
	target, err := collector.targetForNode(node)
	if err != nil {
		t.Fatalf("targetForNode: %v", err)
	}
	collector.mu.Lock()
	collector.targets[node.Name] = target
	collector.mu.Unlock()
	if !collector.enqueue(target) {
		t.Fatal("initial enqueue was suppressed")
	}
	<-started
	for i := 0; i < 100; i++ {
		collector.enqueue(target)
	}
	close(release)
	waitFor(t, time.Second, func() bool {
		return collector.snapshotForNode(node).State == snapshotFresh
	})
	if got := requests.Load(); got != 1 {
		t.Fatalf("exporter requests = %d, want 1 coalesced request", got)
	}
}

func TestCollectorQueueIsBounded(t *testing.T) {
	cfg := testConfig(t)
	cfg.Workers = 1
	cfg.QueueSize = 1
	cfg.SnapshotTTL = time.Hour
	cfg.RefreshInterval = 30 * time.Minute
	started := make(chan struct{})
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		select {
		case <-started:
		default:
			close(started)
		}
		<-req.Context().Done()
		return nil, req.Context().Err()
	})}
	collector, err := newCollector(context.Background(), cfg, client)
	if err != nil {
		t.Fatalf("newCollector: %v", err)
	}
	t.Cleanup(collector.Close)
	targets := make([]scrapeTarget, 3)
	collector.mu.Lock()
	for i := range targets {
		name := fmt.Sprintf("queue-%d", i)
		targets[i] = scrapeTarget{
			NodeName: name, Identity: nodeIdentity{Name: name}, Endpoint: fmt.Sprintf("http://%s:32021/metrics", name),
			Profile: "auto", ProfileVersion: collector.registry.snapshot().Version,
			Generation: uint64(i + 1), Key: name, SeenAt: time.Now(),
		}
		collector.targets[name] = targets[i]
	}
	collector.mu.Unlock()
	if !collector.enqueue(targets[0]) {
		t.Fatal("first enqueue failed")
	}
	<-started
	if !collector.enqueue(targets[1]) {
		t.Fatal("second enqueue should fill the queue")
	}
	if collector.enqueue(targets[2]) {
		t.Fatal("third enqueue succeeded despite queue capacity 1")
	}
}

func TestCollectorBackoffAndCircuitState(t *testing.T) {
	cfg := testConfig(t)
	cfg.CircuitFailures = 2
	cfg.CircuitOpenDuration = time.Minute
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("exporter unavailable")
	})}
	collector, err := newCollector(context.Background(), cfg, client)
	if err != nil {
		t.Fatalf("newCollector: %v", err)
	}
	t.Cleanup(collector.Close)
	node := nodeInfoWithEndpoint("http://backoff-node:32021/metrics").Node()
	node.Name = "backoff-node"
	node.Status.Addresses[0].Address = "backoff-node"
	for i := 0; i < 2; i++ {
		if err := collector.refreshNode(context.Background(), node); err == nil {
			t.Fatal("refreshNode error = nil, want failure")
		}
	}
	target, _ := collector.targetForNode(node)
	collector.mu.RLock()
	failure := collector.failures[target.Key]
	collector.mu.RUnlock()
	if failure.Count != 2 || !failure.OpenUntil.After(time.Now()) {
		t.Fatalf("failure state = %+v, want count 2 and open circuit", failure)
	}
	if collector.enqueue(target) {
		t.Fatal("refresh was enqueued while circuit was open")
	}
}

func TestMalformedTelemetryIsRejected(t *testing.T) {
	tests := map[string]string{
		"NaN":                 strings.Replace(iluvatarMetrics, "} 0\n# HELP ix_mem_free", "} NaN\n# HELP ix_mem_free", 1),
		"positive infinity":   strings.Replace(iluvatarMetrics, "} 0\n# HELP ix_mem_free", "} +Inf\n# HELP ix_mem_free", 1),
		"negative infinity":   strings.Replace(iluvatarMetrics, "} 0\n# HELP ix_mem_free", "} -Inf\n# HELP ix_mem_free", 1),
		"negative capacity":   strings.Replace(iluvatarMetrics, "} 32768\n# HELP ix_mem_used", "} -1\n# HELP ix_mem_used", 1),
		"invalid percentage":  strings.Replace(iluvatarMetrics, "} 0\n# HELP ix_mem_free", "} 101\n# HELP ix_mem_free", 1),
		"inconsistent memory": strings.Replace(iluvatarMetrics, "} 64\n# HELP ix_mem_utilization", "} 128\n# HELP ix_mem_utilization", 1),
		"missing temperature": removeMetricFamily(iluvatarMetrics, "ix_temperature"),
		"missing identity":    removeDeviceIdentity(iluvatarMetrics),
		"invalid health":      strings.Replace(genericNPUMetrics, "k3s_npu_health{npu=\"0\",npu_name=\"Ascend-0\",npu_uuid=\"NPU-0\"} 1", "k3s_npu_health{npu=\"0\",npu_name=\"Ascend-0\",npu_uuid=\"NPU-0\"} 2", 1),
	}
	for name, metrics := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := parseMetrics(strings.NewReader(metrics)); err == nil {
				t.Fatal("parseMetrics error = nil, want rejection")
			}
		})
	}
}

func TestCollectorRejectsOversizedBodyAndRedirect(t *testing.T) {
	t.Run("oversized", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, strings.Repeat("x", 2049))
		}))
		t.Cleanup(server.Close)
		plugin := newHTTPTestPlugin(t, server.Client(), time.Second)
		plugin.config.MaxResponseBytes = 2048
		plugin.collector.config.MaxResponseBytes = 2048
		node := nodeInfoForHTTPServer(t, server.URL, map[v1.ResourceName]int64{"iluvatar.com/gpu": 1})
		if err := plugin.collector.refreshNode(context.Background(), node.Node()); err == nil || !strings.Contains(err.Error(), "exceeds") {
			t.Fatalf("refreshNode error = %v, want oversized response rejection", err)
		}
	})

	t.Run("redirect", func(t *testing.T) {
		destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, iluvatarMetrics)
		}))
		t.Cleanup(destination.Close)
		redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, destination.URL, http.StatusFound)
		}))
		t.Cleanup(redirect.Close)
		plugin := newHTTPTestPlugin(t, redirect.Client(), time.Second)
		node := nodeInfoForHTTPServer(t, redirect.URL, map[v1.ResourceName]int64{"iluvatar.com/gpu": 1})
		if err := plugin.collector.refreshNode(context.Background(), node.Node()); err == nil || !strings.Contains(err.Error(), "redirects are disabled") {
			t.Fatalf("refreshNode error = %v, want redirect rejection", err)
		}
	})
}

func TestCollectorUsesConfiguredTLSRoot(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, iluvatarMetrics)
	}))
	t.Cleanup(server.Close)
	certificate, err := x509.ParseCertificate(server.Certificate().Raw)
	if err != nil {
		t.Fatalf("ParseCertificate: %v", err)
	}
	caPath := t.TempDir() + "/exporter-ca.pem"
	if err := os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Raw}), 0600); err != nil {
		t.Fatalf("write CA: %v", err)
	}
	cfg := testConfig(t)
	cfg.Scheme = "https"
	cfg.AllowInsecureHTTP = false
	cfg.CAFile = caPath
	collector, err := newCollector(context.Background(), cfg, nil)
	if err != nil {
		t.Fatalf("newCollector: %v", err)
	}
	t.Cleanup(collector.Close)
	node := nodeInfoForHTTPServer(t, server.URL, map[v1.ResourceName]int64{"iluvatar.com/gpu": 1})
	if err := collector.refreshNode(context.Background(), node.Node()); err != nil {
		t.Fatalf("TLS refreshNode: %v", err)
	}
}

func TestInvalidTargetMetadataIsRejected(t *testing.T) {
	plugin := newTestPlugin(t, iluvatarMetrics)
	node := nodeInfoWithEndpoint("http://invalid-node:32021/metrics").Node()
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{name: "port", key: AnnotationExporterPort, value: "70000"},
		{name: "path", key: AnnotationExporterPath, value: "/metrics?token=secret"},
		{name: "scheme", key: AnnotationExporterScheme, value: "file"},
		{name: "profile", key: AnnotationExporterProfile, value: "unknown"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			copy := node.DeepCopy()
			copy.Annotations[tt.key] = tt.value
			if _, err := plugin.collector.targetForNode(copy); err == nil {
				t.Fatal("targetForNode error = nil, want rejection")
			}
		})
	}
}

func TestStatePoliciesHaveExplicitMissingAndStaleBehavior(t *testing.T) {
	cfg := testConfig(t)
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("fixture exporter unavailable")
	})}
	collector, err := newCollector(context.Background(), cfg, client)
	if err != nil {
		t.Fatalf("newCollector: %v", err)
	}
	t.Cleanup(collector.Close)
	plugin := &Plugin{config: cfg, collector: collector}
	node := nodeInfoWithEndpoint("http://policy-node:32021/metrics")
	node.Node().Name = "policy-node"
	node.Node().Status.Addresses[0].Address = "policy-node"

	for _, snapshotMode := range []string{"missing", "stale"} {
		t.Run(snapshotMode, func(t *testing.T) {
			for _, test := range []struct {
				policy      StatePolicy
				filterOK    bool
				expectScore int64
			}{
				{policy: StatePolicyStrict, filterOK: false, expectScore: 0},
				{policy: StatePolicyDegraded, filterOK: true, expectScore: plugin.config.DegradedScore},
				{policy: StatePolicyBestEffort, filterOK: true, expectScore: plugin.config.BestEffortScore},
			} {
				t.Run(string(test.policy), func(t *testing.T) {
					if snapshotMode == "stale" {
						target, targetErr := collector.targetForNode(node.Node())
						if targetErr != nil {
							t.Fatalf("targetForNode: %v", targetErr)
						}
						observedAt := time.Now().Add(-2 * cfg.SnapshotTTL)
						collector.store.transition(target, nodeResourceContext{}, observedAt)
						metrics := nodeMetrics{
							FetchedAt: observedAt, ValidUntil: observedAt.Add(cfg.SnapshotTTL),
						}
						if !collector.store.publish(target, metrics, observedAt, metrics.ValidUntil) {
							t.Fatal("publish stale snapshot = false")
						}
					} else {
						collector.store.remove(node.Node().Name)
					}
					pod := gpuPod()
					pod.Annotations = map[string]string{AnnotationStatePolicy: string(test.policy)}
					state := cycleStateForPod(t, plugin, pod)
					status := plugin.Filter(context.Background(), state, pod, node)
					if status.IsSuccess() != test.filterOK {
						t.Fatalf("Filter success = %v, want %v: %v", status.IsSuccess(), test.filterOK, status)
					}
					if status := plugin.PreScore(context.Background(), state, pod, []*framework.NodeInfo{node}); !status.IsSuccess() {
						t.Fatalf("PreScore: %v", status)
					}
					score, status := plugin.Score(context.Background(), state, pod, node)
					if !status.IsSuccess() || score != test.expectScore {
						t.Fatalf("Score = %d, status = %v, want %d", score, status, test.expectScore)
					}
				})
			}
		})
	}
}

func TestMissingOptionalDimensionsArePenalized(t *testing.T) {
	cfg := testConfig(t)
	base := deviceMetrics{
		Fields: map[deviceMetricField]struct{}{
			fieldGPUUtilization: {}, fieldMemoryFreeMiB: {}, fieldMemoryTotalMiB: {}, fieldTemperatureC: {},
		},
		GPUUtilization: 0, MemoryFreeMiB: 32768, MemoryTotalMiB: 32768, TemperatureC: 50,
	}
	complete := base
	complete.Fields = map[deviceMetricField]struct{}{
		fieldGPUUtilization: {}, fieldMemoryFreeMiB: {}, fieldMemoryTotalMiB: {}, fieldTemperatureC: {},
		fieldSMClockMHz: {}, fieldMemClockMHz: {}, fieldHealth: {},
	}
	complete.SMClockMHz = 1500
	complete.MemClockMHz = 1500
	complete.HealthKnown = true
	complete.Healthy = true
	if missing, full := deviceScore(base, cfg, cfg.MaxTemperatureC), deviceScore(complete, cfg, cfg.MaxTemperatureC); missing >= full || missing >= 100 {
		t.Fatalf("missing-dimension score %.1f, complete score %.1f; want explicit penalty and no implicit perfect score", missing, full)
	}
}

func TestMetricProfileFilesRejectUnknownAndIncompleteSchemas(t *testing.T) {
	for name, raw := range map[string]string{
		"unknown field":  `{"profiles":[],"unexpected":true}`,
		"missing fields": `{"profiles":[{"name":"incomplete","matchNames":["x"],"fields":{"gpu_utilization":{"names":["x"]}}}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			path := t.TempDir() + "/profiles.json"
			if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
				t.Fatalf("write fixture: %v", err)
			}
			if _, err := metricProfilesFromFile(path); err == nil {
				t.Fatal("metricProfilesFromFile error = nil, want schema rejection")
			}
		})
	}
}

func TestTypedConfigurationValidationAndPrecedence(t *testing.T) {
	tests := map[string]string{
		"unknown field":           `{"apiVersion":"gpustability.k3s.io/v1alpha1","kind":"K3SGPUStabilityArgs","unknown":true}`,
		"workers zero":            `{"apiVersion":"gpustability.k3s.io/v1alpha1","kind":"K3SGPUStabilityArgs","collector":{"workers":0}}`,
		"implicit insecure HTTP":  `{"apiVersion":"gpustability.k3s.io/v1alpha1","kind":"K3SGPUStabilityArgs","exporter":{"scheme":"http","allowInsecureHTTP":false}}`,
		"wrong version":           `{"apiVersion":"gpustability.k3s.io/v2","kind":"K3SGPUStabilityArgs"}`,
		"invalid scoring total":   `{"apiVersion":"gpustability.k3s.io/v1alpha1","kind":"K3SGPUStabilityArgs","policy":{"scoring":{"utilization":0,"memoryHeadroom":0,"thermalHeadroom":0,"energyHeadroom":0,"computeCapability":0,"health":0,"dataLocality":0,"fragmentation":0}}}`,
		"queue tracking disabled": `{"apiVersion":"gpustability.k3s.io/v1alpha1","kind":"K3SGPUStabilityArgs","queueing":{"maxTrackedPods":0}}`,
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := configFromArgs(&runtime.Unknown{Raw: []byte(raw)}); err == nil {
				t.Fatal("configFromArgs error = nil, want validation failure")
			}
		})
	}

	t.Setenv("K3S_GPU_EXPORTER_PORT", "9999")
	raw := `{"apiVersion":"gpustability.k3s.io/v1alpha1","kind":"K3SGPUStabilityArgs","exporter":{"port":"1234"}}`
	cfg, err := configFromArgs(&runtime.Unknown{Raw: []byte(raw)})
	if err != nil {
		t.Fatalf("configFromArgs: %v", err)
	}
	if cfg.ExporterPort != "1234" {
		t.Fatalf("ExporterPort = %q, want typed value 1234 over legacy env", cfg.ExporterPort)
	}
}

func TestExampleSchedulerConfigurationDecodesTypedArgs(t *testing.T) {
	raw, err := os.ReadFile("../../../../docs/gpu-scheduler/kube-scheduler-gpu-stability.yaml")
	if err != nil {
		t.Fatalf("read example scheduler config: %v", err)
	}
	decoded, _, err := schedulerconfigscheme.Codecs.UniversalDecoder().Decode(raw, nil, nil)
	if err != nil {
		t.Fatalf("decode example scheduler config: %v", err)
	}
	configuration, ok := decoded.(*schedulerconfig.KubeSchedulerConfiguration)
	if !ok {
		t.Fatalf("decoded object type = %T", decoded)
	}
	if len(configuration.Profiles) != 1 || configuration.Profiles[0].SchedulerName != "space-compute-scheduler" {
		t.Fatalf("scheduler profiles = %+v, want only the isolated space profile", configuration.Profiles)
	}
	var args runtime.Object
	for _, pluginConfig := range configuration.Profiles[0].PluginConfig {
		if pluginConfig.Name == Name {
			args = pluginConfig.Args
			break
		}
	}
	if args == nil {
		t.Fatal("space profile has no K3SGPUStability pluginConfig")
	}
	if _, err := configFromArgs(args); err != nil {
		t.Fatalf("decode typed plugin args from example: %v", err)
	}
}

func TestInvalidWorkloadAnnotationsFailPreFilter(t *testing.T) {
	plugin := newTestPlugin(t, iluvatarMetrics)
	for name, annotations := range map[string]map[string]string{
		"NaN memory":        {AnnotationMinFreeMemoryMiB: "NaN"},
		"invalid policy":    {AnnotationStatePolicy: "optimistic"},
		"mixed min devices": {AnnotationMinEligible: "2"},
	} {
		t.Run(name, func(t *testing.T) {
			pod := podWithAcceleratorRequests(map[v1.ResourceName]int64{"hygon.com/dcu": 1, "huawei.com/npu": 1})
			pod.Annotations = annotations
			state := framework.NewCycleState()
			if _, status := plugin.PreFilter(context.Background(), state, pod); status.IsSuccess() {
				t.Fatal("PreFilter status = success, want invalid annotation rejection")
			}
		})
	}
}

func TestConcurrentSnapshotRefreshAndScheduling(t *testing.T) {
	plugin := newTestPlugin(t, iluvatarMetrics)
	node := nodeInfoWithEndpoint("http://gpu-node:32021/metrics")
	pod := gpuPod()
	warmNode(t, plugin, node)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				state := framework.NewCycleState()
				_, _ = plugin.PreFilter(context.Background(), state, pod)
				_ = plugin.Filter(context.Background(), state, pod, node)
				_ = plugin.PreScore(context.Background(), state, pod, []*framework.NodeInfo{node})
				_, _ = plugin.Score(context.Background(), state, pod, node)
			}
		}()
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 25; j++ {
				_ = plugin.collector.refreshNode(context.Background(), node.Node())
			}
		}()
	}
	wg.Wait()
}

func FuzzParseMetricsRejectsUntrustedInput(f *testing.F) {
	f.Add(iluvatarMetrics)
	f.Add("# TYPE ix_gpu_utilization gauge\nix_gpu_utilization NaN\n")
	f.Add("")
	f.Fuzz(func(t *testing.T, raw string) {
		_, _ = parseMetrics(strings.NewReader(raw))
	})
}

func FuzzTypedArgsDecoder(f *testing.F) {
	f.Add(`{"apiVersion":"gpustability.k3s.io/v1alpha1","kind":"K3SGPUStabilityArgs"}`)
	f.Add(`{"unknown":true}`)
	f.Add("")
	f.Fuzz(func(t *testing.T, raw string) {
		_, _ = configFromArgs(&runtime.Unknown{Raw: []byte(raw)})
	})
}

func metricsResponse(raw string) *http.Response {
	return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader(raw))}
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition was not satisfied before timeout")
}

func nodeInfoWithResourceCapacities(nodeName, endpoint string, capacities map[v1.ResourceName]int64) *framework.NodeInfo {
	nodeInfo := framework.NewNodeInfo()
	allocatable := v1.ResourceList{}
	for name, count := range capacities {
		allocatable[name] = *resource.NewQuantity(count, resource.DecimalSI)
	}
	nodeInfo.SetNode(&v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: nodeName},
		Status: v1.NodeStatus{
			Addresses:   []v1.NodeAddress{{Type: v1.NodeInternalIP, Address: endpointHost(endpoint)}},
			Allocatable: allocatable,
		},
	})
	return nodeInfo
}

func genericAcceleratorMetricsWithFree(count int, freeMiB int64) string {
	return strings.ReplaceAll(genericAcceleratorMetricsForDevices(count), " 60000\n", fmt.Sprintf(" %d\n", freeMiB))
}

func genericGPUAndNPUMetrics(gpus, npus int, freeMiB int64) string {
	var result strings.Builder
	writeClass := func(prefix, label, namePrefix string, count int) {
		for _, metric := range []struct {
			suffix string
			value  float64
		}{
			{suffix: "utilization_percent", value: 10},
			{suffix: "memory_free_mib", value: float64(freeMiB)},
			{suffix: "memory_total_mib", value: 32768},
			{suffix: "temperature_celsius", value: 60},
		} {
			metricName := "k3s_" + prefix + "_" + metric.suffix
			fmt.Fprintf(&result, "# HELP %s fixture\n# TYPE %s gauge\n", metricName, metricName)
			for i := 0; i < count; i++ {
				fmt.Fprintf(&result, "%s{%s=\"%d\",%s_name=\"%s-%d\",%s_uuid=\"%s-%d\"} %.0f\n", metricName, label, i, prefix, namePrefix, i, prefix, strings.ToUpper(prefix), i, metric.value)
			}
		}
	}
	writeClass("gpu", "gpu", "gpu", gpus)
	writeClass("npu", "npu", "npu", npus)
	return result.String()
}

func removeMetricFamily(raw, metricName string) string {
	lines := strings.Split(raw, "\n")
	result := lines[:0]
	for _, line := range lines {
		if strings.Contains(line, metricName) {
			continue
		}
		result = append(result, line)
	}
	return strings.Join(result, "\n")
}

func removeDeviceIdentity(raw string) string {
	lines := strings.Split(raw, "\n")
	for i, line := range lines {
		if strings.HasPrefix(line, "ix_") {
			metricName := line[:strings.IndexByte(line, '{')]
			value := line[strings.LastIndexByte(line, '}')+1:]
			lines[i] = metricName + `{name="MRC-V100-0x18"}` + value
		}
	}
	return strings.Join(lines, "\n")
}
