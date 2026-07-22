package gpustability

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestIluvatarMultiDeviceAccuracyAndConsistency(t *testing.T) {
	raw := readPhase2Fixture(t, "iluvatar-2gpu.prom")
	metrics, err := parseMetricsForProfile(strings.NewReader(raw), "iluvatar")
	if err != nil {
		t.Fatal(err)
	}
	if metrics.GPUCount != 2 || metrics.MemoryTotalMiB != 65536 || metrics.MemoryFreeMiB != 46000 {
		t.Fatalf("aggregate count=%d total=%.0f free=%.0f", metrics.GPUCount, metrics.MemoryTotalMiB, metrics.MemoryFreeMiB)
	}
	if metrics.GPUUtilization != 33.5 || metrics.TemperatureC != 78 || metrics.SMClockMHz != 1400 || metrics.MemClockMHz != 1550 || metrics.PowerUsageW != 300 {
		t.Fatalf("aggregate metrics are inaccurate: %+v", metrics)
	}
	if metrics.Devices[0].UUID != "GPU-node-a-0000" || metrics.Devices[1].UUID != "GPU-node-a-0001" {
		t.Fatalf("stable device identities = %q, %q", metrics.Devices[0].UUID, metrics.Devices[1].UUID)
	}

	tests := map[string]string{
		"duplicate gauge":          raw + `\nix_temperature{gpu="0",name="MR-V100-32G",uuid="GPU-node-a-0000"} 62\n`,
		"identity alias collision": strings.Replace(raw, `ix_temperature{gpu="1",name="MR-V100-32G",uuid="GPU-node-a-0001"}`, `ix_temperature{gpu="0",name="MR-V100-32G",uuid="GPU-node-a-0001"}`, 1),
		"memory balance":           strings.Replace(raw, `ix_mem_used{gpu="0",name="MR-V100-32G",uuid="GPU-node-a-0000"} 2768`, `ix_mem_used{gpu="0",name="MR-V100-32G",uuid="GPU-node-a-0000"} 100`, 1),
		"memory utilization":       strings.Replace(raw, `ix_mem_utilization{gpu="0",name="MR-V100-32G",uuid="GPU-node-a-0000"} 8.45`, `ix_mem_utilization{gpu="0",name="MR-V100-32G",uuid="GPU-node-a-0000"} 80`, 1),
	}
	for name, input := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := parseMetricsForProfile(strings.NewReader(input), "iluvatar"); err == nil {
				t.Fatal("corrupt Iluvatar snapshot was accepted")
			}
		})
	}
}

func TestIluvatarDeviceLimitBoundsNormalizedInventory(t *testing.T) {
	raw := readPhase2Fixture(t, "iluvatar-2gpu.prom")
	limits := defaultParserLimits()
	limits.MaxDevices = 1
	if _, err := parseMetricsWithProfilesAndLimits(strings.NewReader(raw), "iluvatar", registeredMetricProfiles(), limits); err == nil || !strings.Contains(err.Error(), "device count") {
		t.Fatalf("device limit error = %v", err)
	}
}

func TestExporterTargetRejectsASCIIOnlyPunycodeHost(t *testing.T) {
	args := defaultArgs()
	args.Discovery.AddressTypes = []string{string(v1.NodeHostName)}
	cfg, err := validateAndConvertArgs(args)
	if err != nil {
		t.Fatalf("validate config: %v", err)
	}
	collector, err := newCollector(context.Background(), cfg, &http.Client{})
	if err != nil {
		t.Fatalf("new collector: %v", err)
	}
	defer collector.Close()
	node := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "punycode-node", UID: types.UID("punycode-uid")}, Status: v1.NodeStatus{Addresses: []v1.NodeAddress{{Type: v1.NodeHostName, Address: "xn--example-.com"}}}}
	if _, err := collector.resolveTarget(node); err == nil || (!strings.Contains(err.Error(), "Punycode") && !strings.Contains(err.Error(), "IDNA")) {
		t.Fatalf("malformed Punycode exporter host error = %v, want validation failure", err)
	}
}

func TestMultipleExporterAgentsAndSchedulableControlPlaneStayIsolated(t *testing.T) {
	type endpoint struct {
		server *httptest.Server
		node   *v1.Node
	}
	roles := []struct {
		name   string
		labels map[string]string
		temp   string
	}{
		{name: "agent-a", labels: map[string]string{"kubernetes.io/hostname": "agent-a"}, temp: "61"},
		{name: "agent-b", labels: map[string]string{"kubernetes.io/hostname": "agent-b"}, temp: "67"},
		{name: "master-a", labels: map[string]string{"node-role.kubernetes.io/control-plane": ""}, temp: "73"},
	}
	endpoints := make([]endpoint, 0, len(roles))
	for index, role := range roles {
		fixture := strings.ReplaceAll(iluvatarMetrics, "GPU-f2a72787", "GPU-"+role.name)
		fixture = strings.Replace(fixture, "} 70\n# HELP ix_power_usage", "} "+role.temp+"\n# HELP ix_power_usage", 1)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(fixture)) }))
		t.Cleanup(server.Close)
		node := phase5NodeForServer(t, server.URL, role.name, role.labels, 1)
		node.UID = types.UID("uid-" + role.name)
		endpoints = append(endpoints, endpoint{server: server, node: node})
		_ = index
	}

	args := defaultArgs()
	configureArgsForServer(t, &args, endpoints[0].server.URL)
	args.Exporter.Profile = "iluvatar"
	args.Collector.Workers = 3
	args.Collector.QueueSize = 8
	args.Collector.CacheMaxEntries = 8
	cfg, err := validateAndConvertArgs(args)
	if err != nil {
		t.Fatal(err)
	}
	collector, err := newCollector(context.Background(), cfg, endpoints[0].server.Client())
	if err != nil {
		t.Fatal(err)
	}
	defer collector.Close()
	for _, value := range endpoints {
		collector.observeNode(value.node)
	}
	waitFor(t, 5*time.Second, func() bool { return phase2ValidSnapshotCount(collector) == len(endpoints) })

	for index, value := range endpoints {
		snapshot := collector.snapshotForNode(value.node)
		if snapshot.State != snapshotFresh || snapshot.Metrics.Devices[0].UUID != "GPU-"+value.node.Name {
			t.Fatalf("node %s snapshot leaked or is unavailable: %+v", value.node.Name, snapshot)
		}
		wantTemperature, _ := strconv.ParseFloat(roles[index].temp, 64)
		if snapshot.Metrics.TemperatureC != wantTemperature {
			t.Fatalf("node %s temperature=%.0f, want %.0f", value.node.Name, snapshot.Metrics.TemperatureC, wantTemperature)
		}
	}
	if result := collector.snapshotForNode(endpoints[2].node); result.State != snapshotFresh {
		t.Fatalf("schedulable control-plane Node was treated differently: %+v", result)
	}

	// A failure on one agent retains only that agent's last validated snapshot;
	// the other agent and control-plane target remain independently fresh.
	endpoints[0].server.Close()
	if err := collector.refreshNode(context.Background(), endpoints[0].node); err == nil {
		t.Fatal("closed agent exporter refresh succeeded")
	}
	if result := collector.snapshotForNode(endpoints[1].node); result.State != snapshotFresh || result.Confidence != confidenceValidated {
		t.Fatalf("agent-b was contaminated by agent-a failure: %+v", result)
	}
	if result := collector.snapshotForNode(endpoints[2].node); result.State != snapshotFresh || result.Confidence != confidenceValidated {
		t.Fatalf("master-a was contaminated by agent-a failure: %+v", result)
	}
}

func TestBackgroundDiscoverySkipsOrdinaryMasterAndRejectsEndpointCollision(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(iluvatarMetrics)) }))
	defer server.Close()
	args := defaultArgs()
	configureArgsForServer(t, &args, server.URL)
	args.Exporter.Profile = "iluvatar"
	cfg, err := validateAndConvertArgs(args)
	if err != nil {
		t.Fatal(err)
	}
	collector, err := newCollector(context.Background(), cfg, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	defer collector.Close()

	ordinary := phase5NodeForServer(t, server.URL, "ordinary-master", map[string]string{"node-role.kubernetes.io/control-plane": ""}, 0)
	ordinary.Annotations = nil
	collector.observeNode(ordinary)
	collector.mu.RLock()
	_, ordinaryTargeted := collector.targets[ordinary.Name]
	collector.mu.RUnlock()
	if ordinaryTargeted {
		t.Fatal("ordinary control-plane Node consumed exporter collection capacity")
	}

	first := phase5NodeForServer(t, server.URL, "agent-a", nil, 1)
	second := phase5NodeForServer(t, server.URL, "agent-b", nil, 1)
	collector.observeNode(first)
	collector.observeNode(second)
	waitFor(t, 5*time.Second, func() bool { return phase2ValidSnapshotCount(collector) == 1 })
	collector.mu.RLock()
	_, firstExists := collector.targets[first.Name]
	_, secondExists := collector.targets[second.Name]
	collector.mu.RUnlock()
	if !firstExists || secondExists {
		t.Fatalf("duplicate endpoint ownership first=%v second=%v", firstExists, secondExists)
	}
}

func TestCollectorCacheBoundIncludesNewestProtectedTarget(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(iluvatarMetrics)) }))
	defer server.Close()
	args := defaultArgs()
	configureArgsForServer(t, &args, server.URL)
	args.Exporter.Profile = "iluvatar"
	args.Collector.CacheMaxEntries = 2
	cfg, err := validateAndConvertArgs(args)
	if err != nil {
		t.Fatal(err)
	}
	collector, err := newCollector(context.Background(), cfg, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	defer collector.Close()

	// Directly exercise target admission with unique endpoints so the test is
	// deterministic and does not weaken endpoint-collision protection.
	for index := 0; index < 3; index++ {
		node := phase5NodeForServer(t, server.URL, fmt.Sprintf("node-%d", index), nil, 1)
		node.Annotations[AnnotationExporterPort] = strconv.Itoa(32021 + index)
		_, _, _ = collector.ensureTarget(node)
	}
	collector.mu.RLock()
	count := len(collector.targets)
	collector.mu.RUnlock()
	if count != 2 || collector.store.len() != 2 {
		t.Fatalf("targets=%d snapshots=%d, want hard bound 2", count, collector.store.len())
	}
}

func TestMultiNodeLifecycleDoesNotGrowGoroutinesWithNodeCount(t *testing.T) {
	baseline := runtime.NumGoroutine()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(iluvatarMetrics)) }))
	defer server.Close()
	args := defaultArgs()
	configureArgsForServer(t, &args, server.URL)
	args.Exporter.Profile = "iluvatar"
	args.Collector.Workers = 4
	args.Collector.QueueSize = 64
	args.Collector.CacheMaxEntries = 64
	cfg, err := validateAndConvertArgs(args)
	if err != nil {
		t.Fatal(err)
	}
	collector, err := newCollector(context.Background(), cfg, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 50; index++ {
		node := phase5NodeForServer(t, server.URL, fmt.Sprintf("agent-%02d", index), nil, 1)
		node.Annotations[AnnotationExporterPort] = strconv.Itoa(33000 + index)
		_, _, _ = collector.ensureTarget(node)
	}
	if delta := runtime.NumGoroutine() - baseline; delta > cfg.Workers+12 {
		collector.Close()
		t.Fatalf("goroutine delta=%d, workers=%d", delta, cfg.Workers)
	}
	collector.Close()
}

func phase5NodeForServer(t *testing.T, rawURL, name string, labels map[string]string, capacity int64) *v1.Node {
	t.Helper()
	parsed, err := url.Parse(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	port := parsed.Port()
	quantity := resource.MustParse(strconv.FormatInt(capacity, 10))
	return &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID("uid-" + name), Labels: labels, Annotations: map[string]string{AnnotationExporterScheme: parsed.Scheme, AnnotationExporterPort: port, AnnotationExporterPath: "/metrics", AnnotationExporterProfile: "iluvatar"}},
		Status:     v1.NodeStatus{Addresses: []v1.NodeAddress{{Type: v1.NodeInternalIP, Address: parsed.Hostname()}}, Capacity: v1.ResourceList{"iluvatar.com/gpu": quantity}, Allocatable: v1.ResourceList{"iluvatar.com/gpu": quantity}},
	}
}
