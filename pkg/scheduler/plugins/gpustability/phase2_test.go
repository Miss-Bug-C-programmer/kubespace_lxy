package gpustability

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestBuiltInAndDeclarativeProfilesThroughProductionCollection(t *testing.T) {
	tests := []struct {
		name, fixture, profile, resourceName, profileFile string
		class                                             DeviceClass
	}{
		{name: "iluvatar ix", fixture: "iluvatar.prom", profile: "iluvatar", resourceName: "iluvatar.com/gpu", class: DeviceClassGPU},
		{name: "dcgm", fixture: "dcgm.prom", profile: "dcgm", resourceName: "nvidia.com/gpu", class: DeviceClassGPU},
		{name: "rocm", fixture: "rocm.prom", profile: "rocm", resourceName: "amd.com/gpu", class: DeviceClassGPU},
		{name: "generic npu", fixture: "generic-npu.prom", profile: "generic", resourceName: "huawei.com/npu", class: DeviceClassNPU},
		{name: "configuration only vendor", fixture: "vendorx.prom", profile: "vendorx", resourceName: "example.com/fpga", class: DeviceClassFPGA, profileFile: "testdata/profiles/vendorx.json"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body := readPhase2Fixture(t, test.fixture)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(body))
			}))
			defer server.Close()

			args := defaultArgs()
			configureArgsForServer(t, &args, server.URL)
			args.Exporter.Profile = test.profile
			if test.profileFile != "" {
				args.ProfileSource.File = test.profileFile
				args.Resources = append(args.Resources, ResourceMappingArgs{
					Name: test.resourceName, Class: string(test.class), Profiles: []string{test.profile},
				})
			}
			cfg, err := validateAndConvertArgs(args)
			if err != nil {
				t.Fatalf("validate config: %v", err)
			}
			collector, err := newCollector(context.Background(), cfg, server.Client())
			if err != nil {
				t.Fatalf("newCollector: %v", err)
			}
			defer collector.Close()
			node := phase2Node(t, server.URL, "profile-node", "profile-uid", test.resourceName)
			if err := collector.refreshNode(context.Background(), node); err != nil {
				t.Fatalf("refreshNode: %v", err)
			}
			snapshot := collector.snapshotForNode(node)
			if snapshot.State != snapshotFresh || snapshot.Metrics.Profile != test.profile {
				t.Fatalf("snapshot = state %q profile %q, want fresh %q: %s", snapshot.State, snapshot.Metrics.Profile, test.profile, snapshot.Reason)
			}
			if len(snapshot.Metrics.Devices) == 0 || snapshot.Metrics.Devices[0].Class != test.class {
				t.Fatalf("devices = %+v, want class %q", snapshot.Metrics.Devices, test.class)
			}
		})
	}
}

func TestNodeAddressResolutionPrecedenceAndFamilies(t *testing.T) {
	tests := []struct {
		name, family, want string
		addresses          []v1.NodeAddress
		addressTypes       []v1.NodeAddressType
		wantError          string
	}{
		{
			name: "internal before external", family: "ipv4", want: "10.0.0.4",
			addressTypes: []v1.NodeAddressType{v1.NodeInternalIP, v1.NodeExternalIP},
			addresses:    []v1.NodeAddress{{Type: v1.NodeExternalIP, Address: "198.51.100.4"}, {Type: v1.NodeInternalIP, Address: "10.0.0.4"}},
		},
		{
			name: "prefer ipv6", family: "ipv6", want: "2001:db8::4", addressTypes: []v1.NodeAddressType{v1.NodeInternalIP},
			addresses: []v1.NodeAddress{{Type: v1.NodeInternalIP, Address: "10.0.0.4"}, {Type: v1.NodeInternalIP, Address: "2001:db8::4"}},
		},
		{
			name: "ipv4 fallback when ipv6 absent", family: "ipv6", want: "10.0.0.4", addressTypes: []v1.NodeAddressType{v1.NodeInternalIP},
			addresses: []v1.NodeAddress{{Type: v1.NodeInternalIP, Address: "10.0.0.4"}},
		},
		{
			name: "ambiguous preferred family", family: "ipv4", addressTypes: []v1.NodeAddressType{v1.NodeInternalIP},
			addresses: []v1.NodeAddress{{Type: v1.NodeInternalIP, Address: "10.0.0.4"}, {Type: v1.NodeInternalIP, Address: "10.0.0.5"}},
			wantError: "ambiguous",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			node := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "address-node"}, Status: v1.NodeStatus{Addresses: test.addresses}}
			got, err := nodeAddress(node, test.addressTypes, test.family)
			if test.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantError) {
					t.Fatalf("nodeAddress error = %v, want containing %q", err, test.wantError)
				}
				return
			}
			if err != nil || got != test.want {
				t.Fatalf("nodeAddress = %q, %v, want %q", got, err, test.want)
			}
		})
	}

	cfg := testConfig(t)
	collector, err := newCollector(context.Background(), cfg, &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return metricsResponse(iluvatarMetrics), nil
	})})
	if err != nil {
		t.Fatalf("newCollector: %v", err)
	}
	defer collector.Close()
	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "ipv6-node", UID: "ipv6-uid"},
		Status:     v1.NodeStatus{Addresses: []v1.NodeAddress{{Type: v1.NodeInternalIP, Address: "2001:db8::4"}}},
	}
	target, err := collector.targetForNode(node)
	if err != nil {
		t.Fatalf("targetForNode: %v", err)
	}
	if !strings.Contains(target.Endpoint, "[2001:db8::4]:32021") {
		t.Fatalf("IPv6 endpoint = %q, want bracketed host", target.Endpoint)
	}
	moving := node.DeepCopy()
	moving.Name = "internal-ip-change"
	moving.UID = "internal-ip-change-uid"
	moving.Status.Addresses[0].Address = "10.0.0.4"
	firstTarget, _, err := collector.ensureTarget(moving)
	if err != nil {
		t.Fatalf("first InternalIP target: %v", err)
	}
	moving.Status.Addresses[0].Address = "10.0.0.5"
	secondTarget, changed, err := collector.ensureTarget(moving)
	if err != nil || !changed || secondTarget.Generation <= firstTarget.Generation || !strings.Contains(secondTarget.Endpoint, "10.0.0.5") {
		t.Fatalf("InternalIP change target = %+v changed=%v err=%v, old=%+v", secondTarget, changed, err, firstTarget)
	}
}

func TestTargetChangesInvalidateOldGeneration(t *testing.T) {
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(iluvatarMetrics))
	}))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(strings.Replace(iluvatarMetrics, "} 70\n# HELP ix_power_usage", "} 60\n# HELP ix_power_usage", 1)))
	}))
	defer second.Close()

	args := defaultArgs()
	configureArgsForServer(t, &args, first.URL)
	args.Exporter.Profile = "iluvatar"
	cfg, err := validateAndConvertArgs(args)
	if err != nil {
		t.Fatalf("validate config: %v", err)
	}
	collector, err := newCollector(context.Background(), cfg, first.Client())
	if err != nil {
		t.Fatalf("newCollector: %v", err)
	}
	defer collector.Close()

	node := phase2Node(t, first.URL, "moving-node", "moving-uid", "iluvatar.com/gpu")
	if err := collector.refreshNode(context.Background(), node); err != nil {
		t.Fatalf("initial refresh: %v", err)
	}
	oldTarget, err := collector.targetForNode(node)
	if err != nil {
		t.Fatalf("old target: %v", err)
	}
	setNodeServer(t, node, second.URL)
	newTarget, changed, err := collector.ensureTarget(node)
	if err != nil || !changed {
		t.Fatalf("ensure changed target = %+v, %v, want change", newTarget, err)
	}
	if newTarget.Generation <= oldTarget.Generation || newTarget.Key == oldTarget.Key {
		t.Fatalf("target generations old=%+v new=%+v", oldTarget, newTarget)
	}
	if snapshot := collector.snapshotForNode(node); snapshot.State != snapshotMissing {
		t.Fatalf("snapshot after endpoint change = %q, want missing", snapshot.State)
	}
	if collector.store.publish(oldTarget, nodeMetrics{Profile: "iluvatar"}, time.Now(), time.Now().Add(time.Minute)) {
		t.Fatal("obsolete endpoint response overwrote the new generation")
	}
	if err := collector.refreshNode(context.Background(), node); err != nil {
		t.Fatalf("new endpoint refresh: %v", err)
	}
	snapshot := collector.snapshotForNode(node)
	if snapshot.State != snapshotFresh || snapshot.TargetGeneration != newTarget.Generation || snapshot.Metrics.Endpoint != newTarget.Endpoint {
		t.Fatalf("new snapshot = %+v, want target generation %d endpoint %q", snapshot, newTarget.Generation, newTarget.Endpoint)
	}
}

func TestTargetChangeCancelsInFlightOldEndpoint(t *testing.T) {
	started := make(chan struct{})
	canceled := make(chan struct{})
	oldServer := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		close(started)
		<-request.Context().Done()
		close(canceled)
	}))
	defer oldServer.Close()
	newServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(iluvatarMetrics))
	}))
	defer newServer.Close()
	args := defaultArgs()
	configureArgsForServer(t, &args, oldServer.URL)
	args.Exporter.Profile = "iluvatar"
	args.Exporter.Timeout = "5s"
	cfg, err := validateAndConvertArgs(args)
	if err != nil {
		t.Fatalf("validate config: %v", err)
	}
	collector, err := newCollector(context.Background(), cfg, oldServer.Client())
	if err != nil {
		t.Fatalf("newCollector: %v", err)
	}
	defer collector.Close()
	node := phase2Node(t, oldServer.URL, "cancel-old-node", "cancel-old-uid", "iluvatar.com/gpu")
	collector.observeNode(node)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("old endpoint collection did not start")
	}
	setNodeServer(t, node, newServer.URL)
	collector.observeNode(node)
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("target change did not cancel the old endpoint request")
	}
	if err := collector.refreshNode(context.Background(), node); err != nil {
		t.Fatalf("new endpoint refresh: %v", err)
	}
}

func TestNodeDeletionRecreationProfileChangeAndInvalidMetadata(t *testing.T) {
	cfg := testConfig(t)
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return metricsResponse(iluvatarMetrics), nil
	})}
	collector, err := newCollector(context.Background(), cfg, client)
	if err != nil {
		t.Fatalf("newCollector: %v", err)
	}
	defer collector.Close()
	node := nodeInfoWithEndpoint("http://lifecycle-node:32021/metrics").Node()
	node.Name = "lifecycle-node"
	node.UID = "uid-one"
	node.Status.Addresses[0].Address = "lifecycle-node"
	old, _, err := collector.ensureTarget(node)
	if err != nil {
		t.Fatalf("initial target: %v", err)
	}

	profileChanged := node.DeepCopy()
	profileChanged.Annotations[AnnotationExporterProfile] = "dcgm"
	changedTarget, changed, err := collector.ensureTarget(profileChanged)
	if err != nil || !changed || changedTarget.Generation <= old.Generation {
		t.Fatalf("profile change target = %+v changed=%v err=%v", changedTarget, changed, err)
	}

	collector.deleteNode(identityForNode(profileChanged))
	if collector.store.len() != 0 {
		t.Fatalf("snapshot count after delete = %d, want 0", collector.store.len())
	}
	recreated := node.DeepCopy()
	recreated.UID = "uid-two"
	recreatedTarget, _, err := collector.ensureTarget(recreated)
	if err != nil {
		t.Fatalf("recreated target: %v", err)
	}
	collector.deleteNode(nodeIdentity{Name: node.Name, UID: types.UID("uid-one")})
	if _, ok := collector.targets[node.Name]; !ok {
		t.Fatal("old UID deletion removed the recreated node")
	}
	if collector.store.publish(changedTarget, nodeMetrics{}, time.Now(), time.Now().Add(time.Minute)) {
		t.Fatal("old UID/profile generation published after recreation")
	}
	if recreatedTarget.Identity.UID != "uid-two" {
		t.Fatalf("recreated target UID = %q", recreatedTarget.Identity.UID)
	}

	invalid := recreated.DeepCopy()
	invalid.Annotations[AnnotationExporterPath] = "https://attacker.invalid/metrics"
	collector.observeNode(invalid)
	collector.mu.RLock()
	_, exists := collector.targets[invalid.Name]
	collector.mu.RUnlock()
	if exists || collector.store.len() != 0 {
		t.Fatal("invalid metadata retained a target or snapshot")
	}
}

func TestProfileRegistryReloadIsAtomicAndLastKnownGood(t *testing.T) {
	original, err := os.ReadFile("testdata/profiles/vendorx.json")
	if err != nil {
		t.Fatalf("read profile fixture: %v", err)
	}
	path := t.TempDir() + "/profiles.json"
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatalf("write initial profile: %v", err)
	}
	args := defaultArgs()
	args.ProfileSource.File = path
	args.Resources = append(args.Resources, ResourceMappingArgs{Name: "example.com/fpga", Class: "fpga", Profiles: []string{"vendorx"}})
	cfg, err := validateAndConvertArgs(args)
	if err != nil {
		t.Fatalf("validate config: %v", err)
	}
	registry, err := newProfileRegistry(cfg)
	if err != nil {
		t.Fatalf("newProfileRegistry: %v", err)
	}
	initial := registry.snapshot()
	updated := strings.ReplaceAll(string(original), "vendorx_busy", "vendorx_busy_v2")
	if err := os.WriteFile(path, []byte(updated), 0o600); err != nil {
		t.Fatalf("write valid update: %v", err)
	}
	changed, err := registry.reload()
	if err != nil || !changed || registry.snapshot().Version != initial.Version+1 {
		t.Fatalf("valid reload changed=%v err=%v version=%d", changed, err, registry.snapshot().Version)
	}
	lastValid := registry.snapshot()
	if err := os.WriteFile(path, []byte(`{"apiVersion":"broken"}`), 0o600); err != nil {
		t.Fatalf("write invalid update: %v", err)
	}
	if changed, err := registry.reload(); err == nil || changed {
		t.Fatalf("invalid reload changed=%v err=%v", changed, err)
	}
	if registry.snapshot() != lastValid {
		t.Fatal("invalid reload replaced the last-known-good registry")
	}
}

func TestConcurrentProfileReloadAndParse(t *testing.T) {
	original := readPhase2Profile(t)
	path := t.TempDir() + "/profiles.json"
	if err := os.WriteFile(path, []byte(original), 0o600); err != nil {
		t.Fatalf("write profile: %v", err)
	}
	args := defaultArgs()
	args.ProfileSource.File = path
	args.Resources = append(args.Resources, ResourceMappingArgs{Name: "example.com/fpga", Class: "fpga", Profiles: []string{"vendorx"}})
	cfg, err := validateAndConvertArgs(args)
	if err != nil {
		t.Fatalf("validate config: %v", err)
	}
	registry, err := newProfileRegistry(cfg)
	if err != nil {
		t.Fatalf("newProfileRegistry: %v", err)
	}
	metrics := readPhase2Fixture(t, "vendorx.prom")
	updated := strings.ReplaceAll(original, "vendorx_busy", "vendorx_busy_v2")
	updatedMetrics := strings.ReplaceAll(metrics, "vendorx_busy", "vendorx_busy_v2")

	var wg sync.WaitGroup
	for worker := 0; worker < 6; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for iteration := 0; iteration < 50; iteration++ {
				body := metrics
				if (worker+iteration)%2 == 0 {
					body = updatedMetrics
				}
				_, _ = registry.parse(strings.NewReader(body), "vendorx", defaultParserLimits())
			}
		}(worker)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for iteration := 0; iteration < 30; iteration++ {
			body := original
			if iteration%2 == 0 {
				body = updated
			}
			if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
				t.Errorf("write reload profile: %v", err)
				return
			}
			_, _ = registry.reload()
		}
	}()
	wg.Wait()
	if registry.snapshot() == nil || registry.snapshot().Version == 0 {
		t.Fatal("concurrent reload lost the active registry")
	}
}

func TestProfilesRejectDuplicatesAmbiguityAndParserLimits(t *testing.T) {
	duplicate := strings.Replace(readPhase2Profile(t), `"name": "vendorx"`, `"name": "iluvatar"`, 1)
	path := t.TempDir() + "/duplicate.json"
	if err := os.WriteFile(path, []byte(duplicate), 0o600); err != nil {
		t.Fatalf("write duplicate profile: %v", err)
	}
	args := defaultArgs()
	args.ProfileSource.File = path
	if _, err := validateAndConvertArgs(args); err == nil || !strings.Contains(err.Error(), "duplicate metric profile") {
		t.Fatalf("duplicate config error = %v", err)
	}

	if _, err := parseMetricsWithProfiles(strings.NewReader(iluvatarMetrics+dcgmMetrics), defaultMetricProfile, registeredMetricProfiles()); err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("ambiguous auto-detection error = %v", err)
	}
	limits := defaultParserLimits()
	limits.MaxSamples = 1
	if _, err := parseMetricsWithProfilesAndLimits(strings.NewReader(iluvatarMetrics), "iluvatar", registeredMetricProfiles(), limits); err == nil || !strings.Contains(err.Error(), "sample count") {
		t.Fatalf("sample limit error = %v", err)
	}
	limits = defaultParserLimits()
	limits.MaxMetricFamilies = 1
	if _, err := parseMetricsWithProfilesAndLimits(strings.NewReader(iluvatarMetrics), "iluvatar", registeredMetricProfiles(), limits); err == nil || !strings.Contains(err.Error(), "family count") {
		t.Fatalf("family limit error = %v", err)
	}
}

func TestCollectorScaleBoundsAndLifecycle(t *testing.T) {
	for _, count := range []int{100, 1000} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(iluvatarMetrics))
			}))
			defer server.Close()
			cfg := phase2ScaleConfig(t, server.URL, count)
			baselineGoroutines := runtime.NumGoroutine()
			collector, err := newCollector(context.Background(), cfg, server.Client())
			if err != nil {
				t.Fatalf("newCollector: %v", err)
			}
			nodes := phase2ScaleNodes(t, server.URL, count)
			for _, node := range nodes {
				collector.observeNode(node)
			}
			waitFor(t, 15*time.Second, func() bool { return phase2ValidSnapshotCount(collector) == count })
			collector.mu.RLock()
			targets, pending := len(collector.targets), len(collector.pending)
			collector.mu.RUnlock()
			if targets != count || collector.store.len() != count {
				t.Fatalf("targets=%d snapshots=%d, want %d", targets, collector.store.len(), count)
			}
			if pending > cfg.QueueSize || len(collector.queue) > cfg.QueueSize {
				t.Fatalf("pending=%d queue=%d exceed queue bound %d", pending, len(collector.queue), cfg.QueueSize)
			}
			if delta := runtime.NumGoroutine() - baselineGoroutines; delta > cfg.Workers*5+20 {
				t.Fatalf("goroutine delta=%d exceeds worker-derived bound %d", delta, cfg.Workers*5+20)
			}
			for _, node := range nodes {
				collector.deleteNode(identityForNode(node))
			}
			if collector.store.len() != 0 {
				t.Fatalf("snapshots after deletion = %d", collector.store.len())
			}
			collector.Close()
			collector.mu.RLock()
			remainingTargets, remainingNodes, remainingPending := len(collector.targets), len(collector.nodes), len(collector.pending)
			collector.mu.RUnlock()
			if remainingTargets+remainingNodes+remainingPending+collector.store.len() != 0 {
				t.Fatalf("shutdown retained targets=%d nodes=%d pending=%d snapshots=%d", remainingTargets, remainingNodes, remainingPending, collector.store.len())
			}
		})
	}
}

func TestUnifiedSnapshotCarriesNodeResourceContext(t *testing.T) {
	plugin := newTestPlugin(t, iluvatarMetrics)
	nodeInfo := nodeInfoWithEndpointAndGPUCapacity("http://gpu-node:32021/metrics", 4)
	nodeInfo.AddPod(gpuPodWithRequestAndLimit(1))
	warmNode(t, plugin, nodeInfo)
	snapshot := plugin.collector.snapshotForNodeInfo(nodeInfo)
	if snapshot.Resources.Allocatable["iluvatar.com/gpu"] != 4 || snapshot.Resources.Requested["iluvatar.com/gpu"] != 1 {
		t.Fatalf("resource context = %+v, want allocatable=4 requested=1", snapshot.Resources)
	}
	before := snapshot.ObservedAt
	_ = plugin.collector.snapshotForNodeInfo(nodeInfo)
	after := plugin.collector.snapshotForNodeInfo(nodeInfo).ObservedAt
	if !after.Equal(before) {
		t.Fatalf("cache read changed observation time from %v to %v", before, after)
	}
}

func BenchmarkCollectorTargetDatasets(b *testing.B) {
	for _, count := range []int{100, 1000} {
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(iluvatarMetrics))
			}))
			defer server.Close()
			cfg := phase2ScaleConfig(b, server.URL, count)
			nodes := phase2ScaleNodes(b, server.URL, count)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				collector, err := newCollector(context.Background(), cfg, server.Client())
				if err != nil {
					b.Fatal(err)
				}
				for _, node := range nodes {
					collector.observeNode(node)
				}
				deadline := time.Now().Add(30 * time.Second)
				for phase2ValidSnapshotCount(collector) != count && time.Now().Before(deadline) {
					runtime.Gosched()
				}
				if got := phase2ValidSnapshotCount(collector); got != count {
					collector.Close()
					b.Fatalf("valid snapshots=%d, want %d", got, count)
				}
				collector.Close()
			}
		})
	}
}

type phase2TB interface {
	Helper()
	Fatalf(string, ...interface{})
}

func readPhase2Fixture(t phase2TB, name string) string {
	t.Helper()
	raw, err := os.ReadFile("testdata/fixtures/" + name)
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return string(raw)
}

func readPhase2Profile(t phase2TB) string {
	t.Helper()
	raw, err := os.ReadFile("testdata/profiles/vendorx.json")
	if err != nil {
		t.Fatalf("read vendor profile: %v", err)
	}
	return string(raw)
}

func configureArgsForServer(t phase2TB, args *GPUStabilityArgs, rawURL string) {
	t.Helper()
	parsed, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("parse server URL: %v", err)
	}
	_, port, err := net.SplitHostPort(parsed.Host)
	if err != nil {
		t.Fatalf("split server address: %v", err)
	}
	args.Exporter.Scheme = parsed.Scheme
	args.Exporter.Port = port
	args.Exporter.Path = "/metrics"
	args.Exporter.AllowInsecureHTTP = parsed.Scheme == "http"
	args.Collector.SnapshotTTL = "1m"
	args.Collector.RefreshInterval = "30s"
}

func phase2Node(t phase2TB, serverURL, name string, uid types.UID, resourceName string) *v1.Node {
	t.Helper()
	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: uid, Annotations: map[string]string{}},
		Status: v1.NodeStatus{Allocatable: v1.ResourceList{
			v1.ResourceName(resourceName): resource.MustParse("1"),
		}},
	}
	setNodeServer(t, node, serverURL)
	return node
}

func setNodeServer(t phase2TB, node *v1.Node, serverURL string) {
	t.Helper()
	parsed, err := url.Parse(serverURL)
	if err != nil {
		t.Fatalf("parse server URL: %v", err)
	}
	host, port, err := net.SplitHostPort(parsed.Host)
	if err != nil {
		t.Fatalf("split server address: %v", err)
	}
	node.Status.Addresses = []v1.NodeAddress{{Type: v1.NodeInternalIP, Address: host}}
	if node.Annotations == nil {
		node.Annotations = map[string]string{}
	}
	node.Annotations[AnnotationExporterScheme] = parsed.Scheme
	node.Annotations[AnnotationExporterPort] = port
	node.Annotations[AnnotationExporterPath] = "/metrics"
}

func phase2ScaleConfig(t phase2TB, serverURL string, count int) Config {
	t.Helper()
	args := defaultArgs()
	configureArgsForServer(t, &args, serverURL)
	args.Exporter.Profile = "iluvatar"
	args.Collector.Workers = 8
	args.Collector.QueueSize = count
	args.Collector.CacheMaxEntries = count
	cfg, err := validateAndConvertArgs(args)
	if err != nil {
		t.Fatalf("scale config: %v", err)
	}
	return cfg
}

func phase2ScaleNodes(t phase2TB, serverURL string, count int) []*v1.Node {
	t.Helper()
	nodes := make([]*v1.Node, count)
	for i := range nodes {
		name := fmt.Sprintf("scale-node-%04d", i)
		nodes[i] = phase2Node(t, serverURL, name, types.UID("uid-"+name), "iluvatar.com/gpu")
	}
	return nodes
}

func phase2ValidSnapshotCount(collector *collector) int {
	collector.store.mu.RLock()
	defer collector.store.mu.RUnlock()
	count := 0
	for _, snapshot := range collector.store.records {
		if !snapshot.ObservedAt.IsZero() && snapshot.CollectionError == "" {
			count++
		}
	}
	return count
}

func TestConcurrentTargetReloadAndSnapshotAccess(t *testing.T) {
	cfg := testConfig(t)
	collector, err := newCollector(context.Background(), cfg, &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return metricsResponse(iluvatarMetrics), nil
	})})
	if err != nil {
		t.Fatalf("newCollector: %v", err)
	}
	defer collector.Close()
	node := nodeInfoWithEndpoint("http://race-node:32021/metrics").Node()
	node.Name = "race-node"
	node.UID = "race-uid"
	node.Status.Addresses[0].Address = "race-node"

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for iteration := 0; iteration < 50; iteration++ {
				copy := node.DeepCopy()
				if (worker+iteration)%2 == 0 {
					copy.Annotations[AnnotationExporterProfile] = "iluvatar"
				}
				collector.observeNode(copy)
				_ = collector.snapshotForNode(copy)
			}
		}(i)
	}
	wg.Wait()
}
