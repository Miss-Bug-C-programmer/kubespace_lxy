package gpustability

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

func TestAnnotationOnlyPodAcrossOrdinaryNodesDoesNotCreateExporterTargets(t *testing.T) {
	cfg := testConfig(t)
	cfg.QueueSize = 1
	cfg.CacheMaxEntries = 4096
	cfg.RefreshInterval = time.Hour
	var networkCalls atomic.Int64
	collector, err := newCollector(context.Background(), cfg, &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		networkCalls.Add(1)
		return nil, fmt.Errorf("scheduler callback attempted exporter HTTP")
	})})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(collector.Close)
	plugin := &Plugin{config: cfg, collector: collector}
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "observer", Annotations: map[string]string{AnnotationEnabled: "true"}},
		Spec:       v1.PodSpec{Containers: []v1.Container{{Name: "observer"}}},
	}
	state := cycleStateForPod(t, plugin, pod)
	nodes := make([]*framework.NodeInfo, 0, 1000)
	for i := 0; i < 1000; i++ {
		nodeInfo := hotPathOrdinaryNodeInfo(i)
		if status := plugin.Filter(context.Background(), state, pod, nodeInfo); !status.IsSuccess() {
			t.Fatalf("Filter node %d: %v", i, status)
		}
		nodes = append(nodes, nodeInfo)
	}
	if status := plugin.PreScore(context.Background(), state, pod, nodes); !status.IsSuccess() {
		t.Fatalf("PreScore: %v", status)
	}
	for i, nodeInfo := range nodes {
		if _, status := plugin.Score(context.Background(), state, pod, nodeInfo); !status.IsSuccess() {
			t.Fatalf("Score node %d: %v", i, status)
		}
	}
	collector.mu.RLock()
	targets, discoveredNodes, pending := len(collector.targets), len(collector.nodes), len(collector.pending)
	collector.mu.RUnlock()
	if targets != 0 || discoveredNodes != 0 || pending != 0 || collector.store.len() != 0 || len(collector.queue) != 0 {
		t.Fatalf("ordinary-node hot path consumed collector quota: targets=%d nodes=%d pending=%d snapshots=%d queue=%d", targets, discoveredNodes, pending, collector.store.len(), len(collector.queue))
	}
	if networkCalls.Load() != 0 {
		t.Fatalf("annotation-only scheduling made %d exporter HTTP calls", networkCalls.Load())
	}
	if hint, err := plugin.queueOnNodeChange(klog.Background(), pod, nil, nodes[0].Node()); err != nil || hint != framework.QueueSkip {
		t.Fatalf("ordinary Node add hint=%v err=%v, want QueueSkip", hint, err)
	}
}

func TestSchedulerCallbacksCannotCreateTargetOrCallHTTP(t *testing.T) {
	cfg := testConfig(t)
	cfg.RefreshInterval = time.Hour
	var networkCalls atomic.Int64
	collector, err := newCollector(context.Background(), cfg, &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		networkCalls.Add(1)
		return nil, fmt.Errorf("scheduler callback attempted exporter HTTP")
	})})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(collector.Close)
	plugin := &Plugin{config: cfg, collector: collector}
	pod := gpuPod()
	nodeInfo := nodeInfoWithNamedEndpointAndCapacity("undiscovered-gpu", "http://undiscovered-gpu:32021/metrics", "iluvatar.com/gpu", 1)
	nodeInfo.Node().UID = types.UID("undiscovered-gpu-uid")
	state := cycleStateForPod(t, plugin, pod)

	if status := plugin.Filter(context.Background(), state, pod, nodeInfo); status.IsSuccess() || !contains(status.Message(), "not been discovered") {
		t.Fatalf("Filter status=%v, want explicit missing discovered target", status)
	}
	if status := plugin.PreScore(context.Background(), state, pod, []*framework.NodeInfo{nodeInfo}); !status.IsSuccess() {
		t.Fatalf("PreScore: %v", status)
	}
	if _, status := plugin.Score(context.Background(), state, pod, nodeInfo); !status.IsSuccess() {
		t.Fatalf("Score: %v", status)
	}
	time.Sleep(25 * time.Millisecond)
	collector.mu.RLock()
	targets, discoveredNodes, pending := len(collector.targets), len(collector.nodes), len(collector.pending)
	collector.mu.RUnlock()
	if targets != 0 || discoveredNodes != 0 || pending != 0 || collector.store.len() != 0 || len(collector.queue) != 0 {
		t.Fatalf("callbacks created collector state: targets=%d nodes=%d pending=%d snapshots=%d queue=%d", targets, discoveredNodes, pending, collector.store.len(), len(collector.queue))
	}
	if networkCalls.Load() != 0 {
		t.Fatalf("callbacks made %d exporter HTTP calls", networkCalls.Load())
	}
}

func TestDiscoveredAcceleratorCacheMissRequestsNonBlockingRefresh(t *testing.T) {
	cfg := testConfig(t)
	cfg.RefreshInterval = time.Hour
	cfg.SnapshotTTL = time.Hour
	started := make(chan struct{}, 1)
	var networkCalls atomic.Int64
	collector, err := newCollector(context.Background(), cfg, &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		networkCalls.Add(1)
		select {
		case started <- struct{}{}:
		default:
		}
		return metricsResponse(iluvatarMetrics), nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(collector.Close)
	plugin := &Plugin{config: cfg, collector: collector}
	pod := gpuPod()
	nodeInfo := nodeInfoWithNamedEndpointAndCapacity("discovered-gpu", "http://discovered-gpu:32021/metrics", "iluvatar.com/gpu", 1)
	nodeInfo.Node().UID = types.UID("discovered-gpu-uid")
	target, _, err := collector.ensureTarget(nodeInfo.Node())
	if err != nil {
		t.Fatalf("discover target: %v", err)
	}
	if result := collector.store.lookup(target, time.Now()); result.State != snapshotMissing {
		t.Fatalf("initial snapshot state=%s, want missing", result.State)
	}

	state := cycleStateForPod(t, plugin, pod)
	start := time.Now()
	status := plugin.Filter(context.Background(), state, pod, nodeInfo)
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Fatalf("Filter blocked for %v while requesting refresh", elapsed)
	}
	if status.IsSuccess() || !contains(status.Message(), "snapshot is not available") {
		t.Fatalf("Filter status=%v, want explicit missing snapshot", status)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("existing discovered target did not receive asynchronous refresh")
	}
	waitFor(t, time.Second, func() bool { return collector.snapshotForNode(nodeInfo.Node()).State == snapshotFresh })
	if networkCalls.Load() != 1 {
		t.Fatalf("exporter calls=%d, want one coalesced asynchronous refresh", networkCalls.Load())
	}
}

func TestSchedulingCycleDoesNotBlockWhenRefreshSuppressed(t *testing.T) {
	t.Run("backoff", func(t *testing.T) {
		testSuppressedRefreshCycle(t, failureState{NextTry: time.Now().Add(time.Minute)})
	})
	t.Run("circuit-open", func(t *testing.T) {
		testSuppressedRefreshCycle(t, failureState{OpenUntil: time.Now().Add(time.Minute)})
	})
	t.Run("queue-full", func(t *testing.T) {
		cfg := testConfig(t)
		cfg.Workers = 1
		cfg.QueueSize = 1
		cfg.CacheMaxEntries = 8
		cfg.RefreshInterval = time.Hour
		started := make(chan struct{}, 1)
		var networkCalls atomic.Int64
		collector, err := newCollector(context.Background(), cfg, &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			networkCalls.Add(1)
			select {
			case started <- struct{}{}:
			default:
			}
			<-req.Context().Done()
			return nil, req.Context().Err()
		})})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(collector.Close)
		plugin := &Plugin{config: cfg, collector: collector}

		active := nodeInfoWithNamedEndpointAndCapacity("queue-active", "http://queue-active:32021/metrics", "iluvatar.com/gpu", 1)
		filler := nodeInfoWithNamedEndpointAndCapacity("queue-filler", "http://queue-filler:32021/metrics", "iluvatar.com/gpu", 1)
		candidate := nodeInfoWithNamedEndpointAndCapacity("queue-candidate", "http://queue-candidate:32021/metrics", "iluvatar.com/gpu", 1)
		for _, nodeInfo := range []*framework.NodeInfo{active, filler, candidate} {
			nodeInfo.Node().UID = types.UID(nodeInfo.Node().Name + "-uid")
		}
		activeTarget, _, err := collector.ensureTarget(active.Node())
		if err != nil {
			t.Fatal(err)
		}
		fillerTarget, _, err := collector.ensureTarget(filler.Node())
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := collector.ensureTarget(candidate.Node()); err != nil {
			t.Fatal(err)
		}
		if !collector.enqueue(activeTarget) {
			t.Fatal("failed to occupy collector worker")
		}
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("collector worker did not start")
		}
		if !collector.enqueue(fillerTarget) {
			t.Fatal("failed to fill collector queue")
		}

		runMissingSnapshotSchedulingCycle(t, plugin, gpuPod(), candidate)
		if networkCalls.Load() != 1 {
			t.Fatalf("queue-full scheduling triggered %d HTTP calls, want only the already-active worker", networkCalls.Load())
		}
		if got := collector.lookupSnapshotForNodeInfo(candidate); got.State != snapshotMissing {
			t.Fatalf("queue-full changed candidate snapshot state to %s, want missing", got.State)
		}
	})
}

func testSuppressedRefreshCycle(t *testing.T, failure failureState) {
	t.Helper()
	cfg := testConfig(t)
	cfg.RefreshInterval = time.Hour
	var networkCalls atomic.Int64
	collector, err := newCollector(context.Background(), cfg, &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		networkCalls.Add(1)
		return nil, fmt.Errorf("suppressed refresh reached HTTP")
	})})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(collector.Close)
	plugin := &Plugin{config: cfg, collector: collector}
	nodeInfo := nodeInfoWithNamedEndpointAndCapacity("suppressed-gpu", "http://suppressed-gpu:32021/metrics", "iluvatar.com/gpu", 1)
	nodeInfo.Node().UID = types.UID("suppressed-gpu-uid")
	target, _, err := collector.ensureTarget(nodeInfo.Node())
	if err != nil {
		t.Fatal(err)
	}
	collector.mu.Lock()
	collector.failures[target.Key] = failure
	collector.mu.Unlock()

	runMissingSnapshotSchedulingCycle(t, plugin, gpuPod(), nodeInfo)
	if networkCalls.Load() != 0 {
		t.Fatalf("suppressed scheduling refresh made %d HTTP calls", networkCalls.Load())
	}
	if got := collector.lookupSnapshotForNodeInfo(nodeInfo); got.State != snapshotMissing {
		t.Fatalf("suppressed refresh changed snapshot state to %s, want missing", got.State)
	}
}

func runMissingSnapshotSchedulingCycle(t *testing.T, plugin *Plugin, pod *v1.Pod, nodeInfo *framework.NodeInfo) {
	t.Helper()
	state := cycleStateForPod(t, plugin, pod)
	start := time.Now()
	if status := plugin.Filter(context.Background(), state, pod, nodeInfo); status.IsSuccess() {
		t.Fatal("strict Filter succeeded with a missing snapshot")
	}
	if status := plugin.PreScore(context.Background(), state, pod, []*framework.NodeInfo{nodeInfo}); !status.IsSuccess() {
		t.Fatalf("PreScore: %v", status)
	}
	if _, status := plugin.Score(context.Background(), state, pod, nodeInfo); !status.IsSuccess() {
		t.Fatalf("Score: %v", status)
	}
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Fatalf("scheduling cycle blocked for %v", elapsed)
	}
}

func hotPathOrdinaryNodeInfo(index int) *framework.NodeInfo {
	nodeInfo := framework.NewNodeInfo()
	nodeInfo.SetNode(&v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("ordinary-%04d", index), UID: types.UID(fmt.Sprintf("ordinary-%04d-uid", index))},
		Status: v1.NodeStatus{Addresses: []v1.NodeAddress{{
			Type:    v1.NodeInternalIP,
			Address: fmt.Sprintf("10.0.%d.%d", index/250+1, index%250+1),
		}}},
	})
	return nodeInfo
}
