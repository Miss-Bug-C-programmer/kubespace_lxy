package gpustability

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

func TestCycleSnapshotPinsGenerationAcrossPreScore(t *testing.T) {
	plugin := newTestPluginWithMetrics(t, map[string]string{
		"cycle-old:32021": iluvatarMetrics,
		"cycle-new:32021": iluvatarMetrics,
	})
	pod := gpuPod()
	nodeInfo := nodeInfoWithNamedEndpointAndCapacity("cycle-generation", "http://cycle-old:32021/metrics", "iluvatar.com/gpu", 1)
	nodeInfo.Node().UID = types.UID("cycle-generation-uid")
	warmNode(t, plugin, nodeInfo)

	state := cycleStateForPod(t, plugin, pod)
	if status := plugin.Filter(context.Background(), state, pod, nodeInfo); !status.IsSuccess() {
		t.Fatalf("Filter before generation change: %v", status)
	}
	pinnedBefore := mustCycleSnapshot(t, state, nodeInfo.Node().Name)
	if pinnedBefore.Snapshot.State != snapshotFresh || pinnedBefore.TargetGeneration == 0 {
		t.Fatalf("initial pinned snapshot = %#v, want fresh generation", pinnedBefore)
	}

	updatedNode := nodeInfo.Node().DeepCopy()
	updatedNode.Status.Addresses = []v1.NodeAddress{{Type: v1.NodeInternalIP, Address: "cycle-new"}}
	nodeInfo.SetNode(updatedNode)
	if err := plugin.collector.refreshNode(context.Background(), updatedNode); err != nil {
		t.Fatalf("refresh new target generation: %v", err)
	}
	current := plugin.collector.lookupSnapshotForNodeInfo(nodeInfo)
	if current.TargetGeneration == pinnedBefore.TargetGeneration {
		t.Fatalf("collector generation did not advance: current=%d pinned=%d", current.TargetGeneration, pinnedBefore.TargetGeneration)
	}

	if status := plugin.PreScore(context.Background(), state, pod, []*framework.NodeInfo{nodeInfo}); !status.IsSuccess() {
		t.Fatalf("PreScore after generation change: %v", status)
	}
	pinnedAfter := mustCycleSnapshot(t, state, nodeInfo.Node().Name)
	assertPinnedSnapshotEqual(t, pinnedBefore, pinnedAfter)
	preScore, status := scoreState(state)
	if !status.IsSuccess() {
		t.Fatalf("score state: %v", status)
	}
	if got := preScore.Nodes[nodeInfo.Node().Name].State; got != pinnedBefore.Snapshot.State {
		t.Fatalf("PreScore state = %s, want pinned state %s", got, pinnedBefore.Snapshot.State)
	}
	if _, status := plugin.Score(context.Background(), state, pod, nodeInfo); !status.IsSuccess() {
		t.Fatalf("Score after generation change: %v", status)
	}
}

func TestCycleSnapshotExpiryAppliesNextCycleOnly(t *testing.T) {
	plugin := newTestPlugin(t, iluvatarMetrics)
	pod := gpuPod()
	nodeInfo := nodeInfoWithEndpointAndGPUCapacity("http://gpu-node:32021/metrics", 1)
	nodeInfo.Node().UID = types.UID("cycle-expiry-uid")

	now := time.Unix(1_800_000_000, 0)
	plugin.collector.now = func() time.Time { return now }
	warmNode(t, plugin, nodeInfo)

	state := cycleStateForPod(t, plugin, pod)
	if status := plugin.Filter(context.Background(), state, pod, nodeInfo); !status.IsSuccess() {
		t.Fatalf("Filter before expiry: %v", status)
	}
	pinned := mustCycleSnapshot(t, state, nodeInfo.Node().Name)
	if pinned.Snapshot.State != snapshotFresh {
		t.Fatalf("pinned state = %s, want fresh", pinned.Snapshot.State)
	}

	now = pinned.Snapshot.ValidUntil.Add(time.Nanosecond)
	if current := plugin.collector.lookupSnapshotForNodeInfo(nodeInfo); current.State != snapshotStale {
		t.Fatalf("collector state after expiry = %s, want stale", current.State)
	}
	if status := plugin.PreScore(context.Background(), state, pod, []*framework.NodeInfo{nodeInfo}); !status.IsSuccess() {
		t.Fatalf("PreScore changed current cycle after expiry: %v", status)
	}
	if _, status := plugin.Score(context.Background(), state, pod, nodeInfo); !status.IsSuccess() {
		t.Fatalf("Score changed current cycle after expiry: %v", status)
	}
	assertPinnedSnapshotEqual(t, pinned, mustCycleSnapshot(t, state, nodeInfo.Node().Name))

	next := cycleStateForPod(t, plugin, pod)
	status := plugin.Filter(context.Background(), next, pod, nodeInfo)
	if status.IsSuccess() || !contains(status.Message(), "stale") {
		t.Fatalf("next-cycle Filter status = %v, want stale rejection", status)
	}
	if got := mustCycleSnapshot(t, next, nodeInfo.Node().Name).Snapshot.State; got != snapshotStale {
		t.Fatalf("next-cycle pinned state = %s, want stale", got)
	}
}

func TestCycleSnapshotSurvivesCollectorFailureAfterFilter(t *testing.T) {
	plugin := newTestPlugin(t, iluvatarMetrics)
	pod := gpuPod()
	nodeInfo := nodeInfoWithEndpointAndGPUCapacity("http://gpu-node:32021/metrics", 1)
	nodeInfo.Node().UID = types.UID("cycle-failure-uid")
	warmNode(t, plugin, nodeInfo)

	state := cycleStateForPod(t, plugin, pod)
	if status := plugin.Filter(context.Background(), state, pod, nodeInfo); !status.IsSuccess() {
		t.Fatalf("Filter before collector failure: %v", status)
	}
	pinned := mustCycleSnapshot(t, state, nodeInfo.Node().Name)
	if pinned.Snapshot.Confidence != confidenceValidated {
		t.Fatalf("initial confidence = %s, want validated", pinned.Snapshot.Confidence)
	}

	plugin.collector.mu.RLock()
	target := plugin.collector.targets[nodeInfo.Node().Name]
	plugin.collector.mu.RUnlock()
	if !plugin.collector.store.recordFailure(target, "forced post-Filter collector failure", pinned.Snapshot.ObservedAt.Add(time.Millisecond)) {
		t.Fatal("failed to record collector failure")
	}
	current := plugin.collector.lookupSnapshotForNodeInfo(nodeInfo)
	if current.Confidence != confidenceDegraded || !contains(current.Reason, "forced post-Filter") {
		t.Fatalf("collector did not expose post-Filter failure: confidence=%s reason=%q", current.Confidence, current.Reason)
	}

	if status := plugin.PreScore(context.Background(), state, pod, []*framework.NodeInfo{nodeInfo}); !status.IsSuccess() {
		t.Fatalf("PreScore after collector failure: %v", status)
	}
	if _, status := plugin.Score(context.Background(), state, pod, nodeInfo); !status.IsSuccess() {
		t.Fatalf("Score contradicted successful Filter after collector failure: %v", status)
	}
	assertPinnedSnapshotEqual(t, pinned, mustCycleSnapshot(t, state, nodeInfo.Node().Name))
}

func TestPreScoreRequiresCompleteFilterPinnedSnapshot(t *testing.T) {
	plugin := newTestPlugin(t, iluvatarMetrics)
	pod := gpuPod()
	nodeInfo := nodeInfoWithEndpointAndGPUCapacity("http://gpu-node:32021/metrics", 1)
	warmNode(t, plugin, nodeInfo)

	state := cycleStateForPod(t, plugin, pod)
	status := plugin.PreScore(context.Background(), state, pod, []*framework.NodeInfo{nodeInfo})
	if status == nil || status.Code() != framework.Error || !contains(status.Message(), "not evaluated during Filter") {
		t.Fatalf("PreScore without Filter status = %v, want framework.Error", status)
	}

	state = cycleStateForPod(t, plugin, pod)
	if status := plugin.Filter(context.Background(), state, pod, nodeInfo); !status.IsSuccess() {
		t.Fatalf("Filter: %v", status)
	}
	cycleSnapshots, cycleStatus := cycleSnapshotsFromState(state)
	if !cycleStatus.IsSuccess() {
		t.Fatal(cycleStatus)
	}
	cycleSnapshots.mu.Lock()
	cycleSnapshots.nodes[nodeInfo.Node().Name] = nodeCycleSnapshot{}
	cycleSnapshots.mu.Unlock()
	status = plugin.PreScore(context.Background(), state, pod, []*framework.NodeInfo{nodeInfo})
	if status == nil || status.Code() != framework.Error {
		t.Fatalf("PreScore with incomplete cycle snapshot status = %v, want framework.Error", status)
	}
}

func TestCycleSnapshotCloneDeepCopiesMutableFields(t *testing.T) {
	resourceName := v1.ResourceName("example.com/gpu")
	state := newCycleNodeSnapshotState()
	state.nodes["node-a"] = nodeCycleSnapshot{
		Snapshot: snapshotResult{
			State: snapshotFresh,
			Metrics: nodeMetrics{
				Fields:  map[deviceMetricField]struct{}{fieldGPUUtilization: {}},
				Devices: []deviceMetrics{{ID: "device-a", Fields: map[deviceMetricField]struct{}{fieldTemperatureC: {}}}},
			},
			Resources:        nodeResourceContext{Allocatable: map[v1.ResourceName]int64{resourceName: 2}, Requested: map[v1.ResourceName]int64{resourceName: 1}},
			ObservedAt:       time.Unix(10, 0),
			ValidUntil:       time.Unix(20, 0),
			TargetGeneration: 7,
			Confidence:       confidenceValidated,
		},
		TargetGeneration: 7,
		NodeResource:     nodeResourceContext{Allocatable: map[v1.ResourceName]int64{resourceName: 2}, Requested: map[v1.ResourceName]int64{resourceName: 1}},
	}

	clone := state.Clone().(*cycleNodeSnapshotState)
	original := state.nodes["node-a"]
	delete(original.Snapshot.Metrics.Fields, fieldGPUUtilization)
	original.Snapshot.Metrics.Devices[0].ID = "mutated"
	delete(original.Snapshot.Metrics.Devices[0].Fields, fieldTemperatureC)
	original.Snapshot.Resources.Allocatable[resourceName] = 99
	original.NodeResource.Requested[resourceName] = 99
	state.nodes["node-a"] = original

	got, ok := clone.load("node-a")
	if !ok {
		t.Fatal("cloned node snapshot is missing")
	}
	if _, ok := got.Snapshot.Metrics.Fields[fieldGPUUtilization]; !ok {
		t.Fatal("snapshot metric field map was shallow-copied")
	}
	if got.Snapshot.Metrics.Devices[0].ID != "device-a" {
		t.Fatal("snapshot device slice was shallow-copied")
	}
	if _, ok := got.Snapshot.Metrics.Devices[0].Fields[fieldTemperatureC]; !ok {
		t.Fatal("device field map was shallow-copied")
	}
	if got.Snapshot.Resources.Allocatable[resourceName] != 2 || got.NodeResource.Requested[resourceName] != 1 {
		t.Fatal("cycle resource maps were shallow-copied")
	}
}

func TestRequestedResourcesRemainCycleLocal(t *testing.T) {
	plugin := newTestPlugin(t, readPhase2Fixture(t, "iluvatar-2gpu.prom"))
	pod := gpuPod()
	nodeInfo := nodeInfoWithEndpointAndGPUCapacity("http://gpu-node:32021/metrics", 2)
	nodeInfo.AddPod(gpuPod())
	warmNode(t, plugin, nodeInfo)

	plugin.collector.mu.RLock()
	target := plugin.collector.targets[nodeInfo.Node().Name]
	plugin.collector.mu.RUnlock()
	global := plugin.collector.store.lookup(target, plugin.collector.now())
	if len(global.Resources.Requested) != 0 {
		t.Fatalf("global snapshot persisted Requested=%v", global.Resources.Requested)
	}

	state := cycleStateForPod(t, plugin, pod)
	if status := plugin.Filter(context.Background(), state, pod, nodeInfo); !status.IsSuccess() {
		t.Fatalf("Filter: %v", status)
	}
	pinned := mustCycleSnapshot(t, state, nodeInfo.Node().Name)
	resourceName := v1.ResourceName("iluvatar.com/gpu")
	if got := pinned.NodeResource.Requested[resourceName]; got != 1 {
		t.Fatalf("cycle-local Requested=%d, want 1", got)
	}
	global = plugin.collector.store.lookup(target, plugin.collector.now())
	if len(global.Resources.Requested) != 0 {
		t.Fatalf("Filter leaked Requested into global snapshot: %v", global.Resources.Requested)
	}
}

func TestParallelFiltersPinCycleSnapshotsRaceFree(t *testing.T) {
	plugin := newTestPluginWithMetrics(t, map[string]string{"gpu-node:32021": iluvatarMetrics})
	pod := gpuPod()
	const nodeCount = 64
	nodes := make([]*framework.NodeInfo, 0, nodeCount)
	for i := 0; i < nodeCount; i++ {
		name := fmt.Sprintf("parallel-%02d", i)
		nodeInfo := nodeInfoWithNamedEndpointAndCapacity(name, "http://"+name+":32021/metrics", "iluvatar.com/gpu", 1)
		nodeInfo.Node().UID = types.UID(name + "-uid")
		warmNode(t, plugin, nodeInfo)
		nodes = append(nodes, nodeInfo)
	}
	state := cycleStateForPod(t, plugin, pod)

	errs := make(chan string, nodeCount)
	var wg sync.WaitGroup
	for _, nodeInfo := range nodes {
		nodeInfo := nodeInfo
		wg.Add(1)
		go func() {
			defer wg.Done()
			if status := plugin.Filter(context.Background(), state, pod, nodeInfo); !status.IsSuccess() {
				errs <- fmt.Sprintf("Filter(%s): %v", nodeInfo.Node().Name, status)
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_ = state.Clone()
		}
	}()
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if t.Failed() {
		return
	}

	cycleSnapshots := mustCycleSnapshotState(t, state)
	cycleSnapshots.mu.RLock()
	gotCount := len(cycleSnapshots.nodes)
	cycleSnapshots.mu.RUnlock()
	if gotCount != nodeCount {
		t.Fatalf("pinned node count=%d, want %d", gotCount, nodeCount)
	}
	if status := plugin.PreScore(context.Background(), state, pod, nodes); !status.IsSuccess() {
		t.Fatalf("PreScore after parallel Filter: %v", status)
	}
	for _, nodeInfo := range nodes {
		if _, status := plugin.Score(context.Background(), state, pod, nodeInfo); !status.IsSuccess() {
			t.Fatalf("Score(%s): %v", nodeInfo.Node().Name, status)
		}
	}
}

func mustCycleSnapshotState(t *testing.T, state *framework.CycleState) *cycleNodeSnapshotState {
	t.Helper()
	cycleSnapshots, status := cycleSnapshotsFromState(state)
	if !status.IsSuccess() {
		t.Fatalf("cycle snapshot state: %v", status)
	}
	return cycleSnapshots
}

func mustCycleSnapshot(t *testing.T, state *framework.CycleState, nodeName string) nodeCycleSnapshot {
	t.Helper()
	snapshot, ok := mustCycleSnapshotState(t, state).load(nodeName)
	if !ok {
		t.Fatalf("node %q has no pinned cycle snapshot", nodeName)
	}
	return snapshot
}

func assertPinnedSnapshotEqual(t *testing.T, want, got nodeCycleSnapshot) {
	t.Helper()
	if want.TargetGeneration != got.TargetGeneration || want.Snapshot.TargetGeneration != got.Snapshot.TargetGeneration {
		t.Fatalf("generation changed within cycle: want=%d got=%d", want.TargetGeneration, got.TargetGeneration)
	}
	if want.Snapshot.State != got.Snapshot.State || want.Snapshot.Confidence != got.Snapshot.Confidence || want.Snapshot.Profile != got.Snapshot.Profile {
		t.Fatalf("snapshot identity changed within cycle: want state/confidence/profile=%s/%s/%q got=%s/%s/%q", want.Snapshot.State, want.Snapshot.Confidence, want.Snapshot.Profile, got.Snapshot.State, got.Snapshot.Confidence, got.Snapshot.Profile)
	}
	if !want.Snapshot.ObservedAt.Equal(got.Snapshot.ObservedAt) || !want.Snapshot.ValidUntil.Equal(got.Snapshot.ValidUntil) {
		t.Fatalf("snapshot timestamps changed within cycle: want=%s/%s got=%s/%s", want.Snapshot.ObservedAt, want.Snapshot.ValidUntil, got.Snapshot.ObservedAt, got.Snapshot.ValidUntil)
	}
	if !resourceContextsEqual(want.NodeResource, got.NodeResource) || !resourceContextsEqual(want.Snapshot.Resources, got.Snapshot.Resources) {
		t.Fatalf("resource context changed within cycle: want=%v got=%v", want.NodeResource, got.NodeResource)
	}
}
