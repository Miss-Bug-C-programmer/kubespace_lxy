package gpustability

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

func TestTypedWorkloadIntentIsStrictAndStoredOnce(t *testing.T) {
	plugin := newTestPlugin(t, iluvatarMetrics)
	pod := gpuPod()
	pod.Annotations = map[string]string{AnnotationWorkloadIntent: `{
  "apiVersion":"gpustability.k3s.io/v1alpha1",
  "kind":"SpaceComputeWorkloadIntent",
  "statePolicy":"degraded",
  "minFreeMemoryMiB":1024,
  "requiredProfiles":["iluvatar"],
  "requiredNodeLabels":{"space-compute.k3s.io/runtime":"cuda-12"},
  "preferredNodeLabels":{"topology.kubernetes.io/zone":"orbit-a"}
}`}
	state := framework.NewCycleState()
	if _, status := plugin.PreFilter(context.Background(), state, pod); !status.IsSuccess() {
		t.Fatalf("PreFilter: %v", status)
	}
	requirement, status := workloadFromState(state)
	if !status.IsSuccess() || requirement.Policy != StatePolicyDegraded || requirement.MinFreeMemoryMiB != 1024 {
		t.Fatalf("cycle requirement = %+v, status=%v", requirement, status)
	}
	pod.Annotations[AnnotationWorkloadIntent] = `{"unknown":true}`
	if requirement.RequiredNodeLabels["space-compute.k3s.io/runtime"] != "cuda-12" {
		t.Fatal("immutable cycle state changed after Pod annotation mutation")
	}

	invalid := gpuPod()
	invalid.Annotations = map[string]string{AnnotationWorkloadIntent: `{"apiVersion":"gpustability.k3s.io/v1alpha1","kind":"SpaceComputeWorkloadIntent","unknown":true}`}
	if _, status := plugin.PreFilter(context.Background(), framework.NewCycleState(), invalid); status.Code() != framework.UnschedulableAndUnresolvable {
		t.Fatalf("invalid intent status = %v, want UnschedulableAndUnresolvable", status)
	}
}

func TestAnnotationOnlyTypedIntentRemainsObservational(t *testing.T) {
	plugin := newTestPlugin(t, iluvatarMetrics)
	pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{AnnotationWorkloadIntent: `{"apiVersion":"gpustability.k3s.io/v1alpha1","kind":"SpaceComputeWorkloadIntent","requiredNodeLabels":{"space-compute.k3s.io/trust":"high"}}`}}, Spec: v1.PodSpec{Containers: []v1.Container{{Name: "observer"}}}}
	state := cycleStateForPod(t, plugin, pod)
	requirement, status := workloadFromState(state)
	if !status.IsSuccess() || !requirement.Observational || len(requirement.RequiredNodeLabels) != 0 || requirement.PreferredNodeLabels["space-compute.k3s.io/trust"] != "high" {
		t.Fatalf("observational requirement = %+v, status=%v", requirement, status)
	}
	node := framework.NewNodeInfo()
	node.SetNode(&v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "untrusted"}})
	if status := plugin.Filter(context.Background(), state, pod, node); !status.IsSuccess() {
		t.Fatalf("annotation-only intent hard-filtered a Node: %v", status)
	}
}

func TestStrictPhysicalCoverageDoesNotClaimUnlinkedDeviceEnforcement(t *testing.T) {
	plugin := newTestPlugin(t, iluvatarMetrics)
	pod := gpuPod()
	node := nodeInfoWithEndpointAndGPUCapacity("http://gpu-node:32021/metrics", 4)
	warmNode(t, plugin, node)
	status := plugin.Filter(context.Background(), cycleStateForPod(t, plugin, pod), pod, node)
	if status.IsSuccess() || !contains(status.Message(), "physical allocation identity is not linked") {
		t.Fatalf("Filter status = %v, want conservative unlinked-identity rejection", status)
	}

	pod.Annotations = map[string]string{AnnotationStatePolicy: string(StatePolicyDegraded)}
	state := cycleStateForPod(t, plugin, pod)
	if status := plugin.Filter(context.Background(), state, pod, node); !status.IsSuccess() {
		t.Fatalf("degraded compatibility path should treat unlinked device signal as soft: %v", status)
	}
	if status := plugin.PreScore(context.Background(), state, pod, []*framework.NodeInfo{node}); !status.IsSuccess() {
		t.Fatalf("PreScore: %v", status)
	}
	if score, status := plugin.Score(context.Background(), state, pod, node); !status.IsSuccess() || score > plugin.config.DegradedScore {
		t.Fatalf("degraded score=%d status=%v, cap=%d", score, status, plugin.config.DegradedScore)
	}
	if _, allocates := interface{}(plugin).(framework.ReservePlugin); allocates {
		t.Fatal("plugin unexpectedly implements Reserve and would duplicate Kubernetes allocation")
	}
}

func TestDRAClaimRemainsOwnedByUpstreamDynamicResources(t *testing.T) {
	plugin := newTestPlugin(t, iluvatarMetrics)
	claimName := "vendor-device"
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "dra-only"},
		Spec: v1.PodSpec{
			ResourceClaims: []v1.PodResourceClaim{{Name: "accelerator", ResourceClaimName: &claimName}},
			Containers:     []v1.Container{{Name: "workload"}},
		},
	}
	requirement, err := plugin.schedulingRequirement(pod)
	if err != nil {
		t.Fatal(err)
	}
	if requirement.Required || requirement.Observational || len(requirement.Resources) != 0 {
		t.Fatalf("DRA-only Pod was claimed by telemetry policy: %+v", requirement)
	}
	state := framework.NewCycleState()
	if _, status := plugin.PreFilter(context.Background(), state, pod); status.Code() != framework.Skip {
		t.Fatalf("DRA-only Pod status=%v, want Skip", status)
	}
	stored, status := workloadFromState(state)
	if !status.IsSuccess() || stored.Required {
		t.Fatalf("DRA-only stored state=%+v status=%v", stored, status)
	}
}

func TestSnapshotPublicationPreciselyActivatesBlockedPods(t *testing.T) {
	cfg := testConfig(t)
	handle := &recordingActivationHandle{}
	collector, err := newCollector(context.Background(), cfg, &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, fmt.Errorf("collector unavailable")
	})})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(collector.Close)
	plugin := &Plugin{config: cfg, collector: collector, handle: handle, blocked: newBlockedPodIndex(10, time.Minute)}
	collector.setSnapshotListener(plugin.activateForSnapshot)
	pod := gpuPod()
	pod.Namespace, pod.Name, pod.UID = "workloads", "blocked", types.UID("blocked-uid")
	nodeA := nodeInfoWithNamedEndpointAndCapacity("node-a", "http://node-a:32021/metrics", "iluvatar.com/gpu", 1)
	nodeB := nodeInfoWithNamedEndpointAndCapacity("node-b", "http://node-b:32021/metrics", "iluvatar.com/gpu", 1)
	state := cycleStateForPod(t, plugin, pod)
	if status := plugin.Filter(context.Background(), state, pod, nodeA); status.IsSuccess() {
		t.Fatal("Filter succeeded without snapshot")
	}
	if status := plugin.Filter(context.Background(), state, pod, nodeB); status.IsSuccess() {
		t.Fatal("Filter succeeded without snapshot")
	}
	collector.notifySnapshotReady("unrelated", 1)
	if handle.count() != 0 {
		t.Fatal("unrelated snapshot activated a Pod")
	}
	collector.notifySnapshotReady("node-a", 1)
	if got := handle.keys(); len(got) != 1 || got[0] != "blocked-uid" {
		t.Fatalf("activated keys = %v, want blocked-uid", got)
	}
	if plugin.blocked.blocked("node-a", pod) || !plugin.blocked.blocked("node-b", pod) {
		t.Fatal("activation did not remove exactly the published node dependency")
	}
}

func TestNodeQueueHintsIgnoreUnrelatedChanges(t *testing.T) {
	plugin := newTestPlugin(t, iluvatarMetrics)
	plugin.blocked = newBlockedPodIndex(10, time.Minute)
	pod := gpuPod()
	pod.Name = "queue-me"
	oldNode := nodeInfoWithEndpointAndGPUCapacity("http://gpu-node:32021/metrics", 1).Node()
	oldNode.Name = "queue-node"
	plugin.blocked.track(oldNode.Name, pod)

	unrelated := oldNode.DeepCopy()
	unrelated.Annotations["example.com/unrelated"] = "changed"
	if hint, err := plugin.queueOnNodeChange(klog.Background(), pod, oldNode, unrelated); err != nil || hint != framework.QueueSkip {
		t.Fatalf("unrelated hint=%v err=%v, want QueueSkip", hint, err)
	}
	relevant := oldNode.DeepCopy()
	relevant.Annotations[AnnotationExporterProfile] = "dcgm"
	if hint, err := plugin.queueOnNodeChange(klog.Background(), pod, oldNode, relevant); err != nil || hint != framework.Queue {
		t.Fatalf("relevant hint=%v err=%v, want Queue", hint, err)
	}
	ordinary := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "ordinary"}, Spec: v1.PodSpec{Containers: []v1.Container{{Name: "app"}}}}
	if hint, err := plugin.queueOnNodeChange(klog.Background(), ordinary, oldNode, relevant); err != nil || hint != framework.QueueSkip {
		t.Fatalf("ordinary hint=%v err=%v, want QueueSkip", hint, err)
	}
}

func TestScoringBreakdownGoldenAndFixedNormalization(t *testing.T) {
	plugin := newTestPlugin(t, iluvatarMetrics)
	pod := gpuPod()
	pod.Annotations = map[string]string{AnnotationWorkloadIntent: `{"apiVersion":"gpustability.k3s.io/v1alpha1","kind":"SpaceComputeWorkloadIntent","preferredNodeLabels":{"topology.kubernetes.io/zone":"orbit-a"}}`}
	node := nodeInfoWithEndpointAndGPUCapacity("http://gpu-node:32021/metrics", 1)
	node.Node().Labels = map[string]string{"topology.kubernetes.io/zone": "orbit-a"}
	warmNode(t, plugin, node)
	state := cycleStateForPod(t, plugin, pod)
	if status := plugin.PreScore(context.Background(), state, pod, []*framework.NodeInfo{node}); !status.IsSuccess() {
		t.Fatal(status)
	}
	preScore, status := scoreState(state)
	if !status.IsSuccess() {
		t.Fatal(status)
	}
	d := preScore.Nodes[node.Node().Name].Evaluation.Dimensions
	if got := fmt.Sprintf("util=%.0f mem=%.0f thermal=%.0f energy=%.0f compute=%.0f health=%.0f locality=%.0f fragment=%.0f", d.Utilization, d.MemoryHeadroom, d.ThermalHeadroom, d.EnergyHeadroom, d.ComputeCapability, d.Health, d.DataLocality, d.Fragmentation); got != "util=100 mem=100 thermal=100 energy=94 compute=100 health=0 locality=100 fragment=100" {
		t.Fatalf("score explanation changed:\n%s", got)
	}
	if plugin.ScoreExtensions() != nil {
		t.Fatal("fixed SLO scoring unexpectedly enabled candidate-relative normalization")
	}
}

func TestBlockedPodIndexConcurrentBound(t *testing.T) {
	index := newBlockedPodIndex(64, time.Minute)
	var wg sync.WaitGroup
	for i := 0; i < 256; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("pod-%03d", i), UID: types.UID(fmt.Sprintf("uid-%03d", i))}}
			index.track(fmt.Sprintf("node-%d", i%8), pod)
			_ = index.blocked(fmt.Sprintf("node-%d", i%8), pod)
		}(i)
	}
	wg.Wait()
	if got := index.len(); got > 64 {
		t.Fatalf("tracked Pods=%d, bound=64", got)
	}
}

func FuzzWorkloadIntent(f *testing.F) {
	f.Add(`{"apiVersion":"gpustability.k3s.io/v1alpha1","kind":"SpaceComputeWorkloadIntent","statePolicy":"strict"}`)
	f.Add(`{"apiVersion":"gpustability.k3s.io/v1alpha1","kind":"SpaceComputeWorkloadIntent","requiredNodeLabels":{"example.com/runtime":"v1"}}`)
	f.Add(`{"apiVersion":"gpustability.k3s.io/v1alpha1","kind":"SpaceComputeWorkloadIntent","minFreeMemoryMiB":NaN}`)
	f.Fuzz(func(t *testing.T, raw string) {
		pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{AnnotationWorkloadIntent: raw}}}
		_, _ = parseWorkloadIntent(pod)
	})
}

func BenchmarkSchedulerCallbacks(b *testing.B) {
	metrics, err := parseMetricsForProfile(strings.NewReader(iluvatarMetrics), "iluvatar")
	if err != nil {
		b.Fatal(err)
	}
	for _, count := range []int{100, 1000, 5000} {
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			args := defaultArgs()
			args.Exporter.Scheme = "http"
			args.Exporter.AllowInsecureHTTP = true
			args.Collector.Workers = 1
			args.Collector.QueueSize = count
			args.Collector.CacheMaxEntries = count
			args.Collector.SnapshotTTL = "1h"
			args.Collector.RefreshInterval = "30m"
			cfg, err := validateAndConvertArgs(args)
			if err != nil {
				b.Fatal(err)
			}
			var networkCalls atomic.Int64
			collector, err := newCollector(context.Background(), cfg, &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				networkCalls.Add(1)
				return nil, fmt.Errorf("benchmark callbacks attempted network I/O")
			})})
			if err != nil {
				b.Fatal(err)
			}
			defer collector.Close()
			plugin := &Plugin{config: cfg, collector: collector}
			nodes := make([]*framework.NodeInfo, 0, count)
			now := time.Now()
			for i := 0; i < count; i++ {
				name := fmt.Sprintf("node-%05d", i)
				nodeInfo := nodeInfoWithNamedEndpointAndCapacity(name, "http://"+name+":32021/metrics", "iluvatar.com/gpu", 1)
				nodeInfo.Node().UID = types.UID(name + "-uid")
				target, _, err := collector.ensureTarget(nodeInfo.Node())
				if err != nil {
					b.Fatal(err)
				}
				copy := metrics
				copy.FetchedAt, copy.ValidUntil = now, now.Add(time.Hour)
				if !collector.store.publish(target, copy, now, copy.ValidUntil) {
					b.Fatal("publish snapshot")
				}
				nodes = append(nodes, nodeInfo)
			}
			pod := gpuPod()
			state := framework.NewCycleState()
			if _, status := plugin.PreFilter(context.Background(), state, pod); !status.IsSuccess() {
				b.Fatal(status)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, node := range nodes {
					if status := plugin.Filter(context.Background(), state, pod, node); !status.IsSuccess() {
						b.Fatal(status)
					}
				}
				if status := plugin.PreScore(context.Background(), state, pod, nodes); !status.IsSuccess() {
					b.Fatal(status)
				}
				for _, node := range nodes {
					if _, status := plugin.Score(context.Background(), state, pod, node); !status.IsSuccess() {
						b.Fatal(status)
					}
				}
			}
			b.StopTimer()
			iterations := b.N
			if iterations < 1 {
				iterations = 1
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(iterations*count), "ns/node-cycle")
			if perNode := b.Elapsed() / time.Duration(iterations*count); perNode > 500*time.Microsecond {
				b.Fatalf("callback cycle latency %v/node exceeds gross-regression budget 500us/node", perNode)
			}
			if networkCalls.Load() != 0 {
				b.Fatalf("scheduler callbacks made %d network calls", networkCalls.Load())
			}
		})
	}
}

type recordingActivationHandle struct {
	framework.Handle
	mu        sync.Mutex
	activated map[string]*v1.Pod
}

func (h *recordingActivationHandle) Activate(_ klog.Logger, pods map[string]*v1.Pod) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.activated == nil {
		h.activated = map[string]*v1.Pod{}
	}
	for key, pod := range pods {
		h.activated[key] = pod.DeepCopy()
	}
}

func (h *recordingActivationHandle) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.activated)
}

func (h *recordingActivationHandle) keys() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	result := make([]string, 0, len(h.activated))
	for key := range h.activated {
		result = append(result, key)
	}
	return result
}

func contains(value, fragment string) bool {
	for i := 0; i+len(fragment) <= len(value); i++ {
		if value[i:i+len(fragment)] == fragment {
			return true
		}
	}
	return false
}
