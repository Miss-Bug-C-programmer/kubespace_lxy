package gpustability

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientsetfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/events"
	"k8s.io/kubernetes/pkg/scheduler"
	schedulerconfig "k8s.io/kubernetes/pkg/scheduler/apis/config"
	schedulerconfigscheme "k8s.io/kubernetes/pkg/scheduler/apis/config/scheme"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	"k8s.io/kubernetes/pkg/scheduler/profile"
)

func TestRealSchedulersBindOnlyTheirOwnPodsAndRecoverSpaceComponent(t *testing.T) {
	var exporterRequests atomic.Int64
	exporter := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		exporterRequests.Add(1)
		_, _ = w.Write([]byte(iluvatarMetrics))
	}))
	defer exporter.Close()
	node := schedulerIntegrationNode(t, exporter.URL)
	client := clientsetfake.NewClientset(&v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "workloads"}}, node)
	factory := scheduler.NewInformerFactory(client, 0)
	broadcaster := events.NewBroadcaster(&events.EventSinkImpl{Interface: client.EventsV1()})
	defer broadcaster.Shutdown()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bindings := &bindingRecorder{nodes: map[string]string{}}
	client.PrependReactor("create", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "binding" {
			return false, nil, nil
		}
		binding := action.(clienttesting.CreateAction).GetObject().(*v1.Binding)
		resource := v1.SchemeGroupVersion.WithResource("pods")
		if object, err := client.Tracker().Get(resource, binding.Namespace, binding.Name); err == nil {
			bound := object.(*v1.Pod).DeepCopy()
			bound.Spec.NodeName = binding.Target.Name
			_ = client.Tracker().Update(resource, bound, binding.Namespace)
		}
		bindings.record(binding.Namespace+"/"+binding.Name, binding.Target.Name)
		return true, binding, nil
	})

	defaultScheduler, err := scheduler.New(ctx, client, factory, nil, profile.NewRecorderFactory(broadcaster))
	if err != nil {
		t.Fatalf("create default scheduler: %v", err)
	}
	originalDefaultFailure := defaultScheduler.FailureHandler
	defaultScheduler.FailureHandler = func(ctx context.Context, fwk framework.Framework, podInfo *framework.QueuedPodInfo, status *framework.Status, nominatingInfo *framework.NominatingInfo, start time.Time) {
		t.Logf("default scheduler failure for %s/%s: %s", podInfo.Pod.Namespace, podInfo.Pod.Name, status.Message())
		originalDefaultFailure(ctx, fwk, podInfo, status, nominatingInfo, start)
	}
	factory.Start(ctx.Done())
	factory.WaitForCacheSync(ctx.Done())
	if err := defaultScheduler.WaitForHandlersSync(ctx); err != nil {
		t.Fatalf("default scheduler handler sync: %v", err)
	}
	go defaultScheduler.Run(ctx)

	ordinary := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "workloads", Name: "ordinary", UID: "ordinary-uid"}, Spec: v1.PodSpec{SchedulerName: v1.DefaultSchedulerName, Containers: []v1.Container{{Name: "app", Image: "pause"}}}}
	space := podWithAcceleratorRequest("iluvatar.com/gpu", 1)
	space.Namespace, space.Name, space.UID, space.Spec.SchedulerName = "workloads", "space", "space-uid", "space-compute-scheduler"
	if _, err := client.CoreV1().Pods("workloads").Create(ctx, ordinary, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CoreV1().Pods("workloads").Create(ctx, space, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	waitForBinding(t, bindings, "workloads/ordinary", 5*time.Second)
	if _, exists := bindings.get("workloads/space"); exists {
		t.Fatal("default scheduler bound a Pod owned by space-compute-scheduler")
	}
	if exporterRequests.Load() != 0 {
		t.Fatal("default scheduler contacted the space exporter")
	}

	spaceProfile := decodeSpaceSchedulerProfile(t, exporter.URL)
	spaceCtx, cancelSpace := context.WithCancel(ctx)
	spaceScheduler, err := scheduler.New(
		spaceCtx, client, factory, nil, profile.NewRecorderFactory(broadcaster),
		scheduler.WithProfiles(spaceProfile),
		scheduler.WithFrameworkOutOfTreeRegistry(frameworkruntime.Registry{Name: New}),
		scheduler.WithPodInitialBackoffSeconds(0),
		scheduler.WithPodMaxBackoffSeconds(1),
	)
	if err != nil {
		t.Fatalf("create space scheduler: %v", err)
	}
	if err := spaceScheduler.WaitForHandlersSync(spaceCtx); err != nil {
		t.Fatalf("space scheduler handler sync: %v", err)
	}
	go spaceScheduler.Run(spaceCtx)
	waitForBinding(t, bindings, "workloads/space", 10*time.Second)
	if got, _ := bindings.get("workloads/space"); got != node.Name {
		t.Fatalf("space Pod bound to %q, want %q", got, node.Name)
	}
	if exporterRequests.Load() == 0 {
		t.Fatal("space scheduler never populated an exporter-backed snapshot")
	}

	// Stop only the space component. Default scheduling remains alive, while a
	// second space Pod remains pending until a fresh scheduler instance rebuilds
	// informers, collector targets, and snapshots.
	cancelSpace()
	if err := client.CoreV1().Pods("workloads").Delete(ctx, "space", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	restartedPod := podWithAcceleratorRequest("iluvatar.com/gpu", 1)
	restartedPod.Namespace, restartedPod.Name, restartedPod.UID, restartedPod.Spec.SchedulerName = "workloads", "space-after-restart", "space-restart-uid", "space-compute-scheduler"
	highPriority := int32(100)
	restartedPod.Spec.Priority = &highPriority
	if _, err := client.CoreV1().Pods("workloads").Create(ctx, restartedPod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	competitor := podWithAcceleratorRequest("iluvatar.com/gpu", 1)
	competitor.Namespace, competitor.Name, competitor.UID, competitor.Spec.SchedulerName = "workloads", "space-no-overcommit", "space-no-overcommit-uid", "space-compute-scheduler"
	if _, err := client.CoreV1().Pods("workloads").Create(ctx, competitor, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if _, exists := bindings.get("workloads/space-after-restart"); exists {
		t.Fatal("space Pod bound while the space scheduler was stopped")
	}

	restartCtx, cancelRestart := context.WithCancel(ctx)
	defer cancelRestart()
	restartFactory := scheduler.NewInformerFactory(client, 0)
	restartedScheduler, err := scheduler.New(
		restartCtx, client, restartFactory, nil, profile.NewRecorderFactory(broadcaster),
		scheduler.WithProfiles(decodeSpaceSchedulerProfile(t, exporter.URL)),
		scheduler.WithFrameworkOutOfTreeRegistry(frameworkruntime.Registry{Name: New}),
		scheduler.WithPodInitialBackoffSeconds(0), scheduler.WithPodMaxBackoffSeconds(1),
	)
	if err != nil {
		t.Fatalf("recreate space scheduler: %v", err)
	}
	restartFactory.Start(restartCtx.Done())
	restartFactory.WaitForCacheSync(restartCtx.Done())
	if err := restartedScheduler.WaitForHandlersSync(restartCtx); err != nil {
		t.Fatalf("restarted scheduler handler sync: %v", err)
	}
	go restartedScheduler.Run(restartCtx)
	waitForBinding(t, bindings, "workloads/space-after-restart", 10*time.Second)
	time.Sleep(300 * time.Millisecond)
	if _, exists := bindings.get("workloads/space-no-overcommit"); exists {
		t.Fatal("second space Pod overcommitted the one-device Kubernetes extended resource")
	}
}

func decodeSpaceSchedulerProfile(t *testing.T, exporterURL string) schedulerconfig.KubeSchedulerProfile {
	t.Helper()
	parsed, err := url.Parse(exporterURL)
	if err != nil {
		t.Fatal(err)
	}
	_, port, err := net.SplitHostPort(parsed.Host)
	if err != nil {
		t.Fatal(err)
	}
	raw := fmt.Sprintf(`apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
profiles:
- schedulerName: space-compute-scheduler
  plugins:
    preFilter: {enabled: [{name: %s}]}
    filter: {enabled: [{name: %s}]}
    preScore: {enabled: [{name: %s}]}
    score: {enabled: [{name: %s, weight: 5}]}
  pluginConfig:
  - name: %s
    args:
      apiVersion: gpustability.k3s.io/v1alpha1
      kind: K3SGPUStabilityArgs
      exporter:
        scheme: http
        port: %q
        allowInsecureHTTP: true
      collector:
        snapshotTTL: 1m
        refreshInterval: 30s
`, Name, Name, Name, Name, Name, port)
	decoded, _, err := schedulerconfigscheme.Codecs.UniversalDecoder().Decode([]byte(raw), nil, nil)
	if err != nil {
		t.Fatalf("decode scheduler profile: %v", err)
	}
	configuration := decoded.(*schedulerconfig.KubeSchedulerConfiguration)
	return configuration.Profiles[0]
}

func schedulerIntegrationNode(t *testing.T, exporterURL string) *v1.Node {
	t.Helper()
	parsed, err := url.Parse(exporterURL)
	if err != nil {
		t.Fatal(err)
	}
	host, port, err := net.SplitHostPort(parsed.Host)
	if err != nil {
		t.Fatal(err)
	}
	return &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "fixture-node", UID: "fixture-node-uid", Annotations: map[string]string{
			AnnotationExporterPort: port, AnnotationExporterScheme: "http", AnnotationExporterPath: "/metrics",
		}},
		Status: v1.NodeStatus{
			Addresses: []v1.NodeAddress{{Type: v1.NodeInternalIP, Address: host}},
			Capacity: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("4"), v1.ResourceMemory: resource.MustParse("8Gi"),
				v1.ResourcePods: resource.MustParse("100"), "iluvatar.com/gpu": resource.MustParse("1"),
			},
			Allocatable: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("4"), v1.ResourceMemory: resource.MustParse("8Gi"),
				v1.ResourcePods: resource.MustParse("100"), "iluvatar.com/gpu": resource.MustParse("1"),
			},
			Conditions: []v1.NodeCondition{{Type: v1.NodeReady, Status: v1.ConditionTrue}},
		},
	}
}

type bindingRecorder struct {
	mu    sync.Mutex
	nodes map[string]string
}

func (b *bindingRecorder) record(key, node string) {
	b.mu.Lock()
	b.nodes[key] = node
	b.mu.Unlock()
}

func (b *bindingRecorder) get(key string) (string, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	node, exists := b.nodes[key]
	return node, exists
}

func (b *bindingRecorder) snapshot() map[string]string {
	b.mu.Lock()
	defer b.mu.Unlock()
	result := make(map[string]string, len(b.nodes))
	for key, node := range b.nodes {
		result[key] = node
	}
	return result
}

func waitForBinding(t *testing.T, bindings *bindingRecorder, key string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, exists := bindings.get(key); exists {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for binding %s; bindings=%v", key, bindings.snapshot())
}
