package gpustability

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

func TestHTTPIntegrationIluvatarFixtureThroughProductionPipeline(t *testing.T) {
	fixture, err := os.ReadFile("testdata/fixtures/iluvatar.prom")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}

	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.URL.Path != defaultExporterPath {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write(fixture)
	}))
	t.Cleanup(server.Close)

	plugin := newHTTPTestPlugin(t, server.Client(), defaultTimeout)
	nodeInfo := nodeInfoForHTTPServer(t, server.URL, map[v1.ResourceName]int64{"iluvatar.com/gpu": 1})
	pod := podWithAcceleratorRequests(map[v1.ResourceName]int64{"iluvatar.com/gpu": 1})
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
	if got := requests.Load(); got != 1 {
		t.Fatalf("exporter requests = %d, want 1 cached request", got)
	}
}

func TestHTTPIntegrationGenericNPUFixtureThroughProductionPipeline(t *testing.T) {
	fixture, err := os.ReadFile("testdata/fixtures/generic-npu.prom")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write(fixture)
	}))
	t.Cleanup(server.Close)

	plugin := newHTTPTestPlugin(t, server.Client(), defaultTimeout)
	nodeInfo := nodeInfoForHTTPServer(t, server.URL, map[v1.ResourceName]int64{"huawei.com/npu": 1})
	pod := podWithAcceleratorRequests(map[v1.ResourceName]int64{"huawei.com/npu": 1})
	warmNode(t, plugin, nodeInfo)

	if status := plugin.Filter(context.Background(), cycleStateForPod(t, plugin, pod), pod, nodeInfo); !status.IsSuccess() {
		t.Fatalf("Filter status = %v, want success", status)
	}
}

func TestHTTPIntegrationExporterTimeoutAndCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	t.Cleanup(server.Close)

	nodeInfo := nodeInfoForHTTPServer(t, server.URL, map[v1.ResourceName]int64{"iluvatar.com/gpu": 1})
	pod := podWithAcceleratorRequests(map[v1.ResourceName]int64{"iluvatar.com/gpu": 1})

	t.Run("timeout", func(t *testing.T) {
		plugin := newHTTPTestPlugin(t, server.Client(), 20*time.Millisecond)
		if err := plugin.collector.refreshNode(context.Background(), nodeInfo.Node()); err == nil {
			t.Fatal("refreshNode error = nil, want exporter timeout")
		}
		status := plugin.Filter(context.Background(), cycleStateForPod(t, plugin, pod), pod, nodeInfo)
		if status.IsSuccess() {
			t.Fatal("Filter status = success, want exporter timeout rejection")
		}
	})

	t.Run("cancellation", func(t *testing.T) {
		plugin := newHTTPTestPlugin(t, server.Client(), time.Second)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := plugin.collector.refreshNode(ctx, nodeInfo.Node()); err == nil {
			t.Fatal("refreshNode error = nil, want cancellation")
		}
		status := plugin.Filter(ctx, cycleStateForPod(t, plugin, pod), pod, nodeInfo)
		if status.IsSuccess() {
			t.Fatal("Filter status = success, want canceled exporter request rejection")
		}
	})
}

func TestRegisteredPluginDoesNotAffectOptOutPod(t *testing.T) {
	var requests atomic.Int64
	plugin := newHTTPTestPlugin(t, &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		requests.Add(1)
		return nil, fmt.Errorf("unexpected exporter request")
	})}, defaultTimeout)
	pod := &v1.Pod{Spec: v1.PodSpec{Containers: []v1.Container{{Name: "ordinary"}}}}
	nodeInfo := framework.NewNodeInfo()
	state := cycleStateForPod(t, plugin, pod)

	if status := plugin.Filter(context.Background(), state, pod, nodeInfo); !status.IsSuccess() {
		t.Fatalf("Filter status = %v, want success", status)
	}
	if status := plugin.PreScore(context.Background(), state, pod, []*framework.NodeInfo{nodeInfo}); status.Code() != framework.Skip {
		t.Fatalf("PreScore status = %v, want Skip", status)
	}
	if score, status := plugin.Score(context.Background(), state, pod, nodeInfo); !status.IsSuccess() || score != framework.MinNodeScore {
		t.Fatalf("Score = %d, status = %v, want unchanged minimum contribution", score, status)
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("exporter requests = %d, want 0 for opt-out Pod", got)
	}
}

func TestSchedulingRequirementPreservesApplicationResourceRequests(t *testing.T) {
	plugin := newHTTPTestPlugin(t, http.DefaultClient, defaultTimeout)
	tests := []struct {
		name string
		pod  *v1.Pod
		want map[v1.ResourceName]int64
	}{
		{
			name: "one GPU",
			pod:  podWithAcceleratorRequests(map[v1.ResourceName]int64{"iluvatar.com/gpu": 1}),
			want: map[v1.ResourceName]int64{"iluvatar.com/gpu": 1},
		},
		{
			name: "one NPU",
			pod:  podWithAcceleratorRequests(map[v1.ResourceName]int64{"huawei.com/npu": 1}),
			want: map[v1.ResourceName]int64{"huawei.com/npu": 1},
		},
		{
			name: "mixed GPU and NPU",
			pod: podWithAcceleratorRequests(map[v1.ResourceName]int64{
				"iluvatar.com/gpu": 1,
				"huawei.com/npu":   1,
			}),
			want: map[v1.ResourceName]int64{
				"iluvatar.com/gpu": 1,
				"huawei.com/npu":   1,
			},
		},
		{
			name: "multiple application containers",
			pod: &v1.Pod{Spec: v1.PodSpec{Containers: []v1.Container{
				containerWithAcceleratorRequests("gpu-worker", map[v1.ResourceName]int64{"iluvatar.com/gpu": 2}),
				containerWithAcceleratorRequests("npu-worker", map[v1.ResourceName]int64{"huawei.com/npu": 3}),
			}}},
			want: map[v1.ResourceName]int64{
				"iluvatar.com/gpu": 2,
				"huawei.com/npu":   3,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requirement, err := plugin.schedulingRequirement(tt.pod)
			if err != nil {
				t.Fatalf("schedulingRequirement: %v", err)
			}
			if !requirement.Required {
				t.Fatal("Required = false, want true")
			}
			if len(requirement.Resources) != len(tt.want) {
				t.Fatalf("Resources = %v, want %v", requirement.Resources, tt.want)
			}
			for name, want := range tt.want {
				if got := requirement.Resources[name]; got != want {
					t.Fatalf("Resources[%q] = %d, want %d", name, got, want)
				}
			}
		})
	}
}

func newHTTPTestPlugin(t *testing.T, client *http.Client, timeout time.Duration) *Plugin {
	t.Helper()
	cfg := testConfig(t)
	cfg.Timeout = timeout
	collector, err := newCollector(context.Background(), cfg, client)
	if err != nil {
		t.Fatalf("newCollector: %v", err)
	}
	t.Cleanup(collector.Close)
	return &Plugin{config: cfg, collector: collector}
}

func nodeInfoForHTTPServer(t *testing.T, endpoint string, capacities map[v1.ResourceName]int64) *framework.NodeInfo {
	t.Helper()
	parsed, err := url.Parse(endpoint)
	if err != nil {
		t.Fatalf("parse test server URL: %v", err)
	}
	host, port, err := net.SplitHostPort(parsed.Host)
	if err != nil {
		t.Fatalf("split test server address: %v", err)
	}
	allocatable := v1.ResourceList{}
	for name, value := range capacities {
		allocatable[name] = *resource.NewQuantity(value, resource.DecimalSI)
	}
	nodeInfo := framework.NewNodeInfo()
	nodeInfo.SetNode(&v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "fixture-node",
			Annotations: map[string]string{
				AnnotationExporterPort:   port,
				AnnotationExporterPath:   defaultExporterPath,
				AnnotationExporterScheme: parsed.Scheme,
			},
		},
		Status: v1.NodeStatus{
			Addresses:   []v1.NodeAddress{{Type: v1.NodeInternalIP, Address: host}},
			Allocatable: allocatable,
		},
	})
	return nodeInfo
}

func podWithAcceleratorRequests(requests map[v1.ResourceName]int64) *v1.Pod {
	return &v1.Pod{Spec: v1.PodSpec{Containers: []v1.Container{
		containerWithAcceleratorRequests("workload", requests),
	}}}
}

func containerWithAcceleratorRequests(name string, requests map[v1.ResourceName]int64) v1.Container {
	resources := v1.ResourceList{}
	for resourceName, value := range requests {
		resources[resourceName] = resource.MustParse(strconv.FormatInt(value, 10))
	}
	return v1.Container{
		Name: name,
		Resources: v1.ResourceRequirements{
			Requests: resources,
			Limits:   resources.DeepCopy(),
		},
	}
}
