package gpustability

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
	spaceplanner "github.com/k3s-io/k3s/contrib/space-compute/pkg/planner"
	spacepolicy "github.com/k3s-io/k3s/contrib/space-compute/pkg/policy"
	spaceworkload "github.com/k3s-io/k3s/contrib/space-compute/pkg/workload"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientsetfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/events"
	"k8s.io/kubernetes/pkg/scheduler"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	"k8s.io/kubernetes/pkg/scheduler/profile"
)

func TestPhase4FullCPUFlowBindsAndTracksResultStatus(t *testing.T) {
	now := time.Now().UTC().Add(-15 * time.Second).Truncate(time.Second)
	mission := phase4Mission(now)
	mission.Spec.SafetyMarginSeconds = 15
	mission.Spec.MaximumClockSkewSeconds = 0
	summary := phase4Summary(now)
	link := phase4ReturnLink(now)
	decision, err := spaceplanner.Plan(mission, []*spacev1.SpaceDomainResourceSummary{summary}, []*spacev1.SpaceLinkSnapshot{link}, phase4Clock{now})
	if err != nil {
		t.Fatal(err)
	}

	var exporterRequests atomic.Int64
	exporter := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		exporterRequests.Add(1)
		_, _ = w.Write([]byte(readPhase2Fixture(t, "iluvatar.prom")))
	}))
	defer exporter.Close()
	node := schedulerIntegrationNode(t, exporter.URL)
	projected, err := spacepolicy.ProjectNode(node, summary, []*spacev1.SpaceLinkSnapshot{link}, phase4Clock{now})
	if err != nil {
		t.Fatal(err)
	}
	pod, err := spaceworkload.BuildAttemptPod(mission, decision.Placement, mission.Spec.WorkloadTemplate)
	if err != nil {
		t.Fatal(err)
	}
	pod.UID = "phase4-pod-uid"

	client := clientsetfake.NewClientset(&v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: mission.Namespace}}, projected)
	factory := scheduler.NewInformerFactory(client, 0)
	broadcaster := events.NewBroadcaster(&events.EventSinkImpl{Interface: client.EventsV1()})
	defer broadcaster.Shutdown()
	bindings := &bindingRecorder{nodes: map[string]string{}}
	client.PrependReactor("create", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "binding" {
			return false, nil, nil
		}
		binding := action.(clienttesting.CreateAction).GetObject().(*v1.Binding)
		resource := v1.SchemeGroupVersion.WithResource("pods")
		if object, getErr := client.Tracker().Get(resource, binding.Namespace, binding.Name); getErr == nil {
			bound := object.(*v1.Pod).DeepCopy()
			bound.Spec.NodeName = binding.Target.Name
			_ = client.Tracker().Update(resource, bound, binding.Namespace)
		}
		bindings.record(binding.Namespace+"/"+binding.Name, binding.Target.Name)
		return true, binding, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	spaceScheduler, err := scheduler.New(ctx, client, factory, nil, profile.NewRecorderFactory(broadcaster), scheduler.WithProfiles(decodeSpaceSchedulerProfile(t, exporter.URL)), scheduler.WithFrameworkOutOfTreeRegistry(frameworkruntime.Registry{Name: New}), scheduler.WithPodInitialBackoffSeconds(0), scheduler.WithPodMaxBackoffSeconds(1))
	if err != nil {
		t.Fatal(err)
	}
	originalFailure := spaceScheduler.FailureHandler
	spaceScheduler.FailureHandler = func(ctx context.Context, fwk framework.Framework, podInfo *framework.QueuedPodInfo, status *framework.Status, nominatingInfo *framework.NominatingInfo, start time.Time) {
		t.Logf("Phase 4 scheduler failure for %s/%s: %s", podInfo.Pod.Namespace, podInfo.Pod.Name, status.Message())
		originalFailure(ctx, fwk, podInfo, status, nominatingInfo, start)
	}
	factory.Start(ctx.Done())
	factory.WaitForCacheSync(ctx.Done())
	if err := spaceScheduler.WaitForHandlersSync(ctx); err != nil {
		t.Fatal(err)
	}
	go spaceScheduler.Run(ctx)
	if _, err := client.CoreV1().Pods(mission.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	waitForBinding(t, bindings, mission.Namespace+"/"+pod.Name, 10*time.Second)
	if got, _ := bindings.get(mission.Namespace + "/" + pod.Name); got != projected.Name {
		t.Fatalf("bound node=%s want=%s", got, projected.Name)
	}
	if exporterRequests.Load() == 0 {
		t.Fatal("exporter snapshot layer was not exercised")
	}

	for sequence, phase := range []string{"dispatched", "running", "return-pending", "completed"} {
		observation := spacev1.ExecutionObservation{Sequence: int64(sequence + 1), Attempt: decision.Placement.Spec.Attempt, PodUID: string(pod.UID), Phase: phase, ObservedAt: metav1.NewTime(time.Now().UTC())}
		changed, applyErr := spaceplanner.ApplyExecutionObservation(decision.Placement, mission, observation, spacev1.RealClock{})
		if applyErr != nil || !changed {
			t.Fatalf("observation %s changed=%v err=%v", phase, changed, applyErr)
		}
	}
	if decision.Placement.Status.Phase != spacev1.PlacementCompleted || !decision.Placement.Status.ResultReturned {
		t.Fatalf("terminal status=%+v", decision.Placement.Status)
	}
}
