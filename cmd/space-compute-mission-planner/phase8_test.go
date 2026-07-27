package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
	spacekube "github.com/k3s-io/k3s/contrib/space-compute/pkg/kube"
	spaceplanner "github.com/k3s-io/k3s/contrib/space-compute/pkg/planner"
)

func TestDependencyIndexesRequeueOnlyAffectedMissions(t *testing.T) {
	missions := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
		missionUIDIndex:               indexMissionUID,
		missionDomainIndex:            indexMissionDomains,
		missionInputDomainIndex:       indexMissionInputDomains,
		missionInputLocationIndex:     indexMissionInputLocations,
		missionResultDomainIndex:      indexMissionResultDomains,
		missionResultDestinationIndex: indexMissionResultDestinations,
		missionCapabilityIndex:        indexMissionCapabilityClasses,
	})
	placements := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
		spacekube.PlacementMissionUIDIndex: indexPlacementMissionUID,
		placementTargetIndex:               indexPlacementTarget,
	})
	ground := spacev1.DomainReference{Name: "ground-a", ClusterID: "cluster-ground", OrbitClass: spacev1.OrbitGround}
	leo := spacev1.DomainReference{Name: "leo-a", ClusterID: "cluster-leo", OrbitClass: spacev1.OrbitLEO}
	other := spacev1.DomainReference{Name: "other", ClusterID: "cluster-other", OrbitClass: spacev1.OrbitLEO}
	missionA := &spacev1.SpaceMission{ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: "missions", UID: "uid-a"}, Spec: spacev1.SpaceMissionSpec{RequiredCapabilities: []spacev1.CapabilityRequirement{{Class: "gpu", Quantity: 1}}, Inputs: []spacev1.DataObject{{ID: "input", Locations: []spacev1.DataLocation{{Domain: ground, URI: "s3://ground/input"}}}}, ResultDestinations: []spacev1.DataLocation{{Domain: leo, URI: "s3://leo/result"}}}}
	missionB := &spacev1.SpaceMission{ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "missions", UID: "uid-b"}, Spec: spacev1.SpaceMissionSpec{RequiredCapabilities: []spacev1.CapabilityRequirement{{Class: "npu", Quantity: 1}}, Inputs: []spacev1.DataObject{{ID: "input", Locations: []spacev1.DataLocation{{Domain: other}}}}}}
	for _, mission := range []*spacev1.SpaceMission{missionA, missionB} {
		if err := missions.Add(mustUnstructured(t, mission)); err != nil {
			t.Fatal(err)
		}
	}
	inputMatches, err := missions.ByIndex(missionInputLocationIndex, dataLocationDependencyKey(missionA.Spec.Inputs[0].Locations[0]))
	if err != nil || len(inputMatches) != 1 {
		t.Fatalf("input location index matches=%d err=%v", len(inputMatches), err)
	}
	resultMatches, err := missions.ByIndex(missionResultDestinationIndex, dataLocationDependencyKey(missionA.Spec.ResultDestinations[0]))
	if err != nil || len(resultMatches) != 1 {
		t.Fatalf("result destination index matches=%d err=%v", len(resultMatches), err)
	}
	placement := &spacev1.SpacePlacementIntent{ObjectMeta: metav1.ObjectMeta{Name: "a-placement", Namespace: "missions"}, Spec: spacev1.SpacePlacementIntentSpec{MissionRef: corev1.ObjectReference{Namespace: "missions", Name: "a", UID: "uid-a"}, Target: leo}}
	if err := placements.Add(mustUnstructured(t, placement)); err != nil {
		t.Fatal(err)
	}

	q := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	defer q.ShutDown()
	summary := &spacev1.SpaceDomainResourceSummary{ObjectMeta: metav1.ObjectMeta{Name: "leo-a"}, Spec: spacev1.SpaceDomainResourceSummarySpec{Domain: leo, Devices: []spacev1.DeviceCapacity{{Class: "gpu", Count: 4}}}}
	enqueueResourceDependents(mustUnstructured(t, summary), missions, placements, q)
	assertOnlyQueuedKey(t, q, "missions/a")

	link := &spacev1.SpaceLinkSnapshot{ObjectMeta: metav1.ObjectMeta{Name: "ground-leo"}, Spec: spacev1.SpaceLinkSnapshotSpec{Source: ground, Destination: leo}}
	enqueueLinkDependents(mustUnstructured(t, link), missions, placements, q)
	assertOnlyQueuedKey(t, q, "missions/a")

	receipt := &spacev1.SpaceTransferReceipt{ObjectMeta: metav1.ObjectMeta{Name: "receipt"}, Spec: spacev1.SpaceTransferReceiptSpec{MissionUID: "uid-b"}}
	enqueueEvidenceDependent(mustUnstructured(t, receipt), missions, q)
	assertOnlyQueuedKey(t, q, "missions/b")
}

func TestBoundedQueueFailsClosedAtUniqueKeyCapacity(t *testing.T) {
	observer := spaceplanner.PrometheusObserver{}
	q := newBoundedRateLimitingQueue("planner_missions", 2, observer)
	defer q.ShutDown()
	base := time.Date(2026, 7, 27, 0, 0, 0, 0, time.UTC)
	now := base
	q.now = func() time.Time { return now }
	q.Add("missions/a")
	now = now.Add(10 * time.Second)
	q.Add("missions/b")
	q.Add("missions/c")
	if q.PendingUnique() != 2 {
		t.Fatalf("pending unique=%d, want 2", q.PendingUnique())
	}
	select {
	case <-q.Saturated():
	default:
		t.Fatal("queue did not signal saturation")
	}
	now = now.Add(5 * time.Second)
	q.Observe()
}

func TestTooManyRequestsIsRateLimitedAndNotForgotten(t *testing.T) {
	q := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	defer q.ShutDown()
	item := "missions/a"
	q.Add(item)
	got, shutdown := q.Get()
	if shutdown || got != item {
		t.Fatalf("get=%v shutdown=%v", got, shutdown)
	}
	cause := apierrors.NewTooManyRequests("throttled", 1)
	if exhausted := retryControllerItem(q, got, "planner_missions", cause, spaceplanner.PrometheusObserver{}); exhausted {
		t.Fatal("429 exhausted retry budget on first failure")
	}
	q.Done(got)
	if q.NumRequeues(item) != 1 {
		t.Fatalf("requeues=%d, want 1", q.NumRequeues(item))
	}
}

func TestConfigureAPIClientThrottling(t *testing.T) {
	config := &rest.Config{}
	if err := configureAPIClient(config, 12.5, 25); err != nil {
		t.Fatal(err)
	}
	if config.QPS != 12.5 || config.Burst != 25 {
		t.Fatalf("qps=%v burst=%d", config.QPS, config.Burst)
	}
	if err := configureAPIClient(config, 0, 25); err == nil {
		t.Fatal("zero QPS accepted")
	}
}

func TestInformerRelistsAfterExpiredWatch(t *testing.T) {
	var listCalls atomic.Int32
	var watchCalls atomic.Int32
	var generation atomic.Int32
	generation.Store(1)
	watchers := make(chan *watch.RaceFreeFakeWatcher, 4)
	lw := &cache.ListWatch{
		ListFunc: func(metav1.ListOptions) (runtime.Object, error) {
			call := listCalls.Add(1)
			value := generation.Load()
			return &corev1.PodList{ListMeta: metav1.ListMeta{ResourceVersion: fmt.Sprintf("%d", call)}, Items: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "n", ResourceVersion: fmt.Sprintf("%d", call), Labels: map[string]string{"generation": fmt.Sprintf("%d", value)}}}}}, nil
		},
		WatchFunc: func(metav1.ListOptions) (watch.Interface, error) {
			watchCalls.Add(1)
			w := watch.NewRaceFreeFake()
			watchers <- w
			return w, nil
		},
	}
	informer := cache.NewSharedIndexInformer(lw, &corev1.Pod{}, 0, cache.Indexers{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go informer.Run(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		t.Fatal("initial informer sync failed")
	}
	var first *watch.RaceFreeFakeWatcher
	select {
	case first = <-watchers:
	case <-time.After(2 * time.Second):
		t.Fatal("initial watch not established")
	}
	generation.Store(2)
	first.Error(&metav1.Status{Status: metav1.StatusFailure, Reason: metav1.StatusReasonExpired, Code: 410, Message: "resource version expired"})
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if listCalls.Load() >= 2 {
			object, exists, err := informer.GetStore().GetByKey("n/p")
			if err == nil && exists {
				if pod, ok := object.(*corev1.Pod); ok && pod.Labels["generation"] == "2" {
					return
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("informer did not relist after expired watch: listCalls=%d watchCalls=%d", listCalls.Load(), watchCalls.Load())
}

func mustUnstructured(t *testing.T, value interface{}) *unstructured.Unstructured {
	t.Helper()
	object, err := runtime.DefaultUnstructuredConverter.ToUnstructured(value)
	if err != nil {
		t.Fatal(err)
	}
	return &unstructured.Unstructured{Object: object}
}

func assertOnlyQueuedKey(t *testing.T, q workqueue.RateLimitingInterface, want string) {
	t.Helper()
	if q.Len() != 1 {
		t.Fatalf("queue len=%d, want 1", q.Len())
	}
	item, shutdown := q.Get()
	if shutdown {
		t.Fatal("queue shut down")
	}
	q.Done(item)
	q.Forget(item)
	if item != want {
		t.Fatalf("queued=%v, want %s", item, want)
	}
}
