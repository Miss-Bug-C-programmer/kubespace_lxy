package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	dynamicinformer "k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	clientcmd "k8s.io/client-go/tools/clientcmd"
	leaderelection "k8s.io/client-go/tools/leaderelection"
	resourcelock "k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/klog/v2"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
	spacekube "github.com/k3s-io/k3s/contrib/space-compute/pkg/kube"
	spaceplanner "github.com/k3s-io/k3s/contrib/space-compute/pkg/planner"
	spacepolicy "github.com/k3s-io/k3s/contrib/space-compute/pkg/policy"
	spaceworkload "github.com/k3s-io/k3s/contrib/space-compute/pkg/workload"
)

const componentName = "space-compute-mission-planner"
const maxControllerRetries = 15
const nodeProjectorFieldManager = "space-compute-node-projector"

type controllerRole string

const (
	rolePlanner    controllerRole = "planner"
	roleDispatcher controllerRole = "workload-dispatcher"
	roleProjector  controllerRole = "node-projector"
	roleTransport  controllerRole = "transport-agent"
)

type options struct {
	kubeconfig, master, metricsAddress, leaderNamespace, leaderName, role string
	workers                                                               int
	leaderElect                                                           bool
}

func main() {
	klog.InitFlags(nil)
	opt := options{}
	flag.StringVar(&opt.kubeconfig, "kubeconfig", "", "Path to kubeconfig; empty uses in-cluster configuration")
	flag.StringVar(&opt.master, "master", "", "Optional API server address")
	flag.StringVar(&opt.metricsAddress, "metrics-bind-address", ":10261", "Health and metrics listen address")
	flag.StringVar(&opt.leaderNamespace, "leader-election-namespace", "kube-system", "Leader Lease namespace")
	flag.StringVar(&opt.leaderName, "leader-election-name", componentName, "Leader Lease name")
	flag.StringVar(&opt.role, "controller-role", string(rolePlanner), "Controller role: planner, workload-dispatcher, node-projector, or transport-agent")
	flag.IntVar(&opt.workers, "workers", 2, "Bounded reconciliation worker count")
	flag.BoolVar(&opt.leaderElect, "leader-elect", true, "Use a namespaced Lease for active/standby operation")
	flag.Parse()
	if opt.workers < 1 || opt.workers > 32 {
		klog.Fatalf("workers must be between 1 and 32")
	}
	if !validControllerRole(controllerRole(opt.role)) {
		klog.Fatalf("controller-role must be planner, workload-dispatcher, node-projector, or transport-agent")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := run(ctx, opt); err != nil {
		klog.Fatalf("%s failed: %v", componentName, err)
	}
}

func validControllerRole(role controllerRole) bool {
	return role == rolePlanner || role == roleDispatcher || role == roleProjector || role == roleTransport
}

func run(ctx context.Context, opt options) error {
	config, err := kubeConfig(opt.master, opt.kubeconfig)
	if err != nil {
		return err
	}
	role := controllerRole(opt.role)
	if !validControllerRole(role) {
		return fmt.Errorf("invalid controller role %q", opt.role)
	}
	config.UserAgent = componentName + "/" + string(role)
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return err
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}
	recorder := eventRecorder(client)
	var ready atomic.Bool
	server := healthServer(opt.metricsAddress, &ready)
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			klog.ErrorS(err, "health server stopped")
		}
	}()
	defer server.Shutdown(context.Background())
	start := func(leaderCtx context.Context) {
		if err := runRoleControllers(leaderCtx, dynamicClient, client, recorder, opt.workers, role, &ready); err != nil {
			klog.ErrorS(err, "controller set stopped", "role", role)
			ready.Store(false)
		}
	}
	if !opt.leaderElect {
		start(ctx)
		return nil
	}
	host, _ := os.Hostname()
	identity := host + "_" + string(uuid.NewUUID())
	lock, err := resourcelock.New(resourcelock.LeasesResourceLock, opt.leaderNamespace, opt.leaderName, client.CoreV1(), client.CoordinationV1(), resourcelock.ResourceLockConfig{Identity: identity, EventRecorder: recorder})
	if err != nil {
		return err
	}
	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{Lock: lock, LeaseDuration: 30 * time.Second, RenewDeadline: 20 * time.Second, RetryPeriod: 5 * time.Second, ReleaseOnCancel: true, Name: componentName + "/" + string(role), Callbacks: leaderelection.LeaderCallbacks{OnStartedLeading: start, OnStoppedLeading: func() { ready.Store(false) }}})
	return nil
}

func runRoleControllers(ctx context.Context, dynamicClient dynamic.Interface, client kubernetes.Interface, recorder record.EventRecorder, workers int, role controllerRole, ready *atomic.Bool) error {
	switch role {
	case rolePlanner:
		return runPlannerControllers(ctx, dynamicClient, recorder, workers, ready)
	case roleDispatcher:
		return runDispatcherControllers(ctx, dynamicClient, client, recorder, workers, ready)
	case roleProjector:
		return runProjectorControllers(ctx, dynamicClient, client, recorder, ready)
	case roleTransport:
		return runTransportControllers(ctx, dynamicClient, recorder, workers, ready)
	default:
		return fmt.Errorf("unsupported controller role %q", role)
	}
}

func runPlannerControllers(ctx context.Context, dynamicClient dynamic.Interface, recorder record.EventRecorder, workers int, ready *atomic.Bool) error {
	factory := dynamicinformer.NewDynamicSharedInformerFactory(dynamicClient, 10*time.Minute)
	missions := factory.ForResource(spacekube.MissionGVR).Informer()
	placements := factory.ForResource(spacekube.PlacementGVR).Informer()
	links := factory.ForResource(spacekube.LinkGVR).Informer()
	resources := factory.ForResource(spacekube.ResourceSummaryGVR).Informer()
	queue := workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "space_compute_planner_missions")
	resourceQueue := workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "space_compute_resource_status")
	defer queue.ShutDown()
	defer resourceQueue.ShutDown()
	enqueueMission := func(object interface{}) {
		if key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(object); err == nil {
			queue.Add(key)
		}
	}
	_, _ = missions.AddEventHandler(cache.ResourceEventHandlerFuncs{AddFunc: enqueueMission, UpdateFunc: func(_, value interface{}) { enqueueMission(value) }, DeleteFunc: enqueueMission})
	_, _ = placements.AddEventHandler(cache.ResourceEventHandlerFuncs{AddFunc: func(value interface{}) { enqueuePlacementMission(value, queue) }, UpdateFunc: func(_, value interface{}) { enqueuePlacementMission(value, queue) }, DeleteFunc: func(value interface{}) { enqueuePlacementMission(value, queue) }})
	resourceHandler := cache.ResourceEventHandlerFuncs{AddFunc: func(interface{}) { resourceQueue.Add("resources") }, UpdateFunc: func(_, _ interface{}) { resourceQueue.Add("resources") }, DeleteFunc: func(interface{}) { resourceQueue.Add("resources") }}
	_, _ = links.AddEventHandler(resourceHandler)
	_, _ = resources.AddEventHandler(resourceHandler)
	factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), missions.HasSynced, placements.HasSynced, links.HasSynced, resources.HasSynced) {
		return fmt.Errorf("planner informer cache synchronization failed")
	}
	observer := spaceplanner.NewPrometheusObserver()
	repository := &spacekube.Repository{Dynamic: dynamicClient, Recorder: recorder, Observer: observer}
	plannerController := &spaceplanner.Controller{Repository: repository, Clock: spacev1.RealClock{}, Observer: observer}
	statusController := &resourceStatusController{dynamic: dynamicClient, recorder: recorder, clock: spacev1.RealClock{}, observer: observer}
	go wait.UntilWithContext(ctx, func(ctx context.Context) {
		processResourceStatus(ctx, resourceQueue, queue, missions.GetStore(), statusController, observer)
	}, time.Second)
	resourceQueue.Add("resources")
	for i := 0; i < workers; i++ {
		go wait.UntilWithContext(ctx, func(ctx context.Context) { processPlannerMission(ctx, queue, plannerController, observer) }, time.Second)
	}
	ready.Store(true)
	<-ctx.Done()
	ready.Store(false)
	return nil
}

func runDispatcherControllers(ctx context.Context, dynamicClient dynamic.Interface, client kubernetes.Interface, recorder record.EventRecorder, workers int, ready *atomic.Bool) error {
	factory := dynamicinformer.NewDynamicSharedInformerFactory(dynamicClient, 10*time.Minute)
	missions := factory.ForResource(spacekube.MissionGVR).Informer()
	placements := factory.ForResource(spacekube.PlacementGVR).Informer()
	transferIntents := factory.ForResource(spacekube.TransferIntentGVR).Informer()
	transferReceipts := factory.ForResource(spacekube.TransferReceiptGVR).Informer()
	executionLeases := factory.ForResource(spacekube.ExecutionLeaseGVR).Informer()
	executionObservations := factory.ForResource(spacekube.ExecutionObservationGVR).Informer()
	resultReceipts := factory.ForResource(spacekube.ResultReceiptGVR).Informer()
	coreFactory := informers.NewSharedInformerFactory(client, 10*time.Minute)
	pods := coreFactory.Core().V1().Pods().Informer()
	queue := workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "space_compute_dispatch_missions")
	evidenceQueue := workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "space_compute_dispatch_evidence")
	defer queue.ShutDown()
	defer evidenceQueue.ShutDown()
	enqueueMission := func(object interface{}) {
		if key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(object); err == nil {
			queue.Add(key)
		}
	}
	_, _ = missions.AddEventHandler(cache.ResourceEventHandlerFuncs{AddFunc: enqueueMission, UpdateFunc: func(_, value interface{}) { enqueueMission(value) }, DeleteFunc: enqueueMission})
	_, _ = placements.AddEventHandler(cache.ResourceEventHandlerFuncs{AddFunc: func(value interface{}) { enqueuePlacementMission(value, queue) }, UpdateFunc: func(_, value interface{}) { enqueuePlacementMission(value, queue) }, DeleteFunc: func(value interface{}) { enqueuePlacementMission(value, queue) }})
	_, _ = pods.AddEventHandler(cache.ResourceEventHandlerFuncs{AddFunc: func(value interface{}) { enqueuePodMission(value, queue) }, UpdateFunc: func(_, value interface{}) { enqueuePodMission(value, queue) }, DeleteFunc: func(value interface{}) { enqueuePodMission(value, queue) }})
	evidenceHandler := cache.ResourceEventHandlerFuncs{AddFunc: func(interface{}) { evidenceQueue.Add("evidence") }, UpdateFunc: func(_, _ interface{}) { evidenceQueue.Add("evidence") }, DeleteFunc: func(interface{}) { evidenceQueue.Add("evidence") }}
	for _, informer := range []cache.SharedIndexInformer{transferIntents, transferReceipts, executionLeases, executionObservations, resultReceipts} {
		_, _ = informer.AddEventHandler(evidenceHandler)
	}
	factory.Start(ctx.Done())
	coreFactory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), missions.HasSynced, placements.HasSynced, transferIntents.HasSynced, transferReceipts.HasSynced, executionLeases.HasSynced, executionObservations.HasSynced, resultReceipts.HasSynced, pods.HasSynced) {
		return fmt.Errorf("workload-dispatcher informer cache synchronization failed")
	}
	observer := spaceplanner.NewPrometheusObserver()
	repository := &spacekube.Repository{Dynamic: dynamicClient, Recorder: recorder, Observer: observer}
	workloadStore := &spacekube.WorkloadStore{Client: client, Repository: repository, Recorder: recorder}
	workloadController := &spaceworkload.Controller{Store: workloadStore, Evidence: workloadStore, Clock: spacev1.RealClock{}}
	if raw := os.Getenv("SPACE_COMPUTE_LOCAL_DOMAIN_JSON"); raw != "" {
		var localDomain spacev1.DomainReference
		if err := json.Unmarshal([]byte(raw), &localDomain); err != nil || localDomain.Name == "" || localDomain.ClusterID == "" {
			return fmt.Errorf("invalid SPACE_COMPUTE_LOCAL_DOMAIN_JSON")
		}
		workloadController.LocalDomain = &localDomain
	}
	go wait.UntilWithContext(ctx, func(ctx context.Context) {
		processEvidenceQueue(ctx, evidenceQueue, queue, missions.GetStore(), observer)
	}, time.Second)
	for i := 0; i < workers; i++ {
		go wait.UntilWithContext(ctx, func(ctx context.Context) {
			processDispatchMission(ctx, queue, repository, workloadController, observer)
		}, time.Second)
	}
	ready.Store(true)
	<-ctx.Done()
	ready.Store(false)
	return nil
}

func runTransportControllers(ctx context.Context, dynamicClient dynamic.Interface, recorder record.EventRecorder, workers int, ready *atomic.Bool) error {
	factory := dynamicinformer.NewDynamicSharedInformerFactory(dynamicClient, 10*time.Minute)
	missions := factory.ForResource(spacekube.MissionGVR).Informer()
	placements := factory.ForResource(spacekube.PlacementGVR).Informer()
	queue := workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "space_compute_transport_missions")
	defer queue.ShutDown()
	enqueueMission := func(object interface{}) {
		if key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(object); err == nil {
			queue.Add(key)
		}
	}
	_, _ = missions.AddEventHandler(cache.ResourceEventHandlerFuncs{AddFunc: enqueueMission, UpdateFunc: func(_, value interface{}) { enqueueMission(value) }, DeleteFunc: enqueueMission})
	_, _ = placements.AddEventHandler(cache.ResourceEventHandlerFuncs{AddFunc: func(value interface{}) { enqueuePlacementMission(value, queue) }, UpdateFunc: func(_, value interface{}) { enqueuePlacementMission(value, queue) }, DeleteFunc: func(value interface{}) { enqueuePlacementMission(value, queue) }})
	factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), missions.HasSynced, placements.HasSynced) {
		return fmt.Errorf("transport-agent informer cache synchronization failed")
	}
	localDomain, err := localDomainIdentity()
	if err != nil {
		return err
	}
	observer := spaceplanner.NewPrometheusObserver()
	repository := &spacekube.Repository{Dynamic: dynamicClient, Recorder: recorder, Observer: observer}
	store := &spacekube.WorkloadStore{Repository: repository, Recorder: recorder}
	for i := 0; i < workers; i++ {
		go wait.UntilWithContext(ctx, func(ctx context.Context) {
			processTransportMission(ctx, queue, repository, store, localDomain, observer)
		}, time.Second)
	}
	ready.Store(true)
	<-ctx.Done()
	ready.Store(false)
	return nil
}

func localDomainIdentity() (spacev1.DomainReference, error) {
	raw := os.Getenv("SPACE_COMPUTE_LOCAL_DOMAIN_JSON")
	if raw == "" {
		return spacev1.DomainReference{}, fmt.Errorf("SPACE_COMPUTE_LOCAL_DOMAIN_JSON is required for transport-agent")
	}
	var localDomain spacev1.DomainReference
	if err := json.Unmarshal([]byte(raw), &localDomain); err != nil || localDomain.Name == "" || localDomain.ClusterID == "" {
		return spacev1.DomainReference{}, fmt.Errorf("invalid SPACE_COMPUTE_LOCAL_DOMAIN_JSON")
	}
	return localDomain, nil
}

func runProjectorControllers(ctx context.Context, dynamicClient dynamic.Interface, client kubernetes.Interface, recorder record.EventRecorder, ready *atomic.Bool) error {
	factory := dynamicinformer.NewDynamicSharedInformerFactory(dynamicClient, 10*time.Minute)
	links := factory.ForResource(spacekube.LinkGVR).Informer()
	resources := factory.ForResource(spacekube.ResourceSummaryGVR).Informer()
	coreFactory := informers.NewSharedInformerFactory(client, 10*time.Minute)
	nodes := coreFactory.Core().V1().Nodes().Informer()
	queue := workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "space_compute_node_projection")
	defer queue.ShutDown()
	handler := cache.ResourceEventHandlerFuncs{AddFunc: func(interface{}) { queue.Add("projection") }, UpdateFunc: func(_, _ interface{}) { queue.Add("projection") }, DeleteFunc: func(interface{}) { queue.Add("projection") }}
	_, _ = links.AddEventHandler(handler)
	_, _ = resources.AddEventHandler(handler)
	_, _ = nodes.AddEventHandler(handler)
	factory.Start(ctx.Done())
	coreFactory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), links.HasSynced, resources.HasSynced, nodes.HasSynced) {
		return fmt.Errorf("node-projector informer cache synchronization failed")
	}
	observer := spaceplanner.NewPrometheusObserver()
	projector := &nodeProjector{dynamic: dynamicClient, client: client, clock: spacev1.RealClock{}, observer: observer}
	go wait.UntilWithContext(ctx, func(ctx context.Context) { processProjector(ctx, queue, projector, observer) }, time.Second)
	queue.Add("projection")
	ready.Store(true)
	<-ctx.Done()
	ready.Store(false)
	_ = recorder
	return nil
}

func processPlannerMission(ctx context.Context, queue workqueue.RateLimitingInterface, plannerController *spaceplanner.Controller, observer spaceplanner.PrometheusObserver) {
	item, shutdown := queue.Get()
	if shutdown {
		return
	}
	defer queue.Done(item)
	defer observer.QueueDepth("planner_missions", queue.Len())
	key, ok := item.(string)
	if !ok {
		queue.Forget(item)
		return
	}
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		queue.Forget(item)
		return
	}
	result, err := plannerController.Reconcile(ctx, spaceplanner.MissionKey{Namespace: namespace, Name: name})
	if err != nil {
		retryControllerItem(queue, item, "planner_missions", err, observer)
		return
	}
	queue.Forget(item)
	if result.RequeueAfter > 0 {
		queue.AddAfter(item, result.RequeueAfter)
	}
}

func processDispatchMission(ctx context.Context, queue workqueue.RateLimitingInterface, repository *spacekube.Repository, workloadController *spaceworkload.Controller, observer spaceplanner.PrometheusObserver) {
	item, shutdown := queue.Get()
	if shutdown {
		return
	}
	defer queue.Done(item)
	defer observer.QueueDepth("dispatch_missions", queue.Len())
	key, ok := item.(string)
	if !ok {
		queue.Forget(item)
		return
	}
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		queue.Forget(item)
		return
	}
	mission, err := repository.GetMission(ctx, spaceplanner.MissionKey{Namespace: namespace, Name: name})
	if errors.Is(err, spaceplanner.ErrNotFound) {
		queue.Forget(item)
		return
	}
	if err != nil {
		retryControllerItem(queue, item, "dispatch_missions", err, observer)
		return
	}
	placement, err := repository.GetPlacement(ctx, spaceplanner.MissionKey{Namespace: namespace, Name: name})
	if errors.Is(err, spaceplanner.ErrNotFound) {
		queue.Forget(item)
		return
	}
	if err != nil {
		retryControllerItem(queue, item, "dispatch_missions", err, observer)
		return
	}
	delay, err := workloadController.ReconcileDispatch(ctx, mission, placement, mission.Spec.WorkloadTemplate)
	if err != nil {
		retryControllerItem(queue, item, "dispatch_missions", err, observer)
		return
	}
	placement, _ = repository.GetPlacement(ctx, spaceplanner.MissionKey{Namespace: namespace, Name: name})
	if placement != nil {
		if _, err := workloadController.ReconcileTrustedEvidence(ctx, mission, placement); err != nil {
			retryControllerItem(queue, item, "dispatch_missions", err, observer)
			return
		}
	}
	placement, _ = repository.GetPlacement(ctx, spaceplanner.MissionKey{Namespace: namespace, Name: name})
	if placement != nil && placement.Status.ActivePod != nil && placement.Status.ActivePod.Name != "" {
		store, ok := workloadController.Store.(*spacekube.WorkloadStore)
		if ok {
			pod, podErr := store.GetPod(ctx, placement.Status.ActivePod.Namespace, placement.Status.ActivePod.Name)
			if podErr == nil {
				if _, err := workloadController.ReconcilePodStatus(ctx, mission, placement, pod); err != nil {
					retryControllerItem(queue, item, "dispatch_missions", err, observer)
					return
				}
			}
		}
	}
	queue.Forget(item)
	if delay > 0 {
		queue.AddAfter(item, delay)
	}
}

func processEvidenceQueue(_ context.Context, evidenceQueue, missionQueue workqueue.RateLimitingInterface, missions cache.Store, observer spaceplanner.PrometheusObserver) {
	item, shutdown := evidenceQueue.Get()
	if shutdown {
		return
	}
	defer evidenceQueue.Done(item)
	evidenceQueue.Forget(item)
	for _, object := range missions.List() {
		if key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(object); err == nil {
			missionQueue.Add(key)
		}
	}
	observer.QueueDepth("dispatch_evidence", evidenceQueue.Len())
}

func processTransportMission(ctx context.Context, queue workqueue.RateLimitingInterface, repository *spacekube.Repository, store *spacekube.WorkloadStore, localDomain spacev1.DomainReference, observer spaceplanner.PrometheusObserver) {
	item, shutdown := queue.Get()
	if shutdown {
		return
	}
	defer queue.Done(item)
	defer observer.QueueDepth("transport_missions", queue.Len())
	key, ok := item.(string)
	if !ok {
		queue.Forget(item)
		return
	}
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		queue.Forget(item)
		return
	}
	missionKey := spaceplanner.MissionKey{Namespace: namespace, Name: name}
	mission, err := repository.GetMission(ctx, missionKey)
	if errors.Is(err, spaceplanner.ErrNotFound) {
		queue.Forget(item)
		return
	}
	if err != nil {
		retryControllerItem(queue, item, "transport_missions", err, observer)
		return
	}
	placement, err := repository.GetPlacement(ctx, missionKey)
	if errors.Is(err, spaceplanner.ErrNotFound) {
		queue.Forget(item)
		return
	}
	if err != nil {
		retryControllerItem(queue, item, "transport_missions", err, observer)
		return
	}
	intents, err := spaceworkload.BuildInputTransferIntents(mission, placement, localDomain)
	if err != nil {
		retryControllerItem(queue, item, "transport_missions", err, observer)
		return
	}
	for _, intent := range intents {
		if err := store.EnsureTransferIntent(ctx, intent); err != nil {
			retryControllerItem(queue, item, "transport_missions", err, observer)
			return
		}
	}
	queue.Forget(item)
}

func processResourceStatus(ctx context.Context, resourceQueue, missionQueue workqueue.RateLimitingInterface, missions cache.Store, controller *resourceStatusController, observer spaceplanner.PrometheusObserver) {
	item, shutdown := resourceQueue.Get()
	if shutdown {
		return
	}
	defer resourceQueue.Done(item)
	if err := controller.Reconcile(ctx); err != nil {
		retryControllerItem(resourceQueue, item, "resource_status", err, observer)
		return
	}
	resourceQueue.Forget(item)
	for _, object := range missions.List() {
		if key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(object); err == nil {
			missionQueue.Add(key)
		}
	}
	observer.QueueDepth("planner_missions", missionQueue.Len())
}

func processProjector(ctx context.Context, queue workqueue.RateLimitingInterface, projector *nodeProjector, observer spaceplanner.PrometheusObserver) {
	item, shutdown := queue.Get()
	if shutdown {
		return
	}
	defer queue.Done(item)
	if err := projector.Reconcile(ctx); err != nil {
		retryControllerItem(queue, item, "node_projection", err, observer)
		return
	}
	queue.Forget(item)
}

func retryControllerItem(queue workqueue.RateLimitingInterface, item interface{}, queueName string, err error, observer spaceplanner.PrometheusObserver) {
	if queue.NumRequeues(item) < maxControllerRetries {
		queue.AddRateLimited(item)
		observer.QueueDepth(queueName, queue.Len())
		return
	}
	queue.Forget(item)
	observer.RetryExhausted(queueName)
	klog.ErrorS(err, "controller retry budget exhausted", "queue", queueName, "retries", maxControllerRetries)
}

type resourceStatusController struct {
	dynamic  dynamic.Interface
	recorder record.EventRecorder
	clock    spacev1.Clock
	observer spaceplanner.PrometheusObserver
}

func (c *resourceStatusController) Reconcile(ctx context.Context) error {
	linkList, err := c.dynamic.Resource(spacekube.LinkGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	for i := range linkList.Items {
		value := &spacev1.SpaceLinkSnapshot{}
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(linkList.Items[i].Object, value); err != nil {
			return err
		}
		status, validationErr := spaceplanner.ReconcileLinkStatus(value, nil, c.clock)
		if !reflect.DeepEqual(status, value.Status) {
			linkList.Items[i].Object["status"], _ = runtime.DefaultUnstructuredConverter.ToUnstructured(&status)
			if _, err := c.dynamic.Resource(spacekube.LinkGVR).UpdateStatus(ctx, &linkList.Items[i], metav1.UpdateOptions{}); err != nil {
				c.observer.APIWrite("link", "status", writeResult(err))
				return err
			}
			c.observer.APIWrite("link", "status", "success")
		}
		if validationErr != nil {
			c.recorder.Event(value, corev1.EventTypeWarning, "LinkSnapshotRejected", validationErr.Error())
		}
	}
	resourceList, err := c.dynamic.Resource(spacekube.ResourceSummaryGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	for i := range resourceList.Items {
		summary := &spacev1.SpaceDomainResourceSummary{}
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(resourceList.Items[i].Object, summary); err != nil {
			return err
		}
		validationErr := spacev1.ValidateResourceSummary(summary, c.clock)
		status := summary.Status
		condition := metav1.Condition{Type: "Validated", ObservedGeneration: summary.Generation, LastTransitionTime: metav1.NewTime(c.clock.Now())}
		if validationErr != nil {
			condition.Status = metav1.ConditionFalse
			condition.Reason = "RejectedSummary"
			condition.Message = validationErr.Error()
		} else {
			condition.Status = metav1.ConditionTrue
			condition.Reason = "ValidatedSummary"
			condition.Message = "resource, exporter freshness and provenance fields are valid"
			status.ObservedGeneration = summary.Generation
		}
		apiMeta.SetStatusCondition(&status.Conditions, condition)
		if !reflect.DeepEqual(status, summary.Status) {
			resourceList.Items[i].Object["status"], _ = runtime.DefaultUnstructuredConverter.ToUnstructured(&status)
			if _, err := c.dynamic.Resource(spacekube.ResourceSummaryGVR).UpdateStatus(ctx, &resourceList.Items[i], metav1.UpdateOptions{}); err != nil {
				c.observer.APIWrite("resource_summary", "status", writeResult(err))
				return err
			}
			c.observer.APIWrite("resource_summary", "status", "success")
		}
		if validationErr != nil {
			c.recorder.Event(summary, corev1.EventTypeWarning, "ResourceSummaryRejected", validationErr.Error())
		}
	}
	return nil
}

type nodeProjector struct {
	dynamic  dynamic.Interface
	client   kubernetes.Interface
	clock    spacev1.Clock
	observer spaceplanner.PrometheusObserver
}

func (c *nodeProjector) Reconcile(ctx context.Context) error {
	linkList, err := c.dynamic.Resource(spacekube.LinkGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	links := make([]*spacev1.SpaceLinkSnapshot, 0, len(linkList.Items))
	for i := range linkList.Items {
		value := &spacev1.SpaceLinkSnapshot{}
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(linkList.Items[i].Object, value); err != nil {
			return err
		}
		if _, validationErr := spaceplanner.ReconcileLinkStatus(value, nil, c.clock); validationErr == nil {
			links = append(links, value)
		}
	}
	resourceList, err := c.dynamic.Resource(spacekube.ResourceSummaryGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	for i := range resourceList.Items {
		summary := &spacev1.SpaceDomainResourceSummary{}
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(resourceList.Items[i].Object, summary); err != nil {
			return err
		}
		condition := apiMeta.FindStatusCondition(summary.Status.Conditions, "Validated")
		if condition == nil || condition.Status != metav1.ConditionTrue || condition.ObservedGeneration != summary.Generation || summary.Status.ObservedGeneration != summary.Generation {
			continue
		}
		if err := c.projectDomainNodes(ctx, summary, links); err != nil {
			return err
		}
	}
	return nil
}

func (c *nodeProjector) projectDomainNodes(ctx context.Context, summary *spacev1.SpaceDomainResourceSummary, links []*spacev1.SpaceLinkSnapshot) error {
	nodes, err := c.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: spacev1.LabelDomain + "=" + summary.Spec.Domain.Name})
	if err != nil {
		return err
	}
	for i := range nodes.Items {
		desired, err := spacepolicy.ProjectNode(&nodes.Items[i], summary, links, c.clock)
		if err != nil {
			return err
		}
		labels := reservedMetadata(desired.Labels)
		annotations := reservedMetadata(desired.Annotations)
		if reflect.DeepEqual(reservedMetadata(nodes.Items[i].Labels), labels) && reflect.DeepEqual(reservedMetadata(nodes.Items[i].Annotations), annotations) {
			continue
		}
		patch := map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "Node",
			"metadata": map[string]interface{}{
				"name":        desired.Name,
				"labels":      stringMapAny(labels),
				"annotations": stringMapAny(annotations),
			},
		}
		raw, err := json.Marshal(patch)
		if err != nil {
			return err
		}
		_, err = c.client.CoreV1().Nodes().Patch(ctx, desired.Name, types.ApplyPatchType, raw, metav1.PatchOptions{FieldManager: nodeProjectorFieldManager, FieldValidation: "Strict"})
		c.observer.APIWrite("node", "apply", writeResult(err))
		if err != nil {
			return err
		}
	}
	return nil
}

func reservedMetadata(values map[string]string) map[string]string {
	out := map[string]string{}
	for key, value := range values {
		if strings.HasPrefix(key, spacev1.GroupName+"/") {
			out[key] = value
		}
	}
	return out
}

func stringMapAny(values map[string]string) map[string]interface{} {
	out := make(map[string]interface{}, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
}

func writeResult(err error) string {
	if apierrors.IsConflict(err) {
		return "conflict"
	}
	if err != nil {
		return "error"
	}
	return "success"
}

func enqueuePlacementMission(value interface{}, queue workqueue.RateLimitingInterface) {
	object, ok := value.(*unstructured.Unstructured)
	if !ok {
		tombstone, ok := value.(cache.DeletedFinalStateUnknown)
		if !ok {
			return
		}
		object, _ = tombstone.Obj.(*unstructured.Unstructured)
	}
	if object == nil {
		return
	}
	namespace, _, _ := unstructured.NestedString(object.Object, "spec", "missionRef", "namespace")
	name, _, _ := unstructured.NestedString(object.Object, "spec", "missionRef", "name")
	if namespace != "" && name != "" {
		queue.Add(namespace + "/" + name)
	}
}

func enqueuePodMission(value interface{}, queue workqueue.RateLimitingInterface) {
	pod, ok := value.(*corev1.Pod)
	if !ok {
		tombstone, ok := value.(cache.DeletedFinalStateUnknown)
		if !ok {
			return
		}
		pod, _ = tombstone.Obj.(*corev1.Pod)
	}
	if pod == nil {
		return
	}
	raw := pod.Annotations[spacev1.AnnotationPlacement]
	if raw == "" {
		return
	}
	projection := &spacepolicy.PodPlacement{}
	if json.Unmarshal([]byte(raw), projection) != nil {
		return
	}
	if projection.Spec.MissionRef.Namespace != "" && projection.Spec.MissionRef.Name != "" {
		queue.Add(projection.Spec.MissionRef.Namespace + "/" + projection.Spec.MissionRef.Name)
	}
}

func kubeConfig(master, kubeconfig string) (*rest.Config, error) {
	if master != "" || kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags(master, kubeconfig)
	}
	return rest.InClusterConfig()
}

func eventRecorder(client kubernetes.Interface) record.EventRecorder {
	broadcaster := record.NewBroadcaster()
	broadcaster.StartStructuredLogging(0)
	broadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: client.CoreV1().Events("")})
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = spacev1.AddToScheme(scheme)
	return broadcaster.NewRecorder(scheme, corev1.EventSource{Component: componentName})
}

func healthServer(address string, ready *atomic.Bool) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", legacyregistry.Handler())
	mux.HandleFunc("/livez", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !ready.Load() {
			http.Error(w, "not leader or caches not synchronized", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok\n"))
	})
	return &http.Server{Addr: address, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
}
