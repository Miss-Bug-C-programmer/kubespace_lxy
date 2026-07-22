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
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
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
	"k8s.io/client-go/util/retry"
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

type options struct {
	kubeconfig, master, metricsAddress, leaderNamespace, leaderName string
	workers                                                         int
	leaderElect                                                     bool
}

func main() {
	klog.InitFlags(nil)
	opt := options{}
	flag.StringVar(&opt.kubeconfig, "kubeconfig", "", "Path to kubeconfig; empty uses in-cluster configuration")
	flag.StringVar(&opt.master, "master", "", "Optional API server address")
	flag.StringVar(&opt.metricsAddress, "metrics-bind-address", ":10261", "Health and metrics listen address")
	flag.StringVar(&opt.leaderNamespace, "leader-election-namespace", "kube-system", "Leader Lease namespace")
	flag.StringVar(&opt.leaderName, "leader-election-name", componentName, "Leader Lease name")
	flag.IntVar(&opt.workers, "workers", 2, "Bounded mission reconciliation worker count")
	flag.BoolVar(&opt.leaderElect, "leader-elect", true, "Use a namespaced Lease for active/standby operation")
	flag.Parse()
	if opt.workers < 1 || opt.workers > 32 {
		klog.Fatalf("workers must be between 1 and 32")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := run(ctx, opt); err != nil {
		klog.Fatalf("%s failed: %v", componentName, err)
	}
}

func run(ctx context.Context, opt options) error {
	config, err := kubeConfig(opt.master, opt.kubeconfig)
	if err != nil {
		return err
	}
	config.UserAgent = componentName
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
		if err := runControllers(leaderCtx, dynamicClient, client, recorder, opt.workers, &ready); err != nil {
			klog.ErrorS(err, "controller set stopped")
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
	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{Lock: lock, LeaseDuration: 30 * time.Second, RenewDeadline: 20 * time.Second, RetryPeriod: 5 * time.Second, ReleaseOnCancel: true, Name: componentName, Callbacks: leaderelection.LeaderCallbacks{OnStartedLeading: start, OnStoppedLeading: func() { ready.Store(false) }}})
	return nil
}

func runControllers(ctx context.Context, dynamicClient dynamic.Interface, client kubernetes.Interface, recorder record.EventRecorder, workers int, ready *atomic.Bool) error {
	factory := dynamicinformer.NewDynamicSharedInformerFactory(dynamicClient, 10*time.Minute)
	missions := factory.ForResource(spacekube.MissionGVR).Informer()
	placements := factory.ForResource(spacekube.PlacementGVR).Informer()
	links := factory.ForResource(spacekube.LinkGVR).Informer()
	resources := factory.ForResource(spacekube.ResourceSummaryGVR).Informer()
	coreFactory := informers.NewSharedInformerFactory(client, 10*time.Minute)
	pods := coreFactory.Core().V1().Pods().Informer()
	queue := workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "space_compute_missions")
	resourceQueue := workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "space_compute_resources")
	defer queue.ShutDown()
	defer resourceQueue.ShutDown()
	enqueueMission := func(object interface{}) {
		if key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(object); err == nil {
			queue.Add(key)
		}
	}
	_, _ = missions.AddEventHandler(cache.ResourceEventHandlerFuncs{AddFunc: enqueueMission, UpdateFunc: func(_, value interface{}) { enqueueMission(value) }, DeleteFunc: enqueueMission})
	_, _ = placements.AddEventHandler(cache.ResourceEventHandlerFuncs{AddFunc: func(value interface{}) { enqueuePlacementMission(value, queue) }, UpdateFunc: func(_, value interface{}) { enqueuePlacementMission(value, queue) }, DeleteFunc: func(value interface{}) { enqueuePlacementMission(value, queue) }})
	// Informer callbacks remain O(1). The resource worker validates/projects the
	// coalesced update first and only then requeues affected mission planning.
	resourceHandler := cache.ResourceEventHandlerFuncs{AddFunc: func(interface{}) { resourceQueue.Add("resources") }, UpdateFunc: func(_, _ interface{}) { resourceQueue.Add("resources") }, DeleteFunc: func(interface{}) { resourceQueue.Add("resources") }}
	_, _ = links.AddEventHandler(resourceHandler)
	_, _ = resources.AddEventHandler(resourceHandler)
	_, _ = pods.AddEventHandler(cache.ResourceEventHandlerFuncs{AddFunc: func(value interface{}) { enqueuePodMission(value, queue) }, UpdateFunc: func(_, value interface{}) { enqueuePodMission(value, queue) }, DeleteFunc: func(value interface{}) { enqueuePodMission(value, queue) }})
	factory.Start(ctx.Done())
	coreFactory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), missions.HasSynced, placements.HasSynced, links.HasSynced, resources.HasSynced, pods.HasSynced) {
		return fmt.Errorf("CRD informer cache synchronization failed")
	}
	observer := spaceplanner.NewPrometheusObserver()
	repository := &spacekube.Repository{Dynamic: dynamicClient, Recorder: recorder, Observer: observer}
	plannerController := &spaceplanner.Controller{Repository: repository, Clock: spacev1.RealClock{}, Observer: observer}
	workloadController := &spaceworkload.Controller{Store: &spacekube.WorkloadStore{Client: client, Repository: repository, Recorder: recorder}, Clock: spacev1.RealClock{}}
	resourceController := &resourceController{dynamic: dynamicClient, client: client, recorder: recorder, clock: spacev1.RealClock{}, observer: observer}
	go wait.UntilWithContext(ctx, func(ctx context.Context) {
		processResources(ctx, resourceQueue, queue, missions.GetStore(), resourceController, observer)
	}, time.Second)
	resourceQueue.Add("resources")
	for i := 0; i < workers; i++ {
		go wait.UntilWithContext(ctx, func(ctx context.Context) {
			processMission(ctx, queue, repository, plannerController, workloadController, observer)
		}, time.Second)
	}
	ready.Store(true)
	<-ctx.Done()
	ready.Store(false)
	return nil
}

func processMission(ctx context.Context, queue workqueue.RateLimitingInterface, repository *spacekube.Repository, plannerController *spaceplanner.Controller, workloadController *spaceworkload.Controller, observer spaceplanner.PrometheusObserver) {
	item, shutdown := queue.Get()
	if shutdown {
		return
	}
	defer queue.Done(item)
	defer observer.QueueDepth("missions", queue.Len())
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
		retryControllerItem(queue, item, "missions", err, observer)
		return
	}
	mission, missionErr := repository.GetMission(ctx, spaceplanner.MissionKey{Namespace: namespace, Name: name})
	placement, placementErr := repository.GetPlacement(ctx, spaceplanner.MissionKey{Namespace: namespace, Name: name})
	if missionErr == nil && placementErr == nil {
		if delay, dispatchErr := workloadController.ReconcileDispatch(ctx, mission, placement, mission.Spec.WorkloadTemplate); dispatchErr != nil {
			retryControllerItem(queue, item, "missions", dispatchErr, observer)
			return
		} else if delay > 0 && (result.RequeueAfter == 0 || delay < result.RequeueAfter) {
			result.RequeueAfter = delay
		}
		placement, _ = repository.GetPlacement(ctx, spaceplanner.MissionKey{Namespace: namespace, Name: name})
		if placement != nil && placement.Status.ActivePod != nil && placement.Status.ActivePod.Name != "" {
			if pod, podErr := workloadController.Store.(*spacekube.WorkloadStore).Client.CoreV1().Pods(placement.Status.ActivePod.Namespace).Get(ctx, placement.Status.ActivePod.Name, metav1.GetOptions{}); podErr == nil {
				if _, observeErr := workloadController.ReconcilePodStatus(ctx, mission, placement, pod); observeErr != nil {
					retryControllerItem(queue, item, "missions", observeErr, observer)
					return
				}
			}
		}
	}
	queue.Forget(item)
	if result.RequeueAfter > 0 {
		queue.AddAfter(item, result.RequeueAfter)
	}
}

func processResources(ctx context.Context, resourceQueue, missionQueue workqueue.RateLimitingInterface, missions cache.Store, controller *resourceController, observer spaceplanner.PrometheusObserver) {
	item, shutdown := resourceQueue.Get()
	if shutdown {
		return
	}
	defer resourceQueue.Done(item)
	defer observer.QueueDepth("resources", resourceQueue.Len())
	if err := controller.Reconcile(ctx); err != nil {
		retryControllerItem(resourceQueue, item, "resources", err, observer)
		return
	}
	resourceQueue.Forget(item)
	for _, object := range missions.List() {
		if key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(object); err == nil {
			missionQueue.Add(key)
		}
	}
	observer.QueueDepth("missions", missionQueue.Len())
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

type resourceController struct {
	dynamic  dynamic.Interface
	client   kubernetes.Interface
	recorder record.EventRecorder
	clock    spacev1.Clock
	observer spaceplanner.PrometheusObserver
}

func (c *resourceController) Reconcile(ctx context.Context) error {
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
		status, validationErr := spaceplanner.ReconcileLinkStatus(value, nil, c.clock)
		if !reflect.DeepEqual(status, value.Status) {
			linkList.Items[i].Object["status"], _ = runtime.DefaultUnstructuredConverter.ToUnstructured(&status)
			if _, err := c.dynamic.Resource(spacekube.LinkGVR).UpdateStatus(ctx, &linkList.Items[i], metav1.UpdateOptions{}); err != nil {
				c.observer.APIWrite("link", "status", writeResult(err))
				return err
			} else {
				c.observer.APIWrite("link", "status", "success")
			}
		}
		if validationErr != nil {
			c.recorder.Event(value, corev1.EventTypeWarning, "LinkSnapshotRejected", validationErr.Error())
			continue
		}
		value.Status = status
		links = append(links, value)
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
			} else {
				c.observer.APIWrite("resource_summary", "status", "success")
			}
		}
		if validationErr == nil {
			if err := c.projectDomainNodes(ctx, summary, links); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *resourceController) projectDomainNodes(ctx context.Context, summary *spacev1.SpaceDomainResourceSummary, links []*spacev1.SpaceLinkSnapshot) error {
	nodes, err := c.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: spacev1.LabelDomain + "=" + summary.Spec.Domain.Name})
	if err != nil {
		return err
	}
	for i := range nodes.Items {
		desired, err := spacepolicy.ProjectNode(&nodes.Items[i], summary, links, c.clock)
		if err != nil {
			return err
		}
		if nodes.Items[i].Annotations[spacev1.AnnotationLinkProjection] == desired.Annotations[spacev1.AnnotationLinkProjection] {
			continue
		}
		name := desired.Name
		err = retry.RetryOnConflict(retry.DefaultBackoff, func() error {
			current, err := c.client.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return err
			}
			projected, err := spacepolicy.ProjectNode(current, summary, links, c.clock)
			if err != nil {
				return err
			}
			_, err = c.client.CoreV1().Nodes().Update(ctx, projected, metav1.UpdateOptions{})
			c.observer.APIWrite("node", "update", writeResult(err))
			return err
		})
		if err != nil {
			return err
		}
	}
	return nil
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
