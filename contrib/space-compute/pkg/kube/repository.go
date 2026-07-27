// Package kube contains the Kubernetes persistence adapters for the planner and
// workload controllers. CRD API access occurs only in these controllers, never
// in scheduler framework callbacks.
package kube

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
	"github.com/k3s-io/k3s/contrib/space-compute/pkg/planner"
)

var (
	MissionGVR         = schema.GroupVersionResource{Group: spacev1.GroupName, Version: "v1alpha1", Resource: "spacemissions"}
	PlacementGVR       = schema.GroupVersionResource{Group: spacev1.GroupName, Version: "v1alpha1", Resource: "spaceplacementintents"}
	LinkGVR            = schema.GroupVersionResource{Group: spacev1.GroupName, Version: "v1alpha1", Resource: "spacelinksnapshots"}
	ResourceSummaryGVR = schema.GroupVersionResource{Group: spacev1.GroupName, Version: "v1alpha1", Resource: "spacedomainresourcesummaries"}
)

const PlacementMissionUIDIndex = "spacecompute.missionUID"

type Repository struct {
	Dynamic               dynamic.Interface
	Recorder              record.EventRecorder
	Observer              WriteObserver
	MissionStore          cache.Store
	PlacementIndexer      cache.Indexer
	ResourceSummaryStore  cache.Store
	LinkSnapshotStore     cache.Store
	CacheResourceVersions func() map[string]string
}

type WriteObserver interface {
	APIWrite(resource, operation, result string)
}

func (r *Repository) GetMission(ctx context.Context, key planner.MissionKey) (*spacev1.SpaceMission, error) {
	if r.MissionStore != nil {
		object, exists, err := r.MissionStore.GetByKey(key.Namespace + "/" + key.Name)
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, planner.ErrNotFound
		}
		return missionFromCache(object)
	}
	object, err := r.Dynamic.Resource(MissionGVR).Namespace(key.Namespace).Get(ctx, key.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, planner.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	result := &spacev1.SpaceMission{}
	if err := fromUnstructured(object, result); err != nil {
		return nil, err
	}
	return result, nil
}

func (r *Repository) PlanningSnapshot(ctx context.Context) (*planner.PlanningInputSnapshot, error) {
	if r.ResourceSummaryStore != nil && r.LinkSnapshotStore != nil {
		before := r.cacheVersions()
		summaries, err := resourceSummariesFromCache(r.ResourceSummaryStore.List())
		if err != nil {
			return nil, err
		}
		links, err := linkSnapshotsFromCache(r.LinkSnapshotStore.List())
		if err != nil {
			return nil, err
		}
		after := r.cacheVersions()
		if !sameStringMap(before, after) {
			return nil, fmt.Errorf("planning informer inputs changed while snapshot was being pinned")
		}
		return buildPlanningSnapshot(summaries, links, after)
	}
	summaries, err := r.listResourceSummariesAPI(ctx)
	if err != nil {
		return nil, err
	}
	links, err := r.listLinkSnapshotsAPI(ctx)
	if err != nil {
		return nil, err
	}
	return buildPlanningSnapshot(summaries, links, nil)
}

// ListResourceSummaries and ListLinkSnapshots remain compatibility helpers for
// tests and non-controller callers. Production planner reconciliation uses the
// single immutable PlanningSnapshot read above.
func (r *Repository) ListResourceSummaries(ctx context.Context) ([]*spacev1.SpaceDomainResourceSummary, error) {
	if r.ResourceSummaryStore != nil {
		return resourceSummariesFromCache(r.ResourceSummaryStore.List())
	}
	return r.listResourceSummariesAPI(ctx)
}
func (r *Repository) listResourceSummariesAPI(ctx context.Context) ([]*spacev1.SpaceDomainResourceSummary, error) {
	list, err := r.Dynamic.Resource(ResourceSummaryGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	result := make([]*spacev1.SpaceDomainResourceSummary, 0, len(list.Items))
	for i := range list.Items {
		value := &spacev1.SpaceDomainResourceSummary{}
		if err := fromUnstructured(&list.Items[i], value); err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, nil
}
func (r *Repository) ListLinkSnapshots(ctx context.Context) ([]*spacev1.SpaceLinkSnapshot, error) {
	if r.LinkSnapshotStore != nil {
		return linkSnapshotsFromCache(r.LinkSnapshotStore.List())
	}
	return r.listLinkSnapshotsAPI(ctx)
}
func (r *Repository) listLinkSnapshotsAPI(ctx context.Context) ([]*spacev1.SpaceLinkSnapshot, error) {
	list, err := r.Dynamic.Resource(LinkGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	result := make([]*spacev1.SpaceLinkSnapshot, 0, len(list.Items))
	for i := range list.Items {
		value := &spacev1.SpaceLinkSnapshot{}
		if err := fromUnstructured(&list.Items[i], value); err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, nil
}

func (r *Repository) GetPlacementFresh(ctx context.Context, key planner.MissionKey) (*spacev1.SpacePlacementIntent, error) {
	mission, err := r.GetMission(ctx, key)
	if err != nil {
		return nil, err
	}
	list, err := r.Dynamic.Resource(PlacementGVR).Namespace(key.Namespace).List(ctx, metav1.ListOptions{LabelSelector: spacev1.LabelMissionUID + "=" + string(mission.UID)})
	if err != nil {
		return nil, err
	}
	if len(list.Items) == 0 {
		return nil, planner.ErrNotFound
	}
	if len(list.Items) > 1 {
		return nil, fmt.Errorf("mission %s/%s has %d placement intents; expected one", key.Namespace, key.Name, len(list.Items))
	}
	result := &spacev1.SpacePlacementIntent{}
	if err := fromUnstructured(&list.Items[0], result); err != nil {
		return nil, err
	}
	return result, nil
}

func (r *Repository) GetPlacement(ctx context.Context, key planner.MissionKey) (*spacev1.SpacePlacementIntent, error) {
	mission, err := r.GetMission(ctx, key)
	if err != nil {
		return nil, err
	}
	if r.PlacementIndexer != nil {
		objects, err := r.PlacementIndexer.ByIndex(PlacementMissionUIDIndex, string(mission.UID))
		if err != nil {
			return nil, err
		}
		if len(objects) == 0 {
			return nil, planner.ErrNotFound
		}
		if len(objects) > 1 {
			return nil, fmt.Errorf("mission %s/%s has %d placement intents; expected one", key.Namespace, key.Name, len(objects))
		}
		return placementFromCache(objects[0])
	}
	list, err := r.Dynamic.Resource(PlacementGVR).Namespace(key.Namespace).List(ctx, metav1.ListOptions{LabelSelector: spacev1.LabelMissionUID + "=" + string(mission.UID)})
	if err != nil {
		return nil, err
	}
	if len(list.Items) == 0 {
		return nil, planner.ErrNotFound
	}
	if len(list.Items) > 1 {
		return nil, fmt.Errorf("mission %s/%s has %d placement intents; expected one", key.Namespace, key.Name, len(list.Items))
	}
	result := &spacev1.SpacePlacementIntent{}
	if err := fromUnstructured(&list.Items[0], result); err != nil {
		return nil, err
	}
	return result, nil
}

func (r *Repository) cacheVersions() map[string]string {
	if r.CacheResourceVersions == nil {
		return nil
	}
	versions := r.CacheResourceVersions()
	out := make(map[string]string, len(versions))
	for key, value := range versions {
		out[key] = value
	}
	return out
}

func sameStringMap(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		if b[key] != value {
			return false
		}
	}
	return true
}

func missionFromCache(object interface{}) (*spacev1.SpaceMission, error) {
	switch value := object.(type) {
	case *spacev1.SpaceMission:
		return value.DeepCopy(), nil
	case *unstructured.Unstructured:
		out := &spacev1.SpaceMission{}
		if err := fromUnstructured(value, out); err != nil {
			return nil, err
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unexpected mission cache object %T", object)
	}
}
func placementFromCache(object interface{}) (*spacev1.SpacePlacementIntent, error) {
	switch value := object.(type) {
	case *spacev1.SpacePlacementIntent:
		return value.DeepCopy(), nil
	case *unstructured.Unstructured:
		out := &spacev1.SpacePlacementIntent{}
		if err := fromUnstructured(value, out); err != nil {
			return nil, err
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unexpected placement cache object %T", object)
	}
}
func resourceSummariesFromCache(objects []interface{}) ([]*spacev1.SpaceDomainResourceSummary, error) {
	out := make([]*spacev1.SpaceDomainResourceSummary, 0, len(objects))
	for _, object := range objects {
		var value *spacev1.SpaceDomainResourceSummary
		switch current := object.(type) {
		case *spacev1.SpaceDomainResourceSummary:
			value = current.DeepCopy()
		case *unstructured.Unstructured:
			value = &spacev1.SpaceDomainResourceSummary{}
			if err := fromUnstructured(current, value); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("unexpected resource summary cache object %T", object)
		}
		out = append(out, value)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}
func linkSnapshotsFromCache(objects []interface{}) ([]*spacev1.SpaceLinkSnapshot, error) {
	out := make([]*spacev1.SpaceLinkSnapshot, 0, len(objects))
	for _, object := range objects {
		var value *spacev1.SpaceLinkSnapshot
		switch current := object.(type) {
		case *spacev1.SpaceLinkSnapshot:
			value = current.DeepCopy()
		case *unstructured.Unstructured:
			value = &spacev1.SpaceLinkSnapshot{}
			if err := fromUnstructured(current, value); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("unexpected link snapshot cache object %T", object)
		}
		out = append(out, value)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func buildPlanningSnapshot(summaries []*spacev1.SpaceDomainResourceSummary, links []*spacev1.SpaceLinkSnapshot, versions map[string]string) (*planner.PlanningInputSnapshot, error) {
	payload := struct {
		Resources []*spacev1.SpaceDomainResourceSummary `json:"resources"`
		Links     []*spacev1.SpaceLinkSnapshot          `json:"links"`
	}{Resources: summaries, Links: links}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("encode planning input snapshot: %w", err)
	}
	digest := sha256.Sum256(raw)
	return &planner.PlanningInputSnapshot{ResourceSummaries: summaries, LinkSnapshots: links, CacheVersions: versions, InputDigest: hex.EncodeToString(digest[:])}, nil
}

func (r *Repository) ApplyPlacement(ctx context.Context, desired *spacev1.SpacePlacementIntent, expectedPlanID string) (bool, error) {
	if desired == nil {
		return false, fmt.Errorf("placement is required")
	}
	changed := false
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		current, err := r.Dynamic.Resource(PlacementGVR).Namespace(desired.Namespace).Get(ctx, desired.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			object, convertErr := toUnstructured(desired)
			if convertErr != nil {
				return convertErr
			}
			_, createErr := r.Dynamic.Resource(PlacementGVR).Namespace(desired.Namespace).Create(ctx, object, metav1.CreateOptions{})
			r.observeWrite("placement", "create", createErr)
			if createErr == nil {
				changed = true
			}
			return createErr
		}
		if err != nil {
			return err
		}
		currentPlanID, _, _ := unstructured.NestedString(current.Object, "spec", "planID")
		if currentPlanID != expectedPlanID {
			return apierrors.NewConflict(PlacementGVR.GroupResource(), desired.Name, fmt.Errorf("expected plan %q, found %q", expectedPlanID, currentPlanID))
		}
		if currentPlanID == desired.Spec.PlanID {
			return nil
		}
		object, convertErr := toUnstructured(desired)
		if convertErr != nil {
			return convertErr
		}
		object.SetResourceVersion(current.GetResourceVersion())
		object.SetUID(current.GetUID())
		object.SetCreationTimestamp(current.GetCreationTimestamp())
		if status, ok := current.Object["status"]; ok {
			object.Object["status"] = status
		}
		_, updateErr := r.Dynamic.Resource(PlacementGVR).Namespace(desired.Namespace).Update(ctx, object, metav1.UpdateOptions{})
		r.observeWrite("placement", "update", updateErr)
		if updateErr == nil {
			changed = true
		}
		return updateErr
	})
	return changed, err
}

func (r *Repository) UpdatePlacementStatus(ctx context.Context, desired *spacev1.SpacePlacementIntent) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		currentObject, err := r.Dynamic.Resource(PlacementGVR).Namespace(desired.Namespace).Get(ctx, desired.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		current := &spacev1.SpacePlacementIntent{}
		if err := fromUnstructured(currentObject, current); err != nil {
			return err
		}
		merged := mergePlacementStatus(current.Status, desired.Status)
		status, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&merged)
		if err != nil {
			return err
		}
		currentObject.Object["status"] = status
		_, err = r.Dynamic.Resource(PlacementGVR).Namespace(desired.Namespace).UpdateStatus(ctx, currentObject, metav1.UpdateOptions{})
		r.observeWrite("placement", "status", err)
		return err
	})
}

func (r *Repository) UpdateMissionStatus(ctx context.Context, desired *spacev1.SpaceMission) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		currentObject, err := r.Dynamic.Resource(MissionGVR).Namespace(desired.Namespace).Get(ctx, desired.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		current := &spacev1.SpaceMission{}
		if err := fromUnstructured(currentObject, current); err != nil {
			return err
		}
		merged := mergeMissionStatus(current.Status, desired.Status)
		status, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&merged)
		if err != nil {
			return err
		}
		currentObject.Object["status"] = status
		_, err = r.Dynamic.Resource(MissionGVR).Namespace(desired.Namespace).UpdateStatus(ctx, currentObject, metav1.UpdateOptions{})
		r.observeWrite("mission", "status", err)
		return err
	})
}

func mergeMissionStatus(current, desired spacev1.SpaceMissionStatus) spacev1.SpaceMissionStatus {
	out := current
	newerGeneration := desired.ObservedGeneration > current.ObservedGeneration
	sameGeneration := desired.ObservedGeneration == current.ObservedGeneration
	if newerGeneration || (sameGeneration && missionPhaseCanAdvance(current.Phase, desired.Phase)) {
		out.ObservedGeneration = desired.ObservedGeneration
		if desired.Phase != "" {
			out.Phase = desired.Phase
		}
		if desired.PlacementName != "" {
			out.PlacementName = desired.PlacementName
		}
		if desired.PlanID != "" {
			out.PlanID = desired.PlanID
		}
		if desired.LastDecisionDigest != "" {
			out.LastDecisionDigest = desired.LastDecisionDigest
		}
	}
	out.Conditions = mergeConditions(current.Conditions, desired.Conditions)
	return out
}

func missionPhaseCanAdvance(from, to spacev1.MissionPhase) bool {
	if to == "" {
		return false
	}
	if from == "" || from == to {
		return true
	}
	// Terminal outcomes are monotonic for a generation. A stale planner status
	// must never turn a succeeded/failed Mission back into planned/blocked.
	if from == spacev1.MissionSucceeded || from == spacev1.MissionFailed {
		return false
	}
	allowed := map[spacev1.MissionPhase]map[spacev1.MissionPhase]bool{
		spacev1.MissionAccepted:   {spacev1.MissionPlanning: true, spacev1.MissionPlanned: true, spacev1.MissionBlocked: true, spacev1.MissionFailed: true},
		spacev1.MissionPlanning:   {spacev1.MissionPlanned: true, spacev1.MissionBlocked: true, spacev1.MissionFailed: true},
		spacev1.MissionPlanned:    {spacev1.MissionExecuting: true, spacev1.MissionReplanning: true, spacev1.MissionBlocked: true, spacev1.MissionFailed: true},
		spacev1.MissionExecuting:  {spacev1.MissionReturning: true, spacev1.MissionSucceeded: true, spacev1.MissionReplanning: true, spacev1.MissionFailed: true},
		spacev1.MissionReturning:  {spacev1.MissionSucceeded: true, spacev1.MissionReplanning: true, spacev1.MissionFailed: true},
		spacev1.MissionBlocked:    {spacev1.MissionPlanning: true, spacev1.MissionPlanned: true, spacev1.MissionFailed: true},
		spacev1.MissionReplanning: {spacev1.MissionPlanning: true, spacev1.MissionPlanned: true, spacev1.MissionExecuting: true, spacev1.MissionBlocked: true, spacev1.MissionFailed: true},
	}
	return allowed[from][to]
}
func mergePlacementStatus(current, desired spacev1.SpacePlacementIntentStatus) spacev1.SpacePlacementIntentStatus {
	out := current
	if desired.ObservedGeneration > out.ObservedGeneration {
		out.ObservedGeneration = desired.ObservedGeneration
	}
	if desired.LastObservationSequence > current.LastObservationSequence || (desired.LastObservationSequence == current.LastObservationSequence && placementPhaseCanAdvance(current.Phase, desired.Phase)) {
		if desired.Phase != "" {
			out.Phase = desired.Phase
		}
		if desired.ActivePod != nil {
			copy := *desired.ActivePod
			out.ActivePod = &copy
		}
		if desired.LastObservation != nil {
			copy := *desired.LastObservation
			out.LastObservation = &copy
		}
		out.LastObservationSequence = desired.LastObservationSequence
	}
	if desired.RetryCount > out.RetryCount {
		out.RetryCount = desired.RetryCount
	}
	out.ResultReturned = out.ResultReturned || desired.ResultReturned
	out.Conditions = mergeConditions(current.Conditions, desired.Conditions)
	return out
}
func placementPhaseCanAdvance(from, to spacev1.PlacementPhase) bool {
	if to == "" || from == to {
		return to != ""
	}
	if from == "" {
		from = spacev1.PlacementPending
	}
	allowed := map[spacev1.PlacementPhase]map[spacev1.PlacementPhase]bool{
		spacev1.PlacementPending:               {spacev1.PlacementTransferPending: true, spacev1.PlacementExecutionLeasePending: true, spacev1.PlacementReady: true, spacev1.PlacementDispatched: true, spacev1.PlacementRunning: true, spacev1.PlacementExpired: true, spacev1.PlacementFailed: true},
		spacev1.PlacementTransferPending:       {spacev1.PlacementExecutionLeasePending: true, spacev1.PlacementReady: true, spacev1.PlacementDispatched: true, spacev1.PlacementRunning: true, spacev1.PlacementExpired: true, spacev1.PlacementFailed: true},
		spacev1.PlacementExecutionLeasePending: {spacev1.PlacementReady: true, spacev1.PlacementDispatched: true, spacev1.PlacementRunning: true, spacev1.PlacementExpired: true, spacev1.PlacementFailed: true},
		spacev1.PlacementReady:                 {spacev1.PlacementDispatched: true, spacev1.PlacementRunning: true, spacev1.PlacementExpired: true, spacev1.PlacementFailed: true},
		spacev1.PlacementDispatched:            {spacev1.PlacementRunning: true, spacev1.PlacementReplanning: true, spacev1.PlacementReturnPending: true, spacev1.PlacementCompleted: true, spacev1.PlacementExpired: true, spacev1.PlacementFailed: true},
		spacev1.PlacementRunning:               {spacev1.PlacementCheckpointed: true, spacev1.PlacementReplanning: true, spacev1.PlacementReturnPending: true, spacev1.PlacementCompleted: true, spacev1.PlacementFailed: true},
		spacev1.PlacementCheckpointed:          {spacev1.PlacementReplanning: true, spacev1.PlacementFailed: true},
		spacev1.PlacementReplanning:            {spacev1.PlacementCheckpointed: true, spacev1.PlacementFailed: true},
		spacev1.PlacementReturnPending:         {spacev1.PlacementCompleted: true, spacev1.PlacementFailed: true},
	}
	return allowed[from][to]
}

func mergeConditions(current, desired []metav1.Condition) []metav1.Condition {
	out := append([]metav1.Condition(nil), current...)
	for _, condition := range desired {
		apiMeta.SetStatusCondition(&out, condition)
	}
	return out
}

func (r *Repository) Event(ctx context.Context, key planner.MissionKey, eventType, reason, message string) {
	if r.Recorder == nil {
		return
	}
	mission, err := r.GetMission(ctx, key)
	if err == nil {
		r.Recorder.Event(mission, eventType, reason, message)
	}
}

type WorkloadStore struct {
	Client                    kubernetes.Interface
	Repository                *Repository
	Recorder                  record.EventRecorder
	PodStore                  cache.Store
	TransferReceiptStore      cache.Store
	ExecutionLeaseStore       cache.Store
	ExecutionObservationStore cache.Store
	ResultReceiptStore        cache.Store
}

func (s *WorkloadStore) GetPod(ctx context.Context, namespace, name string) (*corev1.Pod, error) {
	if s.PodStore != nil {
		object, exists, err := s.PodStore.GetByKey(namespace + "/" + name)
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, planner.ErrNotFound
		}
		pod, ok := object.(*corev1.Pod)
		if !ok {
			return nil, fmt.Errorf("unexpected Pod cache object %T", object)
		}
		return pod.DeepCopy(), nil
	}
	pod, err := s.Client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, planner.ErrNotFound
	}
	return pod, err
}
func (s *WorkloadStore) CreatePod(ctx context.Context, pod *corev1.Pod) (*corev1.Pod, error) {
	created, err := s.Client.CoreV1().Pods(pod.Namespace).Create(ctx, pod, metav1.CreateOptions{})
	if s.Repository != nil {
		s.Repository.observeWrite("pod", "create", err)
	}
	return created, err
}
func (s *WorkloadStore) DeletePod(ctx context.Context, namespace, name string) error {
	grace := int64(30)
	err := s.Client.CoreV1().Pods(namespace).Delete(ctx, name, metav1.DeleteOptions{GracePeriodSeconds: &grace})
	if apierrors.IsNotFound(err) {
		return planner.ErrNotFound
	}
	if s.Repository != nil {
		s.Repository.observeWrite("pod", "delete", err)
	}
	return err
}
func (s *WorkloadStore) UpdatePlacementStatus(ctx context.Context, value *spacev1.SpacePlacementIntent) error {
	return s.Repository.UpdatePlacementStatus(ctx, value)
}
func (s *WorkloadStore) Event(ctx context.Context, namespace, name, eventType, reason, message string) {
	if s.Recorder == nil {
		return
	}
	s.Recorder.Event(&spacev1.SpaceMission{TypeMeta: metav1.TypeMeta{Kind: "SpaceMission", APIVersion: spacev1.SchemeGroupVersion.String()}, ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}, eventType, reason, message)
}

func fromUnstructured(in *unstructured.Unstructured, out interface{}) error {
	return runtime.DefaultUnstructuredConverter.FromUnstructured(in.Object, out)
}
func toUnstructured(in interface{}) (*unstructured.Unstructured, error) {
	value, err := runtime.DefaultUnstructuredConverter.ToUnstructured(in)
	if err != nil {
		return nil, err
	}
	return &unstructured.Unstructured{Object: value}, nil
}

func (r *Repository) observeWrite(resource, operation string, err error) {
	if r == nil || r.Observer == nil {
		return
	}
	result := "success"
	if apierrors.IsConflict(err) {
		result = "conflict"
	} else if err != nil {
		result = "error"
	}
	r.Observer.APIWrite(resource, operation, result)
}
