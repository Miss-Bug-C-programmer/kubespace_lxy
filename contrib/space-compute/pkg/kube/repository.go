// Package kube contains the Kubernetes persistence adapters for the planner and
// workload controllers. CRD API access occurs only in these controllers, never
// in scheduler framework callbacks.
package kube

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
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

type Repository struct {
	Dynamic  dynamic.Interface
	Recorder record.EventRecorder
	Observer WriteObserver
}

type WriteObserver interface {
	APIWrite(resource, operation, result string)
}

func (r *Repository) GetMission(ctx context.Context, key planner.MissionKey) (*spacev1.SpaceMission, error) {
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

func (r *Repository) ListResourceSummaries(ctx context.Context) ([]*spacev1.SpaceDomainResourceSummary, error) {
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

func (r *Repository) GetPlacement(ctx context.Context, key planner.MissionKey) (*spacev1.SpacePlacementIntent, error) {
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
		current, err := r.Dynamic.Resource(PlacementGVR).Namespace(desired.Namespace).Get(ctx, desired.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		status, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&desired.Status)
		if err != nil {
			return err
		}
		current.Object["status"] = status
		_, err = r.Dynamic.Resource(PlacementGVR).Namespace(desired.Namespace).UpdateStatus(ctx, current, metav1.UpdateOptions{})
		r.observeWrite("placement", "status", err)
		return err
	})
}

func (r *Repository) UpdateMissionStatus(ctx context.Context, desired *spacev1.SpaceMission) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		current, err := r.Dynamic.Resource(MissionGVR).Namespace(desired.Namespace).Get(ctx, desired.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		status, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&desired.Status)
		if err != nil {
			return err
		}
		current.Object["status"] = status
		_, err = r.Dynamic.Resource(MissionGVR).Namespace(desired.Namespace).UpdateStatus(ctx, current, metav1.UpdateOptions{})
		r.observeWrite("mission", "status", err)
		return err
	})
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
	Client     kubernetes.Interface
	Repository *Repository
	Recorder   record.EventRecorder
}

func (s *WorkloadStore) GetPod(ctx context.Context, namespace, name string) (*corev1.Pod, error) {
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
