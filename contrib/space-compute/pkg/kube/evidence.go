package kube

import (
	"context"
	"fmt"
	"reflect"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
	"github.com/k3s-io/k3s/contrib/space-compute/pkg/planner"
)

var (
	TransferIntentGVR       = schema.GroupVersionResource{Group: spacev1.GroupName, Version: "v1alpha1", Resource: "spacetransferintents"}
	TransferReceiptGVR      = schema.GroupVersionResource{Group: spacev1.GroupName, Version: "v1alpha1", Resource: "spacetransferreceipts"}
	ExecutionLeaseGVR       = schema.GroupVersionResource{Group: spacev1.GroupName, Version: "v1alpha1", Resource: "spaceexecutionleases"}
	ExecutionObservationGVR = schema.GroupVersionResource{Group: spacev1.GroupName, Version: "v1alpha1", Resource: "spaceexecutionobservations"}
	ResultReceiptGVR        = schema.GroupVersionResource{Group: spacev1.GroupName, Version: "v1alpha1", Resource: "spaceresultreceipts"}
)

func (s *WorkloadStore) EnsureTransferIntent(ctx context.Context, desired *spacev1.SpaceTransferIntent) error {
	if s == nil || s.Repository == nil || s.Repository.Dynamic == nil {
		return fmt.Errorf("dynamic evidence client is required")
	}
	resource := s.Repository.Dynamic.Resource(TransferIntentGVR)
	current, err := resource.Get(ctx, desired.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		u, err := toUnstructured(desired)
		if err != nil {
			return err
		}
		_, err = resource.Create(ctx, u, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	existing := &spacev1.SpaceTransferIntent{}
	if err := fromUnstructured(current, existing); err != nil {
		return err
	}
	if !reflect.DeepEqual(existing.Spec, desired.Spec) {
		return fmt.Errorf("transfer intent %s exists with different immutable spec", desired.Name)
	}
	return nil
}
func cachedUnstructuredList[T any](store interface{ List() []interface{} }, decode func(*unstructured.Unstructured) (*T, error)) ([]*T, error) {
	objects := store.List()
	out := make([]*T, 0, len(objects))
	for _, object := range objects {
		u, ok := object.(*unstructured.Unstructured)
		if !ok {
			return nil, fmt.Errorf("unexpected evidence cache object %T", object)
		}
		value, err := decode(u)
		if err != nil {
			return nil, err
		}
		out = append(out, value)
	}
	return out, nil
}

func (s *WorkloadStore) ListTransferReceipts(ctx context.Context) ([]*spacev1.SpaceTransferReceipt, error) {
	if s.TransferReceiptStore != nil {
		return cachedUnstructuredList(s.TransferReceiptStore, func(u *unstructured.Unstructured) (*spacev1.SpaceTransferReceipt, error) {
			v := &spacev1.SpaceTransferReceipt{}
			return v, fromUnstructured(u, v)
		})
	}
	list, err := s.Repository.Dynamic.Resource(TransferReceiptGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]*spacev1.SpaceTransferReceipt, 0, len(list.Items))
	for i := range list.Items {
		v := &spacev1.SpaceTransferReceipt{}
		if err := fromUnstructured(&list.Items[i], v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}
func (s *WorkloadStore) ListExecutionLeases(ctx context.Context) ([]*spacev1.SpaceExecutionLease, error) {
	if s.ExecutionLeaseStore != nil {
		return cachedUnstructuredList(s.ExecutionLeaseStore, func(u *unstructured.Unstructured) (*spacev1.SpaceExecutionLease, error) {
			v := &spacev1.SpaceExecutionLease{}
			return v, fromUnstructured(u, v)
		})
	}
	list, err := s.Repository.Dynamic.Resource(ExecutionLeaseGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]*spacev1.SpaceExecutionLease, 0, len(list.Items))
	for i := range list.Items {
		v := &spacev1.SpaceExecutionLease{}
		if err := fromUnstructured(&list.Items[i], v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}
func (s *WorkloadStore) GetExecutionLease(ctx context.Context, name string) (*spacev1.SpaceExecutionLease, error) {
	if s.ExecutionLeaseStore != nil {
		object, exists, err := s.ExecutionLeaseStore.GetByKey(name)
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, planner.ErrNotFound
		}
		u, ok := object.(*unstructured.Unstructured)
		if !ok {
			return nil, fmt.Errorf("unexpected execution lease cache object %T", object)
		}
		v := &spacev1.SpaceExecutionLease{}
		if err := fromUnstructured(u, v); err != nil {
			return nil, err
		}
		return v, nil
	}
	u, err := s.Repository.Dynamic.Resource(ExecutionLeaseGVR).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, planner.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	v := &spacev1.SpaceExecutionLease{}
	if err := fromUnstructured(u, v); err != nil {
		return nil, err
	}
	return v, nil
}
func (s *WorkloadStore) ListExecutionObservations(ctx context.Context) ([]*spacev1.SpaceExecutionObservation, error) {
	if s.ExecutionObservationStore != nil {
		return cachedUnstructuredList(s.ExecutionObservationStore, func(u *unstructured.Unstructured) (*spacev1.SpaceExecutionObservation, error) {
			v := &spacev1.SpaceExecutionObservation{}
			return v, fromUnstructured(u, v)
		})
	}
	list, err := s.Repository.Dynamic.Resource(ExecutionObservationGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]*spacev1.SpaceExecutionObservation, 0, len(list.Items))
	for i := range list.Items {
		v := &spacev1.SpaceExecutionObservation{}
		if err := fromUnstructured(&list.Items[i], v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}
func (s *WorkloadStore) ListResultReceipts(ctx context.Context) ([]*spacev1.SpaceResultReceipt, error) {
	if s.ResultReceiptStore != nil {
		return cachedUnstructuredList(s.ResultReceiptStore, func(u *unstructured.Unstructured) (*spacev1.SpaceResultReceipt, error) {
			v := &spacev1.SpaceResultReceipt{}
			return v, fromUnstructured(u, v)
		})
	}
	list, err := s.Repository.Dynamic.Resource(ResultReceiptGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]*spacev1.SpaceResultReceipt, 0, len(list.Items))
	for i := range list.Items {
		v := &spacev1.SpaceResultReceipt{}
		if err := fromUnstructured(&list.Items[i], v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}
