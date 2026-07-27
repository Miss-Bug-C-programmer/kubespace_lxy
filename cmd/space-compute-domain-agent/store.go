package main

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
	spacekube "github.com/k3s-io/k3s/contrib/space-compute/pkg/kube"
	spacetransport "github.com/k3s-io/k3s/contrib/space-compute/pkg/transport"
	spaceworkload "github.com/k3s-io/k3s/contrib/space-compute/pkg/workload"
)

type kubeAgentStore struct {
	dynamic dynamic.Interface
	client  kubernetes.Interface
	remote  *spacetransport.FileAssignmentStore
}

func (s *kubeAgentStore) ListAssignments(ctx context.Context) ([]spacetransport.Assignment, error) {
	list, err := s.dynamic.Resource(spacekube.PlacementGVR).Namespace(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]spacetransport.Assignment, 0, len(list.Items))
	for i := range list.Items {
		p := &spacev1.SpacePlacementIntent{}
		if err := fromU(&list.Items[i], p); err != nil {
			return nil, err
		}
		if p.Status.Phase == spacev1.PlacementCompleted || p.Status.Phase == spacev1.PlacementFailed || p.Status.Phase == spacev1.PlacementExpired {
			continue
		}
		ns := p.Spec.MissionRef.Namespace
		if ns == "" {
			ns = p.Namespace
		}
		u, err := s.dynamic.Resource(spacekube.MissionGVR).Namespace(ns).Get(ctx, p.Spec.MissionRef.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		m := &spacev1.SpaceMission{}
		if err := fromU(u, m); err != nil {
			return nil, err
		}
		if p.Spec.MissionRef.UID != "" && m.UID != p.Spec.MissionRef.UID {
			continue
		}
		out = append(out, spacetransport.Assignment{Mission: m, Placement: p})
	}
	return out, nil
}
func (s *kubeAgentStore) SaveRemoteAssignment(_ context.Context, a spacetransport.Assignment) error {
	return s.remote.Save(a)
}
func (s *kubeAgentStore) ListRemoteAssignments(context.Context) ([]spacetransport.Assignment, error) {
	return s.remote.List()
}
func (s *kubeAgentStore) ListTransferIntents(ctx context.Context) ([]*spacev1.SpaceTransferIntent, error) {
	return listTyped[spacev1.SpaceTransferIntent](ctx, s.dynamic, spacekube.TransferIntentGVR, func(v *spacev1.SpaceTransferIntent) *spacev1.SpaceTransferIntent { return v })
}
func (s *kubeAgentStore) GetTransferIntent(ctx context.Context, name string) (*spacev1.SpaceTransferIntent, error) {
	u, err := s.dynamic.Resource(spacekube.TransferIntentGVR).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	v := &spacev1.SpaceTransferIntent{}
	return v, fromU(u, v)
}
func (s *kubeAgentStore) UpsertTransferIntent(ctx context.Context, v *spacev1.SpaceTransferIntent) error {
	return upsertTyped(ctx, s.dynamic, spacekube.TransferIntentGVR, v)
}
func (s *kubeAgentStore) ListTransferReceipts(ctx context.Context) ([]*spacev1.SpaceTransferReceipt, error) {
	list, err := s.dynamic.Resource(spacekube.TransferReceiptGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]*spacev1.SpaceTransferReceipt, 0, len(list.Items))
	for i := range list.Items {
		v := &spacev1.SpaceTransferReceipt{}
		if err := fromU(&list.Items[i], v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}
func (s *kubeAgentStore) UpsertTransferReceipt(ctx context.Context, v *spacev1.SpaceTransferReceipt) error {
	return upsertTyped(ctx, s.dynamic, spacekube.TransferReceiptGVR, v)
}
func (s *kubeAgentStore) ListExecutionLeases(ctx context.Context) ([]*spacev1.SpaceExecutionLease, error) {
	list, err := s.dynamic.Resource(spacekube.ExecutionLeaseGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]*spacev1.SpaceExecutionLease, 0, len(list.Items))
	for i := range list.Items {
		v := &spacev1.SpaceExecutionLease{}
		if err := fromU(&list.Items[i], v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}
func (s *kubeAgentStore) UpsertExecutionLease(ctx context.Context, v *spacev1.SpaceExecutionLease) error {
	return upsertTyped(ctx, s.dynamic, spacekube.ExecutionLeaseGVR, v)
}
func (s *kubeAgentStore) ListExecutionObservations(ctx context.Context) ([]*spacev1.SpaceExecutionObservation, error) {
	list, err := s.dynamic.Resource(spacekube.ExecutionObservationGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]*spacev1.SpaceExecutionObservation, 0, len(list.Items))
	for i := range list.Items {
		v := &spacev1.SpaceExecutionObservation{}
		if err := fromU(&list.Items[i], v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}
func (s *kubeAgentStore) UpsertExecutionObservation(ctx context.Context, v *spacev1.SpaceExecutionObservation) error {
	return upsertTyped(ctx, s.dynamic, spacekube.ExecutionObservationGVR, v)
}
func (s *kubeAgentStore) UpsertResultReceipt(ctx context.Context, v *spacev1.SpaceResultReceipt) error {
	return upsertTyped(ctx, s.dynamic, spacekube.ResultReceiptGVR, v)
}
func (s *kubeAgentStore) UpsertRemoteReporterObject(ctx context.Context, resource string, raw []byte) error {
	gvr, kind, ok := reporterGVR(resource)
	if !ok {
		return fmt.Errorf("remote reporter resource %q is not allowed", resource)
	}
	var object unstructured.Unstructured
	if err := json.Unmarshal(raw, &object.Object); err != nil {
		return err
	}
	if (object.GetAPIVersion() != spacev1.SchemeGroupVersion.String() && object.GetAPIVersion() != spacev1.CanonicalAPIVersion) || object.GetKind() != kind || object.GetName() == "" {
		return fmt.Errorf("remote reporter object GVK/name mismatch")
	}
	object.SetAPIVersion(spacev1.CanonicalAPIVersion)
	return upsertU(ctx, s.dynamic, gvr, &object)
}
func (s *kubeAgentStore) PutFenceToken(ctx context.Context, namespace string, f spacev1.ExecutionFence, token string) error {
	if namespace == "" {
		return fmt.Errorf("mission namespace is required for fence token")
	}
	name := spacev1.ExecutionTokenSecretName(f)
	secrets := s.client.CoreV1().Secrets(namespace)
	current, err := secrets.Get(ctx, name, metav1.GetOptions{})
	if err == nil {
		if string(current.Data["token"]) != token {
			return fmt.Errorf("fence token Secret collision")
		}
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}
	immutable := true
	_, err = secrets.Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: map[string]string{spacev1.GroupName + "/mission-uid": f.MissionUID, spacev1.GroupName + "/plan-id": f.PlanID, spacev1.GroupName + "/attempt": strconv.Itoa(int(f.Attempt)), spacev1.GroupName + "/lease-epoch": strconv.FormatInt(f.LeaseEpoch, 10)}}, Immutable: &immutable, Type: corev1.SecretTypeOpaque, Data: map[string][]byte{"token": []byte(token)}}, metav1.CreateOptions{})
	return err
}
func (s *kubeAgentStore) GetFenceToken(ctx context.Context, namespace string, f spacev1.ExecutionFence) (string, error) {
	if namespace == "" {
		return "", fmt.Errorf("mission namespace is required")
	}
	v, err := s.client.CoreV1().Secrets(namespace).Get(ctx, spacev1.ExecutionTokenSecretName(f), metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	token := string(v.Data["token"])
	if token == "" {
		return "", fmt.Errorf("fence token key is missing")
	}
	return token, nil
}

func reporterGVR(resource string) (schema.GroupVersionResource, string, bool) {
	switch resource {
	case "spacetransferreceipts":
		return spacekube.TransferReceiptGVR, "SpaceTransferReceipt", true
	case "spaceexecutionleases":
		return spacekube.ExecutionLeaseGVR, "SpaceExecutionLease", true
	case "spaceexecutionobservations":
		return spacekube.ExecutionObservationGVR, "SpaceExecutionObservation", true
	case "spaceresultreceipts":
		return spacekube.ResultReceiptGVR, "SpaceResultReceipt", true
	default:
		return schema.GroupVersionResource{}, "", false
	}
}
func fromU(u *unstructured.Unstructured, out any) error {
	return runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, out)
}
func toU(v any) (*unstructured.Unstructured, error) {
	m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(v)
	if err != nil {
		return nil, err
	}
	return &unstructured.Unstructured{Object: m}, nil
}
func upsertTyped(ctx context.Context, d dynamic.Interface, gvr schema.GroupVersionResource, v any) error {
	u, err := toU(v)
	if err != nil {
		return err
	}
	return upsertU(ctx, d, gvr, u)
}
func upsertU(ctx context.Context, d dynamic.Interface, gvr schema.GroupVersionResource, u *unstructured.Unstructured) error {
	resource := d.Resource(gvr)
	current, err := resource.Get(ctx, u.GetName(), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = resource.Create(ctx, u, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	same, err := sameOrAdvance(current, u)
	if err != nil {
		return err
	}
	if same {
		return nil
	}
	u.SetResourceVersion(current.GetResourceVersion())
	_, err = resource.Update(ctx, u, metav1.UpdateOptions{})
	return err
}
func sameOrAdvance(old, new *unstructured.Unstructured) (bool, error) {
	oldSeq, _, _ := unstructured.NestedInt64(old.Object, "spec", "provenance", "sequence")
	newSeq, _, _ := unstructured.NestedInt64(new.Object, "spec", "provenance", "sequence")
	oldDigest, _, _ := unstructured.NestedString(old.Object, "spec", "provenance", "digest")
	newDigest, _, _ := unstructured.NestedString(new.Object, "spec", "provenance", "digest")
	if oldSeq == 0 && newSeq == 0 {
		return reflect.DeepEqual(old.Object["spec"], new.Object["spec"]), nil
	}
	if newSeq == oldSeq && newDigest == oldDigest {
		return true, nil
	}
	if newSeq <= oldSeq {
		return false, fmt.Errorf("reporter sequence did not advance: old=%d new=%d", oldSeq, newSeq)
	}
	return false, nil
}

// Generic helper is kept only for transfer intents whose concrete conversion is
// compile-time checked by the callers below.
func listTyped[T any](ctx context.Context, d dynamic.Interface, gvr schema.GroupVersionResource, identity func(*T) *T) ([]*T, error) {
	list, err := d.Resource(gvr).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]*T, 0, len(list.Items))
	for i := range list.Items {
		v := new(T)
		if err := fromU(&list.Items[i], v); err != nil {
			return nil, err
		}
		out = append(out, identity(v))
	}
	return out, nil
}

type kubeExecutor struct{ client kubernetes.Interface }

func (e *kubeExecutor) EnsureExecution(ctx context.Context, m *spacev1.SpaceMission, p *spacev1.SpacePlacementIntent, l *spacev1.SpaceExecutionLease) error {
	pod, err := spaceworkload.BuildAttemptPodWithLease(m, p, m.Spec.WorkloadTemplate, l)
	if err != nil {
		return err
	}
	pods := e.client.CoreV1().Pods(m.Namespace)
	current, err := pods.Get(ctx, pod.Name, metav1.GetOptions{})
	if err == nil {
		if current.Labels[spacev1.LabelPlacementID] != p.Spec.PlanID || current.Annotations[spacev1.GroupName+"/execution-lease"] != l.Name || current.Annotations[spacev1.GroupName+"/token-hash"] != l.Spec.Fence.TokenHash {
			return fmt.Errorf("existing remote attempt Pod is fenced by different identity")
		}
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}
	_, err = pods.Create(ctx, pod, metav1.CreateOptions{})
	return err
}

func (e *kubeExecutor) FenceExecution(ctx context.Context, m *spacev1.SpaceMission, p *spacev1.SpacePlacementIntent, reason string) (bool, error) {
	if m == nil || p == nil {
		return false, fmt.Errorf("mission and placement are required")
	}
	name := spaceworkload.AttemptPodName(m.Name, p.Spec.Attempt)
	pods := e.client.CoreV1().Pods(m.Namespace)
	current, err := pods.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if current.Labels[spacev1.LabelPlacementID] != p.Spec.PlanID {
		return false, fmt.Errorf("refusing to fence Pod owned by another plan")
	}
	zero := int64(0)
	if err := pods.Delete(ctx, name, metav1.DeleteOptions{GracePeriodSeconds: &zero}); err != nil && !apierrors.IsNotFound(err) {
		return false, fmt.Errorf("fence execution (%s): %w", reason, err)
	}
	return true, nil
}

var _ spacetransport.AgentStore = (*kubeAgentStore)(nil)
var _ spacetransport.Executor = (*kubeExecutor)(nil)
