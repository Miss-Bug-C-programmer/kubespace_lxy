package spacecomputescheduler

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	resourcehelper "k8s.io/component-helpers/resource"
	"sigs.k8s.io/yaml"
)

const iluvatarHardwareNamespace = "space-compute-iluvatar-hardware"

// TestIluvatarPhysicalAllocationAndWorkload is deliberately opt-in. The input
// Pod must exercise a physical Iluvatar device and write the vendor allocation
// identity to a container termination message. This keeps hardware evidence
// distinct from fixture-driven functional qualification.
func TestIluvatarPhysicalAllocationAndWorkload(t *testing.T) {
	kubeconfig := strings.TrimSpace(os.Getenv("K3S_ILUVATAR_HARDWARE_KUBECONFIG"))
	podFile := strings.TrimSpace(os.Getenv("K3S_ILUVATAR_HARDWARE_POD_FILE"))
	expectedDeviceID := strings.TrimSpace(os.Getenv("K3S_ILUVATAR_EXPECTED_DEVICE_ID"))
	if kubeconfig == "" || podFile == "" || expectedDeviceID == "" {
		t.Skip("set K3S_ILUVATAR_HARDWARE_KUBECONFIG, K3S_ILUVATAR_HARDWARE_POD_FILE, and K3S_ILUVATAR_EXPECTED_DEVICE_ID for physical Iluvatar qualification")
	}
	raw, err := os.ReadFile(podFile)
	if err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{}
	if err := yaml.UnmarshalStrict(raw, pod); err != nil {
		t.Fatalf("decode hardware Pod: %v", err)
	}
	if pod.Spec.SchedulerName != "space-compute-scheduler" {
		t.Fatalf("hardware Pod schedulerName=%q, want space-compute-scheduler", pod.Spec.SchedulerName)
	}
	request := resourcehelper.PodRequests(pod, resourcehelper.PodResourcesOptions{})[corev1.ResourceName("iluvatar.com/gpu")]
	if request.Sign() <= 0 {
		t.Fatal("hardware Pod must request iluvatar.com/gpu")
	}
	if len(pod.Spec.Containers) == 0 {
		t.Fatal("hardware Pod has no containers")
	}
	pod.Namespace = iluvatarHardwareNamespace
	pod.ResourceVersion = ""
	pod.UID = ""
	pod.CreationTimestamp = metav1.Time{}
	pod.ManagedFields = nil

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	client := clientForKubeconfig(t, kubeconfig)
	cleanupIluvatarHardware(t, context.Background(), client)
	defer cleanupIluvatarHardware(t, context.Background(), client)
	if _, err := client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: iluvatarHardwareNamespace}}, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatal(err)
	}
	created, err := client.CoreV1().Pods(iluvatarHardwareNamespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	var completed *corev1.Pod
	for ctx.Err() == nil {
		current, getErr := client.CoreV1().Pods(iluvatarHardwareNamespace).Get(ctx, created.Name, metav1.GetOptions{})
		if getErr != nil {
			t.Fatal(getErr)
		}
		if current.Status.Phase == corev1.PodSucceeded || current.Status.Phase == corev1.PodFailed {
			completed = current
			break
		}
		time.Sleep(time.Second)
	}
	if completed == nil {
		t.Fatalf("hardware Pod did not complete: %v", ctx.Err())
	}
	if completed.Spec.NodeName == "" {
		t.Fatal("hardware Pod completed without a bound Node")
	}
	node, err := client.CoreV1().Nodes().Get(ctx, completed.Spec.NodeName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	allocatable := node.Status.Allocatable[corev1.ResourceName("iluvatar.com/gpu")]
	if allocatable.Cmp(request) < 0 {
		t.Fatalf("Node %s allocatable iluvatar.com/gpu is below Pod request %s", node.Name, request.String())
	}
	if completed.Status.Phase != corev1.PodSucceeded {
		t.Fatalf("hardware Pod phase=%s reason=%s message=%s", completed.Status.Phase, completed.Status.Reason, completed.Status.Message)
	}
	for _, status := range completed.Status.ContainerStatuses {
		if status.State.Terminated != nil && strings.Contains(status.State.Terminated.Message, expectedDeviceID) {
			return
		}
	}
	t.Fatalf("successful hardware Pod did not report allocated device ID %q in a termination message", expectedDeviceID)
}

func cleanupIluvatarHardware(t *testing.T, ctx context.Context, client kubernetes.Interface) {
	t.Helper()
	zero := int64(0)
	_ = client.CoreV1().Pods(iluvatarHardwareNamespace).DeleteCollection(ctx, metav1.DeleteOptions{GracePeriodSeconds: &zero}, metav1.ListOptions{})
	propagation := metav1.DeletePropagationForeground
	err := client.CoreV1().Namespaces().Delete(ctx, iluvatarHardwareNamespace, metav1.DeleteOptions{PropagationPolicy: &propagation})
	if err != nil && !apierrors.IsNotFound(err) {
		t.Logf("cleanup hardware namespace: %v", err)
	}
}
