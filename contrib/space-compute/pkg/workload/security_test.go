package workload

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
)

func TestBuildAttemptPodStripsUserMetadataAndForcesRestrictedSecurity(t *testing.T) {
	now := time.Date(2026, 7, 26, 0, 0, 0, 0, time.UTC)
	mission, placement := dispatchFixture(now)
	placement.Spec.ComputeStart = metav1.NewTime(now)
	placement.Spec.NotBefore = metav1.NewTime(now)
	lease := validLease(mission, placement, placement.Spec.Attempt, 7, now)
	trueValue := true
	template := mission.Spec.WorkloadTemplate.DeepCopy()
	template.Name = "user-name"
	template.GenerateName = "user-prefix-"
	template.UID = types.UID("forged")
	template.ResourceVersion = "99"
	template.Finalizers = []string{"evil.example/finalizer"}
	template.OwnerReferences = []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: "victim", UID: types.UID("victim")}}
	template.Labels = map[string]string{"app.kubernetes.io/name": "science", spacev1.LabelPlacementID: "forged", "evil.example/select": "true"}
	template.Annotations = map[string]string{spacev1.AnnotationPlacement: "forged", "sidecar.istio.io/inject": "true"}
	template.Spec.SchedulerName = "default-scheduler"
	template.Spec.NodeName = "chosen-node"
	template.Spec.NodeSelector = map[string]string{"kubernetes.io/hostname": "chosen-node"}
	template.Spec.Tolerations = []corev1.Toleration{{Key: "node-role.kubernetes.io/control-plane", Operator: corev1.TolerationOpExists}}
	template.Spec.HostNetwork = true
	template.Spec.AutomountServiceAccountToken = &trueValue
	template.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{Privileged: &trueValue, AllowPrivilegeEscalation: &trueValue, Capabilities: &corev1.Capabilities{Add: []corev1.Capability{"SYS_ADMIN"}}}
	template.Spec.Containers[0].Ports = []corev1.ContainerPort{{ContainerPort: 8080, HostPort: 8080, HostIP: "0.0.0.0"}}

	pod, err := BuildAttemptPodWithLease(mission, placement, *template, lease)
	if err != nil {
		t.Fatal(err)
	}
	if pod.Name != AttemptPodName(mission.Name, placement.Spec.Attempt) || pod.GenerateName != "" || pod.UID != "" || pod.ResourceVersion != "" || len(pod.Finalizers) != 0 {
		t.Fatalf("controller metadata not enforced: %#v", pod.ObjectMeta)
	}
	if len(pod.OwnerReferences) != 1 || pod.OwnerReferences[0].Kind != "SpaceMission" || pod.OwnerReferences[0].UID != mission.UID {
		t.Fatalf("ownerReferences not controller-owned: %#v", pod.OwnerReferences)
	}
	if pod.Labels[spacev1.LabelPlacementID] != placement.Spec.PlanID || pod.Labels[spacev1.LabelMissionUID] != string(mission.UID) || pod.Labels["evil.example/select"] != "" {
		t.Fatalf("labels were not allowlisted/controller-owned: %#v", pod.Labels)
	}
	if pod.Annotations["sidecar.istio.io/inject"] != "" || pod.Annotations[spacev1.AnnotationMissionDigest] == "" || pod.Annotations[spacev1.AnnotationPlacementDigest] == "" {
		t.Fatalf("annotations were not sanitized/digested: %#v", pod.Annotations)
	}
	if len(pod.Annotations[spacev1.AnnotationMissionDigest]) != 64 || len(pod.Annotations[spacev1.AnnotationPlacementDigest]) != 64 {
		t.Fatalf("digest length invalid: %#v", pod.Annotations)
	}
	if pod.Spec.SchedulerName != "space-compute-scheduler" || pod.Spec.NodeName != "" || len(pod.Spec.NodeSelector) != 0 || len(pod.Spec.Tolerations) != 0 || pod.Spec.HostNetwork {
		t.Fatalf("planner placement boundary not forced: %#v", pod.Spec)
	}
	if pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken {
		t.Fatal("automountServiceAccountToken was not forced false")
	}
	security := pod.Spec.Containers[0].SecurityContext
	if security == nil || security.Privileged == nil || *security.Privileged || security.AllowPrivilegeEscalation == nil || *security.AllowPrivilegeEscalation || security.RunAsNonRoot == nil || !*security.RunAsNonRoot {
		t.Fatalf("restricted container security not forced: %#v", security)
	}
	if security.Capabilities == nil || len(security.Capabilities.Add) != 0 || len(security.Capabilities.Drop) != 1 || security.Capabilities.Drop[0] != "ALL" {
		t.Fatalf("capabilities not restricted: %#v", security.Capabilities)
	}
	if pod.Spec.Containers[0].Ports[0].HostPort != 0 || pod.Spec.Containers[0].Ports[0].HostIP != "" {
		t.Fatalf("host port survived hardening: %#v", pod.Spec.Containers[0].Ports)
	}
	if strings.Contains(pod.Annotations[spacev1.AnnotationPlacement], "forged") {
		t.Fatal("controller placement annotation reused user value")
	}
}

func TestBuildAttemptPodRejectsHostPathEvenWithoutAdmission(t *testing.T) {
	now := time.Date(2026, 7, 26, 0, 0, 0, 0, time.UTC)
	mission, placement := dispatchFixture(now)
	lease := validLease(mission, placement, placement.Spec.Attempt, 1, now)
	template := mission.Spec.WorkloadTemplate.DeepCopy()
	template.Spec.Volumes = []corev1.Volume{{Name: "host", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/"}}}}
	if _, err := BuildAttemptPodWithLease(mission, placement, *template, lease); err == nil {
		t.Fatal("BuildAttemptPod accepted hostPath after admission bypass")
	}
}
