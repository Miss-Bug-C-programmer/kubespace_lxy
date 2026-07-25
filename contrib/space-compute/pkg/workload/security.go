package workload

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
)

var allowedAttemptLabelPrefixes = []string{"app.kubernetes.io/"}

func secureAttemptPod(mission *spacev1.SpaceMission, placement *spacev1.SpacePlacementIntent, template corev1.PodTemplateSpec) (*corev1.Pod, error) {
	if mission == nil || placement == nil {
		return nil, fmt.Errorf("mission and placement are required")
	}
	pod := &corev1.Pod{Spec: *template.Spec.DeepCopy()}
	pod.Namespace = mission.Namespace
	pod.Name = AttemptPodName(mission.Name, placement.Spec.Attempt)
	pod.GenerateName = ""
	pod.UID = ""
	pod.ResourceVersion = ""
	pod.Generation = 0
	pod.CreationTimestamp = metav1.Time{}
	pod.DeletionTimestamp = nil
	pod.DeletionGracePeriodSeconds = nil
	pod.ManagedFields = nil
	pod.Finalizers = nil
	pod.OwnerReferences = nil
	pod.Labels = copyAllowedAttemptLabels(template.Labels)
	pod.Annotations = map[string]string{}

	pod.Spec.SchedulerName = "space-compute-scheduler"
	pod.Spec.PriorityClassName = ""
	pod.Spec.PreemptionPolicy = nil
	pod.Spec.SchedulingGates = nil
	pod.Spec.NodeName = ""
	pod.Spec.NodeSelector = nil
	pod.Spec.Affinity = nil
	pod.Spec.Tolerations = nil
	pod.Spec.HostNetwork = false
	pod.Spec.HostPID = false
	pod.Spec.HostIPC = false
	pod.Spec.ShareProcessNamespace = nil
	falseValue := false
	pod.Spec.AutomountServiceAccountToken = &falseValue

	if pod.Spec.SecurityContext == nil {
		pod.Spec.SecurityContext = &corev1.PodSecurityContext{}
	}
	trueValue := true
	pod.Spec.SecurityContext.RunAsNonRoot = &trueValue
	pod.Spec.SecurityContext.SeccompProfile = &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}
	pod.Spec.SecurityContext.AppArmorProfile = &corev1.AppArmorProfile{Type: corev1.AppArmorProfileTypeRuntimeDefault}
	pod.Spec.SecurityContext.SELinuxOptions = nil
	if pod.Spec.SecurityContext.WindowsOptions != nil {
		pod.Spec.SecurityContext.WindowsOptions.HostProcess = &falseValue
	}
	if pod.Spec.SecurityContext.RunAsUser != nil && *pod.Spec.SecurityContext.RunAsUser == 0 {
		return nil, fmt.Errorf("workloadTemplate runAsUser=0 violates Pod Security Restricted")
	}
	if len(pod.Spec.SecurityContext.Sysctls) != 0 {
		return nil, fmt.Errorf("workloadTemplate sysctls are not accepted for controlled attempt Pods")
	}
	for _, volume := range pod.Spec.Volumes {
		if volume.Projected != nil {
			for _, source := range volume.Projected.Sources {
				if source.ServiceAccountToken != nil {
					return nil, fmt.Errorf("projected serviceAccountToken is not accepted for controlled attempt Pods")
				}
			}
		}
		if !restrictedAttemptVolume(volume) {
			return nil, fmt.Errorf("volume %q is outside Pod Security Restricted", volume.Name)
		}
	}
	for i := range pod.Spec.InitContainers {
		if err := hardenContainer(&pod.Spec.InitContainers[i]); err != nil {
			return nil, err
		}
	}
	for i := range pod.Spec.Containers {
		if err := hardenContainer(&pod.Spec.Containers[i]); err != nil {
			return nil, err
		}
	}
	if len(pod.Spec.EphemeralContainers) != 0 {
		return nil, fmt.Errorf("ephemeralContainers are not accepted for controlled attempt Pods")
	}
	return pod, nil
}

func hardenContainer(container *corev1.Container) error {
	if container == nil {
		return nil
	}
	filteredEnv := container.Env[:0]
	for _, env := range container.Env {
		switch env.Name {
		case "SPACE_COMPUTE_FENCE_TOKEN", "SPACE_COMPUTE_LEASE_EPOCH", "SPACE_COMPUTE_TOKEN_HASH":
			continue
		default:
			filteredEnv = append(filteredEnv, env)
		}
	}
	container.Env = filteredEnv
	if container.SecurityContext == nil {
		container.SecurityContext = &corev1.SecurityContext{}
	}
	if container.SecurityContext.RunAsUser != nil && *container.SecurityContext.RunAsUser == 0 {
		return fmt.Errorf("container %q runAsUser=0 violates Pod Security Restricted", container.Name)
	}
	falseValue := false
	trueValue := true
	container.SecurityContext.Privileged = &falseValue
	container.SecurityContext.AllowPrivilegeEscalation = &falseValue
	container.SecurityContext.RunAsNonRoot = &trueValue
	container.SecurityContext.Capabilities = &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}
	container.SecurityContext.SeccompProfile = &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}
	container.SecurityContext.AppArmorProfile = &corev1.AppArmorProfile{Type: corev1.AppArmorProfileTypeRuntimeDefault}
	container.SecurityContext.SELinuxOptions = nil
	container.SecurityContext.ProcMount = nil
	if container.SecurityContext.WindowsOptions != nil {
		container.SecurityContext.WindowsOptions.HostProcess = &falseValue
	}
	for i := range container.Ports {
		container.Ports[i].HostPort = 0
		container.Ports[i].HostIP = ""
	}
	return nil
}

func restrictedAttemptVolume(volume corev1.Volume) bool {
	v := volume.VolumeSource
	return v.ConfigMap != nil || v.CSI != nil || v.DownwardAPI != nil || v.EmptyDir != nil || v.Ephemeral != nil || v.PersistentVolumeClaim != nil || v.Projected != nil || v.Secret != nil
}

func copyAllowedAttemptLabels(in map[string]string) map[string]string {
	out := map[string]string{}
	for key, value := range in {
		if strings.HasPrefix(key, spacev1.GroupName+"/") || strings.HasPrefix(key, "gpustability.k3s.io/") {
			continue
		}
		for _, prefix := range allowedAttemptLabelPrefixes {
			if strings.HasPrefix(key, prefix) {
				out[key] = value
				break
			}
		}
	}
	return out
}

func objectDigest(value any) (string, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func missionSecurityDigest(mission *spacev1.SpaceMission) (string, error) {
	return objectDigest(struct {
		UID        string                   `json:"uid"`
		Generation int64                    `json:"generation"`
		Spec       spacev1.SpaceMissionSpec `json:"spec"`
	}{UID: string(mission.UID), Generation: mission.Generation, Spec: mission.Spec})
}

func placementSecurityDigest(placement *spacev1.SpacePlacementIntent) (string, error) {
	return objectDigest(struct {
		UID        string                           `json:"uid,omitempty"`
		Generation int64                            `json:"generation"`
		Spec       spacev1.SpacePlacementIntentSpec `json:"spec"`
	}{UID: string(placement.UID), Generation: placement.Generation, Spec: placement.Spec})
}
