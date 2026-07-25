package admission

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	admissionv1 "k8s.io/api/admission/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
)

const MissionSecurityPolicyVersion = "spacecompute.k3s.io/v1alpha1"

// MissionSecurityPolicy is administrator-owned admission policy. Empty allowlists
// deny the corresponding optional capability. High-risk fields are denied unless
// an explicit boolean is true, while BuildAttemptPod still enforces Restricted
// Pod Security and controller-owned placement metadata as a second boundary.
type MissionSecurityPolicy struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`

	AllowedServiceAccounts    []string `json:"allowedServiceAccounts"`
	AllowedRuntimeClasses     []string `json:"allowedRuntimeClasses,omitempty"`
	AllowedImageRegistries    []string `json:"allowedImageRegistries"`
	AllowedCapabilities       []string `json:"allowedCapabilities,omitempty"`
	AllowedLabelPrefixes      []string `json:"allowedLabelPrefixes,omitempty"`
	AllowedAnnotationPrefixes []string `json:"allowedAnnotationPrefixes,omitempty"`
	AttemptPodCreators        []string `json:"attemptPodCreators"`

	AllowHostNetwork                  bool `json:"allowHostNetwork,omitempty"`
	AllowHostPID                      bool `json:"allowHostPID,omitempty"`
	AllowHostIPC                      bool `json:"allowHostIPC,omitempty"`
	AllowPrivileged                   bool `json:"allowPrivileged,omitempty"`
	AllowPrivilegeEscalation          bool `json:"allowPrivilegeEscalation,omitempty"`
	AllowHostPath                     bool `json:"allowHostPath,omitempty"`
	AllowHostPort                     bool `json:"allowHostPort,omitempty"`
	AllowAutomountServiceAccountToken bool `json:"allowAutomountServiceAccountToken,omitempty"`
	AllowNodeSelector                 bool `json:"allowNodeSelector,omitempty"`
	AllowNodeAffinity                 bool `json:"allowNodeAffinity,omitempty"`
	AllowTolerations                  bool `json:"allowTolerations,omitempty"`
	AllowPreselectedNodeName          bool `json:"allowPreselectedNodeName,omitempty"`
}

func LoadMissionSecurityPolicy(path string) (*MissionSecurityPolicy, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read mission security policy: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var policy MissionSecurityPolicy
	if err := decoder.Decode(&policy); err != nil {
		return nil, fmt.Errorf("decode mission security policy: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return nil, err
	}
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	return &policy, nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("mission security policy contains trailing JSON")
		}
		return fmt.Errorf("decode trailing mission security policy JSON: %w", err)
	}
	return nil
}

func (p *MissionSecurityPolicy) Validate() error {
	if p == nil {
		return fmt.Errorf("mission security policy is required")
	}
	if p.APIVersion != MissionSecurityPolicyVersion || p.Kind != "MissionSecurityPolicy" {
		return fmt.Errorf("mission security policy must use %s MissionSecurityPolicy", MissionSecurityPolicyVersion)
	}
	for name, values := range map[string][]string{
		"allowedServiceAccounts":    p.AllowedServiceAccounts,
		"allowedRuntimeClasses":     p.AllowedRuntimeClasses,
		"allowedImageRegistries":    p.AllowedImageRegistries,
		"allowedCapabilities":       p.AllowedCapabilities,
		"allowedLabelPrefixes":      p.AllowedLabelPrefixes,
		"allowedAnnotationPrefixes": p.AllowedAnnotationPrefixes,
		"attemptPodCreators":        p.AttemptPodCreators,
	} {
		seen := map[string]struct{}{}
		for _, value := range values {
			value = strings.TrimSpace(value)
			if value == "" {
				return fmt.Errorf("%s contains an empty value", name)
			}
			if _, ok := seen[value]; ok {
				return fmt.Errorf("%s contains duplicate %q", name, value)
			}
			seen[value] = struct{}{}
		}
	}
	return nil
}

type SubjectAccessReviewer interface {
	Review(context.Context, authorizationv1.SubjectAccessReviewSpec) (bool, string, error)
}

type KubernetesSubjectAccessReviewer struct{ client kubernetes.Interface }

func NewKubernetesSubjectAccessReviewer(client kubernetes.Interface) (*KubernetesSubjectAccessReviewer, error) {
	if client == nil {
		return nil, fmt.Errorf("Kubernetes client is required")
	}
	return &KubernetesSubjectAccessReviewer{client: client}, nil
}

func (r *KubernetesSubjectAccessReviewer) Review(ctx context.Context, spec authorizationv1.SubjectAccessReviewSpec) (bool, string, error) {
	result, err := r.client.AuthorizationV1().SubjectAccessReviews().Create(ctx, &authorizationv1.SubjectAccessReview{Spec: spec}, metav1CreateOptions)
	if err != nil {
		return false, "", err
	}
	return result.Status.Allowed, result.Status.Reason, nil
}

// metav1CreateOptions is kept package-local to make SAR calls explicit and easy
// to replace with a fake reviewer in tests.
var metav1CreateOptions = metav1.CreateOptions{}

type MissionValidator struct {
	policy   *MissionSecurityPolicy
	reviewer SubjectAccessReviewer
}

func NewMissionValidator(policy *MissionSecurityPolicy, reviewer SubjectAccessReviewer) (*MissionValidator, error) {
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	if reviewer == nil {
		return nil, fmt.Errorf("SubjectAccessReview client is required")
	}
	return &MissionValidator{policy: policy, reviewer: reviewer}, nil
}

func (v *MissionValidator) Validate(ctx context.Context, request *admissionv1.AdmissionRequest) error {
	if request == nil {
		return fmt.Errorf("admission request is required")
	}
	if request.Operation != admissionv1.Create && request.Operation != admissionv1.Update {
		return nil
	}
	if request.Resource.Group == spacev1.GroupName && request.Resource.Version == "v1alpha1" && request.Resource.Resource == "spacemissions" {
		return v.validateMission(ctx, request)
	}
	if request.Resource.Group == "" && request.Resource.Version == "v1" && request.Resource.Resource == "pods" {
		return v.validateAttemptPod(request)
	}
	return nil
}

func (v *MissionValidator) validateMission(ctx context.Context, request *admissionv1.AdmissionRequest) error {
	mission := &spacev1.SpaceMission{}
	if err := decodeRaw(request.Object.Raw, mission); err != nil {
		return fmt.Errorf("decode SpaceMission: %w", err)
	}
	if request.Namespace == "" || mission.Namespace == "" || request.Namespace != mission.Namespace {
		return fmt.Errorf("mission namespace must equal admission namespace")
	}
	if request.Operation == admissionv1.Update {
		old := &spacev1.SpaceMission{}
		if err := decodeRaw(request.OldObject.Raw, old); err != nil {
			return fmt.Errorf("decode previous SpaceMission: %w", err)
		}
		if apiequality.Semantic.DeepEqual(old.Spec.WorkloadTemplate, mission.Spec.WorkloadTemplate) {
			return nil
		}
	}
	if err := v.validateTemplate(mission.Namespace, &mission.Spec.WorkloadTemplate); err != nil {
		return err
	}
	attrs := v.authorizationChecks(mission.Namespace, &mission.Spec.WorkloadTemplate)
	for _, attr := range attrs {
		allowed, reason, err := v.review(ctx, request, attr)
		if err != nil {
			return fmt.Errorf("SubjectAccessReview %s %s/%s: %w", attr.Verb, attr.Resource, attr.Name, err)
		}
		if !allowed {
			if strings.TrimSpace(reason) == "" {
				reason = "authorization denied"
			}
			return fmt.Errorf("requester %q may not %s %s %q in namespace %q: %s", request.UserInfo.Username, attr.Verb, attr.Resource, attr.Name, attr.Namespace, reason)
		}
	}
	return nil
}

func (v *MissionValidator) review(ctx context.Context, request *admissionv1.AdmissionRequest, attr authorizationv1.ResourceAttributes) (bool, string, error) {
	extra := map[string]authorizationv1.ExtraValue{}
	for key, values := range request.UserInfo.Extra {
		extra[key] = authorizationv1.ExtraValue(append([]string(nil), values...))
	}
	return v.reviewer.Review(ctx, authorizationv1.SubjectAccessReviewSpec{
		User:               request.UserInfo.Username,
		Groups:             append([]string(nil), request.UserInfo.Groups...),
		Extra:              extra,
		ResourceAttributes: &attr,
	})
}

func (v *MissionValidator) authorizationChecks(namespace string, template *corev1.PodTemplateSpec) []authorizationv1.ResourceAttributes {
	checks := []authorizationv1.ResourceAttributes{{Namespace: namespace, Verb: "create", Group: "", Version: "v1", Resource: "pods"}}
	serviceAccount := strings.TrimSpace(template.Spec.ServiceAccountName)
	if serviceAccount == "" {
		serviceAccount = "default"
	}
	checks = append(checks, authorizationv1.ResourceAttributes{Namespace: namespace, Verb: "use", Group: "", Version: "v1", Resource: "serviceaccounts", Name: serviceAccount})

	refs := collectReferences(namespace, template)
	for _, ref := range refs {
		checks = append(checks, authorizationv1.ResourceAttributes{Namespace: ref.namespace, Verb: "get", Group: ref.group, Version: ref.version, Resource: ref.resource, Name: ref.name})
	}
	for _, volume := range template.Spec.Volumes {
		if volume.Ephemeral != nil {
			checks = append(checks, authorizationv1.ResourceAttributes{Namespace: namespace, Verb: "create", Group: "", Version: "v1", Resource: "persistentvolumeclaims"})
		}
	}
	if template.Spec.RuntimeClassName != nil && strings.TrimSpace(*template.Spec.RuntimeClassName) != "" {
		checks = append(checks, authorizationv1.ResourceAttributes{Verb: "use", Group: "node.k8s.io", Version: "v1", Resource: "runtimeclasses", Name: strings.TrimSpace(*template.Spec.RuntimeClassName)})
	}
	return dedupeAttributes(checks)
}

type objectReference struct {
	group, version, resource, namespace, name string
}

func collectReferences(namespace string, template *corev1.PodTemplateSpec) []objectReference {
	if template == nil {
		return nil
	}
	ns := namespace
	refs := []objectReference{}
	add := func(resourceName, name string) {
		name = strings.TrimSpace(name)
		if name != "" {
			refs = append(refs, objectReference{version: "v1", resource: resourceName, namespace: ns, name: name})
		}
	}
	for _, volume := range template.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil {
			add("persistentvolumeclaims", volume.PersistentVolumeClaim.ClaimName)
		}
		if volume.Secret != nil {
			add("secrets", volume.Secret.SecretName)
		}
		if volume.ConfigMap != nil {
			add("configmaps", volume.ConfigMap.Name)
		}
		if volume.CSI != nil && volume.CSI.NodePublishSecretRef != nil {
			add("secrets", volume.CSI.NodePublishSecretRef.Name)
		}
		if volume.Projected != nil {
			for _, source := range volume.Projected.Sources {
				if source.Secret != nil {
					add("secrets", source.Secret.Name)
				}
				if source.ConfigMap != nil {
					add("configmaps", source.ConfigMap.Name)
				}
			}
		}
	}
	for _, secret := range template.Spec.ImagePullSecrets {
		add("secrets", secret.Name)
	}
	containers := append([]corev1.Container(nil), template.Spec.InitContainers...)
	containers = append(containers, template.Spec.Containers...)
	for _, container := range containers {
		for _, from := range container.EnvFrom {
			if from.SecretRef != nil {
				add("secrets", from.SecretRef.Name)
			}
			if from.ConfigMapRef != nil {
				add("configmaps", from.ConfigMapRef.Name)
			}
		}
		for _, env := range container.Env {
			if env.ValueFrom == nil {
				continue
			}
			if env.ValueFrom.SecretKeyRef != nil {
				add("secrets", env.ValueFrom.SecretKeyRef.Name)
			}
			if env.ValueFrom.ConfigMapKeyRef != nil {
				add("configmaps", env.ValueFrom.ConfigMapKeyRef.Name)
			}
		}
	}
	return refs
}

func dedupeAttributes(in []authorizationv1.ResourceAttributes) []authorizationv1.ResourceAttributes {
	seen := map[string]struct{}{}
	out := make([]authorizationv1.ResourceAttributes, 0, len(in))
	for _, attr := range in {
		key := strings.Join([]string{attr.Namespace, attr.Verb, attr.Group, attr.Version, attr.Resource, attr.Name}, "\x00")
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, attr)
	}
	return out
}

func (v *MissionValidator) validateTemplate(namespace string, template *corev1.PodTemplateSpec) error {
	if template == nil {
		return fmt.Errorf("workloadTemplate is required")
	}
	if template.Namespace != "" && template.Namespace != namespace {
		return fmt.Errorf("workloadTemplate metadata.namespace may not target another namespace")
	}
	if template.Name != "" || template.GenerateName != "" || len(template.OwnerReferences) != 0 || len(template.Finalizers) != 0 {
		return fmt.Errorf("workloadTemplate may not set name, generateName, ownerReferences, or finalizers")
	}
	if template.UID != "" || template.ResourceVersion != "" || len(template.ManagedFields) != 0 {
		return fmt.Errorf("workloadTemplate may not set UID, resourceVersion, or managedFields")
	}
	if err := validateUserMetadata(template.Labels, v.policy.AllowedLabelPrefixes, "label"); err != nil {
		return err
	}
	if err := validateUserMetadata(template.Annotations, v.policy.AllowedAnnotationPrefixes, "annotation"); err != nil {
		return err
	}
	return validatePodSpec(namespace, &template.Spec, v.policy)
}

func validateUserMetadata(values map[string]string, allow []string, kind string) error {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if isReservedMetadataKey(key) {
			return fmt.Errorf("workloadTemplate %s %q is controller-reserved", kind, key)
		}
		if !matchesPrefix(key, allow) {
			return fmt.Errorf("workloadTemplate %s %q is not allowed by administrator policy", kind, key)
		}
	}
	return nil
}

func isReservedMetadataKey(key string) bool {
	for _, prefix := range []string{spacev1.GroupName + "/", "gpustability.k3s.io/", "scheduler.kubernetes.io/", "scheduler.alpha.kubernetes.io/", "node.kubernetes.io/"} {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}

func matchesPrefix(value string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(value, prefix) {
			return true
		}
	}
	return false
}

func validatePodSpec(namespace string, spec *corev1.PodSpec, policy *MissionSecurityPolicy) error {
	if spec == nil {
		return fmt.Errorf("workloadTemplate spec is required")
	}
	if spec.HostNetwork && !policy.AllowHostNetwork {
		return fmt.Errorf("hostNetwork is denied")
	}
	if spec.HostPID && !policy.AllowHostPID {
		return fmt.Errorf("hostPID is denied")
	}
	if spec.HostIPC && !policy.AllowHostIPC {
		return fmt.Errorf("hostIPC is denied")
	}
	if spec.NodeName != "" && !policy.AllowPreselectedNodeName {
		return fmt.Errorf("preselected nodeName is denied")
	}
	if spec.SchedulerName != "" {
		return fmt.Errorf("schedulerName is controller-owned and must be empty in workloadTemplate")
	}
	if spec.PriorityClassName != "" {
		return fmt.Errorf("priorityClassName is denied in Mission workloadTemplate")
	}
	if len(spec.NodeSelector) != 0 && !policy.AllowNodeSelector {
		return fmt.Errorf("nodeSelector is planner-owned and denied")
	}
	if spec.Affinity != nil && spec.Affinity.NodeAffinity != nil && !policy.AllowNodeAffinity {
		return fmt.Errorf("nodeAffinity is planner-owned and denied")
	}
	if len(spec.Tolerations) != 0 && !policy.AllowTolerations {
		return fmt.Errorf("tolerations are planner-owned and denied")
	}
	if spec.AutomountServiceAccountToken != nil && *spec.AutomountServiceAccountToken && !policy.AllowAutomountServiceAccountToken {
		return fmt.Errorf("automountServiceAccountToken=true is denied")
	}
	serviceAccount := strings.TrimSpace(spec.ServiceAccountName)
	if serviceAccount == "" {
		serviceAccount = "default"
	}
	if !allowedServiceAccount(policy.AllowedServiceAccounts, namespace, serviceAccount) {
		return fmt.Errorf("serviceAccountName %q is not allowed by administrator policy", serviceAccount)
	}
	if spec.RuntimeClassName != nil && strings.TrimSpace(*spec.RuntimeClassName) != "" && !contains(policy.AllowedRuntimeClasses, strings.TrimSpace(*spec.RuntimeClassName)) {
		return fmt.Errorf("runtimeClassName %q is not allowed by administrator policy", *spec.RuntimeClassName)
	}
	if len(spec.EphemeralContainers) != 0 {
		return fmt.Errorf("ephemeralContainers are not allowed in Mission workloadTemplate")
	}
	if spec.SecurityContext != nil {
		if len(spec.SecurityContext.Sysctls) != 0 {
			return fmt.Errorf("Pod sysctls are denied by Mission policy")
		}
		if spec.SecurityContext.RunAsUser != nil && *spec.SecurityContext.RunAsUser == 0 {
			return fmt.Errorf("Pod runAsUser=0 is outside Pod Security Restricted")
		}
		if spec.SecurityContext.WindowsOptions != nil && spec.SecurityContext.WindowsOptions.HostProcess != nil && *spec.SecurityContext.WindowsOptions.HostProcess {
			return fmt.Errorf("Windows hostProcess is denied")
		}
	}
	for _, volume := range spec.Volumes {
		if volume.Projected != nil && !policy.AllowAutomountServiceAccountToken {
			for _, source := range volume.Projected.Sources {
				if source.ServiceAccountToken != nil {
					return fmt.Errorf("projected serviceAccountToken is denied")
				}
			}
		}
		if volume.HostPath != nil && !policy.AllowHostPath {
			return fmt.Errorf("hostPath volume %q is denied", volume.Name)
		}
		if !restrictedVolume(volume) && !(policy.AllowHostPath && volume.HostPath != nil) {
			return fmt.Errorf("volume %q uses a type outside Pod Security Restricted", volume.Name)
		}
	}
	containers := make([]corev1.Container, 0, len(spec.InitContainers)+len(spec.Containers))
	containers = append(containers, spec.InitContainers...)
	containers = append(containers, spec.Containers...)
	if len(containers) == 0 {
		return fmt.Errorf("at least one container is required")
	}
	for _, container := range containers {
		if err := validateContainer(container, policy); err != nil {
			return fmt.Errorf("container %q: %w", container.Name, err)
		}
	}
	return nil
}

func restrictedVolume(volume corev1.Volume) bool {
	v := volume.VolumeSource
	return v.ConfigMap != nil || v.CSI != nil || v.DownwardAPI != nil || v.EmptyDir != nil || v.Ephemeral != nil || v.PersistentVolumeClaim != nil || v.Projected != nil || v.Secret != nil
}

func validateContainer(container corev1.Container, policy *MissionSecurityPolicy) error {
	if !imageAllowed(container.Image, policy.AllowedImageRegistries) {
		return fmt.Errorf("image %q is outside allowed registries", container.Image)
	}
	if err := validateContainerResources(container.Resources); err != nil {
		return err
	}
	security := container.SecurityContext
	if security != nil {
		if security.Privileged != nil && *security.Privileged && !policy.AllowPrivileged {
			return fmt.Errorf("privileged=true is denied")
		}
		if security.AllowPrivilegeEscalation != nil && *security.AllowPrivilegeEscalation && !policy.AllowPrivilegeEscalation {
			return fmt.Errorf("allowPrivilegeEscalation=true is denied")
		}
		if security.RunAsUser != nil && *security.RunAsUser == 0 {
			return fmt.Errorf("runAsUser=0 is outside Pod Security Restricted")
		}
		if security.SeccompProfile != nil && security.SeccompProfile.Type == corev1.SeccompProfileTypeUnconfined {
			return fmt.Errorf("seccompProfile Unconfined is denied")
		}
		if security.WindowsOptions != nil && security.WindowsOptions.HostProcess != nil && *security.WindowsOptions.HostProcess {
			return fmt.Errorf("Windows hostProcess is denied")
		}
		if security.Capabilities != nil {
			for _, capability := range security.Capabilities.Add {
				if !containsFold(policy.AllowedCapabilities, string(capability)) {
					return fmt.Errorf("capability %q is not allowed", capability)
				}
			}
		}
	}
	for _, port := range container.Ports {
		if port.HostPort != 0 && !policy.AllowHostPort {
			return fmt.Errorf("hostPort %d is denied", port.HostPort)
		}
	}
	return nil
}

func validateContainerResources(resources corev1.ResourceRequirements) error {
	cpuRequest := resources.Requests[corev1.ResourceCPU]
	memoryRequest := resources.Requests[corev1.ResourceMemory]
	cpuLimit := resources.Limits[corev1.ResourceCPU]
	memoryLimit := resources.Limits[corev1.ResourceMemory]
	if cpuRequest.Sign() <= 0 || memoryRequest.Sign() <= 0 || cpuLimit.Sign() <= 0 || memoryLimit.Sign() <= 0 {
		return fmt.Errorf("positive cpu and memory requests and limits are required")
	}
	for name, request := range resources.Requests {
		limit, ok := resources.Limits[name]
		if !ok || limit.Sign() <= 0 {
			return fmt.Errorf("resource %s request has no positive limit", name)
		}
		if request.Sign() < 0 || request.Cmp(limit) > 0 {
			return fmt.Errorf("resource %s request exceeds limit", name)
		}
	}
	return nil
}

func allowedServiceAccount(values []string, namespace, name string) bool {
	return contains(values, name) || contains(values, namespace+"/"+name)
}

func imageAllowed(image string, allow []string) bool {
	image = strings.TrimSpace(image)
	if image == "" {
		return false
	}
	normalized := image
	first := image
	if i := strings.IndexByte(image, '/'); i >= 0 {
		first = image[:i]
	}
	if !strings.Contains(first, ".") && !strings.Contains(first, ":") && first != "localhost" {
		normalized = "docker.io/" + image
	}
	for _, prefix := range allow {
		prefix = strings.TrimSuffix(strings.TrimSpace(prefix), "/")
		if normalized == prefix || strings.HasPrefix(normalized, prefix+"/") {
			return true
		}
	}
	return false
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func containsFold(values []string, target string) bool {
	for _, value := range values {
		if strings.EqualFold(value, target) {
			return true
		}
	}
	return false
}

// validateAttemptPod protects controller-generated identity on ordinary Pod
// UPDATE without intercepting unrelated/default-scheduler Pods.
func (v *MissionValidator) validateAttemptPod(request *admissionv1.AdmissionRequest) error {
	current := &corev1.Pod{}
	if err := decodeRaw(request.Object.Raw, current); err != nil {
		return fmt.Errorf("decode Pod: %w", err)
	}
	creator := contains(v.policy.AttemptPodCreators, request.UserInfo.Username)
	controlled := current.Labels[spacev1.LabelMissionUID] != "" || current.Labels[spacev1.LabelPlacementID] != ""
	if request.Operation == admissionv1.Create && creator && !controlled {
		return fmt.Errorf("approved workload dispatcher may create only controlled attempt Pods")
	}
	if !controlled {
		return nil
	}
	if current.Labels[spacev1.LabelMissionUID] == "" || current.Labels[spacev1.LabelPlacementID] == "" {
		return fmt.Errorf("controlled attempt Pod requires both mission and placement identity labels")
	}
	if request.Operation == admissionv1.Create {
		if !creator {
			return fmt.Errorf("only an administrator-approved workload dispatcher may create controlled attempt Pods")
		}
		if current.Spec.SchedulerName != "space-compute-scheduler" {
			return fmt.Errorf("controlled attempt Pod must use space-compute-scheduler")
		}
		if current.Spec.AutomountServiceAccountToken == nil || *current.Spec.AutomountServiceAccountToken {
			return fmt.Errorf("controlled attempt Pod must set automountServiceAccountToken=false")
		}
		if current.Annotations[spacev1.AnnotationMissionDigest] == "" || current.Annotations[spacev1.AnnotationPlacementDigest] == "" {
			return fmt.Errorf("controlled attempt Pod requires immutable mission and placement digests")
		}
		if err := validateControlledPodRestricted(current); err != nil {
			return err
		}
		return nil
	}
	old := &corev1.Pod{}
	if err := decodeRaw(request.OldObject.Raw, old); err != nil {
		return fmt.Errorf("decode previous Pod: %w", err)
	}
	if !apiequality.Semantic.DeepEqual(old.Spec, current.Spec) ||
		!apiequality.Semantic.DeepEqual(old.Labels, current.Labels) ||
		!apiequality.Semantic.DeepEqual(old.Annotations, current.Annotations) ||
		!apiequality.Semantic.DeepEqual(old.OwnerReferences, current.OwnerReferences) ||
		!apiequality.Semantic.DeepEqual(old.Finalizers, current.Finalizers) {
		return fmt.Errorf("controlled attempt Pod spec, metadata, digests, scheduler and ownerReferences are immutable")
	}
	return nil
}

func reservedMetadata(values map[string]string) map[string]string {
	out := map[string]string{}
	for key, value := range values {
		if isReservedMetadataKey(key) {
			out[key] = value
		}
	}
	return out
}

func validateControlledPodRestricted(pod *corev1.Pod) error {
	if len(pod.OwnerReferences) != 1 {
		return fmt.Errorf("controlled attempt Pod requires exactly one SpaceMission controller ownerReference")
	}
	owner := pod.OwnerReferences[0]
	if owner.APIVersion != spacev1.SchemeGroupVersion.String() || owner.Kind != "SpaceMission" || owner.Name == "" || owner.UID == "" || owner.Controller == nil || !*owner.Controller {
		return fmt.Errorf("controlled attempt Pod ownerReference must identify its SpaceMission controller")
	}
	if string(owner.UID) != pod.Labels[spacev1.LabelMissionUID] {
		return fmt.Errorf("controlled attempt Pod mission UID label does not match ownerReference")
	}
	if !strings.HasPrefix(pod.Name, owner.Name+"-attempt-") {
		return fmt.Errorf("controlled attempt Pod name is not derived from its SpaceMission")
	}
	if len(pod.Finalizers) != 0 || pod.GenerateName != "" {
		return fmt.Errorf("controlled attempt Pod may not carry user finalizers or generateName")
	}
	if pod.Spec.HostNetwork || pod.Spec.HostPID || pod.Spec.HostIPC || pod.Spec.NodeName != "" || len(pod.Spec.NodeSelector) != 0 || (pod.Spec.Affinity != nil && pod.Spec.Affinity.NodeAffinity != nil) || len(pod.Spec.Tolerations) != 0 {
		return fmt.Errorf("controlled attempt Pod violates Restricted host/node placement isolation")
	}
	if pod.Spec.SecurityContext == nil || pod.Spec.SecurityContext.RunAsNonRoot == nil || !*pod.Spec.SecurityContext.RunAsNonRoot || pod.Spec.SecurityContext.SeccompProfile == nil || pod.Spec.SecurityContext.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		return fmt.Errorf("controlled attempt Pod must enforce pod-level runAsNonRoot and RuntimeDefault seccomp")
	}
	if pod.Spec.SecurityContext.WindowsOptions != nil && pod.Spec.SecurityContext.WindowsOptions.HostProcess != nil && *pod.Spec.SecurityContext.WindowsOptions.HostProcess {
		return fmt.Errorf("controlled attempt Pod may not use Windows hostProcess")
	}
	for _, volume := range pod.Spec.Volumes {
		if !restrictedVolume(volume) {
			return fmt.Errorf("controlled attempt Pod volume %q is outside Pod Security Restricted", volume.Name)
		}
	}
	containers := append([]corev1.Container(nil), pod.Spec.InitContainers...)
	containers = append(containers, pod.Spec.Containers...)
	for _, container := range containers {
		for _, port := range container.Ports {
			if port.HostPort != 0 || port.HostIP != "" {
				return fmt.Errorf("controlled attempt Pod container %q may not use hostPort/hostIP", container.Name)
			}
		}
		security := container.SecurityContext
		if security == nil || security.AllowPrivilegeEscalation == nil || *security.AllowPrivilegeEscalation || security.Privileged == nil || *security.Privileged {
			return fmt.Errorf("controlled attempt Pod container %q does not enforce privilege restrictions", container.Name)
		}
		if security.Capabilities == nil || len(security.Capabilities.Add) != 0 || !containsCapability(security.Capabilities.Drop, corev1.Capability("ALL")) {
			return fmt.Errorf("controlled attempt Pod container %q must drop ALL capabilities", container.Name)
		}
		if security.RunAsNonRoot == nil || !*security.RunAsNonRoot {
			return fmt.Errorf("controlled attempt Pod container %q must run as non-root", container.Name)
		}
	}
	return nil
}

func containsCapability(values []corev1.Capability, target corev1.Capability) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
