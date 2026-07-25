package admission

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
)

type recordingReviewer struct {
	checks []authorizationv1.SubjectAccessReviewSpec
	deny   string
}

func (r *recordingReviewer) Review(_ context.Context, spec authorizationv1.SubjectAccessReviewSpec) (bool, string, error) {
	r.checks = append(r.checks, spec)
	if spec.ResourceAttributes != nil && r.deny != "" && spec.ResourceAttributes.Resource == r.deny {
		return false, "denied by test RBAC", nil
	}
	return true, "allowed by test RBAC", nil
}

func testMissionPolicy() *MissionSecurityPolicy {
	return &MissionSecurityPolicy{
		APIVersion:             MissionSecurityPolicyVersion,
		Kind:                   "MissionSecurityPolicy",
		AllowedServiceAccounts: []string{"missions/mission-runner"},
		AllowedRuntimeClasses:  []string{"runc"},
		AllowedImageRegistries: []string{"registry.example.com/science"},
		AllowedLabelPrefixes:   []string{"app.kubernetes.io/"},
		AttemptPodCreators: []string{
			"system:serviceaccount:kube-system:space-compute-workload-dispatcher",
			"system:serviceaccount:kube-system:space-compute-domain-agent",
		},
	}
}

func safeMission() *spacev1.SpaceMission {
	cpuRequest := resourceapi.MustParse("100m")
	cpuLimit := resourceapi.MustParse("1")
	memoryRequest := resourceapi.MustParse("64Mi")
	memoryLimit := resourceapi.MustParse("256Mi")
	return &spacev1.SpaceMission{
		TypeMeta:   metav1.TypeMeta{APIVersion: spacev1.SchemeGroupVersion.String(), Kind: "SpaceMission"},
		ObjectMeta: metav1.ObjectMeta{Name: "science", Namespace: "missions", UID: types.UID("mission-uid")},
		Spec: spacev1.SpaceMissionSpec{WorkloadTemplate: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app.kubernetes.io/name": "science"}},
			Spec: corev1.PodSpec{
				ServiceAccountName: "mission-runner",
				RuntimeClassName:   missionPtr("runc"),
				Volumes:            []corev1.Volume{{Name: "config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "mission-config"}}}}, {Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "mission-data"}}}},
				Containers:         []corev1.Container{{Name: "worker", Image: "registry.example.com/science/worker:v1", Env: []corev1.EnvVar{{Name: "TOKEN", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "mission-secret"}, Key: "token"}}}}, Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: cpuRequest, corev1.ResourceMemory: memoryRequest}, Limits: corev1.ResourceList{corev1.ResourceCPU: cpuLimit, corev1.ResourceMemory: memoryLimit}}}},
			},
		}},
	}
}

func missionRequest(t *testing.T, op admissionv1.Operation, mission, old *spacev1.SpaceMission) *admissionv1.AdmissionRequest {
	t.Helper()
	raw, err := json.Marshal(mission)
	if err != nil {
		t.Fatal(err)
	}
	request := &admissionv1.AdmissionRequest{
		UID:       types.UID("review"),
		Kind:      metav1.GroupVersionKind{Group: spacev1.GroupName, Version: "v1alpha1", Kind: "SpaceMission"},
		Resource:  metav1.GroupVersionResource{Group: spacev1.GroupName, Version: "v1alpha1", Resource: "spacemissions"},
		Namespace: mission.Namespace,
		Operation: op,
		UserInfo:  authenticationv1.UserInfo{Username: "alice", Groups: []string{"science-users"}},
		Object:    runtime.RawExtension{Raw: raw},
	}
	if old != nil {
		oldRaw, err := json.Marshal(old)
		if err != nil {
			t.Fatal(err)
		}
		request.OldObject = runtime.RawExtension{Raw: oldRaw}
	}
	return request
}

func TestLoadMissionSecurityPolicyIsStrict(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "policy.json")
	if err := os.WriteFile(path, []byte(`{"apiVersion":"spacecompute.k3s.io/v1alpha1","kind":"MissionSecurityPolicy","allowedServiceAccounts":["default"],"allowedImageRegistries":["registry.k8s.io"],"attemptPodCreators":[],"unknown":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadMissionSecurityPolicy(path); err == nil {
		t.Fatal("unknown policy field was accepted")
	}
}

func TestMissionPolicyExplicitlyAllowsConfiguredHostNetwork(t *testing.T) {
	policy := testMissionPolicy()
	policy.AllowHostNetwork = true
	validator, _ := NewMissionValidator(policy, &recordingReviewer{})
	mission := safeMission()
	mission.Spec.WorkloadTemplate.Spec.HostNetwork = true
	if err := validator.Validate(context.Background(), missionRequest(t, admissionv1.Create, mission, nil)); err != nil {
		t.Fatalf("explicit administrator exception was not honored by admission: %v", err)
	}
}

func TestMissionAdmissionRunsSubjectAccessReviewsForPodAndReferences(t *testing.T) {
	reviewer := &recordingReviewer{}
	validator, err := NewMissionValidator(testMissionPolicy(), reviewer)
	if err != nil {
		t.Fatal(err)
	}
	mission := safeMission()
	if err := validator.Validate(context.Background(), missionRequest(t, admissionv1.Create, mission, nil)); err != nil {
		t.Fatalf("safe mission rejected: %v", err)
	}
	want := map[string]bool{
		"create:pods:":                            false,
		"use:serviceaccounts:mission-runner":      false,
		"get:configmaps:mission-config":           false,
		"get:persistentvolumeclaims:mission-data": false,
		"get:secrets:mission-secret":              false,
		"use:runtimeclasses:runc":                 false,
	}
	for _, check := range reviewer.checks {
		attr := check.ResourceAttributes
		if attr == nil {
			continue
		}
		key := attr.Verb + ":" + attr.Resource + ":" + attr.Name
		if _, ok := want[key]; ok {
			want[key] = true
		}
		if check.User != "alice" || len(check.Groups) != 1 || check.Groups[0] != "science-users" {
			t.Fatalf("identity not preserved in SAR: %#v", check)
		}
	}
	for key, seen := range want {
		if !seen {
			t.Errorf("missing SAR %s; got %#v", key, reviewer.checks)
		}
	}
}

func TestMissionAdmissionFailsClosedOnReferencedSecretAuthorization(t *testing.T) {
	reviewer := &recordingReviewer{deny: "secrets"}
	validator, _ := NewMissionValidator(testMissionPolicy(), reviewer)
	err := validator.Validate(context.Background(), missionRequest(t, admissionv1.Create, safeMission(), nil))
	if err == nil || !strings.Contains(err.Error(), "secrets") {
		t.Fatalf("secret denial not enforced: %v", err)
	}
}

func TestMissionAdmissionRejectsPrivilegeAndPlacementBypassFields(t *testing.T) {
	cases := map[string]func(*corev1.PodTemplateSpec){
		"hostNetwork": func(tpl *corev1.PodTemplateSpec) { tpl.Spec.HostNetwork = true },
		"hostPID":     func(tpl *corev1.PodTemplateSpec) { tpl.Spec.HostPID = true },
		"hostIPC":     func(tpl *corev1.PodTemplateSpec) { tpl.Spec.HostIPC = true },
		"privileged": func(tpl *corev1.PodTemplateSpec) {
			b := true
			tpl.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{Privileged: &b}
		},
		"allowPrivilegeEscalation": func(tpl *corev1.PodTemplateSpec) {
			b := true
			tpl.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{AllowPrivilegeEscalation: &b}
		},
		"capability": func(tpl *corev1.PodTemplateSpec) {
			tpl.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{Capabilities: &corev1.Capabilities{Add: []corev1.Capability{"SYS_ADMIN"}}}
		},
		"hostPath": func(tpl *corev1.PodTemplateSpec) {
			tpl.Spec.Volumes = append(tpl.Spec.Volumes, corev1.Volume{Name: "host", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/"}}})
		},
		"hostPort": func(tpl *corev1.PodTemplateSpec) {
			tpl.Spec.Containers[0].Ports = []corev1.ContainerPort{{ContainerPort: 8080, HostPort: 8080}}
		},
		"nodeName": func(tpl *corev1.PodTemplateSpec) { tpl.Spec.NodeName = "node-a" },
		"nodeSelector": func(tpl *corev1.PodTemplateSpec) {
			tpl.Spec.NodeSelector = map[string]string{"kubernetes.io/hostname": "node-a"}
		},
		"nodeAffinity": func(tpl *corev1.PodTemplateSpec) {
			tpl.Spec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{}}}}}
		},
		"toleration": func(tpl *corev1.PodTemplateSpec) {
			tpl.Spec.Tolerations = []corev1.Toleration{{Key: "control-plane", Operator: corev1.TolerationOpExists}}
		},
		"automount": func(tpl *corev1.PodTemplateSpec) { b := true; tpl.Spec.AutomountServiceAccountToken = &b },
		"reservedAnnotation": func(tpl *corev1.PodTemplateSpec) {
			tpl.Annotations = map[string]string{spacev1.AnnotationPlacement: "forged"}
		},
		"ownerReference": func(tpl *corev1.PodTemplateSpec) {
			tpl.OwnerReferences = []metav1.OwnerReference{{Kind: "Pod", Name: "x"}}
		},
		"finalizer":        func(tpl *corev1.PodTemplateSpec) { tpl.Finalizers = []string{"example.com/finalizer"} },
		"runtimeClass":     func(tpl *corev1.PodTemplateSpec) { tpl.Spec.RuntimeClassName = missionPtr("kata") },
		"serviceAccount":   func(tpl *corev1.PodTemplateSpec) { tpl.Spec.ServiceAccountName = "cluster-admin" },
		"schedulerName":    func(tpl *corev1.PodTemplateSpec) { tpl.Spec.SchedulerName = "default-scheduler" },
		"priorityClass":    func(tpl *corev1.PodTemplateSpec) { tpl.Spec.PriorityClassName = "system-cluster-critical" },
		"missingResources": func(tpl *corev1.PodTemplateSpec) { tpl.Spec.Containers[0].Resources = corev1.ResourceRequirements{} },
		"registry":         func(tpl *corev1.PodTemplateSpec) { tpl.Spec.Containers[0].Image = "evil.example/worker:v1" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			mission := safeMission()
			mutate(&mission.Spec.WorkloadTemplate)
			validator, _ := NewMissionValidator(testMissionPolicy(), &recordingReviewer{})
			if err := validator.Validate(context.Background(), missionRequest(t, admissionv1.Create, mission, nil)); err == nil {
				t.Fatal("unsafe Mission was accepted")
			}
		})
	}
}

func TestMissionAdmissionUpdateReauthorizesOnlyMaterialTemplateChanges(t *testing.T) {
	old := safeMission()
	unchanged := old.DeepCopy()
	unchanged.Spec.Priority++
	reviewer := &recordingReviewer{deny: "pods"}
	validator, _ := NewMissionValidator(testMissionPolicy(), reviewer)
	if err := validator.Validate(context.Background(), missionRequest(t, admissionv1.Update, unchanged, old)); err != nil {
		t.Fatalf("non-template update should not rerun template SAR: %v", err)
	}
	if len(reviewer.checks) != 0 {
		t.Fatalf("unexpected SARs for unchanged template: %d", len(reviewer.checks))
	}
	changed := old.DeepCopy()
	changed.Spec.WorkloadTemplate.Spec.Containers[0].Image = "registry.example.com/science/worker:v2"
	if err := validator.Validate(context.Background(), missionRequest(t, admissionv1.Update, changed, old)); err == nil {
		t.Fatal("material template change bypassed SAR")
	}
}

func TestApprovedDispatcherCannotCreateUncontrolledPod(t *testing.T) {
	policy := testMissionPolicy()
	validator, _ := NewMissionValidator(policy, &recordingReviewer{})
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "uncontrolled", Namespace: "missions"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "x", Image: "registry.example.com/x:v1"}}}}
	raw, _ := json.Marshal(pod)
	request := &admissionv1.AdmissionRequest{Resource: metav1.GroupVersionResource{Version: "v1", Resource: "pods"}, Operation: admissionv1.Create, UserInfo: authenticationv1.UserInfo{Username: policy.AttemptPodCreators[0]}, Object: runtime.RawExtension{Raw: raw}}
	if err := validator.Validate(context.Background(), request); err == nil {
		t.Fatal("dispatcher created an uncontrolled Pod")
	}
}

func TestControlledAttemptPodIdentityIsImmutable(t *testing.T) {
	policy := testMissionPolicy()
	validator, _ := NewMissionValidator(policy, &recordingReviewer{})
	pod := controlledAttemptPod()
	raw, _ := json.Marshal(pod)
	create := &admissionv1.AdmissionRequest{Resource: metav1.GroupVersionResource{Version: "v1", Resource: "pods"}, Operation: admissionv1.Create, UserInfo: authenticationv1.UserInfo{Username: policy.AttemptPodCreators[0]}, Object: runtime.RawExtension{Raw: raw}}
	if err := validator.Validate(context.Background(), create); err != nil {
		t.Fatalf("controlled create rejected: %v", err)
	}
	changed := pod.DeepCopy()
	changed.Annotations[spacev1.AnnotationMissionDigest] = strings.Repeat("f", 64)
	newRaw, _ := json.Marshal(changed)
	update := &admissionv1.AdmissionRequest{Resource: metav1.GroupVersionResource{Version: "v1", Resource: "pods"}, Operation: admissionv1.Update, UserInfo: authenticationv1.UserInfo{Username: "alice"}, Object: runtime.RawExtension{Raw: newRaw}, OldObject: runtime.RawExtension{Raw: raw}}
	if err := validator.Validate(context.Background(), update); err == nil {
		t.Fatal("mission digest mutation was accepted")
	}
}

func controlledAttemptPod() *corev1.Pod {
	f := false
	tr := true
	controller := true
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "science-attempt-1", Namespace: "missions", Labels: map[string]string{spacev1.LabelMissionUID: "uid", spacev1.LabelPlacementID: "plan"}, Annotations: map[string]string{spacev1.AnnotationMissionDigest: strings.Repeat("a", 64), spacev1.AnnotationPlacementDigest: strings.Repeat("b", 64), spacev1.AnnotationMissionIntent: "{}", spacev1.AnnotationPlacement: "{}"}, OwnerReferences: []metav1.OwnerReference{{APIVersion: spacev1.SchemeGroupVersion.String(), Kind: "SpaceMission", Name: "science", UID: types.UID("uid"), Controller: &controller}}}, Spec: corev1.PodSpec{SchedulerName: "space-compute-scheduler", AutomountServiceAccountToken: &f, SecurityContext: &corev1.PodSecurityContext{RunAsNonRoot: &tr, SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}}, Containers: []corev1.Container{{Name: "worker", Image: "registry.example.com/science/worker:v1", SecurityContext: &corev1.SecurityContext{Privileged: &f, AllowPrivilegeEscalation: &f, RunAsNonRoot: &tr, Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}}}}}}
}

func missionPtr[T any](value T) *T { return &value }
