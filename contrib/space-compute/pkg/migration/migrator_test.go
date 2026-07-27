package migration

import (
	"context"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func TestStoredVersionMigrationForwardAndRollback(t *testing.T) {
	ctx := context.Background()
	resource := Resource{Plural: "spacemissions", Namespaced: true}
	crd := migrationCRD(resource.Plural, "v1alpha1", []string{"v1alpha1"})
	betaGVR := schema.GroupVersionResource{Group: "spacecompute.k3s.io", Version: "v1beta1", Resource: resource.Plural}
	alphaGVR := schema.GroupVersionResource{Group: "spacecompute.k3s.io", Version: "v1alpha1", Resource: resource.Plural}
	object := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "spacecompute.k3s.io/v1beta1", "kind": "SpaceMission",
		"metadata": map[string]interface{}{"name": "science", "namespace": "missions"},
		"spec":     map[string]interface{}{"workingMemoryBytes": int64(4096), "futureUnknown": "preserved"},
	}}
	listKinds := map[schema.GroupVersionResource]string{betaGVR: "SpaceMissionList", alphaGVR: "SpaceMissionList"}
	dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), listKinds, object)
	crdClient := apiextfake.NewSimpleClientset(crd)
	m := &Migrator{Dynamic: dynamicClient, APIExtensions: crdClient, Resources: []Resource{resource}}

	if err := m.Migrate(ctx, "v1beta1"); err != nil {
		t.Fatalf("forward migration: %v", err)
	}
	assertCRDStorage(t, crdClient, resource.Plural, "v1beta1")
	stored, err := dynamicClient.Resource(betaGVR).Namespace("missions").Get(ctx, "science", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if stored.GetAPIVersion() != "spacecompute.k3s.io/v1beta1" {
		t.Fatalf("forward apiVersion=%s", stored.GetAPIVersion())
	}
	if got, _, _ := unstructured.NestedString(stored.Object, "spec", "futureUnknown"); got != "preserved" {
		t.Fatalf("forward rewrite lost unknown field: %q", got)
	}
	if got, _, _ := unstructured.NestedInt64(stored.Object, "spec", "workingMemoryBytes"); got != 4096 {
		t.Fatalf("forward rewrite lost hard constraint: %d", got)
	}

	// Fake dynamic clients do not run the conversion webhook, so seed the alpha
	// view exactly as a real API server would expose the same stored object.
	alphaObject := stored.DeepCopy()
	alphaObject.SetAPIVersion("spacecompute.k3s.io/v1alpha1")
	if _, err := dynamicClient.Resource(alphaGVR).Namespace("missions").Create(ctx, alphaObject, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := m.Migrate(ctx, "v1alpha1"); err != nil {
		t.Fatalf("rollback migration: %v", err)
	}
	assertCRDStorage(t, crdClient, resource.Plural, "v1alpha1")
	rolled, err := dynamicClient.Resource(alphaGVR).Namespace("missions").Get(ctx, "science", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if rolled.GetAPIVersion() != "spacecompute.k3s.io/v1alpha1" {
		t.Fatalf("rollback apiVersion=%s", rolled.GetAPIVersion())
	}
	if got, _, _ := unstructured.NestedString(rolled.Object, "spec", "futureUnknown"); got != "preserved" {
		t.Fatalf("rollback rewrite lost unknown field: %q", got)
	}
	if got, _, _ := unstructured.NestedInt64(rolled.Object, "spec", "workingMemoryBytes"); got != 4096 {
		t.Fatalf("rollback rewrite lost hard constraint: %d", got)
	}
}

func TestMigrationRequiresBothVersionsServed(t *testing.T) {
	resource := Resource{Plural: "spacemissions", Namespaced: true}
	crd := migrationCRD(resource.Plural, "v1alpha1", []string{"v1alpha1"})
	crd.Spec.Versions = crd.Spec.Versions[:1]
	m := &Migrator{
		Dynamic:       dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
		APIExtensions: apiextfake.NewSimpleClientset(crd),
		Resources:     []Resource{resource},
	}
	if err := m.Migrate(context.Background(), "v1alpha1"); err == nil {
		t.Fatal("migration accepted CRD without both served versions")
	}
}

func migrationCRD(plural, storage string, storedVersions []string) *apiextensionsv1.CustomResourceDefinition {
	versions := []apiextensionsv1.CustomResourceDefinitionVersion{
		{Name: "v1alpha1", Served: true, Storage: storage == "v1alpha1", Schema: &apiextensionsv1.CustomResourceValidation{OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{Type: "object", XPreserveUnknownFields: boolPtr(true)}}},
		{Name: "v1beta1", Served: true, Storage: storage == "v1beta1", Schema: &apiextensionsv1.CustomResourceValidation{OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{Type: "object", XPreserveUnknownFields: boolPtr(true)}}},
	}
	return &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: plural + ".spacecompute.k3s.io"},
		Spec:       apiextensionsv1.CustomResourceDefinitionSpec{Group: "spacecompute.k3s.io", Scope: apiextensionsv1.NamespaceScoped, Names: apiextensionsv1.CustomResourceDefinitionNames{Plural: plural, Singular: "spacemission", Kind: "SpaceMission", ListKind: "SpaceMissionList"}, Versions: versions},
		Status:     apiextensionsv1.CustomResourceDefinitionStatus{StoredVersions: append([]string(nil), storedVersions...)},
	}
}

func assertCRDStorage(t *testing.T, client *apiextfake.Clientset, plural, version string) {
	t.Helper()
	crd, err := client.ApiextensionsV1().CustomResourceDefinitions().Get(context.Background(), plural+".spacecompute.k3s.io", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, v := range crd.Spec.Versions {
		if v.Name == version && v.Storage {
			found = true
		}
		if v.Name != version && v.Storage {
			t.Fatalf("unexpected storage version %s", v.Name)
		}
	}
	if !found {
		t.Fatalf("storage version %s not selected", version)
	}
	if len(crd.Status.StoredVersions) != 1 || crd.Status.StoredVersions[0] != version {
		t.Fatalf("storedVersions=%v want [%s]", crd.Status.StoredVersions, version)
	}
}

func boolPtr(v bool) *bool { return &v }
