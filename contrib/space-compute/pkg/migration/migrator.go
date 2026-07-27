package migration

import (
	"context"
	"fmt"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/util/retry"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
)

type Resource struct {
	Plural     string
	Namespaced bool
}

var MigratedResources = []Resource{
	{Plural: "spacelinksnapshots"},
	{Plural: "spacedomainresourcesummaries"},
	{Plural: "spacedomainreporterbindings"},
	{Plural: "spacetransferintents"},
	{Plural: "spacetransferreceipts"},
	{Plural: "spaceexecutionleases"},
	{Plural: "spaceexecutionobservations"},
	{Plural: "spaceresultreceipts"},
	{Plural: "spacemissions", Namespaced: true},
	{Plural: "spaceplacementintents", Namespaced: true},
	{Plural: "physicaldeviceinventories"},
}

type Migrator struct {
	Dynamic       dynamic.Interface
	APIExtensions apiextclient.Interface
	Resources     []Resource
}

func (m *Migrator) Migrate(ctx context.Context, targetVersion string) error {
	if m == nil || m.Dynamic == nil || m.APIExtensions == nil {
		return fmt.Errorf("dynamic and API extensions clients are required")
	}
	if targetVersion != "v1alpha1" && targetVersion != "v1beta1" {
		return fmt.Errorf("target version must be v1alpha1 or v1beta1")
	}
	resources := m.Resources
	if len(resources) == 0 {
		resources = MigratedResources
	}
	for _, resource := range resources {
		if err := m.migrateResource(ctx, resource, targetVersion); err != nil {
			return fmt.Errorf("migrate %s: %w", resource.Plural, err)
		}
	}
	return nil
}

func (m *Migrator) migrateResource(ctx context.Context, resource Resource, targetVersion string) error {
	crdName := resource.Plural + "." + spacev1.GroupName
	crds := m.APIExtensions.ApiextensionsV1().CustomResourceDefinitions()
	crd, err := crds.Get(ctx, crdName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if err := validateServedVersions(crd, targetVersion); err != nil {
		return err
	}
	if !storageVersionIs(crd, targetVersion) {
		updated := crd.DeepCopy()
		for i := range updated.Spec.Versions {
			updated.Spec.Versions[i].Storage = updated.Spec.Versions[i].Name == targetVersion
		}
		if _, err := crds.Update(ctx, updated, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("switch CRD storage version: %w", err)
		}
	}

	gvr := schema.GroupVersionResource{Group: spacev1.GroupName, Version: targetVersion, Resource: resource.Plural}
	listClient := m.Dynamic.Resource(gvr)
	var list *unstructured.UnstructuredList
	if resource.Namespaced {
		list, err = listClient.Namespace(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
	} else {
		list, err = listClient.List(ctx, metav1.ListOptions{})
	}
	if err != nil {
		return fmt.Errorf("list target-version objects: %w", err)
	}
	for i := range list.Items {
		item := list.Items[i]
		name, namespace := item.GetName(), item.GetNamespace()
		if resource.Namespaced {
			err = retry.RetryOnConflict(retry.DefaultBackoff, func() error {
				current, getErr := m.Dynamic.Resource(gvr).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
				if getErr != nil {
					return getErr
				}
				current.SetAPIVersion(spacev1.GroupName + "/" + targetVersion)
				_, updateErr := m.Dynamic.Resource(gvr).Namespace(namespace).Update(ctx, current, metav1.UpdateOptions{})
				return updateErr
			})
		} else {
			err = retry.RetryOnConflict(retry.DefaultBackoff, func() error {
				current, getErr := m.Dynamic.Resource(gvr).Get(ctx, name, metav1.GetOptions{})
				if getErr != nil {
					return getErr
				}
				current.SetAPIVersion(spacev1.GroupName + "/" + targetVersion)
				_, updateErr := m.Dynamic.Resource(gvr).Update(ctx, current, metav1.UpdateOptions{})
				return updateErr
			})
		}
		if err != nil {
			return fmt.Errorf("rewrite %s/%s: %w", namespace, name, err)
		}
	}

	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		latest, getErr := crds.Get(ctx, crdName, metav1.GetOptions{})
		if getErr != nil {
			return getErr
		}
		latest.Status.StoredVersions = []string{targetVersion}
		_, updateErr := crds.UpdateStatus(ctx, latest, metav1.UpdateOptions{})
		return updateErr
	})
}

func validateServedVersions(crd *apiextensionsv1.CustomResourceDefinition, target string) error {
	served := map[string]bool{}
	for _, version := range crd.Spec.Versions {
		served[version.Name] = version.Served
	}
	if !served[target] {
		return fmt.Errorf("target version %s is not served", target)
	}
	// Existing Phase 4/5 kinds keep both versions served through migration and
	// rollback. PhysicalDeviceInventory is also dual-served in Phase 9 so the
	// same invariant applies to every managed CRD.
	if !served["v1alpha1"] || !served["v1beta1"] {
		return fmt.Errorf("both v1alpha1 and v1beta1 must remain served during migration")
	}
	return nil
}

func storageVersionIs(crd *apiextensionsv1.CustomResourceDefinition, version string) bool {
	for _, candidate := range crd.Spec.Versions {
		if candidate.Name == version {
			return candidate.Storage
		}
	}
	return false
}
