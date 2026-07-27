package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	GroupName = "spacecompute.k3s.io"
	// CanonicalVersion is the Phase 9 storage/client version. The package remains
	// named v1alpha1 because existing controllers and policy code share these
	// lossless Go structs across both served API versions.
	CanonicalVersion    = "v1beta1"
	CanonicalAPIVersion = GroupName + "/" + CanonicalVersion
)

var (
	SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1alpha1"}
	SchemeBuilder      = runtime.NewSchemeBuilder(addKnownTypes)
	AddToScheme        = SchemeBuilder.AddToScheme
)

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&SpaceLinkSnapshot{}, &SpaceLinkSnapshotList{},
		&SpaceDomainResourceSummary{}, &SpaceDomainResourceSummaryList{},
		&PhysicalDeviceInventory{}, &PhysicalDeviceInventoryList{},
		&SpaceDomainReporterBinding{}, &SpaceDomainReporterBindingList{},
		&SpaceTransferIntent{}, &SpaceTransferIntentList{},
		&SpaceTransferReceipt{}, &SpaceTransferReceiptList{},
		&SpaceExecutionLease{}, &SpaceExecutionLeaseList{},
		&SpaceExecutionObservation{}, &SpaceExecutionObservationList{},
		&SpaceResultReceipt{}, &SpaceResultReceiptList{},
		&SpaceMission{}, &SpaceMissionList{},
		&SpacePlacementIntent{}, &SpacePlacementIntentList{},
	)
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
