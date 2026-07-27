// Package v1beta1 is the canonical storage API for space-compute. Phase 9 keeps
// v1alpha1 fully representational so conversion is lossless and rollback can be
// performed while both versions remain served.
package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
)

const GroupName = spacev1.GroupName

var (
	SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1beta1"}
	SchemeBuilder      = runtime.NewSchemeBuilder(addKnownTypes)
	AddToScheme        = SchemeBuilder.AddToScheme
)

// Shared leaf and root aliases deliberately keep the alpha compatibility view
// lossless. v1beta1 is canonical because CRDs store it, not because conversion
// discards fields from the served v1alpha1 representation.
type (
	OrbitClass                       = spacev1.OrbitClass
	StatePolicy                      = spacev1.StatePolicy
	DomainReference                  = spacev1.DomainReference
	Provenance                       = spacev1.Provenance
	SecretReference                  = spacev1.SecretReference
	SpaceDomainReporterBindingSpec   = spacev1.SpaceDomainReporterBindingSpec
	SpaceDomainReporterBinding       = spacev1.SpaceDomainReporterBinding
	SpaceDomainReporterBindingList   = spacev1.SpaceDomainReporterBindingList
	ContactWindow                    = spacev1.ContactWindow
	LinkHistoryEntry                 = spacev1.LinkHistoryEntry
	SpaceLinkSnapshotSpec            = spacev1.SpaceLinkSnapshotSpec
	SpaceLinkSnapshotStatus          = spacev1.SpaceLinkSnapshotStatus
	SpaceLinkSnapshot                = spacev1.SpaceLinkSnapshot
	SpaceLinkSnapshotList            = spacev1.SpaceLinkSnapshotList
	CapabilityRequirement            = spacev1.CapabilityRequirement
	CapabilitySet                    = spacev1.CapabilitySet
	DataLocation                     = spacev1.DataLocation
	DataObject                       = spacev1.DataObject
	RetryPolicy                      = spacev1.RetryPolicy
	CheckpointPolicy                 = spacev1.CheckpointPolicy
	SpaceMissionSpec                 = spacev1.SpaceMissionSpec
	MissionPhase                     = spacev1.MissionPhase
	SpaceMissionStatus               = spacev1.SpaceMissionStatus
	SpaceMission                     = spacev1.SpaceMission
	SpaceMissionList                 = spacev1.SpaceMissionList
	DeviceCapacity                   = spacev1.DeviceCapacity
	ScalarCapacity                   = spacev1.ScalarCapacity
	StorageCapacity                  = spacev1.StorageCapacity
	NUMAResource                     = spacev1.NUMAResource
	TrustAttestationState            = spacev1.TrustAttestationState
	EnergyBudget                     = spacev1.EnergyBudget
	PhysicalDeviceInventoryReference = spacev1.PhysicalDeviceInventoryReference
	DeviceTopology                   = spacev1.DeviceTopology
	DevicePeerInterconnect           = spacev1.DevicePeerInterconnect
	PhysicalDevice                   = spacev1.PhysicalDevice
	PhysicalDeviceInventorySpec      = spacev1.PhysicalDeviceInventorySpec
	PhysicalDeviceInventoryStatus    = spacev1.PhysicalDeviceInventoryStatus
	PhysicalDeviceInventory          = spacev1.PhysicalDeviceInventory
	PhysicalDeviceInventoryList      = spacev1.PhysicalDeviceInventoryList
	SpaceDomainResourceSummarySpec   = spacev1.SpaceDomainResourceSummarySpec
	SpaceDomainResourceSummaryStatus = spacev1.SpaceDomainResourceSummaryStatus
	SpaceDomainResourceSummary       = spacev1.SpaceDomainResourceSummary
	SpaceDomainResourceSummaryList   = spacev1.SpaceDomainResourceSummaryList
	SpaceTransferReceiptSpec         = spacev1.SpaceTransferReceiptSpec
	SpaceTransferReceipt             = spacev1.SpaceTransferReceipt
	SpaceTransferReceiptList         = spacev1.SpaceTransferReceiptList
	SpaceResultReceiptSpec           = spacev1.SpaceResultReceiptSpec
	SpaceResultReceipt               = spacev1.SpaceResultReceipt
	SpaceResultReceiptList           = spacev1.SpaceResultReceiptList
	TransferEpoch                    = spacev1.TransferEpoch
	DecisionScore                    = spacev1.DecisionScore
	ConstraintExplanation            = spacev1.ConstraintExplanation
	PhysicalDeviceConstraint         = spacev1.PhysicalDeviceConstraint
	TransferState                    = spacev1.TransferState
	SpacePlacementIntentSpec         = spacev1.SpacePlacementIntentSpec
	PlacementPhase                   = spacev1.PlacementPhase
	ExecutionObservation             = spacev1.ExecutionObservation
	SpacePlacementIntentStatus       = spacev1.SpacePlacementIntentStatus
	SpacePlacementIntent             = spacev1.SpacePlacementIntent
	SpacePlacementIntentList         = spacev1.SpacePlacementIntentList
	SpaceTransferIntentSpec          = spacev1.SpaceTransferIntentSpec
	SpaceTransferIntentStatus        = spacev1.SpaceTransferIntentStatus
	SpaceTransferIntent              = spacev1.SpaceTransferIntent
	SpaceTransferIntentList          = spacev1.SpaceTransferIntentList
	ExecutionFence                   = spacev1.ExecutionFence
	SpaceExecutionLeaseSpec          = spacev1.SpaceExecutionLeaseSpec
	SpaceExecutionLease              = spacev1.SpaceExecutionLease
	SpaceExecutionLeaseList          = spacev1.SpaceExecutionLeaseList
	ExecutionObservationPhase        = spacev1.ExecutionObservationPhase
	SpaceExecutionObservationSpec    = spacev1.SpaceExecutionObservationSpec
	SpaceExecutionObservation        = spacev1.SpaceExecutionObservation
	SpaceExecutionObservationList    = spacev1.SpaceExecutionObservationList
)

const (
	OrbitGround      = spacev1.OrbitGround
	OrbitLEO         = spacev1.OrbitLEO
	OrbitMEO         = spacev1.OrbitMEO
	OrbitGEO         = spacev1.OrbitGEO
	OrbitHEO         = spacev1.OrbitHEO
	PolicyStrict     = spacev1.PolicyStrict
	PolicyDegraded   = spacev1.PolicyDegraded
	PolicyBestEffort = spacev1.PolicyBestEffort

	MissionAccepted   = spacev1.MissionAccepted
	MissionPlanning   = spacev1.MissionPlanning
	MissionPlanned    = spacev1.MissionPlanned
	MissionExecuting  = spacev1.MissionExecuting
	MissionReturning  = spacev1.MissionReturning
	MissionSucceeded  = spacev1.MissionSucceeded
	MissionBlocked    = spacev1.MissionBlocked
	MissionReplanning = spacev1.MissionReplanning
	MissionFailed     = spacev1.MissionFailed

	PlacementPending               = spacev1.PlacementPending
	PlacementTransferPending       = spacev1.PlacementTransferPending
	PlacementExecutionLeasePending = spacev1.PlacementExecutionLeasePending
	PlacementReady                 = spacev1.PlacementReady
	PlacementDispatched            = spacev1.PlacementDispatched
	PlacementRunning               = spacev1.PlacementRunning
	PlacementCheckpointed          = spacev1.PlacementCheckpointed
	PlacementReplanning            = spacev1.PlacementReplanning
	PlacementReturnPending         = spacev1.PlacementReturnPending
	PlacementCompleted             = spacev1.PlacementCompleted
	PlacementExpired               = spacev1.PlacementExpired
	PlacementFailed                = spacev1.PlacementFailed

	TransferStateNotRequired = spacev1.TransferStateNotRequired
	TransferStatePending     = spacev1.TransferStatePending
	TransferStateInProgress  = spacev1.TransferStateInProgress
	TransferStateCompleted   = spacev1.TransferStateCompleted
	TransferStateFailed      = spacev1.TransferStateFailed

	TransferIntentPending   = spacev1.TransferIntentPending
	TransferIntentSending   = spacev1.TransferIntentSending
	TransferIntentCompleted = spacev1.TransferIntentCompleted
	TransferIntentFailed    = spacev1.TransferIntentFailed
	TransferPurposeInput    = spacev1.TransferPurposeInput
	TransferPurposeResult   = spacev1.TransferPurposeResult

	ExecutionObservationHeartbeat    = spacev1.ExecutionObservationHeartbeat
	ExecutionObservationStopped      = spacev1.ExecutionObservationStopped
	ExecutionObservationCheckpointed = spacev1.ExecutionObservationCheckpointed
	ExecutionObservationCompleted    = spacev1.ExecutionObservationCompleted
	ExecutionObservationFailed       = spacev1.ExecutionObservationFailed
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
