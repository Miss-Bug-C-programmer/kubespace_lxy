// Package v1alpha1 contains the durable Phase 4 space-compute APIs. The API is
// intentionally conversion-friendly: quantities have fixed units in field
// names, enum values are explicit, and status carries observed generations.
package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
	AnnotationMissionIntent    = GroupName + "/mission-intent"
	AnnotationPlacement        = GroupName + "/placement"
	AnnotationMissionDigest    = GroupName + "/mission-digest"
	AnnotationPlacementDigest  = GroupName + "/placement-digest"
	AnnotationLinkProjection   = GroupName + "/link-projection"
	AnnotationResultReturned   = GroupName + "/result-returned"
	AnnotationCheckpointID     = GroupName + "/checkpoint-id"
	LabelDomain                = GroupName + "/domain"
	LabelOrbitClass            = GroupName + "/orbit-class"
	LabelPlacementID           = GroupName + "/placement-id"
	LabelMissionUID            = GroupName + "/mission-uid"
	FinalizerMissionProtection = GroupName + "/mission-protection"
)

type OrbitClass string

const (
	OrbitGround OrbitClass = "ground"
	OrbitLEO    OrbitClass = "leo"
	OrbitMEO    OrbitClass = "meo"
	OrbitGEO    OrbitClass = "geo"
	OrbitHEO    OrbitClass = "heo"
)

type StatePolicy string

const (
	PolicyStrict     StatePolicy = "strict"
	PolicyDegraded   StatePolicy = "degraded"
	PolicyBestEffort StatePolicy = "best-effort"
)

type DomainReference struct {
	Name       string     `json:"name"`
	ClusterID  string     `json:"clusterID"`
	OrbitClass OrbitClass `json:"orbitClass"`
}

type Provenance struct {
	ReporterID     string `json:"reporterID"`
	Source         string `json:"source"`
	Digest         string `json:"digest"`
	PreviousDigest string `json:"previousDigest,omitempty"`
	Sequence       int64  `json:"sequence"`
	Signature      string `json:"signature,omitempty"`
}

type SecretReference struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	Key       string `json:"key"`
}

type SpaceDomainReporterBindingSpec struct {
	ReporterPrincipal string            `json:"reporterPrincipal"`
	Domain            DomainReference   `json:"domain"`
	AllowedKinds      []string          `json:"allowedKinds"`
	AllowedPeers      []DomainReference `json:"allowedPeers,omitempty"`
	// AllowedGateways are explicit authenticated Kubernetes principals that may
	// persist this reporter's already-signed objects through a controlled local
	// transport gateway. The original reporter signature remains mandatory.
	AllowedGateways []string        `json:"allowedGateways,omitempty"`
	PublicKeyRef    SecretReference `json:"publicKeyRef"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=sreporter
type SpaceDomainReporterBinding struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              SpaceDomainReporterBindingSpec `json:"spec"`
}

// +kubebuilder:object:root=true
type SpaceDomainReporterBindingList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SpaceDomainReporterBinding `json:"items"`
}

// ContactWindow is half-open: Start is inclusive and End is exclusive.
type ContactWindow struct {
	ID                   string      `json:"id"`
	Start                metav1.Time `json:"start"`
	End                  metav1.Time `json:"end"`
	BandwidthBitsPerSec  int64       `json:"bandwidthBitsPerSecond"`
	RTTMicroseconds      int64       `json:"rttMicroseconds"`
	LossPartsPerMillion  int32       `json:"lossPartsPerMillion"`
	ErrorPartsPerMillion int32       `json:"errorPartsPerMillion"`
	StabilityMilli       int32       `json:"stabilityMilli"`
	ConfidenceMilli      int32       `json:"confidenceMilli"`
	Predicted            bool        `json:"predicted"`
}

type LinkHistoryEntry struct {
	Sequence       int64       `json:"sequence"`
	ObservedAt     metav1.Time `json:"observedAt"`
	ValidUntil     metav1.Time `json:"validUntil"`
	WindowDigest   string      `json:"windowDigest"`
	WindowCount    int32       `json:"windowCount"`
	Accepted       bool        `json:"accepted"`
	Reason         string      `json:"reason,omitempty"`
	ProvenanceHash string      `json:"provenanceHash"`
}

type SpaceLinkSnapshotSpec struct {
	Source                  DomainReference `json:"source"`
	Destination             DomainReference `json:"destination"`
	ObservedAt              metav1.Time     `json:"observedAt"`
	ValidUntil              metav1.Time     `json:"validUntil"`
	MaximumClockSkewSeconds int64           `json:"maximumClockSkewSeconds"`
	MinimumUpdateSeconds    int64           `json:"minimumUpdateSeconds"`
	HistoryLimit            int32           `json:"historyLimit"`
	Provenance              Provenance      `json:"provenance"`
	Windows                 []ContactWindow `json:"windows"`
}

type SpaceLinkSnapshotStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	AcceptedSequence   int64              `json:"acceptedSequence,omitempty"`
	History            []LinkHistoryEntry `json:"history,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=slink
// +kubebuilder:subresource:status
type SpaceLinkSnapshot struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              SpaceLinkSnapshotSpec   `json:"spec"`
	Status            SpaceLinkSnapshotStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type SpaceLinkSnapshotList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SpaceLinkSnapshot `json:"items"`
}

type CapabilityRequirement struct {
	Class        string            `json:"class"`
	Quantity     int64             `json:"quantity"`
	Architecture string            `json:"architecture,omitempty"`
	Model        string            `json:"model,omitempty"`
	Precision    []string          `json:"precision,omitempty"`
	Software     map[string]string `json:"software,omitempty"`
}

// CapabilitySet is an alternative; every AllOf entry in one selected set is
// required. RequiredCapabilities always applies.
type CapabilitySet struct {
	Name  string                  `json:"name"`
	AllOf []CapabilityRequirement `json:"allOf"`
}

type DataLocation struct {
	Domain DomainReference `json:"domain"`
	URI    string          `json:"uri,omitempty"`
}

type DataObject struct {
	ID        string         `json:"id"`
	SizeBytes int64          `json:"sizeBytes"`
	Locations []DataLocation `json:"locations"`
	// PayloadDigest is required before a non-local input can be transferred.
	// Local-only inputs remain valid without it.
	PayloadDigest string `json:"payloadDigest,omitempty"`
}

type RetryPolicy struct {
	MaxAttempts             int32 `json:"maxAttempts"`
	AllowMigration          bool  `json:"allowMigration"`
	MaxConcurrentExecutions int32 `json:"maxConcurrentExecutions"`
}

type CheckpointPolicy struct {
	Checkpointable      bool  `json:"checkpointable"`
	MinimumIntervalSecs int64 `json:"minimumIntervalSeconds,omitempty"`
	MaximumStateBytes   int64 `json:"maximumStateBytes,omitempty"`
}

type SpaceMissionSpec struct {
	MissionClass                  string                  `json:"missionClass"`
	Priority                      int32                   `json:"priority"`
	StatePolicy                   StatePolicy             `json:"statePolicy"`
	WorkingMemoryBytes            int64                   `json:"workingMemoryBytes,omitempty"`
	WorkingStorageBytes           int64                   `json:"workingStorageBytes,omitempty"`
	MinimumBandwidthBitsPerSecond int64                   `json:"minimumBandwidthBitsPerSecond,omitempty"`
	MaximumRTTMicroseconds        int64                   `json:"maximumRTTMicroseconds,omitempty"`
	MaximumLossPartsPerMillion    int32                   `json:"maximumLossPartsPerMillion,omitempty"`
	RequiredCapabilities          []CapabilityRequirement `json:"requiredCapabilities,omitempty"`
	AlternativeCapabilities       []CapabilitySet         `json:"alternativeCapabilities,omitempty"`
	RequiredSoftware              map[string]string       `json:"requiredSoftware,omitempty"`
	Inputs                        []DataObject            `json:"inputs,omitempty"`
	OutputSizeBytes               int64                   `json:"outputSizeBytes"`
	ResultDestinations            []DataLocation          `json:"resultDestinations,omitempty"`
	Deadline                      metav1.Time             `json:"deadline"`
	ExpectedDurationSeconds       int64                   `json:"expectedDurationSeconds"`
	MaximumDurationSeconds        int64                   `json:"maximumDurationSeconds"`
	DurationUncertaintySecs       int64                   `json:"durationUncertaintySeconds"`
	SafetyMarginSeconds           int64                   `json:"safetyMarginSeconds"`
	MaximumClockSkewSeconds       int64                   `json:"maximumClockSkewSeconds"`
	ResultReturnRequired          bool                    `json:"resultReturnRequired"`
	Retry                         RetryPolicy             `json:"retry"`
	Checkpoint                    CheckpointPolicy        `json:"checkpoint"`
	WorkloadTemplate              corev1.PodTemplateSpec  `json:"workloadTemplate"`
	Suspend                       bool                    `json:"suspend,omitempty"`
}

type MissionPhase string

const (
	MissionAccepted   MissionPhase = "Accepted"
	MissionPlanning   MissionPhase = "Planning"
	MissionPlanned    MissionPhase = "Planned"
	MissionExecuting  MissionPhase = "Executing"
	MissionReturning  MissionPhase = "Returning"
	MissionSucceeded  MissionPhase = "Succeeded"
	MissionBlocked    MissionPhase = "Blocked"
	MissionReplanning MissionPhase = "Replanning"
	MissionFailed     MissionPhase = "Failed"
)

type SpaceMissionStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Phase              MissionPhase       `json:"phase,omitempty"`
	PlacementName      string             `json:"placementName,omitempty"`
	PlanID             string             `json:"planID,omitempty"`
	LastDecisionDigest string             `json:"lastDecisionDigest,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,shortName=smission
// +kubebuilder:subresource:status
type SpaceMission struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              SpaceMissionSpec   `json:"spec"`
	Status            SpaceMissionStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type SpaceMissionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SpaceMission `json:"items"`
}

type DeviceCapacity struct {
	Class              string   `json:"class"`
	Count              int64    `json:"count"`
	Architectures      []string `json:"architectures,omitempty"`
	Models             []string `json:"models,omitempty"`
	Precision          []string `json:"precision,omitempty"`
	ComputeMilli       int64    `json:"computeMilli"`
	FragmentationMilli int32    `json:"fragmentationMilli"`
}

// ScalarCapacity exposes canonical total/available capacity in fixed units.
type ScalarCapacity struct {
	Capacity  int64 `json:"capacity"`
	Available int64 `json:"available"`
}

type StorageCapacity struct {
	Class          string `json:"class"`
	CapacityBytes  int64  `json:"capacityBytes"`
	AvailableBytes int64  `json:"availableBytes"`
}

type NUMAResource struct {
	ID                   int32 `json:"id"`
	CPUMilliCapacity     int64 `json:"cpuMilliCapacity"`
	CPUMilliAvailable    int64 `json:"cpuMilliAvailable"`
	MemoryCapacityBytes  int64 `json:"memoryCapacityBytes"`
	MemoryAvailableBytes int64 `json:"memoryAvailableBytes"`
}

type TrustAttestationState struct {
	State          string      `json:"state"`
	Provider       string      `json:"provider,omitempty"`
	EvidenceDigest string      `json:"evidenceDigest,omitempty"`
	ObservedAt     metav1.Time `json:"observedAt,omitempty"`
	ValidUntil     metav1.Time `json:"validUntil,omitempty"`
}

type EnergyBudget struct {
	Source                  string `json:"source"`
	CapacityMilliWattHours  int64  `json:"capacityMilliWattHours"`
	AvailableMilliWattHours int64  `json:"availableMilliWattHours"`
}

type PhysicalDeviceInventoryReference struct {
	Name            string `json:"name"`
	Digest          string `json:"digest"`
	ResourceVersion string `json:"resourceVersion,omitempty"`
}

type DeviceTopology struct {
	NUMANode   int32  `json:"numaNode,omitempty"`
	SocketID   string `json:"socketID,omitempty"`
	PCIAddress string `json:"pciAddress,omitempty"`
}

type DevicePeerInterconnect struct {
	PeerStableDeviceID     string `json:"peerStableDeviceID"`
	Type                   string `json:"type"`
	BandwidthBitsPerSecond int64  `json:"bandwidthBitsPerSecond"`
}

type PhysicalDevice struct {
	StableDeviceID                     string                   `json:"stableDeviceID"`
	KubernetesResourceName             string                   `json:"kubernetesResourceName"`
	AllocationID                       string                   `json:"allocationID,omitempty"`
	DRAAllocationID                    string                   `json:"draAllocationID,omitempty"`
	VendorAllocationID                 string                   `json:"vendorAllocationID,omitempty"`
	Class                              string                   `json:"class"`
	Vendor                             string                   `json:"vendor"`
	Model                              string                   `json:"model"`
	Architecture                       string                   `json:"architecture"`
	Topology                           DeviceTopology           `json:"topology"`
	PeerInterconnects                  []DevicePeerInterconnect `json:"peerInterconnects,omitempty"`
	TotalMemoryBytes                   int64                    `json:"totalMemoryBytes"`
	FreeMemoryBytes                    int64                    `json:"freeMemoryBytes"`
	MemoryBandwidthBitsPerSecond       int64                    `json:"memoryBandwidthBitsPerSecond,omitempty"`
	InterconnectBandwidthBitsPerSecond int64                    `json:"interconnectBandwidthBitsPerSecond,omitempty"`
	SupportedPrecision                 []string                 `json:"supportedPrecision,omitempty"`
	Firmware                           string                   `json:"firmware,omitempty"`
	Driver                             string                   `json:"driver,omitempty"`
	Runtime                            string                   `json:"runtime,omitempty"`
	Libraries                          map[string]string        `json:"libraries,omitempty"`
	Health                             string                   `json:"health"`
	TemperatureMilliCelsius            int64                    `json:"temperatureMilliCelsius,omitempty"`
	PowerMilliwatts                    int64                    `json:"powerMilliwatts,omitempty"`
	ConfidenceMilli                    int32                    `json:"confidenceMilli"`
}

type PhysicalDeviceInventorySpec struct {
	Domain          DomainReference  `json:"domain"`
	NodeName        string           `json:"nodeName,omitempty"`
	ObservedAt      metav1.Time      `json:"observedAt"`
	ValidUntil      metav1.Time      `json:"validUntil"`
	ConfidenceMilli int32            `json:"confidenceMilli"`
	Provenance      Provenance       `json:"provenance"`
	Devices         []PhysicalDevice `json:"devices"`
}

type PhysicalDeviceInventoryStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=sdeviceinventory
// +kubebuilder:subresource:status
type PhysicalDeviceInventory struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              PhysicalDeviceInventorySpec   `json:"spec"`
	Status            PhysicalDeviceInventoryStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type PhysicalDeviceInventoryList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PhysicalDeviceInventory `json:"items"`
}

type SpaceDomainResourceSummarySpec struct {
	Domain                     DomainReference                   `json:"domain"`
	ObservedAt                 metav1.Time                       `json:"observedAt"`
	ValidUntil                 metav1.Time                       `json:"validUntil"`
	Provenance                 Provenance                        `json:"provenance"`
	Devices                    []DeviceCapacity                  `json:"devices,omitempty"`
	Software                   map[string]string                 `json:"software,omitempty"`
	DataLocations              []string                          `json:"dataLocations,omitempty"`
	QueueDelaySeconds          int64                             `json:"queueDelaySeconds"`
	EnergyHeadroomMilli        int32                             `json:"energyHeadroomMilli"`
	ThermalHeadroomMilli       int32                             `json:"thermalHeadroomMilli"`
	ResilienceMilli            int32                             `json:"resilienceMilli"`
	MinimumEnergyMilli         int32                             `json:"minimumEnergyMilli,omitempty"`
	MinimumThermalMilli        int32                             `json:"minimumThermalMilli,omitempty"`
	MaximumSnapshotAgeSecs     int64                             `json:"maximumSnapshotAgeSeconds"`
	ExporterSnapshotDigest     string                            `json:"exporterSnapshotDigest"`
	CPU                        ScalarCapacity                    `json:"cpu"`
	SystemMemoryBytes          ScalarCapacity                    `json:"systemMemoryBytes"`
	EphemeralStorageBytes      ScalarCapacity                    `json:"ephemeralStorageBytes"`
	PersistentStorage          []StorageCapacity                 `json:"persistentStorage,omitempty"`
	NUMATopology               []NUMAResource                    `json:"numaTopology,omitempty"`
	Trust                      TrustAttestationState             `json:"trust"`
	AutonomyDurationSeconds    int64                             `json:"autonomyDurationSeconds,omitempty"`
	Energy                     EnergyBudget                      `json:"energy"`
	PhysicalDeviceInventoryRef *PhysicalDeviceInventoryReference `json:"physicalDeviceInventoryRef,omitempty"`
}

type SpaceDomainResourceSummaryStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=sresource
// +kubebuilder:subresource:status
type SpaceDomainResourceSummary struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              SpaceDomainResourceSummarySpec   `json:"spec"`
	Status            SpaceDomainResourceSummaryStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type SpaceDomainResourceSummaryList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SpaceDomainResourceSummary `json:"items"`
}

type SpaceTransferReceiptSpec struct {
	TransferID    string          `json:"transferID"`
	MissionUID    string          `json:"missionUID"`
	PlanID        string          `json:"planID"`
	Attempt       int32           `json:"attempt"`
	Source        DomainReference `json:"source"`
	Destination   DomainReference `json:"destination"`
	DataID        string          `json:"dataID"`
	Bytes         int64           `json:"bytes"`
	PayloadDigest string          `json:"payloadDigest"`
	StartedAt     metav1.Time     `json:"startedAt"`
	CompletedAt   metav1.Time     `json:"completedAt"`
	Provenance    Provenance      `json:"provenance"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=stransferreceipt
type SpaceTransferReceipt struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              SpaceTransferReceiptSpec `json:"spec"`
}

// +kubebuilder:object:root=true
type SpaceTransferReceiptList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SpaceTransferReceipt `json:"items"`
}

type SpaceResultReceiptSpec struct {
	ResultID      string          `json:"resultID"`
	MissionUID    string          `json:"missionUID"`
	PlanID        string          `json:"planID"`
	Attempt       int32           `json:"attempt"`
	Source        DomainReference `json:"source"`
	Destination   DomainReference `json:"destination"`
	Bytes         int64           `json:"bytes"`
	PayloadDigest string          `json:"payloadDigest"`
	// LeaseEpoch and TokenHash bind completion to the exact execution fence.
	// They are optional only so pre-hardening stored v1alpha1 objects remain
	// decodable; a result used by the workload state machine must provide both.
	LeaseEpoch  int64       `json:"leaseEpoch,omitempty"`
	TokenHash   string      `json:"tokenHash,omitempty"`
	CompletedAt metav1.Time `json:"completedAt"`
	Provenance  Provenance  `json:"provenance"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=sresultreceipt
type SpaceResultReceipt struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              SpaceResultReceiptSpec `json:"spec"`
}

// +kubebuilder:object:root=true
type SpaceResultReceiptList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SpaceResultReceipt `json:"items"`
}

type TransferEpoch struct {
	LinkSnapshotName string          `json:"linkSnapshotName"`
	WindowID         string          `json:"windowID"`
	DataID           string          `json:"dataID,omitempty"`
	Source           DomainReference `json:"source,omitempty"`
	SourceURI        string          `json:"sourceURI,omitempty"`
	Destination      DomainReference `json:"destination,omitempty"`
	DestinationURI   string          `json:"destinationURI,omitempty"`
	Start            metav1.Time     `json:"start"`
	End              metav1.Time     `json:"end"`
	Bytes            int64           `json:"bytes"`
}

type DecisionScore struct {
	PredictedCompletion int32 `json:"predictedCompletion"`
	DataLocality        int32 `json:"dataLocality"`
	LinkRisk            int32 `json:"linkRisk"`
	EnergyThermal       int32 `json:"energyThermal"`
	Resilience          int32 `json:"resilience"`
	Fragmentation       int32 `json:"fragmentation"`
	Total               int32 `json:"total"`
}

type ConstraintExplanation struct {
	Code       string `json:"code"`
	Constraint string `json:"constraint"`
	Observed   string `json:"observed,omitempty"`
	Required   string `json:"required,omitempty"`
	Message    string `json:"message"`
}

type PhysicalDeviceConstraint struct {
	Class           string   `json:"class"`
	Quantity        int64    `json:"quantity"`
	Architecture    string   `json:"architecture,omitempty"`
	Model           string   `json:"model,omitempty"`
	Precision       []string `json:"precision,omitempty"`
	ResourceName    string   `json:"resourceName,omitempty"`
	StableDeviceIDs []string `json:"stableDeviceIDs,omitempty"`
	AllocationIDs   []string `json:"allocationIDs,omitempty"`
}

type TransferState string

const (
	TransferStateNotRequired TransferState = "NotRequired"
	TransferStatePending     TransferState = "Pending"
	TransferStateInProgress  TransferState = "InProgress"
	TransferStateCompleted   TransferState = "Completed"
	TransferStateFailed      TransferState = "Failed"
)

type SpacePlacementIntentSpec struct {
	MissionRef                        corev1.ObjectReference     `json:"missionRef"`
	PlanID                            string                     `json:"planID"`
	Attempt                           int32                      `json:"attempt"`
	Target                            DomainReference            `json:"target"`
	NotBefore                         metav1.Time                `json:"notBefore"`
	ExpiresAt                         metav1.Time                `json:"expiresAt"`
	ComputeStart                      metav1.Time                `json:"computeStart"`
	ComputeEnd                        metav1.Time                `json:"computeEnd"`
	InputTransfers                    []TransferEpoch            `json:"inputTransfers,omitempty"`
	ResultTransfer                    *TransferEpoch             `json:"resultTransfer,omitempty"`
	MaterialInputDigest               string                     `json:"materialInputDigest"`
	PlanningInputDigest               string                     `json:"planningInputDigest,omitempty"`
	CacheResourceVersions             map[string]string          `json:"cacheResourceVersions,omitempty"`
	SnapshotSequences                 map[string]int64           `json:"snapshotSequences"`
	Score                             DecisionScore              `json:"score"`
	Explanations                      []ConstraintExplanation    `json:"explanations"`
	SelectedCapabilitySetName         string                     `json:"selectedCapabilitySetName,omitempty"`
	SelectedCapabilities              []CapabilityRequirement    `json:"selectedCapabilities,omitempty"`
	SelectedPhysicalDeviceConstraints []PhysicalDeviceConstraint `json:"selectedPhysicalDeviceConstraints,omitempty"`
}

type PlacementPhase string

const (
	PlacementPending         PlacementPhase = "Pending"
	PlacementTransferPending PlacementPhase = "TransferPending"
	PlacementReady           PlacementPhase = "Ready"
	PlacementDispatched      PlacementPhase = "Dispatched"
	PlacementRunning         PlacementPhase = "Running"
	PlacementCheckpointed    PlacementPhase = "Checkpointed"
	PlacementReplanning      PlacementPhase = "Replanning"
	PlacementReturnPending   PlacementPhase = "ReturnPending"
	PlacementCompleted       PlacementPhase = "Completed"
	PlacementExpired         PlacementPhase = "Expired"
	PlacementFailed          PlacementPhase = "Failed"
)

type ExecutionObservation struct {
	Sequence     int64       `json:"sequence"`
	Attempt      int32       `json:"attempt"`
	PodUID       string      `json:"podUID,omitempty"`
	Phase        string      `json:"phase"`
	ObservedAt   metav1.Time `json:"observedAt"`
	CheckpointID string      `json:"checkpointID,omitempty"`
}

type SpacePlacementIntentStatus struct {
	ObservedGeneration            int64                   `json:"observedGeneration,omitempty"`
	Phase                         PlacementPhase          `json:"phase,omitempty"`
	ActivePod                     *corev1.ObjectReference `json:"activePod,omitempty"`
	LastObservationSequence       int64                   `json:"lastObservationSequence,omitempty"`
	LastObservation               *ExecutionObservation   `json:"lastObservation,omitempty"`
	RetryCount                    int32                   `json:"retryCount,omitempty"`
	ResultReturned                bool                    `json:"resultReturned,omitempty"`
	TransferState                 TransferState           `json:"transferState,omitempty"`
	TransferReceiptReferences     []string                `json:"transferReceiptReferences,omitempty"`
	ExecutionLeaseReference       string                  `json:"executionLeaseReference,omitempty"`
	FencingTokenHash              string                  `json:"fencingTokenHash,omitempty"`
	CheckpointReceipt             string                  `json:"checkpointReceipt,omitempty"`
	ResultReceipt                 string                  `json:"resultReceipt,omitempty"`
	RemoteAcknowledgementSequence int64                   `json:"remoteAcknowledgementSequence,omitempty"`
	Conditions                    []metav1.Condition      `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,shortName=splacement
// +kubebuilder:subresource:status
type SpacePlacementIntent struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              SpacePlacementIntentSpec   `json:"spec"`
	Status            SpacePlacementIntentStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type SpacePlacementIntentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SpacePlacementIntent `json:"items"`
}

func (in *SpaceLinkSnapshot) DeepCopyObject() runtime.Object              { return in.DeepCopy() }
func (in *SpaceLinkSnapshotList) DeepCopyObject() runtime.Object          { return in.DeepCopy() }
func (in *SpaceDomainResourceSummary) DeepCopyObject() runtime.Object     { return in.DeepCopy() }
func (in *SpaceDomainResourceSummaryList) DeepCopyObject() runtime.Object { return in.DeepCopy() }
func (in *PhysicalDeviceInventory) DeepCopyObject() runtime.Object        { return in.DeepCopy() }
func (in *PhysicalDeviceInventoryList) DeepCopyObject() runtime.Object    { return in.DeepCopy() }
func (in *SpaceMission) DeepCopyObject() runtime.Object                   { return in.DeepCopy() }
func (in *SpaceMissionList) DeepCopyObject() runtime.Object               { return in.DeepCopy() }
func (in *SpacePlacementIntent) DeepCopyObject() runtime.Object           { return in.DeepCopy() }
func (in *SpacePlacementIntentList) DeepCopyObject() runtime.Object       { return in.DeepCopy() }
func (in *SpaceDomainReporterBinding) DeepCopyObject() runtime.Object     { return in.DeepCopy() }
func (in *SpaceDomainReporterBindingList) DeepCopyObject() runtime.Object { return in.DeepCopy() }
func (in *SpaceTransferReceipt) DeepCopyObject() runtime.Object           { return in.DeepCopy() }
func (in *SpaceTransferReceiptList) DeepCopyObject() runtime.Object       { return in.DeepCopy() }
func (in *SpaceResultReceipt) DeepCopyObject() runtime.Object             { return in.DeepCopy() }
func (in *SpaceResultReceiptList) DeepCopyObject() runtime.Object         { return in.DeepCopy() }
