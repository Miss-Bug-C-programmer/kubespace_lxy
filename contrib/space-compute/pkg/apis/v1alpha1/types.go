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
	ReporterID string `json:"reporterID"`
	Source     string `json:"source"`
	Digest     string `json:"digest"`
	Sequence   int64  `json:"sequence"`
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

type DataObject struct {
	ID        string   `json:"id"`
	SizeBytes int64    `json:"sizeBytes"`
	Locations []string `json:"locations"`
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
	MissionClass            string                  `json:"missionClass"`
	Priority                int32                   `json:"priority"`
	StatePolicy             StatePolicy             `json:"statePolicy"`
	RequiredCapabilities    []CapabilityRequirement `json:"requiredCapabilities,omitempty"`
	AlternativeCapabilities []CapabilitySet         `json:"alternativeCapabilities,omitempty"`
	RequiredSoftware        map[string]string       `json:"requiredSoftware,omitempty"`
	Inputs                  []DataObject            `json:"inputs,omitempty"`
	OutputSizeBytes         int64                   `json:"outputSizeBytes"`
	ResultDestinations      []string                `json:"resultDestinations,omitempty"`
	Deadline                metav1.Time             `json:"deadline"`
	ExpectedDurationSeconds int64                   `json:"expectedDurationSeconds"`
	MaximumDurationSeconds  int64                   `json:"maximumDurationSeconds"`
	DurationUncertaintySecs int64                   `json:"durationUncertaintySeconds"`
	SafetyMarginSeconds     int64                   `json:"safetyMarginSeconds"`
	MaximumClockSkewSeconds int64                   `json:"maximumClockSkewSeconds"`
	ResultReturnRequired    bool                    `json:"resultReturnRequired"`
	Retry                   RetryPolicy             `json:"retry"`
	Checkpoint              CheckpointPolicy        `json:"checkpoint"`
	WorkloadTemplate        corev1.PodTemplateSpec  `json:"workloadTemplate"`
	Suspend                 bool                    `json:"suspend,omitempty"`
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

type SpaceDomainResourceSummarySpec struct {
	Domain                 DomainReference   `json:"domain"`
	ObservedAt             metav1.Time       `json:"observedAt"`
	ValidUntil             metav1.Time       `json:"validUntil"`
	Provenance             Provenance        `json:"provenance"`
	Devices                []DeviceCapacity  `json:"devices,omitempty"`
	Software               map[string]string `json:"software,omitempty"`
	DataLocations          []string          `json:"dataLocations,omitempty"`
	QueueDelaySeconds      int64             `json:"queueDelaySeconds"`
	EnergyHeadroomMilli    int32             `json:"energyHeadroomMilli"`
	ThermalHeadroomMilli   int32             `json:"thermalHeadroomMilli"`
	ResilienceMilli        int32             `json:"resilienceMilli"`
	MinimumEnergyMilli     int32             `json:"minimumEnergyMilli,omitempty"`
	MinimumThermalMilli    int32             `json:"minimumThermalMilli,omitempty"`
	MaximumSnapshotAgeSecs int64             `json:"maximumSnapshotAgeSeconds"`
	ExporterSnapshotDigest string            `json:"exporterSnapshotDigest"`
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

type TransferEpoch struct {
	LinkSnapshotName string      `json:"linkSnapshotName"`
	WindowID         string      `json:"windowID"`
	Start            metav1.Time `json:"start"`
	End              metav1.Time `json:"end"`
	Bytes            int64       `json:"bytes"`
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

type SpacePlacementIntentSpec struct {
	MissionRef          corev1.ObjectReference  `json:"missionRef"`
	PlanID              string                  `json:"planID"`
	Attempt             int32                   `json:"attempt"`
	Target              DomainReference         `json:"target"`
	NotBefore           metav1.Time             `json:"notBefore"`
	ExpiresAt           metav1.Time             `json:"expiresAt"`
	ComputeStart        metav1.Time             `json:"computeStart"`
	ComputeEnd          metav1.Time             `json:"computeEnd"`
	InputTransfers      []TransferEpoch         `json:"inputTransfers,omitempty"`
	ResultTransfer      *TransferEpoch          `json:"resultTransfer,omitempty"`
	MaterialInputDigest string                  `json:"materialInputDigest"`
	SnapshotSequences   map[string]int64        `json:"snapshotSequences"`
	Score               DecisionScore           `json:"score"`
	Explanations        []ConstraintExplanation `json:"explanations"`
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
	ObservedGeneration      int64                   `json:"observedGeneration,omitempty"`
	Phase                   PlacementPhase          `json:"phase,omitempty"`
	ActivePod               *corev1.ObjectReference `json:"activePod,omitempty"`
	LastObservationSequence int64                   `json:"lastObservationSequence,omitempty"`
	LastObservation         *ExecutionObservation   `json:"lastObservation,omitempty"`
	RetryCount              int32                   `json:"retryCount,omitempty"`
	ResultReturned          bool                    `json:"resultReturned,omitempty"`
	Conditions              []metav1.Condition      `json:"conditions,omitempty"`
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
func (in *SpaceMission) DeepCopyObject() runtime.Object                   { return in.DeepCopy() }
func (in *SpaceMissionList) DeepCopyObject() runtime.Object               { return in.DeepCopy() }
func (in *SpacePlacementIntent) DeepCopyObject() runtime.Object           { return in.DeepCopy() }
func (in *SpacePlacementIntentList) DeepCopyObject() runtime.Object       { return in.DeepCopy() }
