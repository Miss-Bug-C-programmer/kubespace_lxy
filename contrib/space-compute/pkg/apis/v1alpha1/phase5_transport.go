package v1alpha1

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// PlacementExecutionLeasePending is intentionally distinct from transfer
// waiting: all inputs may be present while remote execution is still fenced.
const PlacementExecutionLeasePending PlacementPhase = "ExecutionLeasePending"

const (
	TransferIntentPending   = "Pending"
	TransferIntentSending   = "Sending"
	TransferIntentCompleted = "Completed"
	TransferIntentFailed    = "Failed"
	TransferPurposeInput    = "Input"
	TransferPurposeResult   = "Result"
)

// SpaceTransferIntent is planner/workload-owned desired state. It is not a
// reporter assertion and therefore is not signed by the reporter admission path.
type SpaceTransferIntentSpec struct {
	TransferID    string          `json:"transferID"`
	MissionUID    string          `json:"missionUID"`
	PlanID        string          `json:"planID"`
	Attempt       int32           `json:"attempt"`
	Purpose       string          `json:"purpose"`
	Coordinator   DomainReference `json:"coordinator"`
	Source        DomainReference `json:"source"`
	Destination   DomainReference `json:"destination"`
	DataID        string          `json:"dataID"`
	Bytes         int64           `json:"bytes"`
	PayloadDigest string          `json:"payloadDigest"`
	LeaseEpoch    int64           `json:"leaseEpoch,omitempty"`
	TokenHash     string          `json:"tokenHash,omitempty"`
	Window        TransferEpoch   `json:"window"`
	ExpiresAt     metav1.Time     `json:"expiresAt"`
}

type SpaceTransferIntentStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Phase              string             `json:"phase,omitempty"`
	ReceiptName        string             `json:"receiptName,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=stransfer
// +kubebuilder:subresource:status
type SpaceTransferIntent struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              SpaceTransferIntentSpec   `json:"spec"`
	Status            SpaceTransferIntentStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type SpaceTransferIntentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SpaceTransferIntent `json:"items"`
}

// ExecutionFence is the durable, non-reusable identity of one execution lease.
// TokenHash is SHA-256 of a random token; the plaintext token is never stored in
// this CRD and is delivered only through the mutually-authenticated agent path.
type ExecutionFence struct {
	MissionUID string      `json:"missionUID"`
	PlanID     string      `json:"planID"`
	Attempt    int32       `json:"attempt"`
	LeaseEpoch int64       `json:"leaseEpoch"`
	TokenHash  string      `json:"tokenHash"`
	ExpiresAt  metav1.Time `json:"expiresAt"`
}

type SpaceExecutionLeaseSpec struct {
	Source                  DomainReference `json:"source"`
	Destination             DomainReference `json:"destination"`
	Fence                   ExecutionFence  `json:"fence"`
	HeartbeatAt             metav1.Time     `json:"heartbeatAt"`
	MaximumClockSkewSeconds int64           `json:"maximumClockSkewSeconds"`
	Provenance              Provenance      `json:"provenance"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=sexeclease
type SpaceExecutionLease struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              SpaceExecutionLeaseSpec `json:"spec"`
}

// +kubebuilder:object:root=true
type SpaceExecutionLeaseList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SpaceExecutionLease `json:"items"`
}

type ExecutionObservationPhase string

const (
	ExecutionObservationHeartbeat    ExecutionObservationPhase = "Heartbeat"
	ExecutionObservationStopped      ExecutionObservationPhase = "Stopped"
	ExecutionObservationCheckpointed ExecutionObservationPhase = "Checkpointed"
	ExecutionObservationCompleted    ExecutionObservationPhase = "Completed"
	ExecutionObservationFailed       ExecutionObservationPhase = "Failed"
)

type SpaceExecutionObservationSpec struct {
	ObservationID string                    `json:"observationID"`
	MissionUID    string                    `json:"missionUID"`
	PlanID        string                    `json:"planID"`
	Attempt       int32                     `json:"attempt"`
	LeaseEpoch    int64                     `json:"leaseEpoch"`
	TokenHash     string                    `json:"tokenHash"`
	Source        DomainReference           `json:"source"`
	Destination   DomainReference           `json:"destination"`
	Phase         ExecutionObservationPhase `json:"phase"`
	CheckpointID  string                    `json:"checkpointID,omitempty"`
	ObservedAt    metav1.Time               `json:"observedAt"`
	Provenance    Provenance                `json:"provenance"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=sexecobs
type SpaceExecutionObservation struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              SpaceExecutionObservationSpec `json:"spec"`
}

// +kubebuilder:object:root=true
type SpaceExecutionObservationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SpaceExecutionObservation `json:"items"`
}

func (in *SpaceTransferIntent) DeepCopy() *SpaceTransferIntent {
	if in == nil {
		return nil
	}
	out := *in
	out.ObjectMeta = *in.ObjectMeta.DeepCopy()
	out.Status.Conditions = append([]metav1.Condition(nil), in.Status.Conditions...)
	return &out
}
func (in *SpaceTransferIntentList) DeepCopy() *SpaceTransferIntentList {
	if in == nil {
		return nil
	}
	out := *in
	out.Items = make([]SpaceTransferIntent, len(in.Items))
	for i := range in.Items {
		out.Items[i] = *in.Items[i].DeepCopy()
	}
	return &out
}
func (in *SpaceExecutionLease) DeepCopy() *SpaceExecutionLease {
	if in == nil {
		return nil
	}
	out := *in
	out.ObjectMeta = *in.ObjectMeta.DeepCopy()
	return &out
}
func (in *SpaceExecutionLeaseList) DeepCopy() *SpaceExecutionLeaseList {
	if in == nil {
		return nil
	}
	out := *in
	out.Items = make([]SpaceExecutionLease, len(in.Items))
	for i := range in.Items {
		out.Items[i] = *in.Items[i].DeepCopy()
	}
	return &out
}
func (in *SpaceExecutionObservation) DeepCopy() *SpaceExecutionObservation {
	if in == nil {
		return nil
	}
	out := *in
	out.ObjectMeta = *in.ObjectMeta.DeepCopy()
	return &out
}
func (in *SpaceExecutionObservationList) DeepCopy() *SpaceExecutionObservationList {
	if in == nil {
		return nil
	}
	out := *in
	out.Items = make([]SpaceExecutionObservation, len(in.Items))
	for i := range in.Items {
		out.Items[i] = *in.Items[i].DeepCopy()
	}
	return &out
}

func (in *SpaceTransferIntent) DeepCopyObject() runtime.Object           { return in.DeepCopy() }
func (in *SpaceTransferIntentList) DeepCopyObject() runtime.Object       { return in.DeepCopy() }
func (in *SpaceExecutionLease) DeepCopyObject() runtime.Object           { return in.DeepCopy() }
func (in *SpaceExecutionLeaseList) DeepCopyObject() runtime.Object       { return in.DeepCopy() }
func (in *SpaceExecutionObservation) DeepCopyObject() runtime.Object     { return in.DeepCopy() }
func (in *SpaceExecutionObservationList) DeepCopyObject() runtime.Object { return in.DeepCopy() }

func InputTransferID(index int, dataID string) string {
	value := strings.ToLower(strings.TrimSpace(dataID))
	var b strings.Builder
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			b.WriteRune(r)
		} else {
			b.WriteByte('-')
		}
	}
	value = strings.Trim(b.String(), "-")
	if value == "" {
		value = "data"
	}
	if len(value) > 40 {
		value = value[:40]
	}
	return fmt.Sprintf("input-%d-%s", index+1, value)
}

func ResultTransferID(attempt int32) string { return fmt.Sprintf("result-attempt-%d", attempt) }

func TransferIntentName(source, destination DomainReference, missionUID, planID, transferID string) string {
	return derivedObjectName("transfer-intent", normalizedDomainIdentity(source)+"->"+normalizedDomainIdentity(destination)+"|"+strings.TrimSpace(missionUID)+"|"+strings.ToLower(strings.TrimSpace(planID))+"|"+strings.ToLower(strings.TrimSpace(transferID)))
}

func ExecutionLeaseName(missionUID, planID string, attempt int32, epoch int64) string {
	return derivedObjectName("execution-lease", fmt.Sprintf("%s|%s|%d|%d", strings.TrimSpace(missionUID), strings.ToLower(strings.TrimSpace(planID)), attempt, epoch))
}

func ExecutionObservationName(source, destination DomainReference, missionUID, planID, observationID string) string {
	return derivedObjectName("execution-observation", normalizedDomainIdentity(source)+"->"+normalizedDomainIdentity(destination)+"|"+strings.TrimSpace(missionUID)+"|"+strings.ToLower(strings.TrimSpace(planID))+"|"+strings.ToLower(strings.TrimSpace(observationID)))
}

// ExecutionTokenSecretName is deterministic but reveals no token material.
func ExecutionTokenSecretName(f ExecutionFence) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s|%s|%d|%d", f.MissionUID, f.PlanID, f.Attempt, f.LeaseEpoch)))
	return "space-exec-token-" + hex.EncodeToString(sum[:12])
}
