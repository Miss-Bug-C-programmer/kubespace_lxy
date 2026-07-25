#!/usr/bin/env python3
from pathlib import Path


def read(path):
    return Path(path).read_text()

def write(path, text):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text)

def replace_once(path, old, new):
    text = read(path)
    if text.count(old) != 1:
        raise SystemExit(f"{path}: marker count {text.count(old)} for {old[:80]!r}")
    write(path, text.replace(old, new, 1))

# Existing API additions kept additive within v1alpha1.
replace_once(
    "contrib/space-compute/pkg/apis/v1alpha1/types.go",
    '''type SpaceDomainReporterBindingSpec struct {
\tReporterPrincipal string            `json:"reporterPrincipal"`
\tDomain            DomainReference   `json:"domain"`
\tAllowedKinds      []string          `json:"allowedKinds"`
\tAllowedPeers      []DomainReference `json:"allowedPeers,omitempty"`
\tPublicKeyRef      SecretReference   `json:"publicKeyRef"`
}''',
    '''type SpaceDomainReporterBindingSpec struct {
\tReporterPrincipal string            `json:"reporterPrincipal"`
\tDomain            DomainReference   `json:"domain"`
\tAllowedKinds      []string          `json:"allowedKinds"`
\tAllowedPeers      []DomainReference `json:"allowedPeers,omitempty"`
\t// AllowedGateways are explicit authenticated Kubernetes principals that may
\t// persist this reporter's already-signed objects through a controlled local
\t// transport gateway. The original reporter signature remains mandatory.
\tAllowedGateways   []string          `json:"allowedGateways,omitempty"`
\tPublicKeyRef      SecretReference   `json:"publicKeyRef"`
}''')
replace_once(
    "contrib/space-compute/pkg/apis/v1alpha1/types.go",
    '''type SpaceResultReceiptSpec struct {
\tResultID      string          `json:"resultID"`
\tMissionUID    string          `json:"missionUID"`
\tPlanID        string          `json:"planID"`
\tAttempt       int32           `json:"attempt"`
\tSource        DomainReference `json:"source"`
\tDestination   DomainReference `json:"destination"`
\tBytes         int64           `json:"bytes"`
\tPayloadDigest string          `json:"payloadDigest"`
\tCompletedAt   metav1.Time     `json:"completedAt"`
\tProvenance    Provenance      `json:"provenance"`
}''',
    '''type SpaceResultReceiptSpec struct {
\tResultID      string          `json:"resultID"`
\tMissionUID    string          `json:"missionUID"`
\tPlanID        string          `json:"planID"`
\tAttempt       int32           `json:"attempt"`
\tSource        DomainReference `json:"source"`
\tDestination   DomainReference `json:"destination"`
\tBytes         int64           `json:"bytes"`
\tPayloadDigest string          `json:"payloadDigest"`
\t// LeaseEpoch and TokenHash bind completion to the exact execution fence.
\t// They are optional only so pre-hardening stored v1alpha1 objects remain
\t// decodable; a result used by the workload state machine must provide both.
\tLeaseEpoch    int64           `json:"leaseEpoch,omitempty"`
\tTokenHash     string          `json:"tokenHash,omitempty"`
\tCompletedAt   metav1.Time     `json:"completedAt"`
\tProvenance    Provenance      `json:"provenance"`
}''')
replace_once(
    "contrib/space-compute/pkg/apis/v1alpha1/types.go",
    '''type TransferEpoch struct {
\tLinkSnapshotName string      `json:"linkSnapshotName"`
\tWindowID         string      `json:"windowID"`
\tStart            metav1.Time `json:"start"`
\tEnd              metav1.Time `json:"end"`
\tBytes            int64       `json:"bytes"`
}''',
    '''type TransferEpoch struct {
\tLinkSnapshotName string          `json:"linkSnapshotName"`
\tWindowID         string          `json:"windowID"`
\tDataID           string          `json:"dataID,omitempty"`
\tSource           DomainReference `json:"source,omitempty"`
\tDestination      DomainReference `json:"destination,omitempty"`
\tStart            metav1.Time     `json:"start"`
\tEnd              metav1.Time     `json:"end"`
\tBytes            int64           `json:"bytes"`
}''')

write("contrib/space-compute/pkg/apis/v1alpha1/phase5_transport.go", r'''package v1alpha1

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
)

// SpaceTransferIntent is planner/workload-owned desired state. It is not a
// reporter assertion and therefore is not signed by the reporter admission path.
type SpaceTransferIntentSpec struct {
    TransferID    string          `json:"transferID"`
    MissionUID    string          `json:"missionUID"`
    PlanID        string          `json:"planID"`
    Attempt       int32           `json:"attempt"`
    Source        DomainReference `json:"source"`
    Destination   DomainReference `json:"destination"`
    DataID        string          `json:"dataID"`
    Bytes         int64           `json:"bytes"`
    PayloadDigest string          `json:"payloadDigest"`
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
    if in == nil { return nil }
    out := *in
    out.ObjectMeta = *in.ObjectMeta.DeepCopy()
    out.Status.Conditions = append([]metav1.Condition(nil), in.Status.Conditions...)
    return &out
}
func (in *SpaceTransferIntentList) DeepCopy() *SpaceTransferIntentList {
    if in == nil { return nil }
    out := *in
    out.Items = make([]SpaceTransferIntent, len(in.Items))
    for i := range in.Items { out.Items[i] = *in.Items[i].DeepCopy() }
    return &out
}
func (in *SpaceExecutionLease) DeepCopy() *SpaceExecutionLease {
    if in == nil { return nil }
    out := *in
    out.ObjectMeta = *in.ObjectMeta.DeepCopy()
    return &out
}
func (in *SpaceExecutionLeaseList) DeepCopy() *SpaceExecutionLeaseList {
    if in == nil { return nil }
    out := *in
    out.Items = make([]SpaceExecutionLease, len(in.Items))
    for i := range in.Items { out.Items[i] = *in.Items[i].DeepCopy() }
    return &out
}
func (in *SpaceExecutionObservation) DeepCopy() *SpaceExecutionObservation {
    if in == nil { return nil }
    out := *in
    out.ObjectMeta = *in.ObjectMeta.DeepCopy()
    return &out
}
func (in *SpaceExecutionObservationList) DeepCopy() *SpaceExecutionObservationList {
    if in == nil { return nil }
    out := *in
    out.Items = make([]SpaceExecutionObservation, len(in.Items))
    for i := range in.Items { out.Items[i] = *in.Items[i].DeepCopy() }
    return &out
}

func (in *SpaceTransferIntent) DeepCopyObject() runtime.Object { return in.DeepCopy() }
func (in *SpaceTransferIntentList) DeepCopyObject() runtime.Object { return in.DeepCopy() }
func (in *SpaceExecutionLease) DeepCopyObject() runtime.Object { return in.DeepCopy() }
func (in *SpaceExecutionLeaseList) DeepCopyObject() runtime.Object { return in.DeepCopy() }
func (in *SpaceExecutionObservation) DeepCopyObject() runtime.Object { return in.DeepCopy() }
func (in *SpaceExecutionObservationList) DeepCopyObject() runtime.Object { return in.DeepCopy() }

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
''')

replace_once(
    "contrib/space-compute/pkg/apis/v1alpha1/register.go",
    '''\t\t&SpaceTransferReceipt{}, &SpaceTransferReceiptList{},
\t\t&SpaceResultReceipt{}, &SpaceResultReceiptList{},
\t\t&SpaceMission{}, &SpaceMissionList{},''',
    '''\t\t&SpaceTransferIntent{}, &SpaceTransferIntentList{},
\t\t&SpaceTransferReceipt{}, &SpaceTransferReceiptList{},
\t\t&SpaceExecutionLease{}, &SpaceExecutionLeaseList{},
\t\t&SpaceExecutionObservation{}, &SpaceExecutionObservationList{},
\t\t&SpaceResultReceipt{}, &SpaceResultReceiptList{},
\t\t&SpaceMission{}, &SpaceMissionList{},''')
replace_once(
    "contrib/space-compute/pkg/apis/v1alpha1/zz_generated.deepcopy.go",
    '''\tout.Spec.AllowedKinds = append([]string(nil), in.Spec.AllowedKinds...)
\tout.Spec.AllowedPeers = append([]DomainReference(nil), in.Spec.AllowedPeers...)
\treturn &out''',
    '''\tout.Spec.AllowedKinds = append([]string(nil), in.Spec.AllowedKinds...)
\tout.Spec.AllowedPeers = append([]DomainReference(nil), in.Spec.AllowedPeers...)
\tout.Spec.AllowedGateways = append([]string(nil), in.Spec.AllowedGateways...)
\treturn &out''')

# Canonical signed evidence: lease/observation and result fence identity.
replace_once(
    "contrib/space-compute/pkg/apis/v1alpha1/canonical.go",
    '''\tcase *SpaceTransferReceipt:
\t\treturn canonicalTransferReceipt(object)
\tcase *SpaceResultReceipt:
\t\treturn canonicalResultReceipt(object)''',
    '''\tcase *SpaceTransferReceipt:
\t\treturn canonicalTransferReceipt(object)
\tcase *SpaceExecutionLease:
\t\treturn canonicalExecutionLease(object)
\tcase *SpaceExecutionObservation:
\t\treturn canonicalExecutionObservation(object)
\tcase *SpaceResultReceipt:
\t\treturn canonicalResultReceipt(object)''')
replace_once(
    "contrib/space-compute/pkg/apis/v1alpha1/canonical.go",
    '''\tw.integer("bytes", receipt.Spec.Bytes)
\tw.string("payloadDigest", receipt.Spec.PayloadDigest)
\tw.timestamp("completedAt", receipt.Spec.CompletedAt.Time)''',
    '''\tw.integer("bytes", receipt.Spec.Bytes)
\tw.string("payloadDigest", receipt.Spec.PayloadDigest)
\tw.integer("leaseEpoch", receipt.Spec.LeaseEpoch)
\tw.string("tokenHash", receipt.Spec.TokenHash)
\tw.timestamp("completedAt", receipt.Spec.CompletedAt.Time)''')
insert_marker = '''func canonicalResultReceipt(receipt *SpaceResultReceipt) ([]byte, error) {'''
text = read("contrib/space-compute/pkg/apis/v1alpha1/canonical.go")
if text.count(insert_marker) != 1:
    raise SystemExit("canonical result marker mismatch")
extra = r'''func canonicalExecutionLease(lease *SpaceExecutionLease) ([]byte, error) {
    if lease == nil { return nil, fmt.Errorf("execution lease is required") }
    w := newCanonicalWriter("SpaceExecutionLease", lease.Name)
    w.domain("source", lease.Spec.Source)
    w.domain("destination", lease.Spec.Destination)
    w.string("fence.missionUID", lease.Spec.Fence.MissionUID)
    w.string("fence.planID", lease.Spec.Fence.PlanID)
    w.integer("fence.attempt", int64(lease.Spec.Fence.Attempt))
    w.integer("fence.leaseEpoch", lease.Spec.Fence.LeaseEpoch)
    w.string("fence.tokenHash", lease.Spec.Fence.TokenHash)
    w.timestamp("fence.expiresAt", lease.Spec.Fence.ExpiresAt.Time)
    w.timestamp("heartbeatAt", lease.Spec.HeartbeatAt.Time)
    w.integer("maximumClockSkewSeconds", lease.Spec.MaximumClockSkewSeconds)
    w.provenance(lease.Spec.Provenance)
    return w.bytes(), nil
}

func canonicalExecutionObservation(observation *SpaceExecutionObservation) ([]byte, error) {
    if observation == nil { return nil, fmt.Errorf("execution observation is required") }
    w := newCanonicalWriter("SpaceExecutionObservation", observation.Name)
    w.string("observationID", observation.Spec.ObservationID)
    w.string("missionUID", observation.Spec.MissionUID)
    w.string("planID", observation.Spec.PlanID)
    w.integer("attempt", int64(observation.Spec.Attempt))
    w.integer("leaseEpoch", observation.Spec.LeaseEpoch)
    w.string("tokenHash", observation.Spec.TokenHash)
    w.domain("source", observation.Spec.Source)
    w.domain("destination", observation.Spec.Destination)
    w.string("phase", string(observation.Spec.Phase))
    w.string("checkpointID", observation.Spec.CheckpointID)
    w.timestamp("observedAt", observation.Spec.ObservedAt.Time)
    w.provenance(observation.Spec.Provenance)
    return w.bytes(), nil
}

'''
write("contrib/space-compute/pkg/apis/v1alpha1/canonical.go", text.replace(insert_marker, extra + insert_marker, 1))

# Validation and trust-binding additions.
replace_once(
    "contrib/space-compute/pkg/apis/v1alpha1/truth_validation.go",
    '''\t"SpaceTransferReceipt":       {},
\t"SpaceResultReceipt":         {},''',
    '''\t"SpaceTransferReceipt":       {},
\t"SpaceExecutionLease":        {},
\t"SpaceExecutionObservation":  {},
\t"SpaceResultReceipt":         {},''')
replace_once(
    "contrib/space-compute/pkg/apis/v1alpha1/truth_validation.go",
    '''\tif len(binding.Spec.AllowedPeers) > 64 {
\t\terrs.add("spec.allowedPeers", "cannot exceed 64 domains")
\t}''',
    '''\tif len(binding.Spec.AllowedPeers) > 64 {
\t\terrs.add("spec.allowedPeers", "cannot exceed 64 domains")
\t}
\tif len(binding.Spec.AllowedGateways) > 16 {
\t\terrs.add("spec.allowedGateways", "cannot exceed 16 principals")
\t}
\tseenGateways := map[string]struct{}{}
\tfor i, gateway := range binding.Spec.AllowedGateways {
\t\tgateway = strings.TrimSpace(gateway)
\t\tif gateway == "" || len(gateway) > 253 || strings.ContainsAny(gateway, "\\r\\n\\x00") {
\t\t\terrs.addf(fmt.Sprintf("spec.allowedGateways[%d]", i), "must be a non-empty principal of at most 253 bytes")
\t\t}
\t\tif gateway == principal {
\t\t\terrs.addf(fmt.Sprintf("spec.allowedGateways[%d]", i), "must differ from reporterPrincipal")
\t\t}
\t\tif _, ok := seenGateways[gateway]; ok { errs.addf(fmt.Sprintf("spec.allowedGateways[%d]", i), "duplicate gateway principal") }
\t\tseenGateways[gateway] = struct{}{}
\t}''')
replace_once(
    "contrib/space-compute/pkg/apis/v1alpha1/truth_validation.go",
    '''\tvalidateReceiptCommon(receipt.Spec.MissionUID, receipt.Spec.PlanID, receipt.Spec.Attempt, receipt.Spec.Source, receipt.Spec.Destination, receipt.Spec.Bytes, receipt.Spec.PayloadDigest, receipt.Spec.Provenance, &errs)
\tif receipt.Spec.CompletedAt.IsZero() {''',
    '''\tvalidateReceiptCommon(receipt.Spec.MissionUID, receipt.Spec.PlanID, receipt.Spec.Attempt, receipt.Spec.Source, receipt.Spec.Destination, receipt.Spec.Bytes, receipt.Spec.PayloadDigest, receipt.Spec.Provenance, &errs)
\tif receipt.Spec.LeaseEpoch < 0 { errs.add("spec.leaseEpoch", "cannot be negative") }
\tif receipt.Spec.TokenHash != "" { validateLowerSHA256("spec.tokenHash", receipt.Spec.TokenHash, &errs) }
\tif (receipt.Spec.LeaseEpoch == 0) != (receipt.Spec.TokenHash == "") { errs.add("spec", "leaseEpoch and tokenHash must either both be set or both be absent") }
\tif receipt.Spec.CompletedAt.IsZero() {''')

write("contrib/space-compute/pkg/apis/v1alpha1/phase5_validation.go", r'''package v1alpha1

import (
    "fmt"
    "strings"
    "time"

    utilvalidation "k8s.io/apimachinery/pkg/util/validation"
)

func ValidateTransferIntent(intent *SpaceTransferIntent, clock Clock) error {
    var errs ValidationErrors
    if intent == nil { errs.add("intent", "is required"); return errs }
    if clock == nil { errs.add("clock", "is required"); return errs }
    validateReceiptIdentity("spec.transferID", intent.Spec.TransferID, &errs)
    validateReceiptCommon(intent.Spec.MissionUID, intent.Spec.PlanID, intent.Spec.Attempt, intent.Spec.Source, intent.Spec.Destination, intent.Spec.Bytes, intent.Spec.PayloadDigest, Provenance{ReporterID:"local", Source:"intent", Digest:strings.Repeat("0",64), Sequence:1}, &errs)
    if intent.Spec.DataID == "" || len(intent.Spec.DataID) > 253 || strings.ContainsAny(intent.Spec.DataID, "\r\n\x00") { errs.add("spec.dataID", "must be non-empty and bounded") }
    if intent.Spec.Window.Start.IsZero() || intent.Spec.Window.End.IsZero() || !intent.Spec.Window.End.After(intent.Spec.Window.Start.Time) { errs.add("spec.window", "must have a positive interval") }
    if intent.Spec.Window.Bytes != intent.Spec.Bytes { errs.add("spec.window.bytes", "must equal spec.bytes") }
    if intent.Spec.ExpiresAt.IsZero() || !intent.Spec.ExpiresAt.After(clock.Now()) { errs.add("spec.expiresAt", "must be in the future") }
    if intent.Name != TransferIntentName(intent.Spec.Source, intent.Spec.Destination, intent.Spec.MissionUID, intent.Spec.PlanID, intent.Spec.TransferID) { errs.add("metadata.name", "must be derived from transfer identity") }
    return errs.errOrNil()
}

func ValidateExecutionFence(path string, fence ExecutionFence, errs *ValidationErrors) {
    if fence.MissionUID == "" || len(fence.MissionUID) > 128 { errs.add(path+".missionUID", "must be non-empty and at most 128 bytes") }
    if problems := utilvalidation.IsDNS1123Label(fence.PlanID); len(problems)>0 { errs.add(path+".planID", strings.Join(problems, ", ")) }
    if fence.Attempt < 1 || fence.Attempt > 100 { errs.add(path+".attempt", "must be between 1 and 100") }
    if fence.LeaseEpoch < 1 { errs.add(path+".leaseEpoch", "must be positive") }
    validateLowerSHA256(path+".tokenHash", fence.TokenHash, errs)
    if fence.ExpiresAt.IsZero() { errs.add(path+".expiresAt", "is required") }
}

func ValidateExecutionLease(lease *SpaceExecutionLease, clock Clock) error {
    var errs ValidationErrors
    if lease == nil { errs.add("lease", "is required"); return errs }
    if clock == nil { errs.add("clock", "is required"); return errs }
    validateDomain("spec.source", lease.Spec.Source, &errs); validateDomain("spec.destination", lease.Spec.Destination, &errs)
    if lease.Spec.Source == lease.Spec.Destination { errs.add("spec.destination", "must differ from source") }
    ValidateExecutionFence("spec.fence", lease.Spec.Fence, &errs)
    validateProvenance("spec.provenance", lease.Spec.Provenance, &errs)
    if lease.Spec.HeartbeatAt.IsZero() { errs.add("spec.heartbeatAt", "is required") }
    if lease.Spec.MaximumClockSkewSeconds < 0 || lease.Spec.MaximumClockSkewSeconds > MaxClockSkewSecs { errs.add("spec.maximumClockSkewSeconds", fmt.Sprintf("must be between 0 and %d", MaxClockSkewSecs)) }
    skew := time.Duration(lease.Spec.MaximumClockSkewSeconds) * time.Second
    if lease.Spec.HeartbeatAt.After(clock.Now().Add(skew)) { errs.add("spec.heartbeatAt", "is beyond allowed clock skew") }
    if !lease.Spec.Fence.ExpiresAt.After(lease.Spec.HeartbeatAt.Time) { errs.add("spec.fence.expiresAt", "must be after heartbeatAt") }
    if lease.Name != ExecutionLeaseName(lease.Spec.Fence.MissionUID, lease.Spec.Fence.PlanID, lease.Spec.Fence.Attempt, lease.Spec.Fence.LeaseEpoch) { errs.add("metadata.name", "must be derived from fence identity") }
    return errs.errOrNil()
}

func ValidateExecutionObservation(observation *SpaceExecutionObservation, clock Clock) error {
    var errs ValidationErrors
    if observation == nil { errs.add("observation", "is required"); return errs }
    if clock == nil { errs.add("clock", "is required"); return errs }
    validateReceiptIdentity("spec.observationID", observation.Spec.ObservationID, &errs)
    validateReceiptCommon(observation.Spec.MissionUID, observation.Spec.PlanID, observation.Spec.Attempt, observation.Spec.Source, observation.Spec.Destination, 0, strings.Repeat("0",64), observation.Spec.Provenance, &errs)
    if observation.Spec.LeaseEpoch < 1 { errs.add("spec.leaseEpoch", "must be positive") }
    validateLowerSHA256("spec.tokenHash", observation.Spec.TokenHash, &errs)
    switch observation.Spec.Phase {
    case ExecutionObservationHeartbeat, ExecutionObservationStopped, ExecutionObservationCompleted, ExecutionObservationFailed:
        if observation.Spec.CheckpointID != "" { errs.add("spec.checkpointID", "is allowed only for Checkpointed phase") }
    case ExecutionObservationCheckpointed:
        if observation.Spec.CheckpointID == "" || len(observation.Spec.CheckpointID) > 253 { errs.add("spec.checkpointID", "is required and bounded for Checkpointed phase") }
    default:
        errs.add("spec.phase", "is not a supported execution observation phase")
    }
    if observation.Spec.ObservedAt.IsZero() || observation.Spec.ObservedAt.After(clock.Now().Add(time.Duration(MaxClockSkewSecs)*time.Second)) { errs.add("spec.observedAt", "is required and cannot exceed clock skew") }
    if observation.Name != ExecutionObservationName(observation.Spec.Source, observation.Spec.Destination, observation.Spec.MissionUID, observation.Spec.PlanID, observation.Spec.ObservationID) { errs.add("metadata.name", "must be derived from observation identity") }
    return errs.errOrNil()
}
''')

# Admission supports signed execution evidence and controlled gateway persistence.
replace_once(
    "contrib/space-compute/pkg/admission/validator.go",
    '''\tprincipal := request.UserInfo.Username
\tif principal == "" || current.provenance.ReporterID != principal {
\t\treturn fmt.Errorf("spec.provenance.reporterID must equal authenticated principal")
\t}
\tbinding, err := v.trust.Binding(ctx, principal)''',
    '''\tprincipal := request.UserInfo.Username
\tif principal == "" { return fmt.Errorf("authenticated principal is required") }
\treporterPrincipal := current.provenance.ReporterID
\tif reporterPrincipal == "" { return fmt.Errorf("spec.provenance.reporterID is required") }
\tbinding, err := v.trust.Binding(ctx, reporterPrincipal)''')
replace_once(
    "contrib/space-compute/pkg/admission/validator.go",
    '''\tif binding.Spec.ReporterPrincipal != principal {
\t\treturn fmt.Errorf("reporter binding principal does not match authenticated principal")
\t}''',
    '''\tif binding.Spec.ReporterPrincipal != reporterPrincipal {
\t\treturn fmt.Errorf("reporter binding principal does not match signed reporter identity")
\t}
\tif principal != reporterPrincipal && !principalAllowed(binding.Spec.AllowedGateways, principal) {
\t\treturn fmt.Errorf("authenticated principal is neither the signed reporter nor an explicitly allowed transport gateway")
\t}''')
replace_once(
    "contrib/space-compute/pkg/admission/validator.go",
    '''\tcase "spaceresultreceipts":
\t\tvalue := &spacev1.SpaceResultReceipt{}''',
    '''\tcase "spaceexecutionleases":
\t\tvalue := &spacev1.SpaceExecutionLease{}
\t\tif err := decodeRaw(raw, value); err != nil { return nil, fmt.Errorf("decode SpaceExecutionLease: %w", err) }
\t\tif err := spacev1.ValidateExecutionLease(value, v.clock); err != nil { return nil, err }
\t\tdestination := value.Spec.Destination
\t\treturn &reporterEnvelope{kind:"SpaceExecutionLease", name:value.Name, provenance:&value.Spec.Provenance, source:value.Spec.Source, destination:&destination, digestObject:value, observedAtNano:value.Spec.HeartbeatAt.UnixNano(), identity:normalizedEnvelopeIdentity(value.Spec.Source, &value.Spec.Destination, fmt.Sprintf("%s|%s|%d|%d", value.Spec.Fence.MissionUID, value.Spec.Fence.PlanID, value.Spec.Fence.Attempt, value.Spec.Fence.LeaseEpoch))}, nil
\tcase "spaceexecutionobservations":
\t\tvalue := &spacev1.SpaceExecutionObservation{}
\t\tif err := decodeRaw(raw, value); err != nil { return nil, fmt.Errorf("decode SpaceExecutionObservation: %w", err) }
\t\tif err := spacev1.ValidateExecutionObservation(value, v.clock); err != nil { return nil, err }
\t\tdestination := value.Spec.Destination
\t\treturn &reporterEnvelope{kind:"SpaceExecutionObservation", name:value.Name, provenance:&value.Spec.Provenance, source:value.Spec.Source, destination:&destination, digestObject:value, observedAtNano:value.Spec.ObservedAt.UnixNano(), identity:normalizedEnvelopeIdentity(value.Spec.Source, &value.Spec.Destination, fmt.Sprintf("%s|%s|%d|%d|%s", value.Spec.MissionUID, value.Spec.PlanID, value.Spec.Attempt, value.Spec.LeaseEpoch, value.Spec.ObservationID))}, nil
\tcase "spaceresultreceipts":
\t\tvalue := &spacev1.SpaceResultReceipt{}''')
replace_once(
    "contrib/space-compute/pkg/admission/validator.go",
    '''\tcase *spacev1.SpaceResultReceipt:
\t\treturn spacev1.ResultReceiptName(object.Spec.Source, object.Spec.Destination, object.Spec.MissionUID, object.Spec.PlanID, object.Spec.ResultID)''',
    '''\tcase *spacev1.SpaceExecutionLease:
\t\treturn spacev1.ExecutionLeaseName(object.Spec.Fence.MissionUID, object.Spec.Fence.PlanID, object.Spec.Fence.Attempt, object.Spec.Fence.LeaseEpoch)
\tcase *spacev1.SpaceExecutionObservation:
\t\treturn spacev1.ExecutionObservationName(object.Spec.Source, object.Spec.Destination, object.Spec.MissionUID, object.Spec.PlanID, object.Spec.ObservationID)
\tcase *spacev1.SpaceResultReceipt:
\t\treturn spacev1.ResultReceiptName(object.Spec.Source, object.Spec.Destination, object.Spec.MissionUID, object.Spec.PlanID, object.Spec.ResultID)''')
text = read("contrib/space-compute/pkg/admission/validator.go")
append_marker = '''func peerAllowed(values []spacev1.DomainReference, target spacev1.DomainReference) bool {'''
if text.count(append_marker) != 1: raise SystemExit("peer marker mismatch")
helper = '''func principalAllowed(values []string, target string) bool {\n\tfor _, value := range values { if strings.TrimSpace(value) == target { return true } }\n\treturn false\n}\n\n'''
write("contrib/space-compute/pkg/admission/validator.go", text.replace(append_marker, helper+append_marker,1))

print("stage5 API patch applied")
