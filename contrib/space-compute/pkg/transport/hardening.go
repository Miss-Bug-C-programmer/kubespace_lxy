package transport

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
	spaceexecution "github.com/k3s-io/k3s/contrib/space-compute/pkg/execution"
)

type LeaseAck struct {
	Lease spacev1.SpaceExecutionLease `json:"lease"`
}

type fenceExecutor interface {
	FenceExecution(context.Context, *spacev1.SpaceMission, *spacev1.SpacePlacementIntent, string) (bool, error)
}

type agentClock struct{ now time.Time }

func (c agentClock) Now() time.Time { return c.now }

type leaseConfirmation struct {
	Sequence int64  `json:"sequence"`
	Digest   string `json:"digest"`
}

func (a *Agent) confirmationPath(lease *spacev1.SpaceExecutionLease) string {
	return filepath.Join(a.StateDir, "lease-confirmations", lease.Name+".json")
}

func (a *Agent) markLeaseConfirmed(lease *spacev1.SpaceExecutionLease) error {
	if lease == nil {
		return fmt.Errorf("lease is required")
	}
	return writeAtomic(a.confirmationPath(lease), leaseConfirmation{Sequence: lease.Spec.Provenance.Sequence, Digest: lease.Spec.Provenance.Digest})
}

func (a *Agent) leaseConfirmed(lease *spacev1.SpaceExecutionLease) (bool, error) {
	if lease == nil {
		return false, nil
	}
	if lease.Spec.Destination == a.Local {
		return true, nil
	}
	var confirmation leaseConfirmation
	if err := readJSON(a.confirmationPath(lease), &confirmation); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return confirmation.Sequence >= lease.Spec.Provenance.Sequence && confirmation.Digest == lease.Spec.Provenance.Digest, nil
}

func latestMissionEpoch(leases []*spacev1.SpaceExecutionLease, missionUID string) int64 {
	var max int64
	for _, lease := range leases {
		if lease != nil && lease.Spec.Fence.MissionUID == missionUID && lease.Spec.Fence.LeaseEpoch > max {
			max = lease.Spec.Fence.LeaseEpoch
		}
	}
	return max
}

func latestLeaseForAttemptAny(leases []*spacev1.SpaceExecutionLease, missionUID, planID string, attempt int32) *spacev1.SpaceExecutionLease {
	var best *spacev1.SpaceExecutionLease
	for _, lease := range leases {
		if lease == nil {
			continue
		}
		f := lease.Spec.Fence
		if f.MissionUID == missionUID && f.PlanID == planID && f.Attempt == attempt && (best == nil || f.LeaseEpoch > best.Spec.Fence.LeaseEpoch) {
			best = lease
		}
	}
	return best
}

func latestLeaseForMissionAttemptAny(leases []*spacev1.SpaceExecutionLease, missionUID string, attempt int32) *spacev1.SpaceExecutionLease {
	var best *spacev1.SpaceExecutionLease
	for _, lease := range leases {
		if lease == nil || lease.Spec.Fence.MissionUID != missionUID || lease.Spec.Fence.Attempt != attempt {
			continue
		}
		if best == nil || lease.Spec.Fence.LeaseEpoch > best.Spec.Fence.LeaseEpoch {
			best = lease
		}
	}
	return best
}

func priorFenceProof(mission *spacev1.SpaceMission, placement *spacev1.SpacePlacementIntent, leases []*spacev1.SpaceExecutionLease, observations []*spacev1.SpaceExecutionObservation, now time.Time) (*spacev1.SpaceExecutionLease, []spacev1.SpaceExecutionObservation, error) {
	previous := latestLeaseForMissionAttemptAny(leases, string(mission.UID), placement.Spec.Attempt-1)
	if previous == nil {
		return nil, nil, fmt.Errorf("previous attempt has no trusted execution lease")
	}
	proof := make([]spacev1.SpaceExecutionObservation, 0, 2)
	for _, observation := range observations {
		if observation != nil && spaceexecution.ValidateObservationAgainstLease(observation, previous, now) == nil {
			proof = append(proof, *observation.DeepCopy())
		}
	}
	if err := spaceexecution.CanStartAttempt(mission, previous, observations, now); err != nil {
		return nil, nil, err
	}
	return previous.DeepCopy(), proof, nil
}

func (a *Agent) peerPublicKey(domain spacev1.DomainReference) (ed25519.PublicKey, error) {
	if domain == a.Local {
		key, ok := a.PrivateKey.Public().(ed25519.PublicKey)
		if !ok {
			return nil, fmt.Errorf("local signing public key is not Ed25519")
		}
		return key, nil
	}
	if a.PeerKeys == nil {
		return nil, fmt.Errorf("peer key registry unavailable")
	}
	return a.PeerKeys.PublicKey(domain)
}

func (a *Agent) verifyReporterEvidence(object runtime.Object, provenance spacev1.Provenance, source spacev1.DomainReference) error {
	digest, err := spacev1.ReporterDigest(object)
	if err != nil {
		return err
	}
	if digest != provenance.Digest {
		return fmt.Errorf("reporter evidence digest mismatch")
	}
	digestBytes, err := hex.DecodeString(digest)
	if err != nil {
		return err
	}
	sig, err := base64.StdEncoding.DecodeString(provenance.Signature)
	if err != nil {
		return err
	}
	publicKey, err := a.peerPublicKey(source)
	if err != nil {
		return err
	}
	if !ed25519.Verify(publicKey, digestBytes, sig) {
		return fmt.Errorf("reporter evidence signature verification failed")
	}
	return nil
}

func (a *Agent) verifyPriorFenceEvidence(mission *spacev1.SpaceMission, previous *spacev1.SpaceExecutionLease, values []spacev1.SpaceExecutionObservation) error {
	if previous == nil {
		return fmt.Errorf("previous lease required")
	}
	if err := spacev1.ValidateExecutionLease(previous, agentClock{a.now()}); err != nil {
		return err
	}
	if err := a.verifyReporterEvidence(previous, previous.Spec.Provenance, previous.Spec.Source); err != nil {
		return fmt.Errorf("verify previous lease: %w", err)
	}
	observations := make([]*spacev1.SpaceExecutionObservation, 0, len(values))
	for i := range values {
		value := values[i].DeepCopy()
		if err := spacev1.ValidateExecutionObservation(value, agentClock{a.now()}); err != nil {
			return err
		}
		if err := a.verifyReporterEvidence(value, value.Spec.Provenance, value.Spec.Source); err != nil {
			return fmt.Errorf("verify previous observation: %w", err)
		}
		observations = append(observations, value)
	}
	return spaceexecution.CanStartAttempt(mission, previous, observations, a.now())
}

func validateIncomingLease(values []*spacev1.SpaceExecutionLease, next *spacev1.SpaceExecutionLease, now time.Time) error {
	if err := spaceexecution.ValidateLease(next, now); err != nil {
		return err
	}
	var latest *spacev1.SpaceExecutionLease
	var same *spacev1.SpaceExecutionLease
	for _, value := range values {
		if value == nil || value.Spec.Fence.MissionUID != next.Spec.Fence.MissionUID {
			continue
		}
		if latest == nil || value.Spec.Fence.LeaseEpoch > latest.Spec.Fence.LeaseEpoch {
			latest = value
		}
		if value.Name == next.Name {
			same = value
		}
	}
	if latest == nil {
		return nil
	}
	if next.Spec.Fence.LeaseEpoch < latest.Spec.Fence.LeaseEpoch {
		return fmt.Errorf("lease epoch %d is stale; observed %d", next.Spec.Fence.LeaseEpoch, latest.Spec.Fence.LeaseEpoch)
	}
	if next.Spec.Fence.LeaseEpoch > latest.Spec.Fence.LeaseEpoch {
		return spaceexecution.ValidateLeaseAdvance(latest, next, now)
	}
	if same == nil {
		return fmt.Errorf("conflicting lease shares current epoch %d", next.Spec.Fence.LeaseEpoch)
	}
	if same.Spec.Provenance.Sequence == next.Spec.Provenance.Sequence && same.Spec.Provenance.Digest == next.Spec.Provenance.Digest {
		return nil
	}
	return spaceexecution.ValidateLeaseAdvance(same, next, now)
}

func (a *Agent) enqueueLeaseAck(destination spacev1.DomainReference, lease *spacev1.SpaceExecutionLease) error {
	raw, err := json.Marshal(LeaseAck{Lease: *lease.DeepCopy()})
	if err != nil {
		return err
	}
	expiry := lease.Spec.Fence.ExpiresAt.Time.Add(a.Limits.MaximumClockSkew)
	if !expiry.After(a.now()) {
		expiry = a.now().Add(a.Limits.MaximumClockSkew + time.Second)
	}
	e := NewEnvelope("lease-ack-"+lease.Name, LeaseAckKind, a.Local, destination, lease.Spec.Fence.MissionUID, lease.Spec.Fence.PlanID, lease.Spec.Fence.Attempt, lease.Spec.Provenance.Sequence, a.now(), expiry, raw)
	if err := e.Sign(a.PrivateKey); err != nil {
		return err
	}
	return a.Queue.Enqueue(e)
}

func (a *Agent) acceptLeaseAck(ctx context.Context, e *Envelope, ack *LeaseAck) error {
	lease := &ack.Lease
	if lease.Spec.Source != a.Local || lease.Spec.Destination != e.Source || lease.Spec.Fence.MissionUID != e.MissionUID || lease.Spec.Fence.PlanID != e.PlanID || lease.Spec.Fence.Attempt != e.Attempt || lease.Spec.Provenance.Sequence != e.Sequence {
		return fmt.Errorf("lease ack metadata mismatch")
	}
	if err := a.verifyReporterEvidence(lease, lease.Spec.Provenance, a.Local); err != nil {
		return err
	}
	leases, err := a.Store.ListExecutionLeases(ctx)
	if err != nil {
		return err
	}
	current := latestLeaseForAttemptAny(leases, lease.Spec.Fence.MissionUID, lease.Spec.Fence.PlanID, lease.Spec.Fence.Attempt)
	if current == nil {
		return fmt.Errorf("local lease for ack not found")
	}
	if current.Spec.Provenance.Sequence != lease.Spec.Provenance.Sequence || current.Spec.Provenance.Digest != lease.Spec.Provenance.Digest {
		if err := validateIncomingLease(leases, lease, a.now()); err != nil {
			return err
		}
		if err := a.Store.UpsertExecutionLease(ctx, lease); err != nil {
			return err
		}
	}
	return a.markLeaseConfirmed(lease)
}

func (a *Agent) acceptReporterObject(ctx context.Context, e *Envelope, object *ReporterObject) error {
	switch object.Resource {
	case "spaceexecutionleases":
		var lease spacev1.SpaceExecutionLease
		if err := json.Unmarshal(object.Object, &lease); err != nil {
			return err
		}
		if lease.Spec.Source != e.Source || lease.Spec.Destination != a.Local || lease.Spec.Fence.MissionUID != e.MissionUID || lease.Spec.Fence.PlanID != e.PlanID || lease.Spec.Fence.Attempt != e.Attempt || lease.Spec.Provenance.Sequence != e.Sequence {
			return fmt.Errorf("lease reporter envelope metadata mismatch")
		}
		leases, err := a.Store.ListExecutionLeases(ctx)
		if err != nil {
			return err
		}
		if err := validateIncomingLease(leases, &lease, a.now()); err != nil {
			return err
		}
		if err := a.Store.UpsertRemoteReporterObject(ctx, object.Resource, object.Object); err != nil {
			return err
		}
		return a.enqueueLeaseAck(lease.Spec.Source, &lease)
	case "spaceexecutionobservations":
		var observation spacev1.SpaceExecutionObservation
		if err := json.Unmarshal(object.Object, &observation); err != nil {
			return err
		}
		leases, err := a.Store.ListExecutionLeases(ctx)
		if err != nil {
			return err
		}
		if observation.Spec.LeaseEpoch != latestMissionEpoch(leases, observation.Spec.MissionUID) {
			return fmt.Errorf("execution observation uses stale lease epoch")
		}
		lease := latestLeaseForAttemptAny(leases, observation.Spec.MissionUID, observation.Spec.PlanID, observation.Spec.Attempt)
		if lease == nil || spaceexecution.ValidateObservationAgainstLease(&observation, lease, a.now()) != nil {
			return fmt.Errorf("execution observation does not match current trusted fence")
		}
	case "spaceresultreceipts":
		var receipt spacev1.SpaceResultReceipt
		if err := json.Unmarshal(object.Object, &receipt); err != nil {
			return err
		}
		leases, err := a.Store.ListExecutionLeases(ctx)
		if err != nil {
			return err
		}
		if receipt.Spec.LeaseEpoch != latestMissionEpoch(leases, receipt.Spec.MissionUID) {
			return fmt.Errorf("result receipt uses stale lease epoch")
		}
		lease := latestLeaseForAttemptAny(leases, receipt.Spec.MissionUID, receipt.Spec.PlanID, receipt.Spec.Attempt)
		if lease == nil || spaceexecution.ValidateResultAgainstLease(&receipt, lease, a.now()) != nil {
			return fmt.Errorf("result receipt does not match current trusted fence")
		}
	case "spacetransferreceipts":
	default:
		return fmt.Errorf("remote reporter resource %q is not allowed", object.Resource)
	}
	return a.Store.UpsertRemoteReporterObject(ctx, object.Resource, object.Object)
}

func (a *Agent) enqueueTransferIntent(intent *spacev1.SpaceTransferIntent, destination spacev1.DomainReference) error {
	copy := intent.DeepCopy()
	copy.ResourceVersion = ""
	copy.UID = types.UID("")
	copy.Generation = 0
	copy.CreationTimestamp = metav1.Time{}
	copy.DeletionTimestamp = nil
	copy.DeletionGracePeriodSeconds = nil
	copy.ManagedFields = nil
	copy.OwnerReferences = nil
	copy.Finalizers = nil
	raw, err := json.Marshal(copy)
	if err != nil {
		return err
	}
	id := transferIntentEnvelopeID(copy.Name, destination)
	e := NewEnvelope(id, TransferIntentKind, a.Local, destination, copy.Spec.MissionUID, copy.Spec.PlanID, copy.Spec.Attempt, 1, a.now(), copy.Spec.ExpiresAt.Time, raw)
	if err := e.Sign(a.PrivateKey); err != nil {
		return err
	}
	return a.Queue.Enqueue(e)
}

func transferIntentEnvelopeID(name string, destination spacev1.DomainReference) string {
	sum := sha256.Sum256([]byte("transfer-intent|" + name + "|" + strings.ToLower(string(destination.OrbitClass)+"/"+destination.ClusterID+"/"+destination.Name)))
	return "transfer-intent-" + hex.EncodeToString(sum[:20])
}

func validateChunkAgainstIntent(chunk *TransferChunk, intent *spacev1.SpaceTransferIntent, local spacev1.DomainReference, now time.Time) error {
	if chunk == nil || intent == nil {
		return fmt.Errorf("chunk and transfer intent are required")
	}
	i := intent.Spec
	if i.Destination != local || i.TransferID != chunk.TransferID || i.MissionUID != chunk.MissionUID || i.PlanID != chunk.PlanID || i.Attempt != chunk.Attempt || i.Purpose != chunk.Purpose || i.Source != chunk.Source || i.Destination != chunk.Destination || i.DataID != chunk.DataID || i.Bytes != chunk.TotalBytes || i.PayloadDigest != chunk.PayloadDigest || i.LeaseEpoch != chunk.LeaseEpoch || i.TokenHash != chunk.TokenHash {
		return fmt.Errorf("transfer chunk does not match durable transfer intent")
	}
	if now.Before(i.Window.Start.Time) || !i.Window.End.After(now) || !i.ExpiresAt.After(now) {
		return fmt.Errorf("transfer chunk is outside the trusted transfer window")
	}
	if chunk.StartedAt.Before(i.Window.Start.Time) || chunk.StartedAt.After(now.Add(time.Second)) {
		return fmt.Errorf("transfer chunk start time is outside trusted bounds")
	}
	return nil
}

func hasTransferReceipt(intent *spacev1.SpaceTransferIntent, receipts []*spacev1.SpaceTransferReceipt) bool {
	if intent == nil {
		return false
	}
	for _, receipt := range receipts {
		if receipt == nil {
			continue
		}
		s := receipt.Spec
		i := intent.Spec
		if s.TransferID == i.TransferID && s.MissionUID == i.MissionUID && s.PlanID == i.PlanID && s.Attempt == i.Attempt && s.Source == i.Source && s.Destination == i.Destination && s.DataID == i.DataID && s.Bytes == i.Bytes && s.PayloadDigest == i.PayloadDigest {
			return true
		}
	}
	return false
}

func uniqueDomains(values ...spacev1.DomainReference) []spacev1.DomainReference {
	seen := map[string]struct{}{}
	out := make([]spacev1.DomainReference, 0, len(values))
	for _, value := range values {
		key := strings.ToLower(string(value.OrbitClass) + "/" + value.ClusterID + "/" + value.Name)
		if _, ok := seen[key]; ok || value.Name == "" || value.ClusterID == "" {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, value)
	}
	return out
}

func reporterEnvelopeID(resource, name string, destination spacev1.DomainReference) string {
	sum := sha256.Sum256([]byte(resource + "|" + name + "|" + strings.ToLower(string(destination.OrbitClass)+"/"+destination.ClusterID+"/"+destination.Name)))
	return "reporter-" + hex.EncodeToString(sum[:20])
}

func assignmentForLease(lease *spacev1.SpaceExecutionLease, groups ...[]Assignment) *Assignment {
	if lease == nil {
		return nil
	}
	f := lease.Spec.Fence
	for _, values := range groups {
		for i := range values {
			a := &values[i]
			if a.Mission != nil && a.Placement != nil && string(a.Mission.UID) == f.MissionUID && a.Placement.Spec.PlanID == f.PlanID && a.Placement.Spec.Attempt == f.Attempt {
				return a
			}
		}
	}
	return nil
}

func terminalObservation(lease *spacev1.SpaceExecutionLease, observations []*spacev1.SpaceExecutionObservation, now time.Time) *spacev1.SpaceExecutionObservation {
	valid := make([]*spacev1.SpaceExecutionObservation, 0, len(observations))
	for _, observation := range observations {
		if observation == nil || spaceexecution.ValidateObservationAgainstLease(observation, lease, now) != nil {
			continue
		}
		switch observation.Spec.Phase {
		case spacev1.ExecutionObservationStopped, spacev1.ExecutionObservationCompleted, spacev1.ExecutionObservationFailed:
			valid = append(valid, observation)
		}
	}
	if len(valid) == 0 {
		return nil
	}
	sort.Slice(valid, func(i, j int) bool { return valid[i].Spec.ObservedAt.After(valid[j].Spec.ObservedAt.Time) })
	return valid[0]
}

func matchingReportObservation(report spaceexecution.Report, lease *spacev1.SpaceExecutionLease, observations []*spacev1.SpaceExecutionObservation) *spacev1.SpaceExecutionObservation {
	for _, observation := range observations {
		if observation == nil {
			continue
		}
		s := observation.Spec
		if s.MissionUID == report.MissionUID && s.PlanID == report.PlanID && s.Attempt == report.Attempt && s.LeaseEpoch == report.LeaseEpoch && s.TokenHash == lease.Spec.Fence.TokenHash && s.Phase == report.Phase && s.CheckpointID == report.CheckpointID {
			return observation
		}
	}
	return nil
}

func reportObservationID(report spaceexecution.Report, now time.Time) string {
	phase := strings.ToLower(string(report.Phase))
	if report.Phase == spacev1.ExecutionObservationHeartbeat {
		return fmt.Sprintf("heartbeat-%d-%d", report.LeaseEpoch, now.UnixNano())
	}
	extra := report.CheckpointID
	if extra != "" {
		sum := sha256.Sum256([]byte(extra))
		extra = "-" + hex.EncodeToString(sum[:4])
	}
	id := fmt.Sprintf("report-%s-%d%s", phase, report.LeaseEpoch, extra)
	if len(id) > 63 {
		id = id[:63]
	}
	return id
}

func (a *Agent) ensureResultReturn(ctx context.Context, assignment Assignment, lease *spacev1.SpaceExecutionLease, report spaceexecution.Report) error {
	if report.Phase != spacev1.ExecutionObservationCompleted || assignment.Mission == nil || !assignment.Mission.Spec.ResultReturnRequired {
		return nil
	}
	if report.ResultDataID == "" {
		return fmt.Errorf("completed execution requires resultDataID when result return is required")
	}
	if assignment.Placement == nil || assignment.Placement.Spec.ResultTransfer == nil {
		return fmt.Errorf("result return is required but placement has no result transfer")
	}
	path, err := DataPath(a.DataRoot, report.ResultDataID)
	if err != nil {
		return err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if assignment.Mission.Spec.OutputSizeBytes > 0 && int64(len(raw)) != assignment.Mission.Spec.OutputSizeBytes {
		return fmt.Errorf("result size does not match mission outputSizeBytes")
	}
	digestBytes := sha256.Sum256(raw)
	digest := hex.EncodeToString(digestBytes[:])
	transfer := *assignment.Placement.Spec.ResultTransfer
	transfer.DataID = report.ResultDataID
	transfer.Source = a.Local
	transfer.Bytes = int64(len(raw))
	coordinator := lease.Spec.Destination
	if transfer.Destination == a.Local {
		result := &spacev1.SpaceResultReceipt{TypeMeta: metav1.TypeMeta{APIVersion: spacev1.SchemeGroupVersion.String(), Kind: "SpaceResultReceipt"}, ObjectMeta: metav1.ObjectMeta{Name: spacev1.ResultReceiptName(a.Local, a.Local, report.MissionUID, report.PlanID, spacev1.ResultTransferID(report.Attempt))}, Spec: spacev1.SpaceResultReceiptSpec{ResultID: spacev1.ResultTransferID(report.Attempt), MissionUID: report.MissionUID, PlanID: report.PlanID, Attempt: report.Attempt, Source: a.Local, Destination: a.Local, Bytes: int64(len(raw)), PayloadDigest: digest, LeaseEpoch: lease.Spec.Fence.LeaseEpoch, TokenHash: lease.Spec.Fence.TokenHash, CompletedAt: metav1.NewTime(a.now()), Provenance: a.baseProvenance(1)}}
		if err := a.signResultReceipt(result); err != nil {
			return err
		}
		if err := a.Store.UpsertResultReceipt(ctx, result); err != nil {
			return err
		}
		if coordinator != a.Local {
			return a.enqueueReporterObject(coordinator, "spaceresultreceipts", result, report.MissionUID, report.PlanID, report.Attempt, 1, assignment.Placement.Spec.ExpiresAt.Time)
		}
		return nil
	}
	transferID := spacev1.ResultTransferID(report.Attempt)
	intent := &spacev1.SpaceTransferIntent{TypeMeta: metav1.TypeMeta{APIVersion: spacev1.SchemeGroupVersion.String(), Kind: "SpaceTransferIntent"}, ObjectMeta: metav1.ObjectMeta{Name: spacev1.TransferIntentName(a.Local, transfer.Destination, report.MissionUID, report.PlanID, transferID)}, Spec: spacev1.SpaceTransferIntentSpec{TransferID: transferID, MissionUID: report.MissionUID, PlanID: report.PlanID, Attempt: report.Attempt, Purpose: spacev1.TransferPurposeResult, Coordinator: coordinator, Source: a.Local, Destination: transfer.Destination, DataID: report.ResultDataID, Bytes: int64(len(raw)), PayloadDigest: digest, LeaseEpoch: lease.Spec.Fence.LeaseEpoch, TokenHash: lease.Spec.Fence.TokenHash, Window: transfer, ExpiresAt: assignment.Placement.Spec.ExpiresAt}}
	return a.Store.UpsertTransferIntent(ctx, intent)
}

func (a *Agent) fenceRemoteExecution(ctx context.Context, assignment Assignment, lease *spacev1.SpaceExecutionLease, reason string, observations []*spacev1.SpaceExecutionObservation) error {
	if terminal := terminalObservation(lease, observations, a.now()); terminal != nil && terminal.Spec.Phase == spacev1.ExecutionObservationStopped {
		return nil
	}
	executor, ok := a.Executor.(fenceExecutor)
	if !ok {
		return fmt.Errorf("executor does not implement execution fencing")
	}
	deleted, err := executor.FenceExecution(ctx, assignment.Mission, assignment.Placement, reason)
	if err != nil {
		return err
	}
	if !deleted {
		return nil
	}
	id := fmt.Sprintf("stopped-%d-%d", lease.Spec.Fence.LeaseEpoch, a.now().UnixNano())
	if len(id) > 63 {
		id = id[:63]
	}
	observation := &spacev1.SpaceExecutionObservation{TypeMeta: metav1.TypeMeta{APIVersion: spacev1.SchemeGroupVersion.String(), Kind: "SpaceExecutionObservation"}, ObjectMeta: metav1.ObjectMeta{Name: spacev1.ExecutionObservationName(a.Local, lease.Spec.Destination, lease.Spec.Fence.MissionUID, lease.Spec.Fence.PlanID, id)}, Spec: spacev1.SpaceExecutionObservationSpec{ObservationID: id, MissionUID: lease.Spec.Fence.MissionUID, PlanID: lease.Spec.Fence.PlanID, Attempt: lease.Spec.Fence.Attempt, LeaseEpoch: lease.Spec.Fence.LeaseEpoch, TokenHash: lease.Spec.Fence.TokenHash, Source: a.Local, Destination: lease.Spec.Destination, Phase: spacev1.ExecutionObservationStopped, ObservedAt: metav1.NewTime(a.now()), Provenance: a.baseProvenance(1)}}
	if err := a.signObservation(observation); err != nil {
		return err
	}
	if observation.Spec.Destination != a.Local {
		expiry := a.now().Add(a.Limits.DiskRetention)
		if assignment.Placement != nil && assignment.Placement.Spec.ExpiresAt.After(a.now()) {
			expiry = assignment.Placement.Spec.ExpiresAt.Time
		}
		if err := a.enqueueReporterObject(observation.Spec.Destination, "spaceexecutionobservations", observation, observation.Spec.MissionUID, observation.Spec.PlanID, observation.Spec.Attempt, 1, expiry); err != nil {
			return err
		}
	}
	return a.Store.UpsertExecutionObservation(ctx, observation)
}
