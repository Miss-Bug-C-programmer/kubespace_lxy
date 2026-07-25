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
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
	spaceexecution "github.com/k3s-io/k3s/contrib/space-compute/pkg/execution"
)

type Assignment struct {
	Mission   *spacev1.SpaceMission         `json:"mission"`
	Placement *spacev1.SpacePlacementIntent `json:"placement"`
}
type LeaseRequest struct {
	Namespace            string                              `json:"namespace"`
	Assignment           Assignment                          `json:"assignment"`
	PreviousLease        *spacev1.SpaceExecutionLease        `json:"previousLease,omitempty"`
	PreviousObservations []spacev1.SpaceExecutionObservation `json:"previousObservations,omitempty"`
}
type LeaseGrant struct {
	Namespace string                      `json:"namespace"`
	Lease     spacev1.SpaceExecutionLease `json:"lease"`
	Token     string                      `json:"token"`
}
type ReporterObject struct {
	Resource string `json:"resource"`
	Object   []byte `json:"object"`
}

type AgentStore interface {
	ListAssignments(context.Context) ([]Assignment, error)
	SaveRemoteAssignment(context.Context, Assignment) error
	ListRemoteAssignments(context.Context) ([]Assignment, error)
	ListTransferIntents(context.Context) ([]*spacev1.SpaceTransferIntent, error)
	GetTransferIntent(context.Context, string) (*spacev1.SpaceTransferIntent, error)
	UpsertTransferIntent(context.Context, *spacev1.SpaceTransferIntent) error
	ListTransferReceipts(context.Context) ([]*spacev1.SpaceTransferReceipt, error)
	UpsertTransferReceipt(context.Context, *spacev1.SpaceTransferReceipt) error
	ListExecutionLeases(context.Context) ([]*spacev1.SpaceExecutionLease, error)
	UpsertExecutionLease(context.Context, *spacev1.SpaceExecutionLease) error
	ListExecutionObservations(context.Context) ([]*spacev1.SpaceExecutionObservation, error)
	UpsertExecutionObservation(context.Context, *spacev1.SpaceExecutionObservation) error
	UpsertResultReceipt(context.Context, *spacev1.SpaceResultReceipt) error
	UpsertRemoteReporterObject(context.Context, string, []byte) error
	PutFenceToken(context.Context, string, spacev1.ExecutionFence, string) error
	GetFenceToken(context.Context, string, spacev1.ExecutionFence) (string, error)
}

type Executor interface {
	EnsureExecution(context.Context, *spacev1.SpaceMission, *spacev1.SpacePlacementIntent, *spacev1.SpaceExecutionLease) error
}

type Agent struct {
	Local             spacev1.DomainReference
	ReporterPrincipal string
	PrivateKey        ed25519.PrivateKey
	PeerKeys          PeerKeys
	StateDir          string
	Queue             *DiskQueue
	Store             AgentStore
	Executor          Executor
	Assembler         *FileAssembler
	DataRoot          string
	LeaseTTL          time.Duration
	LeaseClockSkew    time.Duration
	MaxChunkBytes     int
	Limits            Limits
	Now               func() time.Time
	mu                sync.Mutex
}

func (a *Agent) Validate() error {
	if a.Local.Name == "" || a.Local.ClusterID == "" || a.ReporterPrincipal == "" {
		return fmt.Errorf("local domain and reporter principal are required")
	}
	if len(a.PrivateKey) != ed25519.PrivateKeySize {
		return fmt.Errorf("Ed25519 reporter private key is required")
	}
	if a.Queue == nil || a.Store == nil || a.Assembler == nil || a.PeerKeys == nil || strings.TrimSpace(a.StateDir) == "" {
		return fmt.Errorf("queue, store, assembler, peer keys and persistent state directory are required")
	}
	if a.LeaseTTL < 30*time.Second || a.LeaseTTL > 24*time.Hour {
		return fmt.Errorf("lease TTL out of bounds")
	}
	if a.LeaseClockSkew < 0 || a.LeaseClockSkew > 30*time.Second || 4*a.LeaseClockSkew >= a.LeaseTTL {
		return fmt.Errorf("lease clock skew must be non-negative, at most 30s and below one quarter of lease TTL")
	}
	if a.MaxChunkBytes < 1024 || int64(a.MaxChunkBytes) > a.Limits.MaxMessageBytes/2 {
		return fmt.Errorf("chunk size out of bounds")
	}
	return a.Limits.Validate()
}
func (a *Agent) now() time.Time {
	if a.Now != nil {
		return a.Now().UTC()
	}
	return time.Now().UTC()
}

func (a *Agent) ReconcileOnce(ctx context.Context) error {
	if err := a.Validate(); err != nil {
		return err
	}
	if err := a.reconcileTransfers(ctx); err != nil {
		return err
	}
	if err := a.reconcileAssignments(ctx); err != nil {
		return err
	}
	if err := a.reconcileRemoteAssignments(ctx); err != nil {
		return err
	}
	return a.reconcileHeartbeats(ctx)
}

func (a *Agent) reconcileTransfers(ctx context.Context) error {
	intents, err := a.Store.ListTransferIntents(ctx)
	if err != nil {
		return err
	}
	receipts, err := a.Store.ListTransferReceipts(ctx)
	if err != nil {
		return err
	}
	now := a.now()
	for _, intent := range intents {
		if intent == nil || !intent.Spec.ExpiresAt.After(now) {
			continue
		}
		if err := spacev1.ValidateTransferIntent(intent, agentClock{now}); err != nil {
			return fmt.Errorf("transfer intent %s: %w", intent.Name, err)
		}
		if hasTransferReceipt(intent, receipts) {
			continue
		}
		if intent.Spec.Purpose == spacev1.TransferPurposeInput && intent.Spec.Coordinator == a.Local {
			for _, destination := range uniqueDomains(intent.Spec.Source, intent.Spec.Destination) {
				if destination == a.Local {
					continue
				}
				if err := a.enqueueTransferIntent(intent, destination); err != nil {
					return err
				}
			}
		}
		if intent.Spec.Purpose == spacev1.TransferPurposeResult && intent.Spec.Source == a.Local && intent.Spec.Destination != a.Local {
			if err := a.enqueueTransferIntent(intent, intent.Spec.Destination); err != nil {
				return err
			}
		}
		if intent.Spec.Source != a.Local {
			continue
		}
		// Planner transfer windows are already clock-skew/safety-margin adjusted.
		// Do not turn NotBefore or wall-clock arrival into an assumption of success.
		if now.Before(intent.Spec.Window.Start.Time) || !intent.Spec.Window.End.After(now) {
			continue
		}
		chunks, err := ReadChunks(intent, a.DataRoot, a.MaxChunkBytes)
		if err != nil {
			return fmt.Errorf("transfer %s: %w", intent.Name, err)
		}
		for _, chunk := range chunks {
			sequence := int64(chunk.ChunkIndex) + 1
			queued, err := a.Queue.Contains(intent.Name, sequence)
			if err != nil {
				return err
			}
			if queued {
				continue
			}
			raw, err := json.Marshal(chunk)
			if err != nil {
				return err
			}
			e := NewEnvelope(intent.Name, TransferChunkKind, a.Local, intent.Spec.Destination, intent.Spec.MissionUID, intent.Spec.PlanID, intent.Spec.Attempt, sequence, now, intent.Spec.ExpiresAt.Time, raw)
			if err := e.Sign(a.PrivateKey); err != nil {
				return err
			}
			if err := a.Queue.Enqueue(e); err != nil {
				return err
			}
		}
	}
	return nil
}

func (a *Agent) reconcileAssignments(ctx context.Context) error {
	assignments, err := a.Store.ListAssignments(ctx)
	if err != nil {
		return err
	}
	leases, err := a.Store.ListExecutionLeases(ctx)
	if err != nil {
		return err
	}
	observations, err := a.Store.ListExecutionObservations(ctx)
	if err != nil {
		return err
	}
	now := a.now()
	for _, assignment := range assignments {
		if assignment.Mission == nil || assignment.Placement == nil || !assignment.Placement.Spec.ExpiresAt.After(now) {
			continue
		}
		p := assignment.Placement
		m := assignment.Mission
		lease, _ := spaceexecution.LatestLeaseForAttempt(leases, string(m.UID), p.Spec.PlanID, p.Spec.Attempt, now)
		if lease != nil {
			continue
		}
		var previous *spacev1.SpaceExecutionLease
		var proof []spacev1.SpaceExecutionObservation
		if p.Spec.Attempt > 1 {
			previous, proof, err = priorFenceProof(m, p, leases, observations, now)
			if err != nil || previous == nil {
				// Fail closed: a replacement attempt is not even requested until the
				// coordinator has trusted old-attempt evidence.
				continue
			}
		}
		minEpoch := int64(0)
		if previous != nil {
			minEpoch = previous.Spec.Fence.LeaseEpoch
		}
		if p.Spec.Target == a.Local {
			if _, _, err := a.issueLease(ctx, assignment, a.Local, minEpoch); err != nil {
				return err
			}
			continue
		}
		request := LeaseRequest{Namespace: m.Namespace, Assignment: assignment, PreviousLease: previous, PreviousObservations: proof}
		raw, err := json.Marshal(request)
		if err != nil {
			return err
		}
		id := "lease-request-" + p.Spec.PlanID + fmt.Sprintf("-%d", p.Spec.Attempt)
		e := NewEnvelope(id, LeaseRequestKind, a.Local, p.Spec.Target, string(m.UID), p.Spec.PlanID, p.Spec.Attempt, 1, now, p.Spec.ExpiresAt.Time, raw)
		if err := e.Sign(a.PrivateKey); err != nil {
			return err
		}
		if err := a.Queue.Enqueue(e); err != nil {
			return err
		}
	}
	return nil
}

func (a *Agent) reconcileRemoteAssignments(ctx context.Context) error {
	assignments, err := a.Store.ListRemoteAssignments(ctx)
	if err != nil {
		return err
	}
	leases, err := a.Store.ListExecutionLeases(ctx)
	if err != nil {
		return err
	}
	receipts, err := a.Store.ListTransferReceipts(ctx)
	if err != nil {
		return err
	}
	observations, err := a.Store.ListExecutionObservations(ctx)
	if err != nil {
		return err
	}
	now := a.now()
	for _, assignment := range assignments {
		if assignment.Mission == nil || assignment.Placement == nil || assignment.Placement.Spec.Target != a.Local {
			continue
		}
		m, p := assignment.Mission, assignment.Placement
		lease := latestLeaseForAttemptAny(leases, string(m.UID), p.Spec.PlanID, p.Spec.Attempt)
		if lease == nil {
			continue
		}
		if latestMissionEpoch(leases, string(m.UID)) > lease.Spec.Fence.LeaseEpoch {
			if err := a.fenceRemoteExecution(ctx, assignment, lease, "superseded lease epoch", observations); err != nil {
				return err
			}
			continue
		}
		terminal := terminalObservation(lease, observations, now)
		if terminal != nil {
			if terminal.Spec.Phase == spacev1.ExecutionObservationStopped {
				if err := a.fenceRemoteExecution(ctx, assignment, lease, "trusted stop", observations); err != nil {
					return err
				}
			}
			continue
		}
		confirmed, err := a.leaseConfirmed(lease)
		if err != nil {
			return err
		}
		skew := time.Duration(lease.Spec.MaximumClockSkewSeconds) * time.Second
		safety := skew + 2*time.Second
		if !confirmed || spaceexecution.ValidateLease(lease, now) != nil || !lease.Spec.Fence.ExpiresAt.Time.After(now.Add(safety)) {
			if err := a.fenceRemoteExecution(ctx, assignment, lease, "lease confirmation/expiry fence", observations); err != nil {
				return err
			}
			continue
		}
		if err := spaceexecution.CanDispatch(m, p, lease, receipts, now); err != nil {
			continue
		}
		if a.Executor == nil {
			return fmt.Errorf("remote assignment ready but executor is unavailable")
		}
		if err := a.Executor.EnsureExecution(ctx, m, p, lease); err != nil {
			return err
		}
	}
	return nil
}

func (a *Agent) reconcileHeartbeats(ctx context.Context) error {
	leases, err := a.Store.ListExecutionLeases(ctx)
	if err != nil {
		return err
	}
	localAssignments, err := a.Store.ListAssignments(ctx)
	if err != nil {
		return err
	}
	remoteAssignments, err := a.Store.ListRemoteAssignments(ctx)
	if err != nil {
		return err
	}
	observations, err := a.Store.ListExecutionObservations(ctx)
	if err != nil {
		return err
	}
	now := a.now()
	for _, lease := range leases {
		if lease == nil || lease.Spec.Source != a.Local || latestMissionEpoch(leases, lease.Spec.Fence.MissionUID) != lease.Spec.Fence.LeaseEpoch {
			continue
		}
		assignment := assignmentForLease(lease, localAssignments, remoteAssignments)
		if assignment == nil || assignment.Placement == nil || !assignment.Placement.Spec.ExpiresAt.After(now) {
			continue
		}
		terminal := terminalObservation(lease, observations, now)
		if terminal != nil {
			switch terminal.Spec.Phase {
			case spacev1.ExecutionObservationStopped, spacev1.ExecutionObservationFailed:
				continue
			case spacev1.ExecutionObservationCompleted:
				if assignment.Mission == nil || !assignment.Mission.Spec.ResultReturnRequired {
					continue
				}
			}
		}
		if lease.Spec.Destination != a.Local {
			confirmed, err := a.leaseConfirmed(lease)
			if err != nil {
				return err
			}
			if !confirmed {
				continue
			}
		}
		nextHeartbeat := lease.Spec.HeartbeatAt.Time.Add(a.LeaseTTL / 2)
		if now.Before(nextHeartbeat) {
			continue
		}
		nextExpiry := lease.Spec.Fence.ExpiresAt.Time.Add(a.LeaseTTL / 2)
		if nextExpiry.After(assignment.Placement.Spec.ExpiresAt.Time) {
			nextExpiry = assignment.Placement.Spec.ExpiresAt.Time
		}
		if !nextExpiry.After(nextHeartbeat) || !nextExpiry.After(now.Add(time.Duration(lease.Spec.MaximumClockSkewSeconds)*time.Second)) {
			continue
		}
		next := lease.DeepCopy()
		next.Spec.Provenance.Sequence++
		next.Spec.Provenance.PreviousDigest = lease.Spec.Provenance.Digest
		next.Spec.HeartbeatAt = metav1.NewTime(nextHeartbeat.UTC())
		next.Spec.Fence.ExpiresAt = metav1.NewTime(nextExpiry.UTC())
		if err := a.signLease(next); err != nil {
			return err
		}
		if next.Spec.Destination == a.Local {
			if err := a.Store.UpsertExecutionLease(ctx, next); err != nil {
				return err
			}
			continue
		}
		// Two-phase renewal: the remote execution domain does not persist the
		// extended expiry until the coordinator echoes a LeaseAck.
		if err := a.enqueueReporterObject(next.Spec.Destination, "spaceexecutionleases", next, next.Spec.Fence.MissionUID, next.Spec.Fence.PlanID, next.Spec.Fence.Attempt, next.Spec.Provenance.Sequence, next.Spec.Fence.ExpiresAt.Time); err != nil {
			return err
		}
	}
	return nil
}

func (a *Agent) HandleEnvelope(ctx context.Context, e *Envelope) error {
	if e == nil {
		return fmt.Errorf("envelope required")
	}
	switch e.Kind {
	case TransferIntentKind:
		var intent spacev1.SpaceTransferIntent
		if err := json.Unmarshal(e.Payload, &intent); err != nil {
			return err
		}
		if (a.Local != intent.Spec.Source && a.Local != intent.Spec.Destination) || intent.Spec.MissionUID != e.MissionUID || intent.Spec.PlanID != e.PlanID || intent.Spec.Attempt != e.Attempt {
			return fmt.Errorf("transfer intent envelope metadata mismatch")
		}
		authorized := e.Source == intent.Spec.Coordinator
		if intent.Spec.Purpose == spacev1.TransferPurposeResult && e.Source == intent.Spec.Source {
			authorized = true
		}
		if !authorized {
			return fmt.Errorf("transfer intent sender is not coordinator/source authority")
		}
		if err := spacev1.ValidateTransferIntent(&intent, agentClock{a.now()}); err != nil {
			return err
		}
		return a.Store.UpsertTransferIntent(ctx, &intent)
	case TransferChunkKind:
		var chunk TransferChunk
		if err := json.Unmarshal(e.Payload, &chunk); err != nil {
			return err
		}
		if chunk.Source != e.Source || chunk.Destination != e.Destination || chunk.MissionUID != e.MissionUID || chunk.PlanID != e.PlanID || chunk.Attempt != e.Attempt {
			return fmt.Errorf("transfer chunk metadata does not match envelope")
		}
		intent, err := a.Store.GetTransferIntent(ctx, chunk.IntentName)
		if err != nil {
			return fmt.Errorf("transfer intent is not durable at receiver: %w", err)
		}
		if err := validateChunkAgainstIntent(&chunk, intent, a.Local, a.now()); err != nil {
			return err
		}
		complete, ack, err := a.Assembler.Accept(chunk)
		if err != nil {
			return err
		}
		if complete {
			return a.enqueueAck(ack)
		}
		return nil
	case TransferAckKind:
		var ack TransferAck
		if err := json.Unmarshal(e.Payload, &ack); err != nil {
			return err
		}
		return a.acceptAck(ctx, e, &ack)
	case LeaseRequestKind:
		var request LeaseRequest
		if err := json.Unmarshal(e.Payload, &request); err != nil {
			return err
		}
		return a.acceptLeaseRequest(ctx, e, &request)
	case LeaseGrantKind:
		var grant LeaseGrant
		if err := json.Unmarshal(e.Payload, &grant); err != nil {
			return err
		}
		return a.acceptLeaseGrant(ctx, e, &grant)
	case LeaseAckKind:
		var ack LeaseAck
		if err := json.Unmarshal(e.Payload, &ack); err != nil {
			return err
		}
		return a.acceptLeaseAck(ctx, e, &ack)
	case ReporterObjectKind:
		var object ReporterObject
		if err := json.Unmarshal(e.Payload, &object); err != nil {
			return err
		}
		return a.acceptReporterObject(ctx, e, &object)
	default:
		return fmt.Errorf("unsupported envelope kind %q", e.Kind)
	}
}

func (a *Agent) acceptLeaseRequest(ctx context.Context, e *Envelope, r *LeaseRequest) error {
	if r.Assignment.Mission == nil || r.Assignment.Placement == nil {
		return fmt.Errorf("lease request assignment is required")
	}
	m, p := r.Assignment.Mission, r.Assignment.Placement
	if p.Spec.Target != a.Local || string(m.UID) != e.MissionUID || p.Spec.PlanID != e.PlanID || p.Spec.Attempt != e.Attempt {
		return fmt.Errorf("lease request does not target local domain/assignment")
	}
	if err := spacev1.ValidateMission(m, spacev1.RealClock{}); err != nil {
		return err
	}
	if err := spacev1.ValidatePlacement(p, m); err != nil {
		return err
	}
	minEpoch := int64(0)
	if p.Spec.Attempt > 1 {
		if r.PreviousLease == nil || r.PreviousLease.Spec.Fence.MissionUID != string(m.UID) || r.PreviousLease.Spec.Fence.Attempt != p.Spec.Attempt-1 {
			return fmt.Errorf("replacement lease request lacks exact previous-attempt fence proof")
		}
		if err := a.verifyPriorFenceEvidence(m, r.PreviousLease, r.PreviousObservations); err != nil {
			return err
		}
		minEpoch = r.PreviousLease.Spec.Fence.LeaseEpoch
	}
	if err := a.Store.SaveRemoteAssignment(ctx, r.Assignment); err != nil {
		return err
	}
	lease, token, err := a.issueLease(ctx, r.Assignment, e.Source, minEpoch)
	if err != nil {
		return err
	}
	return a.enqueueLeaseGrant(r.Namespace, lease, token)
}

func (a *Agent) issueLease(ctx context.Context, assignment Assignment, destination spacev1.DomainReference, minimumEpoch int64) (*spacev1.SpaceExecutionLease, string, error) {
	m, p := assignment.Mission, assignment.Placement
	leases, err := a.Store.ListExecutionLeases(ctx)
	if err != nil {
		return nil, "", err
	}
	for _, existing := range leases {
		if existing == nil || existing.Spec.Source != a.Local {
			continue
		}
		f := existing.Spec.Fence
		if f.MissionUID == string(m.UID) && f.PlanID == p.Spec.PlanID && f.Attempt == p.Spec.Attempt {
			if f.LeaseEpoch <= minimumEpoch || spaceexecution.ValidateLease(existing, a.now()) != nil {
				return nil, "", fmt.Errorf("existing lease for attempt is expired/stale; a new attempt is required")
			}
			token, err := a.Store.GetFenceToken(ctx, m.Namespace, f)
			return existing, token, err
		}
	}
	maxEpoch := minimumEpoch
	for _, l := range leases {
		if l != nil && l.Spec.Fence.MissionUID == string(m.UID) && l.Spec.Fence.LeaseEpoch > maxEpoch {
			maxEpoch = l.Spec.Fence.LeaseEpoch
		}
	}
	token, hash, err := spaceexecution.NewFenceToken()
	if err != nil {
		return nil, "", err
	}
	f := spacev1.ExecutionFence{MissionUID: string(m.UID), PlanID: p.Spec.PlanID, Attempt: p.Spec.Attempt, LeaseEpoch: maxEpoch + 1, TokenHash: hash, ExpiresAt: metav1.NewTime(a.now().Add(a.LeaseTTL))}
	lease := &spacev1.SpaceExecutionLease{TypeMeta: metav1.TypeMeta{APIVersion: spacev1.SchemeGroupVersion.String(), Kind: "SpaceExecutionLease"}, ObjectMeta: metav1.ObjectMeta{Name: spacev1.ExecutionLeaseName(f.MissionUID, f.PlanID, f.Attempt, f.LeaseEpoch)}, Spec: spacev1.SpaceExecutionLeaseSpec{Source: a.Local, Destination: destination, Fence: f, HeartbeatAt: metav1.NewTime(a.now()), MaximumClockSkewSeconds: int64(a.LeaseClockSkew / time.Second), Provenance: a.baseProvenance(1)}}
	if err := a.signLease(lease); err != nil {
		return nil, "", err
	}
	if err := a.Store.PutFenceToken(ctx, m.Namespace, f, token); err != nil {
		return nil, "", err
	}
	if err := a.Store.UpsertExecutionLease(ctx, lease); err != nil {
		return nil, "", err
	}
	if destination == a.Local {
		if err := a.markLeaseConfirmed(lease); err != nil {
			return nil, "", err
		}
	}
	return lease, token, nil
}

func (a *Agent) enqueueLeaseGrant(namespace string, lease *spacev1.SpaceExecutionLease, token string) error {
	grant := LeaseGrant{Namespace: namespace, Lease: *lease.DeepCopy(), Token: token}
	raw, err := json.Marshal(grant)
	if err != nil {
		return err
	}
	e := NewEnvelope(lease.Name, LeaseGrantKind, a.Local, lease.Spec.Destination, lease.Spec.Fence.MissionUID, lease.Spec.Fence.PlanID, lease.Spec.Fence.Attempt, lease.Spec.Provenance.Sequence, a.now(), lease.Spec.Fence.ExpiresAt.Time, raw)
	if err := e.Sign(a.PrivateKey); err != nil {
		return err
	}
	return a.Queue.Enqueue(e)
}
func (a *Agent) acceptLeaseGrant(ctx context.Context, e *Envelope, g *LeaseGrant) error {
	l := &g.Lease
	if l.Spec.Source != e.Source || l.Spec.Destination != a.Local || l.Spec.Fence.MissionUID != e.MissionUID || l.Spec.Fence.PlanID != e.PlanID || l.Spec.Fence.Attempt != e.Attempt {
		return fmt.Errorf("lease grant metadata mismatch")
	}
	hash, err := spaceexecution.TokenHash(g.Token)
	if err != nil || hash != l.Spec.Fence.TokenHash {
		return fmt.Errorf("lease grant token hash mismatch")
	}
	leases, err := a.Store.ListExecutionLeases(ctx)
	if err != nil {
		return err
	}
	if err := validateIncomingLease(leases, l, a.now()); err != nil {
		return err
	}
	if err := a.Store.PutFenceToken(ctx, g.Namespace, l.Spec.Fence, g.Token); err != nil {
		return err
	}
	if err := a.Store.UpsertRemoteReporterObject(ctx, "spaceexecutionleases", mustJSON(l)); err != nil {
		return err
	}
	return a.enqueueLeaseAck(l.Spec.Source, l)
}

func (a *Agent) enqueueAck(ack *TransferAck) error {
	raw, err := json.Marshal(ack)
	if err != nil {
		return err
	}
	e := NewEnvelope("ack-"+ack.TransferID, TransferAckKind, a.Local, ack.Destination, ack.MissionUID, ack.PlanID, ack.Attempt, 1, a.now(), a.now().Add(a.Limits.DiskRetention), raw)
	if err := e.Sign(a.PrivateKey); err != nil {
		return err
	}
	return a.Queue.Enqueue(e)
}
func (a *Agent) acceptAck(ctx context.Context, e *Envelope, ack *TransferAck) error {
	intent, err := a.Store.GetTransferIntent(ctx, ack.IntentName)
	if err != nil {
		return err
	}
	s := intent.Spec
	if s.TransferID != ack.TransferID || s.MissionUID != ack.MissionUID || s.PlanID != ack.PlanID || s.Attempt != ack.Attempt || s.Purpose != ack.Purpose || s.Source != ack.Destination || s.Destination != ack.Source || s.DataID != ack.DataID || s.Bytes != ack.Bytes || s.PayloadDigest != ack.PayloadDigest || s.LeaseEpoch != ack.LeaseEpoch || s.TokenHash != ack.TokenHash {
		return fmt.Errorf("transfer ACK does not exactly match local intent")
	}
	receipt := &spacev1.SpaceTransferReceipt{TypeMeta: metav1.TypeMeta{APIVersion: spacev1.SchemeGroupVersion.String(), Kind: "SpaceTransferReceipt"}, ObjectMeta: metav1.ObjectMeta{Name: spacev1.TransferReceiptName(s.Source, s.Destination, s.MissionUID, s.PlanID, s.TransferID)}, Spec: spacev1.SpaceTransferReceiptSpec{TransferID: s.TransferID, MissionUID: s.MissionUID, PlanID: s.PlanID, Attempt: s.Attempt, Source: s.Source, Destination: s.Destination, DataID: s.DataID, Bytes: s.Bytes, PayloadDigest: s.PayloadDigest, StartedAt: metav1.NewTime(ack.StartedAt), CompletedAt: metav1.NewTime(ack.CompletedAt), Provenance: a.baseProvenance(1)}}
	if err := a.signTransferReceipt(receipt); err != nil {
		return err
	}
	if err := a.Store.UpsertTransferReceipt(ctx, receipt); err != nil {
		return err
	}
	for _, destination := range uniqueDomains(s.Destination, s.Coordinator) {
		if destination == a.Local {
			continue
		}
		if err := a.enqueueReporterObject(destination, "spacetransferreceipts", receipt, s.MissionUID, s.PlanID, s.Attempt, 1, intent.Spec.ExpiresAt.Time); err != nil {
			return err
		}
	}
	if s.Purpose == spacev1.TransferPurposeResult {
		result := &spacev1.SpaceResultReceipt{TypeMeta: metav1.TypeMeta{APIVersion: spacev1.SchemeGroupVersion.String(), Kind: "SpaceResultReceipt"}, ObjectMeta: metav1.ObjectMeta{Name: spacev1.ResultReceiptName(s.Source, s.Destination, s.MissionUID, s.PlanID, spacev1.ResultTransferID(s.Attempt))}, Spec: spacev1.SpaceResultReceiptSpec{ResultID: spacev1.ResultTransferID(s.Attempt), MissionUID: s.MissionUID, PlanID: s.PlanID, Attempt: s.Attempt, Source: s.Source, Destination: s.Destination, Bytes: s.Bytes, PayloadDigest: s.PayloadDigest, LeaseEpoch: s.LeaseEpoch, TokenHash: s.TokenHash, CompletedAt: metav1.NewTime(ack.CompletedAt), Provenance: a.baseProvenance(1)}}
		if err := a.signResultReceipt(result); err != nil {
			return err
		}
		if err := a.Store.UpsertResultReceipt(ctx, result); err != nil {
			return err
		}
		for _, destination := range uniqueDomains(s.Destination, s.Coordinator) {
			if destination == a.Local {
				continue
			}
			if err := a.enqueueReporterObject(destination, "spaceresultreceipts", result, s.MissionUID, s.PlanID, s.Attempt, 1, intent.Spec.ExpiresAt.Time); err != nil {
				return err
			}
		}
	}
	return nil
}

func (a *Agent) ReportExecution(ctx context.Context, namespace string, report spaceexecution.Report) error {
	leases, err := a.Store.ListExecutionLeases(ctx)
	if err != nil {
		return err
	}
	if latestMissionEpoch(leases, report.MissionUID) != report.LeaseEpoch {
		return fmt.Errorf("execution report uses a superseded lease epoch")
	}
	lease := latestLeaseForAttemptAny(leases, report.MissionUID, report.PlanID, report.Attempt)
	if lease == nil || lease.Spec.Fence.LeaseEpoch != report.LeaseEpoch {
		return fmt.Errorf("execution lease not found")
	}
	if lease.Spec.Source != a.Local {
		return fmt.Errorf("only local-domain execution may report through this agent")
	}
	if err := spaceexecution.ValidateReport(report, lease, a.now()); err != nil {
		return err
	}
	localAssignments, err := a.Store.ListAssignments(ctx)
	if err != nil {
		return err
	}
	remoteAssignments, err := a.Store.ListRemoteAssignments(ctx)
	if err != nil {
		return err
	}
	assignment := assignmentForLease(lease, localAssignments, remoteAssignments)
	if assignment == nil || assignment.Mission == nil || assignment.Placement == nil || assignment.Placement.Spec.Target != a.Local {
		return fmt.Errorf("execution assignment not found for fence")
	}
	observations, err := a.Store.ListExecutionObservations(ctx)
	if err != nil {
		return err
	}
	if existing := matchingReportObservation(report, lease, observations); existing != nil {
		if err := a.ensureResultReturn(ctx, *assignment, lease, report); err != nil {
			return err
		}
		if existing.Spec.Destination != a.Local {
			return a.enqueueReporterObject(existing.Spec.Destination, "spaceexecutionobservations", existing, report.MissionUID, report.PlanID, report.Attempt, existing.Spec.Provenance.Sequence, assignment.Placement.Spec.ExpiresAt.Time)
		}
		return nil
	}
	if terminalObservation(lease, observations, a.now()) != nil {
		return fmt.Errorf("execution fence is terminal; further token reports are rejected")
	}
	if err := a.ensureResultReturn(ctx, *assignment, lease, report); err != nil {
		return err
	}
	id := reportObservationID(report, a.now())
	obs := &spacev1.SpaceExecutionObservation{TypeMeta: metav1.TypeMeta{APIVersion: spacev1.SchemeGroupVersion.String(), Kind: "SpaceExecutionObservation"}, ObjectMeta: metav1.ObjectMeta{Name: spacev1.ExecutionObservationName(a.Local, lease.Spec.Destination, report.MissionUID, report.PlanID, id)}, Spec: spacev1.SpaceExecutionObservationSpec{ObservationID: id, MissionUID: report.MissionUID, PlanID: report.PlanID, Attempt: report.Attempt, LeaseEpoch: report.LeaseEpoch, TokenHash: lease.Spec.Fence.TokenHash, Source: a.Local, Destination: lease.Spec.Destination, Phase: report.Phase, CheckpointID: report.CheckpointID, ObservedAt: metav1.NewTime(a.now()), Provenance: a.baseProvenance(1)}}
	if err := a.signObservation(obs); err != nil {
		return err
	}
	if obs.Spec.Destination != a.Local {
		if err := a.enqueueReporterObject(obs.Spec.Destination, "spaceexecutionobservations", obs, report.MissionUID, report.PlanID, report.Attempt, 1, assignment.Placement.Spec.ExpiresAt.Time); err != nil {
			return err
		}
	}
	return a.Store.UpsertExecutionObservation(ctx, obs)
}

func (a *Agent) enqueueReporterObject(destination spacev1.DomainReference, resource string, object any, missionUID, planID string, attempt int32, sequence int64, expiry time.Time) error {
	raw, err := json.Marshal(object)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(ReporterObject{Resource: resource, Object: raw})
	if err != nil {
		return err
	}
	id := reporterEnvelopeID(resource, objectName(object), destination)
	e := NewEnvelope(id, ReporterObjectKind, a.Local, destination, missionUID, planID, attempt, sequence, a.now(), expiry, payload)
	if err := e.Sign(a.PrivateKey); err != nil {
		return err
	}
	return a.Queue.Enqueue(e)
}
func objectName(v any) string {
	switch x := v.(type) {
	case *spacev1.SpaceTransferReceipt:
		return x.Name
	case *spacev1.SpaceExecutionObservation:
		return x.Name
	case *spacev1.SpaceResultReceipt:
		return x.Name
	case *spacev1.SpaceExecutionLease:
		return x.Name
	}
	return "unknown"
}
func (a *Agent) baseProvenance(sequence int64) spacev1.Provenance {
	return spacev1.Provenance{ReporterID: a.ReporterPrincipal, Source: "space-compute-domain-agent", Sequence: sequence}
}
func (a *Agent) signTransferReceipt(v *spacev1.SpaceTransferReceipt) error {
	return a.signReporter(v, &v.Spec.Provenance)
}
func (a *Agent) signResultReceipt(v *spacev1.SpaceResultReceipt) error {
	return a.signReporter(v, &v.Spec.Provenance)
}
func (a *Agent) signLease(v *spacev1.SpaceExecutionLease) error {
	return a.signReporter(v, &v.Spec.Provenance)
}
func (a *Agent) signObservation(v *spacev1.SpaceExecutionObservation) error {
	return a.signReporter(v, &v.Spec.Provenance)
}
func (a *Agent) signReporter(object runtime.Object, p *spacev1.Provenance) error {
	p.Digest = ""
	p.Signature = ""
	digest, err := spacev1.ReporterDigest(object)
	if err != nil {
		return err
	}
	p.Digest = digest
	raw, err := hex.DecodeString(digest)
	if err != nil {
		return err
	}
	p.Signature = base64.StdEncoding.EncodeToString(ed25519.Sign(a.PrivateKey, raw))
	return nil
}
func mustJSON(v any) []byte         { raw, _ := json.Marshal(v); return raw }
func sha256Bytes(v []byte) [32]byte { return sha256.Sum256(v) }

// FileAssignmentStore gives lease requests restart durability before they reach
// Kubernetes Pod execution. The Kubernetes adapter may compose this with CRD state.
type FileAssignmentStore struct {
	mu  sync.Mutex
	dir string
	max int
}

func OpenFileAssignmentStore(dir string, max int) (*FileAssignmentStore, error) {
	if max < 1 || max > 10000 {
		return nil, fmt.Errorf("remote assignment bound is invalid")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	return &FileAssignmentStore{dir: dir, max: max}, nil
}
func (s *FileAssignmentStore) Save(a Assignment) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if a.Mission == nil || a.Placement == nil {
		return fmt.Errorf("assignment required")
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}
	name := assignmentName(a)
	path := filepath.Join(s.dir, name+".json")
	if _, err := os.Stat(path); os.IsNotExist(err) && len(entries) >= s.max {
		return fmt.Errorf("remote assignment store is full")
	}
	return writeAtomic(path, a)
}
func (s *FileAssignmentStore) List() ([]Assignment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	out := make([]Assignment, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		var a Assignment
		if err := readJSON(filepath.Join(s.dir, entry.Name()), &a); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, nil
}
func assignmentName(a Assignment) string {
	return fmt.Sprintf("%s-%d", a.Placement.Spec.PlanID, a.Placement.Spec.Attempt)
}
