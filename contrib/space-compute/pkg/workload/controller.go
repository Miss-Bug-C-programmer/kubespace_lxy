// Package workload owns local durable dispatch and consumes trusted transfer /
// execution evidence. It never performs cross-domain network I/O.
package workload

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
	spaceexecution "github.com/k3s-io/k3s/contrib/space-compute/pkg/execution"
	"github.com/k3s-io/k3s/contrib/space-compute/pkg/planner"
	spacepolicy "github.com/k3s-io/k3s/contrib/space-compute/pkg/policy"
)

type Store interface {
	GetPod(context.Context, string, string) (*corev1.Pod, error)
	CreatePod(context.Context, *corev1.Pod) (*corev1.Pod, error)
	DeletePod(context.Context, string, string) error
	UpdatePlacementStatus(context.Context, *spacev1.SpacePlacementIntent) error
	Event(context.Context, string, string, string, string, string)
}

type EvidenceStore interface {
	EnsureTransferIntent(context.Context, *spacev1.SpaceTransferIntent) error
	ListTransferReceipts(context.Context) ([]*spacev1.SpaceTransferReceipt, error)
	ListExecutionLeases(context.Context) ([]*spacev1.SpaceExecutionLease, error)
	GetExecutionLease(context.Context, string) (*spacev1.SpaceExecutionLease, error)
	ListExecutionObservations(context.Context) ([]*spacev1.SpaceExecutionObservation, error)
	ListResultReceipts(context.Context) ([]*spacev1.SpaceResultReceipt, error)
}

type Controller struct {
	Store       Store
	Evidence    EvidenceStore
	Clock       spacev1.Clock
	LocalDomain *spacev1.DomainReference
}

func (c *Controller) clock() spacev1.Clock {
	if c.Clock == nil {
		return spacev1.RealClock{}
	}
	return c.Clock
}

// ReconcileDispatch is fail-closed. Time alone never proves transfer or fencing.
func (c *Controller) ReconcileDispatch(ctx context.Context, mission *spacev1.SpaceMission, placement *spacev1.SpacePlacementIntent, template corev1.PodTemplateSpec) (time.Duration, error) {
	clock := c.clock()
	now := clock.Now().UTC()
	if err := spacev1.ValidateMission(mission, clock); err != nil {
		return 0, err
	}
	if err := spacev1.ValidatePlacement(placement, mission); err != nil {
		return 0, err
	}
	if placement.Status.Phase == spacev1.PlacementCompleted || placement.Status.Phase == spacev1.PlacementFailed {
		return 0, nil
	}
	if !placement.Spec.ExpiresAt.After(now) {
		placement.Status.Phase = spacev1.PlacementExpired
		return 0, c.Store.UpdatePlacementStatus(ctx, placement)
	}
	if c.Evidence == nil {
		return c.wait(ctx, mission, placement, phaseBeforeLease(placement), "TrustedEvidenceUnavailable", "transfer/lease evidence store is unavailable")
	}

	receipts, err := c.Evidence.ListTransferReceipts(ctx)
	if err != nil {
		return 0, err
	}
	for i, epoch := range placement.Spec.InputTransfers {
		transferID := spacev1.InputTransferID(i, epoch.DataID)
		payloadDigest := lookupInputDigest(mission, epoch.DataID)
		if payloadDigest == "" {
			return 0, fmt.Errorf("cross-domain input %q requires a trusted payloadDigest", epoch.DataID)
		}
		intent := &spacev1.SpaceTransferIntent{TypeMeta: metav1.TypeMeta{APIVersion: spacev1.SchemeGroupVersion.String(), Kind: "SpaceTransferIntent"}, ObjectMeta: metav1.ObjectMeta{Name: spacev1.TransferIntentName(epoch.Source, epoch.Destination, string(mission.UID), placement.Spec.PlanID, transferID)}, Spec: spacev1.SpaceTransferIntentSpec{TransferID: transferID, MissionUID: string(mission.UID), PlanID: placement.Spec.PlanID, Attempt: placement.Spec.Attempt, Source: epoch.Source, Destination: epoch.Destination, DataID: epoch.DataID, Bytes: epoch.Bytes, PayloadDigest: payloadDigest, Window: epoch, ExpiresAt: placement.Spec.ExpiresAt}}
		if err := c.Evidence.EnsureTransferIntent(ctx, intent); err != nil {
			return 0, err
		}
		if !matchingTransferReceipt(intent, receipts) {
			return c.wait(ctx, mission, placement, spacev1.PlacementTransferPending, "TransferReceiptPending", fmt.Sprintf("input %s has no trusted transfer receipt", epoch.DataID))
		}
	}

	leases, err := c.Evidence.ListExecutionLeases(ctx)
	if err != nil {
		return 0, err
	}
	lease, err := spaceexecution.LatestLeaseForAttempt(leases, string(mission.UID), placement.Spec.PlanID, placement.Spec.Attempt, now)
	if err != nil {
		return 0, err
	}
	if lease == nil || lease.Spec.Source != placement.Spec.Target {
		return c.wait(ctx, mission, placement, spacev1.PlacementExecutionLeasePending, "ExecutionLeasePending", "no current trusted execution lease from target domain")
	}
	if placement.Spec.Attempt > 1 {
		previous := latestLeaseAnyPlan(leases, string(mission.UID), placement.Spec.Attempt-1)
		observations, err := c.Evidence.ListExecutionObservations(ctx)
		if err != nil {
			return 0, err
		}
		if err := spaceexecution.CanStartAttempt(mission, previous, observations, now); err != nil {
			return c.wait(ctx, mission, placement, spacev1.PlacementExecutionLeasePending, "PreviousAttemptNotFenced", err.Error())
		}
		if previous != nil && lease.Spec.Fence.LeaseEpoch <= previous.Spec.Fence.LeaseEpoch {
			return 0, fmt.Errorf("replacement execution lease epoch %d is not higher than previous %d", lease.Spec.Fence.LeaseEpoch, previous.Spec.Fence.LeaseEpoch)
		}
	}
	dispatchAt := placement.Spec.ComputeStart.Time.UTC()
	if placement.Spec.NotBefore.Time.After(dispatchAt) {
		dispatchAt = placement.Spec.NotBefore.Time.UTC()
	}
	if now.Before(dispatchAt) {
		placement.Status.Phase = spacev1.PlacementExecutionLeasePending
		if err := c.Store.UpdatePlacementStatus(ctx, placement); err != nil {
			return 0, err
		}
		return dispatchAt.Sub(now), nil
	}

	if c.LocalDomain != nil && placement.Spec.Target != *c.LocalDomain {
		// Remote target execution is owned by the target domain-agent. The planner
		// domain never creates a local shadow Pod for a remote placement.
		placement.Status.Phase = spacev1.PlacementDispatched
		if err := c.Store.UpdatePlacementStatus(ctx, placement); err != nil {
			return 0, err
		}
		c.Store.Event(ctx, mission.Namespace, mission.Name, "Normal", "RemoteMissionDispatched", fmt.Sprintf("remote attempt %d fenced by lease epoch %d; waiting for signed remote observation", placement.Spec.Attempt, lease.Spec.Fence.LeaseEpoch))
		return 0, nil
	}

	name := AttemptPodName(mission.Name, placement.Spec.Attempt)
	if active := placement.Status.ActivePod; active != nil && active.Name != "" && active.Name != name {
		ns := active.Namespace
		if ns == "" {
			ns = mission.Namespace
		}
		_, err := c.Store.GetPod(ctx, ns, active.Name)
		if err == nil {
			if err := c.Store.DeletePod(ctx, ns, active.Name); err != nil {
				return 0, err
			}
			c.Store.Event(ctx, mission.Namespace, mission.Name, "Normal", "PreviousAttemptLocalCleanup", fmt.Sprintf("deleted local Pod %s only after remote execution fence was proven", active.Name))
			return time.Second, nil
		}
		if !errors.Is(err, planner.ErrNotFound) {
			return 0, err
		}
		placement.Status.ActivePod = nil
	}
	existing, err := c.Store.GetPod(ctx, mission.Namespace, name)
	if err == nil {
		if existing.Labels[spacev1.LabelPlacementID] != placement.Spec.PlanID {
			return 0, fmt.Errorf("deterministic attempt Pod %s is fenced by a different plan", name)
		}
		if !podMatchesLease(existing, lease) {
			return 0, fmt.Errorf("existing attempt Pod is fenced by a different execution lease/token")
		}
		if placement.Status.ActivePod == nil || placement.Status.ActivePod.UID != existing.UID {
			placement.Status.ActivePod = &corev1.ObjectReference{Namespace: existing.Namespace, Name: existing.Name, UID: existing.UID}
			placement.Status.Phase = spacev1.PlacementDispatched
			return 0, c.Store.UpdatePlacementStatus(ctx, placement)
		}
		return 0, nil
	}
	if !errors.Is(err, planner.ErrNotFound) {
		return 0, err
	}
	pod, err := BuildAttemptPodWithLease(mission, placement, template, lease)
	if err != nil {
		return 0, err
	}
	created, err := c.Store.CreatePod(ctx, pod)
	if err != nil {
		return 0, err
	}
	placement.Status.ActivePod = &corev1.ObjectReference{Namespace: created.Namespace, Name: created.Name, UID: created.UID}
	placement.Status.Phase = spacev1.PlacementDispatched
	placement.Status.RetryCount = placement.Spec.Attempt - 1
	if err := c.Store.UpdatePlacementStatus(ctx, placement); err != nil {
		return 0, err
	}
	c.Store.Event(ctx, mission.Namespace, mission.Name, "Normal", "MissionAttemptDispatched", fmt.Sprintf("created attempt %d Pod %s under execution lease epoch %d", placement.Spec.Attempt, created.Name, lease.Spec.Fence.LeaseEpoch))
	return 0, nil
}

func phaseBeforeLease(p *spacev1.SpacePlacementIntent) spacev1.PlacementPhase {
	if len(p.Spec.InputTransfers) > 0 {
		return spacev1.PlacementTransferPending
	}
	return spacev1.PlacementExecutionLeasePending
}
func (c *Controller) wait(ctx context.Context, mission *spacev1.SpaceMission, p *spacev1.SpacePlacementIntent, phase spacev1.PlacementPhase, reason, message string) (time.Duration, error) {
	p.Status.Phase = phase
	if err := c.Store.UpdatePlacementStatus(ctx, p); err != nil {
		return 0, err
	}
	c.Store.Event(ctx, mission.Namespace, mission.Name, "Normal", reason, message)
	return time.Second, nil
}

// ReconcilePodStatus consumes only local Pod lifecycle under the accepted lease.
// result-returned/checkpoint-id annotations are untrusted hints and are ignored.
func (c *Controller) ReconcilePodStatus(ctx context.Context, mission *spacev1.SpaceMission, placement *spacev1.SpacePlacementIntent, pod *corev1.Pod) (bool, error) {
	if mission == nil || placement == nil || pod == nil {
		return false, fmt.Errorf("mission, placement and Pod are required")
	}
	if c.Evidence == nil {
		return false, fmt.Errorf("trusted execution evidence store is required")
	}
	leaseName := pod.Annotations[spacev1.GroupName+"/execution-lease"]
	lease, err := c.Evidence.GetExecutionLease(ctx, leaseName)
	if err != nil {
		return false, err
	}
	if !podMatchesLease(pod, lease) {
		return false, fmt.Errorf("Pod execution fence does not match trusted lease")
	}
	if placement.Status.ActivePod != nil && placement.Status.ActivePod.UID != "" && placement.Status.ActivePod.UID != pod.UID {
		return false, fmt.Errorf("Pod UID is fenced by active execution")
	}
	phase := "dispatched"
	switch pod.Status.Phase {
	case corev1.PodRunning:
		phase = "running"
	case corev1.PodFailed:
		phase = "failed"
	case corev1.PodSucceeded:
		if mission.Spec.ResultReturnRequired {
			phase = "return-pending"
		} else {
			phase = "completed"
		}
	}
	if last := placement.Status.LastObservation; last != nil && last.PodUID == string(pod.UID) && last.Phase == phase {
		return false, nil
	}
	obs := spacev1.ExecutionObservation{Sequence: placement.Status.LastObservationSequence + 1, Attempt: placement.Spec.Attempt, PodUID: string(pod.UID), Phase: phase, ObservedAt: metav1.NewTime(c.clock().Now())}
	changed, err := planner.ApplyExecutionObservation(placement, mission, obs, c.clock())
	if err != nil {
		return false, err
	}
	if changed {
		if err := c.Store.UpdatePlacementStatus(ctx, placement); err != nil {
			return false, err
		}
	}
	return changed, nil
}

// ReconcileTrustedEvidence is the only path that accepts checkpoint/result
// completion across a domain boundary.
func (c *Controller) ReconcileTrustedEvidence(ctx context.Context, mission *spacev1.SpaceMission, placement *spacev1.SpacePlacementIntent) (bool, error) {
	if c.Evidence == nil {
		return false, nil
	}
	now := c.clock().Now().UTC()
	leases, err := c.Evidence.ListExecutionLeases(ctx)
	if err != nil {
		return false, err
	}
	lease, err := spaceexecution.LatestLeaseForAttempt(leases, string(mission.UID), placement.Spec.PlanID, placement.Spec.Attempt, now)
	if err != nil || lease == nil {
		return false, err
	}
	if placement.Status.Phase == spacev1.PlacementReplanning && mission.Spec.Checkpoint.Checkpointable {
		observations, err := c.Evidence.ListExecutionObservations(ctx)
		if err != nil {
			return false, err
		}
		for _, o := range observations {
			if o.Spec.Phase != spacev1.ExecutionObservationCheckpointed {
				continue
			}
			if spaceexecution.ValidateObservationAgainstLease(o, lease, now) != nil {
				continue
			}
			obs := spacev1.ExecutionObservation{Sequence: placement.Status.LastObservationSequence + 1, Attempt: placement.Spec.Attempt, PodUID: activeUID(placement), Phase: "checkpointed", ObservedAt: o.Spec.ObservedAt, CheckpointID: o.Spec.CheckpointID}
			changed, err := planner.ApplyExecutionObservation(placement, mission, obs, c.clock())
			if err != nil {
				return false, err
			}
			if changed {
				return true, c.Store.UpdatePlacementStatus(ctx, placement)
			}
		}
	}
	if placement.Status.Phase == spacev1.PlacementReturnPending && mission.Spec.ResultReturnRequired {
		receipts, err := c.Evidence.ListResultReceipts(ctx)
		if err != nil {
			return false, err
		}
		for _, r := range receipts {
			if spaceexecution.ValidateResultAgainstLease(r, lease, now) != nil {
				continue
			}
			obs := spacev1.ExecutionObservation{Sequence: placement.Status.LastObservationSequence + 1, Attempt: placement.Spec.Attempt, PodUID: activeUID(placement), Phase: "completed", ObservedAt: r.Spec.CompletedAt}
			changed, err := planner.ApplyExecutionObservation(placement, mission, obs, c.clock())
			if err != nil {
				return false, err
			}
			if changed {
				placement.Status.ResultReturned = true
				return true, c.Store.UpdatePlacementStatus(ctx, placement)
			}
		}
	}
	return false, nil
}

func matchingTransferReceipt(intent *spacev1.SpaceTransferIntent, receipts []*spacev1.SpaceTransferReceipt) bool {
	for _, r := range receipts {
		if r == nil {
			continue
		}
		s := r.Spec
		if s.TransferID == intent.Spec.TransferID && s.MissionUID == intent.Spec.MissionUID && s.PlanID == intent.Spec.PlanID && s.Attempt == intent.Spec.Attempt && s.Source == intent.Spec.Source && s.Destination == intent.Spec.Destination && s.DataID == intent.Spec.DataID && s.Bytes == intent.Spec.Bytes && s.PayloadDigest == intent.Spec.PayloadDigest {
			return true
		}
	}
	return false
}
func latestLeaseAnyPlan(values []*spacev1.SpaceExecutionLease, uid string, attempt int32) *spacev1.SpaceExecutionLease {
	var best *spacev1.SpaceExecutionLease
	for _, v := range values {
		if v != nil && v.Spec.Fence.MissionUID == uid && v.Spec.Fence.Attempt == attempt && (best == nil || v.Spec.Fence.LeaseEpoch > best.Spec.Fence.LeaseEpoch) {
			best = v
		}
	}
	return best
}
func lookupInputDigest(m *spacev1.SpaceMission, id string) string {
	for _, input := range m.Spec.Inputs {
		if input.ID == id {
			return input.PayloadDigest
		}
	}
	return ""
}
func activeUID(p *spacev1.SpacePlacementIntent) string {
	if p.Status.ActivePod == nil {
		return ""
	}
	return string(p.Status.ActivePod.UID)
}

func BuildAttemptPod(mission *spacev1.SpaceMission, placement *spacev1.SpacePlacementIntent, template corev1.PodTemplateSpec) (*corev1.Pod, error) {
	return nil, fmt.Errorf("execution lease is required; use BuildAttemptPodWithLease")
}
func BuildAttemptPodWithLease(mission *spacev1.SpaceMission, placement *spacev1.SpacePlacementIntent, template corev1.PodTemplateSpec, lease *spacev1.SpaceExecutionLease) (*corev1.Pod, error) {
	if mission == nil || placement == nil || lease == nil {
		return nil, fmt.Errorf("mission, placement and execution lease are required")
	}
	f := lease.Spec.Fence
	if f.MissionUID != string(mission.UID) || f.PlanID != placement.Spec.PlanID || f.Attempt != placement.Spec.Attempt || lease.Spec.Source != placement.Spec.Target {
		return nil, fmt.Errorf("execution lease does not match placement")
	}
	missionIntent := spacepolicy.PodMissionIntent{TypeMeta: metav1.TypeMeta{APIVersion: spacev1.SchemeGroupVersion.String(), Kind: "PodMissionIntent"}, MissionUID: string(mission.UID), Spec: mission.Spec}
	podPlacement := spacepolicy.PodPlacement{TypeMeta: metav1.TypeMeta{APIVersion: spacev1.SchemeGroupVersion.String(), Kind: "PodPlacement"}, Spec: placement.Spec}
	missionRaw, err := json.Marshal(missionIntent)
	if err != nil {
		return nil, err
	}
	placementRaw, err := json.Marshal(podPlacement)
	if err != nil {
		return nil, err
	}
	pod := &corev1.Pod{ObjectMeta: *template.ObjectMeta.DeepCopy(), Spec: *template.Spec.DeepCopy()}
	pod.Namespace = mission.Namespace
	pod.Name = AttemptPodName(mission.Name, placement.Spec.Attempt)
	pod.GenerateName = ""
	if pod.Labels == nil {
		pod.Labels = map[string]string{}
	}
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	pod.Labels[spacev1.LabelPlacementID] = placement.Spec.PlanID
	pod.Labels[spacev1.LabelMissionUID] = string(mission.UID)
	pod.Annotations[spacev1.AnnotationMissionIntent] = string(missionRaw)
	pod.Annotations[spacev1.AnnotationPlacement] = string(placementRaw)
	pod.Annotations[spacev1.GroupName+"/execution-lease"] = lease.Name
	pod.Annotations[spacev1.GroupName+"/lease-epoch"] = strconv.FormatInt(f.LeaseEpoch, 10)
	pod.Annotations[spacev1.GroupName+"/token-hash"] = f.TokenHash
	pod.Spec.SchedulerName = "space-compute-scheduler"
	tokenEnv := corev1.EnvVar{Name: "SPACE_COMPUTE_FENCE_TOKEN", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: spacev1.ExecutionTokenSecretName(f)}, Key: "token"}}}
	for i := range pod.Spec.Containers {
		pod.Spec.Containers[i].Env = append(pod.Spec.Containers[i].Env, tokenEnv, corev1.EnvVar{Name: "SPACE_COMPUTE_LEASE_EPOCH", Value: strconv.FormatInt(f.LeaseEpoch, 10)}, corev1.EnvVar{Name: "SPACE_COMPUTE_TOKEN_HASH", Value: f.TokenHash})
	}
	controller := true
	block := true
	pod.OwnerReferences = []metav1.OwnerReference{{APIVersion: spacev1.SchemeGroupVersion.String(), Kind: "SpaceMission", Name: mission.Name, UID: mission.UID, Controller: &controller, BlockOwnerDeletion: &block}}
	return pod, nil
}
func podMatchesLease(pod *corev1.Pod, lease *spacev1.SpaceExecutionLease) bool {
	if pod == nil || lease == nil {
		return false
	}
	epoch, err := strconv.ParseInt(pod.Annotations[spacev1.GroupName+"/lease-epoch"], 10, 64)
	return err == nil && pod.Annotations[spacev1.GroupName+"/execution-lease"] == lease.Name && epoch == lease.Spec.Fence.LeaseEpoch && pod.Annotations[spacev1.GroupName+"/token-hash"] == lease.Spec.Fence.TokenHash
}
func AttemptPodName(missionName string, attempt int32) string {
	suffix := fmt.Sprintf("-attempt-%d", attempt)
	limit := 253 - len(suffix)
	if len(missionName) > limit {
		missionName = missionName[:limit]
	}
	return missionName + suffix
}
