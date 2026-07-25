// Package execution owns execution-fence semantics. It does not create Pods or
// perform cross-domain I/O; callers supply already-authenticated lease/evidence.
package execution

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"sort"
	"time"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
)

const TokenBytes = 32

func NewFenceToken() (string, string, error) {
	raw := make([]byte, TokenBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", "", fmt.Errorf("generate fence token: %w", err)
	}
	sum := sha256.Sum256(raw)
	return base64.RawURLEncoding.EncodeToString(raw), hex.EncodeToString(sum[:]), nil
}

func TokenHash(token string) (string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(raw) != TokenBytes {
		return "", fmt.Errorf("fence token must be %d random bytes encoded base64url", TokenBytes)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func ValidateLease(lease *spacev1.SpaceExecutionLease, now time.Time) error {
	if lease == nil {
		return fmt.Errorf("execution lease is required")
	}
	if err := spacev1.ValidateExecutionLease(lease, fixedClock{now.UTC()}); err != nil {
		return err
	}
	skew := time.Duration(lease.Spec.MaximumClockSkewSeconds) * time.Second
	if !lease.Spec.Fence.ExpiresAt.Time.After(now.UTC().Add(-skew)) {
		return fmt.Errorf("execution lease expired")
	}
	return nil
}

// ValidateLeaseAdvance enforces one monotonic epoch stream. A heartbeat may
// update the same epoch only when the complete fence identity is unchanged.
// A replacement lease must use a strictly higher epoch and attempt.
func ValidateLeaseAdvance(previous, next *spacev1.SpaceExecutionLease, now time.Time) error {
	if err := ValidateLease(next, now); err != nil {
		return err
	}
	if previous == nil {
		return nil
	}
	a, b := previous.Spec.Fence, next.Spec.Fence
	if a.MissionUID != b.MissionUID {
		return fmt.Errorf("mission UID is immutable across lease stream")
	}
	if b.LeaseEpoch < a.LeaseEpoch {
		return fmt.Errorf("lease epoch regressed")
	}
	if b.LeaseEpoch == a.LeaseEpoch {
		if b.PlanID != a.PlanID || b.Attempt != a.Attempt || b.TokenHash != a.TokenHash {
			return fmt.Errorf("same-epoch heartbeat changed fence identity")
		}
		if !next.Spec.HeartbeatAt.After(previous.Spec.HeartbeatAt.Time) {
			return fmt.Errorf("same-epoch heartbeat time must increase")
		}
		if !b.ExpiresAt.After(a.ExpiresAt.Time) {
			return fmt.Errorf("same-epoch expiry must increase")
		}
		return nil
	}
	if b.Attempt <= a.Attempt {
		return fmt.Errorf("higher lease epoch must advance attempt")
	}
	if b.TokenHash == a.TokenHash {
		return fmt.Errorf("replacement lease must use a non-reusable token")
	}
	return nil
}

func ValidateObservationAgainstLease(observation *spacev1.SpaceExecutionObservation, lease *spacev1.SpaceExecutionLease, now time.Time) error {
	if observation == nil || lease == nil {
		return fmt.Errorf("observation and lease are required")
	}
	if err := spacev1.ValidateExecutionObservation(observation, fixedClock{now.UTC()}); err != nil {
		return err
	}
	f := lease.Spec.Fence
	if observation.Spec.MissionUID != f.MissionUID || observation.Spec.PlanID != f.PlanID || observation.Spec.Attempt != f.Attempt || observation.Spec.LeaseEpoch != f.LeaseEpoch || observation.Spec.TokenHash != f.TokenHash {
		return fmt.Errorf("execution observation is fenced by a different lease/token")
	}
	if observation.Spec.Source != lease.Spec.Source || observation.Spec.Destination != lease.Spec.Destination {
		return fmt.Errorf("execution observation domain identity does not match lease")
	}
	if observation.Spec.Phase != spacev1.ExecutionObservationStopped && observation.Spec.ObservedAt.After(f.ExpiresAt.Time) {
		return fmt.Errorf("execution observation was produced after lease expiry")
	}
	return nil
}

func ValidateResultAgainstLease(receipt *spacev1.SpaceResultReceipt, lease *spacev1.SpaceExecutionLease, now time.Time) error {
	if receipt == nil || lease == nil {
		return fmt.Errorf("result receipt and lease are required")
	}
	if err := spacev1.ValidateResultReceipt(receipt, fixedClock{now.UTC()}); err != nil {
		return err
	}
	f := lease.Spec.Fence
	if receipt.Spec.MissionUID != f.MissionUID || receipt.Spec.PlanID != f.PlanID || receipt.Spec.Attempt != f.Attempt || receipt.Spec.LeaseEpoch != f.LeaseEpoch || receipt.Spec.TokenHash != f.TokenHash {
		return fmt.Errorf("result receipt is fenced by a different lease/token")
	}
	if receipt.Spec.Source != lease.Spec.Source {
		return fmt.Errorf("result receipt source does not match lease issuer domain")
	}
	if receipt.Spec.CompletedAt.After(f.ExpiresAt.Time) {
		return fmt.Errorf("result receipt was produced after lease expiry")
	}
	return nil
}

// CanStartAttempt is deliberately conservative under partition. Local Pod
// deletion is not an input. A non-checkpointable prior execution requires a
// signed remote Stopped observation. A checkpointable execution additionally
// requires a signed Checkpointed observation before migration; after that,
// either a remote stop or expiry beyond the declared skew fences the old lease.
func CanStartAttempt(mission *spacev1.SpaceMission, previous *spacev1.SpaceExecutionLease, observations []*spacev1.SpaceExecutionObservation, now time.Time) error {
	if mission == nil {
		return fmt.Errorf("mission is required")
	}
	if previous == nil {
		return nil
	}
	stopped := false
	checkpointed := false
	for _, observation := range observations {
		if observation == nil {
			continue
		}
		if err := ValidateObservationAgainstLease(observation, previous, now); err != nil {
			continue
		}
		switch observation.Spec.Phase {
		case spacev1.ExecutionObservationStopped:
			stopped = true
		case spacev1.ExecutionObservationCheckpointed:
			checkpointed = true
		}
	}
	if !mission.Spec.Checkpoint.Checkpointable {
		if !stopped {
			return fmt.Errorf("non-checkpointable prior attempt has no trusted remote stop; partition cannot create a duplicate")
		}
		return nil
	}
	if !checkpointed {
		return fmt.Errorf("checkpointable migration requires a signed checkpoint receipt")
	}
	skew := time.Duration(previous.Spec.MaximumClockSkewSeconds) * time.Second
	expired := !previous.Spec.Fence.ExpiresAt.Time.After(now.UTC().Add(-skew))
	if !stopped && !expired {
		return fmt.Errorf("previous checkpointed execution is not remotely stopped and lease has not expired")
	}
	return nil
}

func LatestLeaseForAttempt(leases []*spacev1.SpaceExecutionLease, missionUID, planID string, attempt int32, now time.Time) (*spacev1.SpaceExecutionLease, error) {
	candidates := make([]*spacev1.SpaceExecutionLease, 0, len(leases))
	for _, lease := range leases {
		if lease == nil {
			continue
		}
		f := lease.Spec.Fence
		if f.MissionUID != missionUID || f.PlanID != planID || f.Attempt != attempt {
			continue
		}
		if ValidateLease(lease, now) != nil {
			continue
		}
		candidates = append(candidates, lease)
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].Spec.Fence.LeaseEpoch > candidates[j].Spec.Fence.LeaseEpoch })
	if len(candidates) > 1 && candidates[0].Spec.Fence.LeaseEpoch == candidates[1].Spec.Fence.LeaseEpoch && candidates[0].Spec.Fence.TokenHash != candidates[1].Spec.Fence.TokenHash {
		return nil, fmt.Errorf("conflicting execution leases share epoch %d", candidates[0].Spec.Fence.LeaseEpoch)
	}
	return candidates[0], nil
}

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }
