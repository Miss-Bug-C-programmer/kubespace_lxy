package planner

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
)

const ConditionLinkValidated = "Validated"

// ReconcileLinkStatus applies validation and bounded history rules used by the
// resource controller. It never turns a rejected observation into active state.
func ReconcileLinkStatus(incoming, previous *spacev1.SpaceLinkSnapshot, clock spacev1.Clock) (spacev1.SpaceLinkSnapshotStatus, error) {
	status := spacev1.SpaceLinkSnapshotStatus{}
	if previous != nil {
		status = previous.Status
		status.History = append([]spacev1.LinkHistoryEntry(nil), previous.Status.History...)
		status.Conditions = append([]metav1.Condition(nil), previous.Status.Conditions...)
	} else if incoming != nil {
		status = incoming.Status
		status.History = append([]spacev1.LinkHistoryEntry(nil), incoming.Status.History...)
		status.Conditions = append([]metav1.Condition(nil), incoming.Status.Conditions...)
		if status.ObservedGeneration == incoming.Generation {
			if status.AcceptedSequence == incoming.Spec.Provenance.Sequence {
				return status, nil
			}
			if len(status.History) > 0 {
				last := status.History[len(status.History)-1]
				if last.Sequence == incoming.Spec.Provenance.Sequence && !last.Accepted {
					return status, fmt.Errorf("%s", last.Reason)
				}
			}
		}
	}
	err := spacev1.ValidateLinkSnapshot(incoming, previous, clock)
	entry := historyEntry(incoming, err)
	if err == nil && previous == nil {
		for i := len(status.History) - 1; i >= 0; i-- {
			last := status.History[i]
			if !last.Accepted {
				continue
			}
			if incoming.Spec.Provenance.Sequence <= last.Sequence {
				err = fmt.Errorf("spec.provenance.sequence must increase beyond %d", last.Sequence)
			}
			if incoming.Spec.ObservedAt.Time.Sub(last.ObservedAt.Time) < time.Duration(incoming.Spec.MinimumUpdateSeconds)*time.Second && entry.WindowDigest == last.WindowDigest {
				err = fmt.Errorf("spec.observedAt unchanged update is faster than minimumUpdateSeconds")
			}
			break
		}
		entry = historyEntry(incoming, err)
	}
	status.History = append(status.History, entry)
	limit := int(spacev1.MaxLinkHistory)
	if incoming != nil && incoming.Spec.HistoryLimit > 0 && int(incoming.Spec.HistoryLimit) < limit {
		limit = int(incoming.Spec.HistoryLimit)
	}
	if len(status.History) > limit {
		status.History = append([]spacev1.LinkHistoryEntry(nil), status.History[len(status.History)-limit:]...)
	}
	condition := metav1.Condition{Type: ConditionLinkValidated, ObservedGeneration: incoming.Generation, LastTransitionTime: metav1.NewTime(clock.Now())}
	if err != nil {
		status.ObservedGeneration = incoming.Generation
		condition.Status = metav1.ConditionFalse
		condition.Reason = "RejectedObservation"
		condition.Message = boundedMessage(err.Error(), 512)
		apiMeta.SetStatusCondition(&status.Conditions, condition)
		return status, err
	}
	status.ObservedGeneration = incoming.Generation
	status.AcceptedSequence = incoming.Spec.Provenance.Sequence
	condition.Status = metav1.ConditionTrue
	condition.Reason = "ValidatedObservation"
	condition.Message = "identity, measurements, timestamps and contact windows are valid"
	apiMeta.SetStatusCondition(&status.Conditions, condition)
	return status, nil
}

func historyEntry(snapshot *spacev1.SpaceLinkSnapshot, err error) spacev1.LinkHistoryEntry {
	entry := spacev1.LinkHistoryEntry{}
	if snapshot == nil {
		entry.Reason = "snapshot is nil"
		return entry
	}
	entry.Sequence = snapshot.Spec.Provenance.Sequence
	entry.ObservedAt = snapshot.Spec.ObservedAt
	entry.ValidUntil = snapshot.Spec.ValidUntil
	entry.WindowCount = int32(len(snapshot.Spec.Windows))
	entry.Accepted = err == nil
	if err != nil {
		entry.Reason = boundedMessage(err.Error(), 256)
	}
	raw, _ := json.Marshal(snapshot.Spec.Windows)
	digest := sha256.Sum256(raw)
	entry.WindowDigest = hex.EncodeToString(digest[:])
	provenance := sha256.Sum256([]byte(snapshot.Spec.Provenance.ReporterID + "\x00" + snapshot.Spec.Provenance.Source + "\x00" + snapshot.Spec.Provenance.Digest))
	entry.ProvenanceHash = hex.EncodeToString(provenance[:])
	return entry
}

func boundedMessage(value string, limit int) string {
	value = strings.TrimSpace(value)
	if len(value) <= limit {
		return value
	}
	return value[:limit]
}
