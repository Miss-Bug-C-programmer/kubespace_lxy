// Package planner implements deterministic cross-domain planning. It has no
// Kubernetes scheduler-framework dependency and performs no I/O; controllers
// provide already validated snapshots and durable persistence.
package planner

import (
	"fmt"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
)

const (
	completionWeight    = int32(30)
	localityWeight      = int32(20)
	linkRiskWeight      = int32(20)
	energyWeight        = int32(15)
	resilienceWeight    = int32(10)
	fragmentationWeight = int32(5)
)

type CandidateRejection struct {
	Domain       spacev1.DomainReference
	Explanations []spacev1.ConstraintExplanation
}

type Decision struct {
	Placement *spacev1.SpacePlacementIntent
	Rejected  []CandidateRejection
}

type candidate struct {
	summary              *spacev1.SpaceDomainResourceSummary
	selectedCapabilities []spacev1.CapabilityRequirement
	selectedSetName      string
	capacityAllocations  []capacityAllocation
	inputTransfers       []spacev1.TransferEpoch
	resultTransfer       *spacev1.TransferEpoch
	computeStart         time.Time
	computeEnd           time.Time
	completion           time.Time
	notBefore            time.Time
	expiresAt            time.Time
	sequences            map[string]int64
	score                spacev1.DecisionScore
	explanations         []spacev1.ConstraintExplanation
	linkQualityMilli     int32
	localBytes           int64
	totalBytes           int64
}

// Plan chooses a domain and complete transfer/compute/return epoch. Direct
// callers retain the legacy API; production controllers reuse PreparedPlanningInputs
// so unchanged informer generations do not rebuild indexes or canonical digests.
func Plan(mission *spacev1.SpaceMission, summaries []*spacev1.SpaceDomainResourceSummary, links []*spacev1.SpaceLinkSnapshot, clock spacev1.Clock) (Decision, error) {
	prepared, err := PreparePlanningInputs(summaries, links)
	if err != nil {
		return Decision{}, err
	}
	return PlanPrepared(mission, prepared, clock)
}

func PlanPrepared(mission *spacev1.SpaceMission, prepared *PreparedPlanningInputs, clock spacev1.Clock) (Decision, error) {
	if err := spacev1.ValidateMission(mission, clock); err != nil {
		return Decision{}, err
	}
	if prepared == nil {
		return Decision{}, fmt.Errorf("prepared planning inputs are required")
	}
	now := clock.Now().UTC()
	sortedSummaries := prepared.resourceSummaries
	linkIndex := usablePreparedLinkIndex(prepared, clock)
	decision := Decision{}
	var feasible []candidate
	for _, summary := range sortedSummaries {
		if summary == nil {
			continue
		}
		current, rejection, err := evaluateCandidate(mission, summary, linkIndex, clock, now)
		if err != nil {
			return decision, fmt.Errorf("evaluate domain %s: %w", fullDomainKey(summary.Spec.Domain), err)
		}
		if len(rejection) > 0 {
			decision.Rejected = append(decision.Rejected, CandidateRejection{Domain: summary.Spec.Domain, Explanations: rejection})
			continue
		}
		feasible = append(feasible, current)
	}
	if len(feasible) == 0 {
		return decision, fmt.Errorf("no feasible domain: %s", summarizeRejections(decision.Rejected))
	}
	sort.SliceStable(feasible, func(i, j int) bool {
		if feasible[i].score.Total != feasible[j].score.Total {
			return feasible[i].score.Total > feasible[j].score.Total
		}
		if !feasible[i].completion.Equal(feasible[j].completion) {
			return feasible[i].completion.Before(feasible[j].completion)
		}
		return domainKey(feasible[i].summary.Spec.Domain) < domainKey(feasible[j].summary.Spec.Domain)
	})
	selected := feasible[0]
	digest, err := materialDigestPrepared(mission, prepared)
	if err != nil {
		return decision, fmt.Errorf("calculate material input digest: %w", err)
	}
	planID := "plan-" + digest[:20]
	placementName := mission.Name + "-placement"
	if len(placementName) > 253 {
		placementName = placementName[:232] + "-" + digest[:20]
	}
	placement := &spacev1.SpacePlacementIntent{
		TypeMeta:   metav1.TypeMeta{APIVersion: spacev1.CanonicalAPIVersion, Kind: "SpacePlacementIntent"},
		ObjectMeta: metav1.ObjectMeta{Name: placementName, Namespace: mission.Namespace, Labels: map[string]string{spacev1.LabelPlacementID: planID, spacev1.LabelMissionUID: string(mission.UID)}},
		Spec: spacev1.SpacePlacementIntentSpec{
			MissionRef: corev1.ObjectReference{APIVersion: spacev1.CanonicalAPIVersion, Kind: "SpaceMission", Namespace: mission.Namespace, Name: mission.Name, UID: mission.UID},
			PlanID:     planID, Attempt: nextAttempt(mission), Target: selected.summary.Spec.Domain,
			NotBefore: metav1.NewTime(selected.computeStart), ExpiresAt: metav1.NewTime(selected.expiresAt),
			ComputeStart: metav1.NewTime(selected.computeStart), ComputeEnd: metav1.NewTime(selected.computeEnd),
			InputTransfers: selected.inputTransfers, ResultTransfer: selected.resultTransfer,
			MaterialInputDigest: digest, SnapshotSequences: selected.sequences, Score: selected.score,
			Explanations:                      selected.explanations,
			SelectedCapabilitySetName:         selected.selectedSetName,
			SelectedCapabilities:              copyCapabilities(selected.selectedCapabilities),
			SelectedPhysicalDeviceConstraints: physicalDeviceConstraints(selected.capacityAllocations),
		},
		Status: spacev1.SpacePlacementIntentStatus{Phase: initialPlacementPhase(selected, now)},
	}
	if err := spacev1.ValidatePlacement(placement, mission); err != nil {
		return decision, fmt.Errorf("planner produced invalid placement: %w", err)
	}
	decision.Placement = placement
	return decision, nil
}

func evaluateCandidate(mission *spacev1.SpaceMission, summary *spacev1.SpaceDomainResourceSummary, links map[string][]*spacev1.SpaceLinkSnapshot, clock spacev1.Clock, now time.Time) (candidate, []spacev1.ConstraintExplanation, error) {
	result := candidate{summary: summary, sequences: map[string]int64{}, notBefore: now, expiresAt: mission.Spec.Deadline.Time.UTC(), linkQualityMilli: 1000}
	var rejected []spacev1.ConstraintExplanation
	if !resourceSummaryAccepted(summary) {
		return result, []spacev1.ConstraintExplanation{reject("resource_snapshot_unaccepted", "resource-controller acceptance", fmt.Sprint(summary.Status.ObservedGeneration), fmt.Sprint(summary.Generation), "domain resource summary generation has not been accepted")}, nil
	}
	if err := spacev1.ValidateResourceSummary(summary, clock); err != nil {
		return result, []spacev1.ConstraintExplanation{reject("resource_snapshot_invalid", "resource snapshot validation", err.Error(), "fresh validated summary", "domain resource summary is invalid or stale")}, nil
	}
	result.sequences["resource/"+summary.Name] = summary.Spec.Provenance.Sequence
	result.expiresAt = earlier(result.expiresAt, summary.Spec.ValidUntil.Time.UTC())
	selection, capacityReason, err := selectCapabilitySet(mission.Spec, summary.Spec)
	if err != nil {
		return result, nil, fmt.Errorf("capacity allocation: %w", err)
	}
	if capacityReason != "" {
		code := "required_capability_missing"
		constraint := "required capabilities"
		required := "all required capabilities"
		message := "domain cannot satisfy required device capabilities without capacity reuse"
		if len(mission.Spec.AlternativeCapabilities) > 0 {
			code = "alternative_capability_missing"
			constraint = "required plus alternative capability set"
			required = "required capabilities plus one complete alternative set"
			message = "domain cannot satisfy a complete capability set without capacity reuse"
		}
		rejected = append(rejected, reject(code, constraint, capacityReason, required, message))
	} else {
		result.selectedCapabilities = copyCapabilities(selection.selectedCapabilities)
		result.selectedSetName = selection.selectedSetName
		result.capacityAllocations = append([]capacityAllocation(nil), selection.allocations...)
	}
	if missing := softwareMismatch(mission.Spec.RequiredSoftware, summary.Spec.Software); missing != "" {
		rejected = append(rejected, reject("software_incompatible", "required software", missing, "all required versions", "domain software stack is incompatible"))
	}
	if mission.Spec.WorkingMemoryBytes > 0 && summary.Spec.SystemMemoryBytes.Available < mission.Spec.WorkingMemoryBytes {
		rejected = append(rejected, reject("working_memory_insufficient", "working memory", fmt.Sprint(summary.Spec.SystemMemoryBytes.Available), fmt.Sprint(mission.Spec.WorkingMemoryBytes), hardConstraintMessage(mission.Spec.StatePolicy, "available system memory")))
	}
	workingStorageAvailable := summary.Spec.EphemeralStorageBytes.Available
	for _, storage := range summary.Spec.PersistentStorage {
		workingStorageAvailable, err = checkedAddInt64(workingStorageAvailable, storage.AvailableBytes)
		if err != nil {
			return result, nil, fmt.Errorf("working storage availability: %w", err)
		}
	}
	if mission.Spec.WorkingStorageBytes > 0 && workingStorageAvailable < mission.Spec.WorkingStorageBytes {
		rejected = append(rejected, reject("working_storage_insufficient", "working storage", fmt.Sprint(workingStorageAvailable), fmt.Sprint(mission.Spec.WorkingStorageBytes), hardConstraintMessage(mission.Spec.StatePolicy, "available local storage")))
	}
	energyLow := summary.Spec.EnergyHeadroomMilli < summary.Spec.MinimumEnergyMilli
	thermalLow := summary.Spec.ThermalHeadroomMilli < summary.Spec.MinimumThermalMilli
	if mission.Spec.StatePolicy == spacev1.PolicyStrict && energyLow {
		rejected = append(rejected, reject("energy_below_minimum", "energy headroom", fmt.Sprint(summary.Spec.EnergyHeadroomMilli), fmt.Sprint(summary.Spec.MinimumEnergyMilli), "strict policy rejects insufficient energy headroom"))
	}
	if mission.Spec.StatePolicy == spacev1.PolicyStrict && thermalLow {
		rejected = append(rejected, reject("thermal_below_minimum", "thermal headroom", fmt.Sprint(summary.Spec.ThermalHeadroomMilli), fmt.Sprint(summary.Spec.MinimumThermalMilli), "strict policy rejects insufficient thermal headroom"))
	}
	if len(rejected) > 0 {
		return result, rejected, nil
	}

	cursor := now
	for _, input := range mission.Spec.Inputs {
		result.totalBytes, err = checkedAddInt64(result.totalBytes, input.SizeBytes)
		if err != nil {
			return result, nil, fmt.Errorf("total input bytes: %w", err)
		}
		if input.SizeBytes == 0 || locationMatchesDomain(input.Locations, summary.Spec.Domain) || contains(summary.Spec.DataLocations, input.ID) {
			result.localBytes, err = checkedAddInt64(result.localBytes, input.SizeBytes)
			if err != nil {
				return result, nil, fmt.Errorf("local input bytes: %w", err)
			}
			continue
		}
		transfer, snapshot, ok, reasons, transferErr := findIngress(input, summary.Spec.Domain, cursor, mission.Spec, links, now)
		if transferErr != nil {
			return result, nil, transferErr
		}
		if !ok {
			rejected = append(rejected, reasons...)
			continue
		}
		result.inputTransfers = append(result.inputTransfers, transfer)
		if len(result.inputTransfers) > spacev1.MaxTransferEpochs {
			return result, nil, fmt.Errorf("planned input transfer epochs exceed %d", spacev1.MaxTransferEpochs)
		}
		result.sequences["link/"+snapshot.Name] = snapshot.Spec.Provenance.Sequence
		result.expiresAt = earlier(result.expiresAt, snapshot.Spec.ValidUntil.Time.UTC())
		result.linkQualityMilli = min32(result.linkQualityMilli, linkQuality(snapshot, transfer.WindowID))
		cursor = transfer.End.Time.UTC()
		if result.notBefore.Equal(now) || transfer.Start.Time.Before(result.notBefore) {
			result.notBefore = transfer.Start.Time.UTC()
		}
	}
	if len(rejected) > 0 {
		return result, rejected, nil
	}
	safety, err := checkedSecondsDuration(mission.Spec.SafetyMarginSeconds)
	if err != nil {
		return result, nil, fmt.Errorf("safety margin: %w", err)
	}
	queueDelay, err := checkedSecondsDuration(summary.Spec.QueueDelaySeconds)
	if err != nil {
		return result, nil, fmt.Errorf("queue delay: %w", err)
	}
	result.computeStart, err = checkedTimeAdd(cursor, safety)
	if err != nil {
		return result, nil, fmt.Errorf("compute start safety margin: %w", err)
	}
	result.computeStart, err = checkedTimeAdd(result.computeStart, queueDelay)
	if err != nil {
		return result, nil, fmt.Errorf("compute start queue delay: %w", err)
	}
	computeSeconds, err := predictedComputeSeconds(mission.Spec, result.capacityAllocations)
	if err != nil {
		return result, nil, err
	}
	computeDuration, err := checkedSecondsDuration(computeSeconds)
	if err != nil {
		return result, nil, fmt.Errorf("compute duration: %w", err)
	}
	result.computeEnd, err = checkedTimeAdd(result.computeStart, computeDuration)
	if err != nil {
		return result, nil, fmt.Errorf("compute end: %w", err)
	}
	result.completion = result.computeEnd
	if mission.Spec.ResultReturnRequired {
		transfer, snapshot, ok, reasons, transferErr := findEgress(mission.Spec.OutputSizeBytes, summary.Spec.Domain, mission.Spec.ResultDestinations, result.computeEnd, mission.Spec, links, now)
		if transferErr != nil {
			return result, nil, transferErr
		}
		if !ok {
			return result, append(rejected, reasons...), nil
		}
		result.resultTransfer = &transfer
		result.sequences["link/"+snapshot.Name] = snapshot.Spec.Provenance.Sequence
		result.expiresAt = earlier(result.expiresAt, snapshot.Spec.ValidUntil.Time.UTC())
		result.linkQualityMilli = min32(result.linkQualityMilli, linkQuality(snapshot, transfer.WindowID))
		result.completion = transfer.End.Time.UTC()
	}
	guardSeconds, err := checkedAddInt64(mission.Spec.SafetyMarginSeconds, mission.Spec.MaximumClockSkewSeconds)
	if err != nil {
		return result, nil, fmt.Errorf("deadline guard: %w", err)
	}
	deadlineGuard, err := checkedSecondsDuration(guardSeconds)
	if err != nil {
		return result, nil, fmt.Errorf("deadline guard: %w", err)
	}
	guardedCompletion, err := checkedTimeAdd(result.completion, deadlineGuard)
	if err != nil {
		return result, nil, fmt.Errorf("guarded completion: %w", err)
	}
	if guardedCompletion.After(mission.Spec.Deadline.Time) {
		return result, []spacev1.ConstraintExplanation{reject("deadline_missed", "mission deadline", guardedCompletion.Format(time.RFC3339Nano), mission.Spec.Deadline.Time.Format(time.RFC3339Nano), "execution or result return cannot complete before the guarded deadline")}, nil
	}
	if !result.expiresAt.After(result.completion) {
		return result, []spacev1.ConstraintExplanation{reject("plan_inputs_expire", "snapshot validity", result.expiresAt.Format(time.RFC3339Nano), result.completion.Format(time.RFC3339Nano), "material snapshot expires before planned completion")}, nil
	}
	result.score, err = scoreCandidate(result, mission, energyLow, thermalLow, now)
	if err != nil {
		return result, nil, err
	}
	result.explanations = append(selectionExplanations(selection),
		statePolicyHardConstraintExplanation(mission.Spec.StatePolicy),
		accept("deadline_feasible", "guarded completion", guardedCompletion.Format(time.RFC3339Nano), mission.Spec.Deadline.Time.Format(time.RFC3339Nano)),
		scoreExplanation("predicted_completion", result.score.PredictedCompletion),
		scoreExplanation("data_locality", result.score.DataLocality),
		scoreExplanation("link_risk", result.score.LinkRisk),
		scoreExplanation("energy_thermal", result.score.EnergyThermal),
		scoreExplanation("resilience", result.score.Resilience),
		scoreExplanation("fragmentation", result.score.Fragmentation),
	)
	return result, nil, nil
}

func findIngress(input spacev1.DataObject, target spacev1.DomainReference, earliest time.Time, mission spacev1.SpaceMissionSpec, links map[string][]*spacev1.SpaceLinkSnapshot, now time.Time) (spacev1.TransferEpoch, *spacev1.SpaceLinkSnapshot, bool, []spacev1.ConstraintExplanation, error) {
	locations := sortedDataLocations(input.Locations)
	var best spacev1.TransferEpoch
	var bestSnapshot *spacev1.SpaceLinkSnapshot
	for _, location := range locations {
		for _, snapshot := range links[directedDomainKey(location.Domain, target)] {
			if snapshot.Spec.Source != location.Domain || snapshot.Spec.Destination != target {
				continue
			}
			transfer, ok, err := fitTransfer(snapshot, input.SizeBytes, earliest, mission, now)
			if err != nil {
				return spacev1.TransferEpoch{}, nil, false, nil, fmt.Errorf("input %s transfer arithmetic: %w", input.ID, err)
			}
			better := ok && (bestSnapshot == nil || transfer.End.Before(&best.End) || (transfer.End.Equal(&best.End) && (snapshot.Name < bestSnapshot.Name || (snapshot.Name == bestSnapshot.Name && location.URI < best.SourceURI))))
			if better {
				transfer.DataID = input.ID
				transfer.Source = snapshot.Spec.Source
				transfer.SourceURI = location.URI
				transfer.Destination = target
				best, bestSnapshot = transfer, snapshot
			}
		}
	}
	if bestSnapshot == nil {
		return spacev1.TransferEpoch{}, nil, false, []spacev1.ConstraintExplanation{reject("input_transfer_window_missing", "input transfer for "+input.ID, formatDataLocations(input.Locations), fullDomainKey(target), fmt.Sprintf("no validated contact window satisfies timing/bandwidth/RTT/loss hard constraints; statePolicy=%s never relaxes declared link limits", mission.StatePolicy))}, nil
	}
	return best, bestSnapshot, true, nil, nil
}

func findEgress(size int64, source spacev1.DomainReference, destinations []spacev1.DataLocation, earliest time.Time, mission spacev1.SpaceMissionSpec, links map[string][]*spacev1.SpaceLinkSnapshot, now time.Time) (spacev1.TransferEpoch, *spacev1.SpaceLinkSnapshot, bool, []spacev1.ConstraintExplanation, error) {
	values := sortedDataLocations(destinations)
	for _, destination := range values {
		if destination.Domain == source {
			at := metav1.NewTime(earliest)
			return spacev1.TransferEpoch{WindowID: "local-result", DataID: "result", Source: source, Destination: source, DestinationURI: destination.URI, Start: at, End: at, Bytes: size}, &spacev1.SpaceLinkSnapshot{ObjectMeta: metav1.ObjectMeta{Name: "local"}, Spec: spacev1.SpaceLinkSnapshotSpec{Provenance: spacev1.Provenance{Sequence: 1}, ValidUntil: mission.Deadline}}, true, nil, nil
		}
	}
	var best spacev1.TransferEpoch
	var bestSnapshot *spacev1.SpaceLinkSnapshot
	for _, destination := range values {
		for _, snapshot := range links[directedDomainKey(source, destination.Domain)] {
			if snapshot.Spec.Source != source || snapshot.Spec.Destination != destination.Domain {
				continue
			}
			transfer, ok, err := fitTransfer(snapshot, size, earliest, mission, now)
			if err != nil {
				return spacev1.TransferEpoch{}, nil, false, nil, fmt.Errorf("result transfer arithmetic: %w", err)
			}
			better := ok && (bestSnapshot == nil || transfer.End.Before(&best.End) || (transfer.End.Equal(&best.End) && (snapshot.Name < bestSnapshot.Name || (snapshot.Name == bestSnapshot.Name && destination.URI < best.DestinationURI))))
			if better {
				transfer.DataID = "result"
				transfer.Source = source
				transfer.Destination = snapshot.Spec.Destination
				transfer.DestinationURI = destination.URI
				best, bestSnapshot = transfer, snapshot
			}
		}
	}
	if bestSnapshot == nil {
		return spacev1.TransferEpoch{}, nil, false, []spacev1.ConstraintExplanation{reject("result_return_window_missing", "result return", fullDomainKey(source), formatDataLocations(destinations), fmt.Sprintf("execution fits but no validated return window satisfies timing/bandwidth/RTT/loss hard constraints; statePolicy=%s never relaxes declared link limits", mission.StatePolicy))}, nil
	}
	return best, bestSnapshot, true, nil, nil
}

func fitTransfer(snapshot *spacev1.SpaceLinkSnapshot, size int64, earliest time.Time, mission spacev1.SpaceMissionSpec, now time.Time) (spacev1.TransferEpoch, bool, error) {
	if snapshot == nil || !snapshot.Spec.ValidUntil.After(now) {
		return spacev1.TransferEpoch{}, false, nil
	}
	windows := append([]spacev1.ContactWindow(nil), snapshot.Spec.Windows...)
	sort.SliceStable(windows, func(i, j int) bool {
		if windows[i].Start.Equal(&windows[j].Start) {
			return windows[i].ID < windows[j].ID
		}
		return windows[i].Start.Before(&windows[j].Start)
	})
	for _, window := range windows {
		if mission.MinimumBandwidthBitsPerSecond > 0 && window.BandwidthBitsPerSec < mission.MinimumBandwidthBitsPerSecond {
			continue
		}
		if mission.MaximumRTTMicroseconds > 0 && window.RTTMicroseconds > mission.MaximumRTTMicroseconds {
			continue
		}
		if mission.MaximumLossPartsPerMillion > 0 && window.LossPartsPerMillion > mission.MaximumLossPartsPerMillion {
			continue
		}
		if window.Predicted && snapshot.Spec.Provenance.Sequence == 0 {
			continue
		}
		skewSeconds, err := checkedAddInt64(mission.MaximumClockSkewSeconds, snapshot.Spec.MaximumClockSkewSeconds)
		if err != nil {
			return spacev1.TransferEpoch{}, false, fmt.Errorf("clock skew: %w", err)
		}
		skew, err := checkedSecondsDuration(skewSeconds)
		if err != nil {
			return spacev1.TransferEpoch{}, false, fmt.Errorf("clock skew: %w", err)
		}
		windowStart, err := checkedTimeAdd(window.Start.Time, skew)
		if err != nil {
			return spacev1.TransferEpoch{}, false, fmt.Errorf("window start: %w", err)
		}
		start := later(earliest, windowStart, now)
		bits, err := checkedMulInt64(size, 8)
		if err != nil {
			return spacev1.TransferEpoch{}, false, fmt.Errorf("transfer bytes to bits: %w", err)
		}
		payloadSeconds, err := checkedCeilDiv(bits, window.BandwidthBitsPerSec)
		if err != nil {
			return spacev1.TransferEpoch{}, false, err
		}
		rttSeconds, err := checkedCeilDiv(window.RTTMicroseconds, 1_000_000)
		if err != nil {
			return spacev1.TransferEpoch{}, false, err
		}
		seconds, err := checkedAddInt64(payloadSeconds, rttSeconds)
		if err != nil {
			return spacev1.TransferEpoch{}, false, fmt.Errorf("transfer duration: %w", err)
		}
		if seconds < 1 {
			seconds = 1
		}
		transferDuration, err := checkedSecondsDuration(seconds)
		if err != nil {
			return spacev1.TransferEpoch{}, false, fmt.Errorf("transfer duration: %w", err)
		}
		end, err := checkedTimeAdd(start, transferDuration)
		if err != nil {
			return spacev1.TransferEpoch{}, false, fmt.Errorf("transfer end: %w", err)
		}
		usableEnd, err := checkedTimeAdd(window.End.Time, -skew)
		if err != nil {
			return spacev1.TransferEpoch{}, false, fmt.Errorf("usable window end: %w", err)
		}
		safety, err := checkedSecondsDuration(mission.SafetyMarginSeconds)
		if err != nil {
			return spacev1.TransferEpoch{}, false, fmt.Errorf("transfer safety margin: %w", err)
		}
		usableEnd, err = checkedTimeAdd(usableEnd, -safety)
		if err != nil {
			return spacev1.TransferEpoch{}, false, fmt.Errorf("usable window safety margin: %w", err)
		}
		if !end.After(usableEnd) && !end.After(mission.Deadline.Time) {
			return spacev1.TransferEpoch{LinkSnapshotName: snapshot.Name, WindowID: window.ID, Start: metav1.NewTime(start.UTC()), End: metav1.NewTime(end.UTC()), Bytes: size}, true, nil
		}
	}
	return spacev1.TransferEpoch{}, false, nil
}

func hardConstraintMessage(policy spacev1.StatePolicy, observed string) string {
	return fmt.Sprintf("%s is below a declared hard constraint; statePolicy=%s never relaxes explicit working-memory/storage/link limits", observed, policy)
}

func statePolicyHardConstraintExplanation(policy spacev1.StatePolicy) spacev1.ConstraintExplanation {
	message := "declared working-memory, working-storage, bandwidth, RTT and loss limits are hard and are never relaxed"
	switch policy {
	case spacev1.PolicyStrict:
		message += "; strict additionally rejects configured energy/thermal minimum violations"
	case spacev1.PolicyDegraded:
		message += "; degraded may accept soft energy/thermal degradation with a score penalty"
	case spacev1.PolicyBestEffort:
		message += "; best-effort may rank degraded soft telemetry but cannot violate declared hard limits"
	}
	return spacev1.ConstraintExplanation{Code: "state_policy_hard_constraints", Constraint: "state policy hard-constraint semantics", Observed: string(policy), Required: "non-relaxable declared constraints", Message: message}
}

func physicalDeviceConstraints(allocations []capacityAllocation) []spacev1.PhysicalDeviceConstraint {
	byKey := map[string]spacev1.PhysicalDeviceConstraint{}
	for _, allocation := range allocations {
		r := allocation.Requirement
		key := capabilityRequirementKey(r)
		v := byKey[key]
		v.Class = r.Class
		v.Architecture = r.Architecture
		v.Model = r.Model
		v.Precision = append([]string(nil), r.Precision...)
		v.Quantity += allocation.Quantity
		byKey[key] = v
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]spacev1.PhysicalDeviceConstraint, 0, len(keys))
	for _, key := range keys {
		out = append(out, byKey[key])
	}
	return out
}

func buildLinkIndex(links []*spacev1.SpaceLinkSnapshot, clock spacev1.Clock) map[string][]*spacev1.SpaceLinkSnapshot {
	result := map[string][]*spacev1.SpaceLinkSnapshot{}
	for _, link := range links {
		if link != nil && linkSnapshotAccepted(link) && spacev1.ValidateLinkSnapshot(link, nil, clock) == nil {
			key := directedDomainKey(link.Spec.Source, link.Spec.Destination)
			result[key] = append(result[key], link)
		}
	}
	for key := range result {
		sort.SliceStable(result[key], func(i, j int) bool {
			if result[key][i].Spec.Provenance.Sequence != result[key][j].Spec.Provenance.Sequence {
				return result[key][i].Spec.Provenance.Sequence > result[key][j].Spec.Provenance.Sequence
			}
			return result[key][i].Name < result[key][j].Name
		})
	}
	return result
}

// API-server objects (generation > 0) are usable only after the resource
// controller has accepted that exact generation. Generation-zero values are
// deterministic typed fixtures that still pass the same production validators.
func linkSnapshotAccepted(link *spacev1.SpaceLinkSnapshot) bool {
	if link == nil || link.Generation == 0 {
		return link != nil
	}
	condition := apiMeta.FindStatusCondition(link.Status.Conditions, ConditionLinkValidated)
	return link.Status.ObservedGeneration == link.Generation && link.Status.AcceptedSequence == link.Spec.Provenance.Sequence && condition != nil && condition.ObservedGeneration == link.Generation && condition.Status == metav1.ConditionTrue
}

func resourceSummaryAccepted(summary *spacev1.SpaceDomainResourceSummary) bool {
	if summary == nil || summary.Generation == 0 {
		return summary != nil
	}
	condition := apiMeta.FindStatusCondition(summary.Status.Conditions, "Validated")
	return summary.Status.ObservedGeneration == summary.Generation && condition != nil && condition.ObservedGeneration == summary.Generation && condition.Status == metav1.ConditionTrue
}

func scoreCandidate(value candidate, mission *spacev1.SpaceMission, energyLow, thermalLow bool, now time.Time) (spacev1.DecisionScore, error) {
	totalHorizon := mission.Spec.Deadline.Time.Sub(now)
	slack := mission.Spec.Deadline.Time.Sub(value.completion)
	completion := int64(0)
	var err error
	if totalHorizon > 0 {
		completion, err = checkedRatioMilli(int64(slack), int64(totalHorizon))
		if err != nil {
			return spacev1.DecisionScore{}, fmt.Errorf("predicted completion score: %w", err)
		}
	}
	locality := int64(1000)
	if value.totalBytes > 0 {
		locality, err = checkedRatioMilli(value.localBytes, value.totalBytes)
		if err != nil {
			return spacev1.DecisionScore{}, fmt.Errorf("data locality score: %w", err)
		}
	}
	energySum, err := checkedAddInt64(int64(value.summary.Spec.EnergyHeadroomMilli), int64(value.summary.Spec.ThermalHeadroomMilli))
	if err != nil {
		return spacev1.DecisionScore{}, fmt.Errorf("energy score: %w", err)
	}
	energy, err := checkedDivInt64(energySum, 2)
	if err != nil {
		return spacev1.DecisionScore{}, fmt.Errorf("energy score: %w", err)
	}
	if (energyLow || thermalLow) && mission.Spec.StatePolicy != spacev1.PolicyStrict {
		energy, err = checkedDivInt64(energy, 2)
		if err != nil {
			return spacev1.DecisionScore{}, fmt.Errorf("degraded energy score: %w", err)
		}
	}
	fragmentation := int64(0)
	allocatedUnits := int64(0)
	for _, allocation := range value.capacityAllocations {
		term, err := checkedMulInt64(allocation.Quantity, int64(allocation.Bucket.FragmentationMilli))
		if err != nil {
			return spacev1.DecisionScore{}, fmt.Errorf("fragmentation score: %w", err)
		}
		fragmentation, err = checkedAddInt64(fragmentation, term)
		if err != nil {
			return spacev1.DecisionScore{}, fmt.Errorf("fragmentation score: %w", err)
		}
		allocatedUnits, err = checkedAddInt64(allocatedUnits, allocation.Quantity)
		if err != nil {
			return spacev1.DecisionScore{}, fmt.Errorf("fragmentation allocation count: %w", err)
		}
	}
	if allocatedUnits > 0 {
		fragmentation, err = checkedDivInt64(fragmentation, allocatedUnits)
		if err != nil {
			return spacev1.DecisionScore{}, fmt.Errorf("fragmentation score: %w", err)
		}
	}
	result := spacev1.DecisionScore{
		PredictedCompletion: int32(clampMilli(completion) / 10),
		DataLocality:        int32(clampMilli(locality) / 10),
		LinkRisk:            value.linkQualityMilli / 10,
		EnergyThermal:       int32(clampMilli(energy) / 10),
		Resilience:          value.summary.Spec.ResilienceMilli / 10,
		Fragmentation:       int32(clampMilli(fragmentation) / 10),
	}
	components := []struct {
		value  int64
		weight int64
	}{
		{completion, int64(completionWeight)},
		{locality, int64(localityWeight)},
		{int64(value.linkQualityMilli), int64(linkRiskWeight)},
		{energy, int64(energyWeight)},
		{int64(value.summary.Spec.ResilienceMilli), int64(resilienceWeight)},
		{fragmentation, int64(fragmentationWeight)},
	}
	weightedMilli := int64(0)
	for _, component := range components {
		term, err := checkedMulInt64(component.value, component.weight)
		if err != nil {
			return spacev1.DecisionScore{}, fmt.Errorf("weighted score multiplication: %w", err)
		}
		weightedMilli, err = checkedAddInt64(weightedMilli, term)
		if err != nil {
			return spacev1.DecisionScore{}, fmt.Errorf("weighted score addition: %w", err)
		}
	}
	total, err := checkedDivInt64(weightedMilli, 1000)
	if err != nil {
		return spacev1.DecisionScore{}, fmt.Errorf("weighted score division: %w", err)
	}
	if total < 0 || total > int64(^uint32(0)>>1) {
		return spacev1.DecisionScore{}, fmt.Errorf("weighted score %d is outside int32 range", total)
	}
	result.Total = int32(total)
	return result, nil
}

func linkQuality(snapshot *spacev1.SpaceLinkSnapshot, windowID string) int32 {
	if snapshot.Name == "local" || windowID == "local-result" {
		return 1000
	}
	for _, window := range snapshot.Spec.Windows {
		if window.ID == windowID {
			loss := int32((int64(window.LossPartsPerMillion) + int64(window.ErrorPartsPerMillion)) * 1000 / 2_000_000)
			return min32(window.StabilityMilli, window.ConfidenceMilli, 1000-loss)
		}
	}
	return 0
}

func materialDigest(mission *spacev1.SpaceMission, summaries []*spacev1.SpaceDomainResourceSummary, links []*spacev1.SpaceLinkSnapshot) (string, error) {
	prepared, err := PreparePlanningInputs(summaries, links)
	if err != nil {
		return "", err
	}
	return materialDigestPrepared(mission, prepared)
}

func nextAttempt(mission *spacev1.SpaceMission) int32 {
	if mission.Status.PlanID == "" {
		return 1
	}
	return 1
}
func initialPlacementPhase(value candidate, now time.Time) spacev1.PlacementPhase {
	if len(value.inputTransfers) > 0 && value.inputTransfers[0].Start.Time.After(now) {
		return spacev1.PlacementTransferPending
	}
	if len(value.inputTransfers) > 0 {
		return spacev1.PlacementTransferPending
	}
	return spacev1.PlacementExecutionLeasePending
}
func domainKey(value spacev1.DomainReference) string {
	return string(value.OrbitClass) + "/" + value.ClusterID + "/" + value.Name
}
func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
func optionalContains(values []string, required string) bool {
	return required == "" || contains(values, required)
}
func containsAll(actual, required []string) bool {
	for _, value := range required {
		if !contains(actual, value) {
			return false
		}
	}
	return true
}
func softwareMismatch(required, actual map[string]string) string {
	keys := make([]string, 0, len(required))
	for key := range required {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if actual[key] != required[key] {
			return fmt.Sprintf("%s=%q, requires %q", key, actual[key], required[key])
		}
	}
	return ""
}
func ceilDiv(value, divisor int64) int64 {
	if value <= 0 {
		return 0
	}
	return 1 + (value-1)/divisor
}
func earlier(values ...time.Time) time.Time {
	result := values[0]
	for _, value := range values[1:] {
		if value.Before(result) {
			result = value
		}
	}
	return result
}
func later(values ...time.Time) time.Time {
	result := values[0]
	for _, value := range values[1:] {
		if value.After(result) {
			result = value
		}
	}
	return result
}
func min32(values ...int32) int32 {
	result := values[0]
	for _, value := range values[1:] {
		if value < result {
			result = value
		}
	}
	return result
}
func clampMilli(value int64) int32 {
	if value < 0 {
		return 0
	}
	if value > 1000 {
		return 1000
	}
	return int32(value)
}
func reject(code, constraint, observed, required, message string) spacev1.ConstraintExplanation {
	return spacev1.ConstraintExplanation{Code: code, Constraint: constraint, Observed: observed, Required: required, Message: message}
}
func accept(code, constraint, observed, required string) spacev1.ConstraintExplanation {
	return spacev1.ConstraintExplanation{Code: code, Constraint: constraint, Observed: observed, Required: required, Message: "constraint satisfied"}
}
func scoreExplanation(component string, value int32) spacev1.ConstraintExplanation {
	return spacev1.ConstraintExplanation{Code: "score_" + component, Constraint: component, Observed: fmt.Sprintf("%d/100", value), Required: "fixed SLO normalization", Message: "deterministic score component"}
}
func summarizeRejections(values []CandidateRejection) string {
	if len(values) == 0 {
		return "no resource summaries"
	}
	parts := make([]string, 0, len(values))
	for _, value := range values {
		codes := make([]string, len(value.Explanations))
		for i := range value.Explanations {
			codes[i] = value.Explanations[i].Code
		}
		parts = append(parts, value.Domain.Name+"["+strings.Join(codes, ",")+"]")
	}
	return strings.Join(parts, "; ")
}
