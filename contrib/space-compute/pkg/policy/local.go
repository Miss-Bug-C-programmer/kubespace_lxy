// Package policy evaluates only immediate local scheduling facts. It parses
// durable planner projections from watched Pod/Node objects and performs no I/O.
package policy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
)

const maxProjectionBytes = 128 << 10

type PodMissionIntent struct {
	metav1.TypeMeta `json:",inline"`
	MissionUID      string                   `json:"missionUID"`
	Spec            spacev1.SpaceMissionSpec `json:"spec"`
}

type PodPlacement struct {
	metav1.TypeMeta `json:",inline"`
	Spec            spacev1.SpacePlacementIntentSpec `json:"spec"`
}

type ProjectedLink struct {
	Name string                        `json:"name"`
	Spec spacev1.SpaceLinkSnapshotSpec `json:"spec"`
}

type NodeProjection struct {
	metav1.TypeMeta `json:",inline"`
	Domain          spacev1.DomainReference `json:"domain"`
	ObservedAt      metav1.Time             `json:"observedAt"`
	ValidUntil      metav1.Time             `json:"validUntil"`
	ResourceDigest  string                  `json:"resourceDigest"`
	ResilienceMilli int32                   `json:"resilienceMilli"`
	Links           []ProjectedLink         `json:"links,omitempty"`
}

type Requirement struct {
	Mission   PodMissionIntent
	Placement PodPlacement
}

func (r *Requirement) Clone() *Requirement {
	if r == nil {
		return nil
	}
	raw, _ := json.Marshal(r)
	out := &Requirement{}
	_ = json.Unmarshal(raw, out)
	return out
}

type Dimensions struct {
	PredictedCompletion float64 `json:"predictedCompletion"`
	DataLocality        float64 `json:"dataLocality"`
	LinkRisk            float64 `json:"linkRisk"`
	Resilience          float64 `json:"resilience"`
}

type Evaluation struct {
	Feasible     bool                            `json:"feasible"`
	Degraded     bool                            `json:"degraded"`
	ReasonCode   string                          `json:"reasonCode"`
	Reason       string                          `json:"reason"`
	Dimensions   Dimensions                      `json:"dimensions"`
	Explanations []spacev1.ConstraintExplanation `json:"explanations"`
}

func ParsePod(pod *corev1.Pod, clock spacev1.Clock) (*Requirement, error) {
	if pod == nil {
		return nil, nil
	}
	missionRaw := strings.TrimSpace(pod.Annotations[spacev1.AnnotationMissionIntent])
	placementRaw := strings.TrimSpace(pod.Annotations[spacev1.AnnotationPlacement])
	if missionRaw == "" && placementRaw == "" {
		return nil, nil
	}
	if missionRaw == "" || placementRaw == "" {
		return nil, fmt.Errorf("annotations %s and %s must be supplied together", spacev1.AnnotationMissionIntent, spacev1.AnnotationPlacement)
	}
	var mission PodMissionIntent
	if err := decodeStrict(missionRaw, &mission); err != nil {
		return nil, fmt.Errorf("annotation %s: %w", spacev1.AnnotationMissionIntent, err)
	}
	var placement PodPlacement
	if err := decodeStrict(placementRaw, &placement); err != nil {
		return nil, fmt.Errorf("annotation %s: %w", spacev1.AnnotationPlacement, err)
	}
	if mission.APIVersion != spacev1.SchemeGroupVersion.String() || mission.Kind != "PodMissionIntent" {
		return nil, fmt.Errorf("annotation %s must use apiVersion %q and kind PodMissionIntent", spacev1.AnnotationMissionIntent, spacev1.SchemeGroupVersion.String())
	}
	if placement.APIVersion != spacev1.SchemeGroupVersion.String() || placement.Kind != "PodPlacement" {
		return nil, fmt.Errorf("annotation %s must use apiVersion %q and kind PodPlacement", spacev1.AnnotationPlacement, spacev1.SchemeGroupVersion.String())
	}
	if mission.MissionUID == "" {
		return nil, fmt.Errorf("annotation %s missionUID is required", spacev1.AnnotationMissionIntent)
	}
	syntheticMission := &spacev1.SpaceMission{ObjectMeta: metav1.ObjectMeta{Name: placement.Spec.MissionRef.Name, Namespace: placement.Spec.MissionRef.Namespace, UID: placement.Spec.MissionRef.UID}, Spec: mission.Spec}
	if err := spacev1.ValidateMission(syntheticMission, clock); err != nil {
		return nil, fmt.Errorf("annotation %s: %w", spacev1.AnnotationMissionIntent, err)
	}
	syntheticPlacement := &spacev1.SpacePlacementIntent{ObjectMeta: metav1.ObjectMeta{Name: "pod-projection"}, Spec: placement.Spec}
	if err := spacev1.ValidatePlacement(syntheticPlacement, syntheticMission); err != nil {
		return nil, fmt.Errorf("annotation %s: %w", spacev1.AnnotationPlacement, err)
	}
	if string(placement.Spec.MissionRef.UID) != mission.MissionUID {
		return nil, fmt.Errorf("missionUID does not match placement missionRef UID")
	}
	return &Requirement{Mission: mission, Placement: placement}, nil
}

func ParseNode(node *corev1.Node, clock spacev1.Clock) (*NodeProjection, error) {
	if node == nil {
		return nil, fmt.Errorf("node is required")
	}
	raw := strings.TrimSpace(node.Annotations[spacev1.AnnotationLinkProjection])
	if raw == "" {
		return nil, nil
	}
	var projection NodeProjection
	if err := decodeStrict(raw, &projection); err != nil {
		return nil, err
	}
	if projection.APIVersion != spacev1.SchemeGroupVersion.String() || projection.Kind != "NodeLinkProjection" {
		return nil, fmt.Errorf("must use apiVersion %q and kind NodeLinkProjection", spacev1.SchemeGroupVersion.String())
	}
	if node.Labels[spacev1.LabelDomain] != projection.Domain.Name || node.Labels[spacev1.LabelOrbitClass] != string(projection.Domain.OrbitClass) {
		return nil, fmt.Errorf("projection domain does not match watched Node labels")
	}
	if projection.ObservedAt.IsZero() || projection.ValidUntil.IsZero() || !projection.ValidUntil.After(projection.ObservedAt.Time) {
		return nil, fmt.Errorf("projection validUntil must be after observedAt")
	}
	if projection.ObservedAt.After(clock.Now().Add(time.Duration(spacev1.MaxClockSkewSecs) * time.Second)) {
		return nil, fmt.Errorf("projection observedAt exceeds allowed clock skew")
	}
	if projection.ResilienceMilli < 0 || projection.ResilienceMilli > 1000 {
		return nil, fmt.Errorf("projection resilienceMilli must be between 0 and 1000")
	}
	seen := map[string]struct{}{}
	for i, projected := range projection.Links {
		if projected.Name == "" {
			return nil, fmt.Errorf("links[%d].name is required", i)
		}
		if _, exists := seen[projected.Name]; exists {
			return nil, fmt.Errorf("duplicate projected link %q", projected.Name)
		}
		seen[projected.Name] = struct{}{}
		link := &spacev1.SpaceLinkSnapshot{ObjectMeta: metav1.ObjectMeta{Name: projected.Name}, Spec: projected.Spec}
		if err := spacev1.ValidateLinkSnapshot(link, nil, clock); err != nil {
			return nil, fmt.Errorf("links[%d]: %w", i, err)
		}
	}
	return &projection, nil
}

// Evaluate checks the planner-selected target and guarded immediate epoch. A
// stale projection follows the declared policy but never invents a window: the
// durable placement's exact window and sequence remain mandatory.
func Evaluate(requirement *Requirement, node *corev1.Node, clock spacev1.Clock) Evaluation {
	if requirement == nil {
		return Evaluation{Feasible: true, ReasonCode: "not_space_mission"}
	}
	if node == nil || clock == nil {
		return rejectEvaluation("node_projection_unavailable", "node and clock are required")
	}
	now := clock.Now().UTC()
	mission := requirement.Mission.Spec
	placement := requirement.Placement.Spec
	if node.Labels[spacev1.LabelDomain] != placement.Target.Name || node.Labels[spacev1.LabelOrbitClass] != string(placement.Target.OrbitClass) {
		return rejectEvaluation("target_domain_mismatch", fmt.Sprintf("node domain %q/%q does not match planned %q/%q", node.Labels[spacev1.LabelOrbitClass], node.Labels[spacev1.LabelDomain], placement.Target.OrbitClass, placement.Target.Name))
	}
	if id := node.Labels[spacev1.LabelPlacementID]; id != "" && id != placement.PlanID {
		return rejectEvaluation("placement_id_mismatch", "node placement fence does not match the Pod plan ID")
	}
	skew := time.Duration(mission.MaximumClockSkewSeconds) * time.Second
	if now.Add(skew).Before(placement.NotBefore.Time) {
		return rejectEvaluation("execution_epoch_not_open", fmt.Sprintf("execution epoch opens at %s", placement.NotBefore.Time.Format(time.RFC3339Nano)))
	}
	if !placement.ExpiresAt.After(now.Add(skew)) {
		return rejectEvaluation("placement_expired", "placement intent has expired and must be replanned")
	}
	guardedEnd := now.Add(time.Duration(mission.MaximumDurationSeconds) * time.Second)
	if guardedEnd.After(placement.ComputeEnd.Time.Add(skew).Add(time.Duration(mission.SafetyMarginSeconds) * time.Second)) {
		return rejectEvaluation("execution_window_too_short", fmt.Sprintf("remaining guarded execution ends %s after planned compute end %s", guardedEnd.Format(time.RFC3339Nano), placement.ComputeEnd.Time.Format(time.RFC3339Nano)))
	}
	if mission.ResultReturnRequired && placement.ResultTransfer == nil {
		return rejectEvaluation("result_return_unplanned", "required result return has no planned transfer")
	}
	projection, err := ParseNode(node, clock)
	if err != nil {
		return projectionFailure(mission.StatePolicy, "link_projection_invalid", err.Error(), placement)
	}
	if projection == nil {
		return projectionFailure(mission.StatePolicy, "link_projection_missing", "validated local link projection is absent", placement)
	}
	stale := !projection.ValidUntil.After(now.Add(-skew))
	links := make(map[string]ProjectedLink, len(projection.Links))
	for _, link := range projection.Links {
		links[link.Name] = link
	}
	var requiredTransfers []spacev1.TransferEpoch
	for _, transfer := range placement.InputTransfers {
		if transfer.End.After(now) {
			requiredTransfers = append(requiredTransfers, transfer)
		}
	}
	if placement.ResultTransfer != nil && placement.ResultTransfer.WindowID != "local-result" {
		requiredTransfers = append(requiredTransfers, *placement.ResultTransfer)
	}
	quality := int32(1000)
	for _, transfer := range requiredTransfers {
		projected, ok := links[transfer.LinkSnapshotName]
		if !ok {
			return projectionFailure(mission.StatePolicy, "planned_link_missing", "projection does not contain planned link "+transfer.LinkSnapshotName, placement)
		}
		sequence := placement.SnapshotSequences["link/"+transfer.LinkSnapshotName]
		if projected.Spec.Provenance.Sequence != sequence {
			return projectionFailure(mission.StatePolicy, "planned_link_sequence_changed", "planned link sequence changed materially", placement)
		}
		window, ok := projectedWindow(projected.Spec.Windows, transfer.WindowID)
		if !ok || transfer.Start.Before(&window.Start) || transfer.End.After(window.End.Time) {
			return projectionFailure(mission.StatePolicy, "planned_window_changed", "planned transfer is not covered by the projected contact window", placement)
		}
		quality = minQuality(quality, projectedQuality(window))
	}
	degraded := stale
	if stale && mission.StatePolicy == spacev1.PolicyStrict {
		return rejectEvaluation("link_projection_stale", "strict mission requires a fresh local link projection")
	}
	if stale && mission.StatePolicy == spacev1.PolicyDegraded {
		quality = minQuality(quality, 200)
	}
	if stale && mission.StatePolicy == spacev1.PolicyBestEffort {
		quality = 0
	}
	inputBytes := int64(0)
	transferBytes := int64(0)
	for _, input := range mission.Inputs {
		inputBytes += input.SizeBytes
	}
	for _, transfer := range placement.InputTransfers {
		transferBytes += transfer.Bytes
	}
	locality := 100.0
	if inputBytes > 0 {
		locality = float64(inputBytes-transferBytes) * 100 / float64(inputBytes)
		if locality < 0 {
			locality = 0
		}
	}
	horizon := mission.Deadline.Time.Sub(now)
	slack := mission.Deadline.Time.Sub(placement.ComputeEnd.Time)
	completion := 0.0
	if horizon > 0 {
		completion = clamp(float64(slack) * 100 / float64(horizon))
	}
	reason := "immediate_constraints_satisfied"
	message := "target domain, guarded epoch and contact projections are feasible"
	if degraded {
		reason = "stale_link_projection_fallback"
		message = "durable planned windows retained with an explicit stale-state penalty"
	}
	return Evaluation{Feasible: true, Degraded: degraded, ReasonCode: reason, Reason: message, Dimensions: Dimensions{PredictedCompletion: completion, DataLocality: locality, LinkRisk: float64(quality) / 10, Resilience: float64(projection.ResilienceMilli) / 10}, Explanations: []spacev1.ConstraintExplanation{
		{Code: "target_domain", Constraint: "planned target domain", Observed: projection.Domain.Name, Required: placement.Target.Name, Message: "constraint satisfied"},
		{Code: "guarded_execution", Constraint: "remaining guarded execution", Observed: guardedEnd.Format(time.RFC3339Nano), Required: placement.ComputeEnd.Time.Format(time.RFC3339Nano), Message: "constraint satisfied"},
		{Code: "score_predicted_completion", Constraint: "predicted completion", Observed: fmt.Sprintf("%.0f/100", completion), Message: "fixed deadline-slack normalization"},
		{Code: "score_data_locality", Constraint: "data locality", Observed: fmt.Sprintf("%.0f/100", locality), Message: "fixed local-byte fraction"},
		{Code: "score_link_risk", Constraint: "link risk", Observed: fmt.Sprintf("%d/100", quality/10), Message: "minimum confidence/stability/loss score"},
		{Code: "score_resilience", Constraint: "resilience", Observed: fmt.Sprintf("%d/100", projection.ResilienceMilli/10), Message: "validated domain summary score"},
	}}
}

func projectionFailure(policy spacev1.StatePolicy, code, message string, placement spacev1.SpacePlacementIntentSpec) Evaluation {
	// A validated durable plan is trusted static state in degraded/best-effort
	// mode. Exact transfer/window IDs remain in the placement; no new window is
	// inferred. Strict mode requires the live projection.
	if policy == spacev1.PolicyStrict {
		return rejectEvaluation(code, message)
	}
	return Evaluation{Feasible: true, Degraded: true, ReasonCode: code, Reason: message, Dimensions: Dimensions{PredictedCompletion: 0, DataLocality: float64(placement.Score.DataLocality), LinkRisk: 0, Resilience: 0}, Explanations: []spacev1.ConstraintExplanation{{Code: code, Constraint: "fresh link projection", Observed: message, Required: "validated projection", Message: "durable plan fallback with zero link score"}}}
}

func decodeStrict(raw string, into interface{}) error {
	if len(raw) > maxProjectionBytes {
		return fmt.Errorf("JSON exceeds %d bytes", maxProjectionBytes)
	}
	decoder := json.NewDecoder(bytes.NewBufferString(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(into); err != nil {
		return err
	}
	var trailing interface{}
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("contains multiple JSON values")
		}
		return fmt.Errorf("contains trailing data: %w", err)
	}
	return nil
}
func rejectEvaluation(code, message string) Evaluation {
	return Evaluation{Feasible: false, ReasonCode: code, Reason: message, Explanations: []spacev1.ConstraintExplanation{{Code: code, Constraint: code, Message: message}}}
}
func projectedWindow(windows []spacev1.ContactWindow, id string) (spacev1.ContactWindow, bool) {
	values := append([]spacev1.ContactWindow(nil), windows...)
	sort.Slice(values, func(i, j int) bool { return values[i].ID < values[j].ID })
	for _, value := range values {
		if value.ID == id {
			return value, true
		}
	}
	return spacev1.ContactWindow{}, false
}
func projectedQuality(window spacev1.ContactWindow) int32 {
	lossPenalty := int32((int64(window.LossPartsPerMillion) + int64(window.ErrorPartsPerMillion)) * 1000 / 2_000_000)
	return minQuality(window.StabilityMilli, window.ConfidenceMilli, 1000-lossPenalty)
}
func minQuality(values ...int32) int32 {
	result := values[0]
	for _, value := range values[1:] {
		if value < result {
			result = value
		}
	}
	return result
}
func clamp(value float64) float64 {
	if value < 0 {
		return 0
	}
	if value > 100 {
		return 100
	}
	return value
}
