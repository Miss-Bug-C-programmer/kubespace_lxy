package v1alpha1

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
)

const (
	MaxContactWindows        = 256
	MaxLinkHistory           = 64
	MaxCapabilities          = 64
	MaxDataObjects           = 128
	MaxDataBytes             = int64(1 << 50)
	MaxMissionDurationSecs   = int64(30 * 24 * time.Hour / time.Second)
	MaxSafetyMarginSecs      = int64(24 * time.Hour / time.Second)
	MaxClockSkewSecs         = int64(10 * time.Minute / time.Second)
	MaxSnapshotLifetimeSecs  = int64(7 * 24 * time.Hour / time.Second)
	MaxWorkloadTemplateBytes = 64 << 10
)

// Clock is deliberately small so production and deterministic tests execute
// identical validation and planning code.
type Clock interface{ Now() time.Time }

type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }

type FieldViolation struct {
	Field   string
	Message string
}

type ValidationErrors []FieldViolation

func (e ValidationErrors) Error() string {
	parts := make([]string, len(e))
	for i := range e {
		parts[i] = e[i].Field + ": " + e[i].Message
	}
	return strings.Join(parts, "; ")
}

func (e *ValidationErrors) add(field, message string) {
	*e = append(*e, FieldViolation{Field: field, Message: message})
}

func (e *ValidationErrors) addf(field, format string, args ...interface{}) {
	*e = append(*e, FieldViolation{Field: field, Message: fmt.Sprintf(format, args...)})
}

func (e ValidationErrors) errOrNil() error {
	if len(e) == 0 {
		return nil
	}
	return e
}

func validOrbitClass(value OrbitClass) bool {
	switch value {
	case OrbitGround, OrbitLEO, OrbitMEO, OrbitGEO, OrbitHEO:
		return true
	}
	return false
}

func validPolicy(value StatePolicy) bool {
	return value == PolicyStrict || value == PolicyDegraded || value == PolicyBestEffort
}

func validateDomain(path string, domain DomainReference, errs *ValidationErrors) {
	if values := utilvalidation.IsDNS1123Subdomain(domain.Name); len(values) > 0 {
		errs.add(path+".name", strings.Join(values, ", "))
	}
	if values := utilvalidation.IsDNS1123Subdomain(domain.ClusterID); len(values) > 0 {
		errs.add(path+".clusterID", strings.Join(values, ", "))
	}
	if !validOrbitClass(domain.OrbitClass) {
		errs.add(path+".orbitClass", "must be ground, leo, meo, geo, or heo")
	}
}

func validateProvenance(path string, value Provenance, errs *ValidationErrors) {
	if strings.TrimSpace(value.ReporterID) == "" || len(value.ReporterID) > 253 || strings.ContainsAny(value.ReporterID, "\r\n\x00") {
		errs.add(path+".reporterID", "must be a non-empty authenticated principal of at most 253 bytes without control separators")
	}
	if strings.TrimSpace(value.Source) == "" || len(value.Source) > 256 || strings.ContainsAny(value.Source, "\r\n\x00") {
		errs.add(path+".source", "must be non-empty and at most 256 bytes without control separators")
	}
	decoded, err := hex.DecodeString(value.Digest)
	if err != nil || len(decoded) != sha256.Size {
		errs.add(path+".digest", "must be a lowercase hexadecimal SHA-256 digest")
	}
	if value.Digest != strings.ToLower(value.Digest) {
		errs.add(path+".digest", "must be lowercase")
	}
	if value.Sequence < 1 {
		errs.add(path+".sequence", "must be positive")
	}
}

// ValidateLinkSnapshot validates untrusted timestamps and measurements. Previous
// is the last accepted observation for the same directed link, when available.
func ValidateLinkSnapshot(snapshot *SpaceLinkSnapshot, previous *SpaceLinkSnapshot, clock Clock) error {
	var errs ValidationErrors
	if snapshot == nil {
		errs.add("snapshot", "is required")
		return errs
	}
	if clock == nil {
		errs.add("clock", "is required")
		return errs
	}
	validateDomain("spec.source", snapshot.Spec.Source, &errs)
	validateDomain("spec.destination", snapshot.Spec.Destination, &errs)
	if snapshot.Spec.Source.Name == snapshot.Spec.Destination.Name && snapshot.Spec.Source.ClusterID == snapshot.Spec.Destination.ClusterID {
		errs.add("spec.destination", "must differ from source")
	}
	validateProvenance("spec.provenance", snapshot.Spec.Provenance, &errs)
	if snapshot.Spec.MaximumClockSkewSeconds < 0 || snapshot.Spec.MaximumClockSkewSeconds > MaxClockSkewSecs {
		errs.addf("spec.maximumClockSkewSeconds", "must be between 0 and %d", MaxClockSkewSecs)
	}
	if snapshot.Spec.MinimumUpdateSeconds < 1 || snapshot.Spec.MinimumUpdateSeconds > 3600 {
		errs.add("spec.minimumUpdateSeconds", "must be between 1 and 3600")
	}
	if snapshot.Spec.HistoryLimit < 1 || snapshot.Spec.HistoryLimit > MaxLinkHistory {
		errs.addf("spec.historyLimit", "must be between 1 and %d", MaxLinkHistory)
	}
	observed := snapshot.Spec.ObservedAt.Time
	validUntil := snapshot.Spec.ValidUntil.Time
	now := clock.Now()
	skew := time.Duration(snapshot.Spec.MaximumClockSkewSeconds) * time.Second
	if observed.IsZero() {
		errs.add("spec.observedAt", "is required")
	}
	if validUntil.IsZero() || !validUntil.After(observed) {
		errs.add("spec.validUntil", "must be after observedAt")
	}
	if validUntil.Sub(observed) > time.Duration(MaxSnapshotLifetimeSecs)*time.Second {
		errs.addf("spec.validUntil", "snapshot lifetime exceeds %d seconds", MaxSnapshotLifetimeSecs)
	}
	if observed.After(now.Add(skew)) {
		errs.add("spec.observedAt", "is beyond allowed clock skew")
	}
	if !validUntil.After(now.Add(-skew)) {
		errs.add("spec.validUntil", "snapshot is stale")
	}
	if len(snapshot.Spec.Windows) == 0 || len(snapshot.Spec.Windows) > MaxContactWindows {
		errs.addf("spec.windows", "must contain between 1 and %d windows", MaxContactWindows)
	}
	windows := append([]ContactWindow(nil), snapshot.Spec.Windows...)
	sort.SliceStable(windows, func(i, j int) bool {
		if windows[i].Start.Equal(&windows[j].Start) {
			return windows[i].ID < windows[j].ID
		}
		return windows[i].Start.Before(&windows[j].Start)
	})
	seen := map[string]struct{}{}
	for i, window := range windows {
		path := fmt.Sprintf("spec.windows[%d]", i)
		if values := utilvalidation.IsDNS1123Label(window.ID); len(values) > 0 {
			errs.add(path+".id", strings.Join(values, ", "))
		}
		if _, ok := seen[window.ID]; ok {
			errs.addf(path+".id", "duplicate window ID %q", window.ID)
		}
		seen[window.ID] = struct{}{}
		if window.Start.IsZero() || window.End.IsZero() || !window.End.After(window.Start.Time) {
			errs.add(path+".end", "must be after start")
		}
		if window.BandwidthBitsPerSec < 1 || window.BandwidthBitsPerSec > 10_000_000_000_000 {
			errs.add(path+".bandwidthBitsPerSecond", "must be between 1 and 10000000000000")
		}
		if window.RTTMicroseconds < 0 || window.RTTMicroseconds > int64((24*time.Hour)/time.Microsecond) {
			errs.add(path+".rttMicroseconds", "must be between 0 and one day")
		}
		for field, value := range map[string]int32{"lossPartsPerMillion": window.LossPartsPerMillion, "errorPartsPerMillion": window.ErrorPartsPerMillion} {
			if value < 0 || value > 1_000_000 {
				errs.add(path+"."+field, "must be between 0 and 1000000")
			}
		}
		for field, value := range map[string]int32{"stabilityMilli": window.StabilityMilli, "confidenceMilli": window.ConfidenceMilli} {
			if value < 0 || value > 1000 {
				errs.add(path+"."+field, "must be between 0 and 1000")
			}
		}
		if i > 0 && windows[i-1].End.After(window.Start.Time) {
			errs.addf("spec.windows", "windows %q and %q overlap", windows[i-1].ID, window.ID)
		}
	}
	if previous != nil {
		if previous.Spec.Source != snapshot.Spec.Source || previous.Spec.Destination != snapshot.Spec.Destination {
			errs.add("spec", "previous observation is for a different directed link")
		}
		if snapshot.Spec.Provenance.ReporterID != previous.Spec.Provenance.ReporterID {
			errs.add("spec.provenance.reporterID", "cannot change for an existing directed link")
		}
		if snapshot.Spec.Provenance.Sequence <= previous.Spec.Provenance.Sequence {
			errs.addf("spec.provenance.sequence", "must increase beyond %d", previous.Spec.Provenance.Sequence)
		}
		minimum := time.Duration(snapshot.Spec.MinimumUpdateSeconds) * time.Second
		if observed.Sub(previous.Spec.ObservedAt.Time) < minimum && contactWindowsDigest(snapshot.Spec.Windows) == contactWindowsDigest(previous.Spec.Windows) {
			errs.add("spec.observedAt", "unchanged update is faster than minimumUpdateSeconds")
		}
	}
	return errs.errOrNil()
}

func contactWindowsDigest(windows []ContactWindow) string {
	copyWindows := append([]ContactWindow(nil), windows...)
	sort.Slice(copyWindows, func(i, j int) bool { return copyWindows[i].ID < copyWindows[j].ID })
	h := sha256.New()
	for _, w := range copyWindows {
		fmt.Fprintf(h, "%s|%d|%d|%d|%d|%d|%d|%d|%t\n", w.ID, w.Start.UnixNano(), w.End.UnixNano(), w.BandwidthBitsPerSec, w.RTTMicroseconds, w.LossPartsPerMillion, w.ErrorPartsPerMillion, w.ConfidenceMilli, w.Predicted)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func ValidateMission(mission *SpaceMission, clock Clock) error {
	var errs ValidationErrors
	if mission == nil {
		errs.add("mission", "is required")
		return errs
	}
	if clock == nil {
		errs.add("clock", "is required")
		return errs
	}
	spec := mission.Spec
	if values := utilvalidation.IsDNS1123Label(spec.MissionClass); len(values) > 0 {
		errs.add("spec.missionClass", strings.Join(values, ", "))
	}
	if spec.Priority < 0 || spec.Priority > 1000 {
		errs.add("spec.priority", "must be between 0 and 1000")
	}
	if !validPolicy(spec.StatePolicy) {
		errs.add("spec.statePolicy", "must be strict, degraded, or best-effort")
	}
	if len(spec.RequiredCapabilities) > MaxCapabilities {
		errs.addf("spec.requiredCapabilities", "cannot exceed %d entries", MaxCapabilities)
	}
	if len(spec.AlternativeCapabilities) > MaxCapabilities {
		errs.addf("spec.alternativeCapabilities", "cannot exceed %d sets", MaxCapabilities)
	}
	validateCapabilities("spec.requiredCapabilities", spec.RequiredCapabilities, &errs)
	seenSets := map[string]struct{}{}
	for i, set := range spec.AlternativeCapabilities {
		path := fmt.Sprintf("spec.alternativeCapabilities[%d]", i)
		if values := utilvalidation.IsDNS1123Label(set.Name); len(values) > 0 {
			errs.add(path+".name", strings.Join(values, ", "))
		}
		if _, ok := seenSets[set.Name]; ok {
			errs.add(path+".name", "duplicate alternative set")
		}
		seenSets[set.Name] = struct{}{}
		if len(set.AllOf) == 0 {
			errs.add(path+".allOf", "cannot be empty")
		}
		validateCapabilities(path+".allOf", set.AllOf, &errs)
	}
	if len(spec.RequiredCapabilities) == 0 && len(spec.AlternativeCapabilities) == 0 {
		errs.add("spec.requiredCapabilities", "at least one required capability or alternative set is required")
	}
	validateStringMap("spec.requiredSoftware", spec.RequiredSoftware, &errs)
	if len(spec.Inputs) > MaxDataObjects {
		errs.addf("spec.inputs", "cannot exceed %d entries", MaxDataObjects)
	}
	seenData := map[string]struct{}{}
	for i, input := range spec.Inputs {
		path := fmt.Sprintf("spec.inputs[%d]", i)
		if values := utilvalidation.IsDNS1123Subdomain(input.ID); len(values) > 0 {
			errs.add(path+".id", strings.Join(values, ", "))
		}
		if _, ok := seenData[input.ID]; ok {
			errs.add(path+".id", "duplicate input ID")
		}
		seenData[input.ID] = struct{}{}
		if input.SizeBytes < 0 || input.SizeBytes > MaxDataBytes {
			errs.addf(path+".sizeBytes", "must be between 0 and %d", MaxDataBytes)
		}
		if input.SizeBytes > 0 && len(input.Locations) == 0 {
			errs.add(path+".locations", "is required for non-empty input")
		}
		validateLocations(path+".locations", input.Locations, &errs)
	}
	if spec.OutputSizeBytes < 0 || spec.OutputSizeBytes > MaxDataBytes {
		errs.addf("spec.outputSizeBytes", "must be between 0 and %d", MaxDataBytes)
	}
	if spec.ResultReturnRequired && len(spec.ResultDestinations) == 0 {
		errs.add("spec.resultDestinations", "is required when resultReturnRequired is true")
	}
	validateLocations("spec.resultDestinations", spec.ResultDestinations, &errs)
	if spec.Deadline.IsZero() || !spec.Deadline.After(clock.Now()) {
		errs.add("spec.deadline", "must be in the future")
	}
	if spec.ExpectedDurationSeconds < 1 || spec.ExpectedDurationSeconds > MaxMissionDurationSecs {
		errs.addf("spec.expectedDurationSeconds", "must be between 1 and %d", MaxMissionDurationSecs)
	}
	if spec.MaximumDurationSeconds < spec.ExpectedDurationSeconds || spec.MaximumDurationSeconds > MaxMissionDurationSecs {
		errs.addf("spec.maximumDurationSeconds", "must be at least expectedDurationSeconds and at most %d", MaxMissionDurationSecs)
	}
	if spec.DurationUncertaintySecs < 0 || spec.ExpectedDurationSeconds+spec.DurationUncertaintySecs > spec.MaximumDurationSeconds {
		errs.add("spec.durationUncertaintySeconds", "must be non-negative and fit within maximumDurationSeconds")
	}
	if spec.SafetyMarginSeconds < 0 || spec.SafetyMarginSeconds > MaxSafetyMarginSecs {
		errs.addf("spec.safetyMarginSeconds", "must be between 0 and %d", MaxSafetyMarginSecs)
	}
	if spec.MaximumClockSkewSeconds < 0 || spec.MaximumClockSkewSeconds > MaxClockSkewSecs {
		errs.addf("spec.maximumClockSkewSeconds", "must be between 0 and %d", MaxClockSkewSecs)
	}
	minimumFinish := clock.Now().Add(time.Duration(spec.MaximumDurationSeconds+spec.SafetyMarginSeconds+spec.MaximumClockSkewSeconds) * time.Second)
	if !spec.Deadline.After(minimumFinish) {
		errs.add("spec.deadline", "cannot accommodate maximum duration, safety margin and clock skew")
	}
	if spec.Retry.MaxAttempts < 1 || spec.Retry.MaxAttempts > 100 {
		errs.add("spec.retry.maxAttempts", "must be between 1 and 100")
	}
	if spec.Retry.MaxConcurrentExecutions != 1 {
		errs.add("spec.retry.maxConcurrentExecutions", "must be exactly 1 in v1alpha1")
	}
	if spec.Retry.AllowMigration && !spec.Checkpoint.Checkpointable {
		errs.add("spec.retry.allowMigration", "requires checkpoint.checkpointable")
	}
	if spec.Checkpoint.MinimumIntervalSecs < 0 || spec.Checkpoint.MaximumStateBytes < 0 {
		errs.add("spec.checkpoint", "interval and state size cannot be negative")
	}
	if !spec.Checkpoint.Checkpointable && (spec.Checkpoint.MinimumIntervalSecs != 0 || spec.Checkpoint.MaximumStateBytes != 0) {
		errs.add("spec.checkpoint", "non-checkpointable missions cannot configure checkpoint interval or state size")
	}
	if len(spec.WorkloadTemplate.Spec.Containers) == 0 {
		errs.add("spec.workloadTemplate.spec.containers", "must contain at least one container")
	}
	if spec.WorkloadTemplate.Spec.NodeName != "" {
		errs.add("spec.workloadTemplate.spec.nodeName", "must be empty because the local scheduler owns Node placement")
	}
	if scheduler := spec.WorkloadTemplate.Spec.SchedulerName; scheduler != "" && scheduler != "space-compute-scheduler" {
		errs.add("spec.workloadTemplate.spec.schedulerName", "must be empty or space-compute-scheduler")
	}
	if raw, err := json.Marshal(spec.WorkloadTemplate); err != nil {
		errs.add("spec.workloadTemplate", "must be serializable")
	} else if len(raw) > MaxWorkloadTemplateBytes {
		errs.addf("spec.workloadTemplate", "serialized size cannot exceed %d bytes", MaxWorkloadTemplateBytes)
	}
	return errs.errOrNil()
}

func validateCapabilities(path string, values []CapabilityRequirement, errs *ValidationErrors) {
	for i, value := range values {
		item := fmt.Sprintf("%s[%d]", path, i)
		if value.Class == "" || len(value.Class) > 63 {
			errs.add(item+".class", "must be non-empty and at most 63 bytes")
		}
		if value.Quantity < 1 || value.Quantity > 1_000_000 {
			errs.add(item+".quantity", "must be between 1 and 1000000")
		}
		validateStringMap(item+".software", value.Software, errs)
		if len(value.Architecture) > 128 || len(value.Model) > 128 {
			errs.add(item, "architecture and model cannot exceed 128 bytes")
		}
		if len(value.Precision) > 32 {
			errs.add(item+".precision", "cannot exceed 32 entries")
		}
		seenPrecision := map[string]struct{}{}
		for j, precision := range value.Precision {
			if strings.TrimSpace(precision) == "" || len(precision) > 63 {
				errs.addf(fmt.Sprintf("%s.precision[%d]", item, j), "must be non-empty and at most 63 bytes")
			}
			if _, exists := seenPrecision[precision]; exists {
				errs.addf(fmt.Sprintf("%s.precision[%d]", item, j), "duplicate precision")
			}
			seenPrecision[precision] = struct{}{}
		}
	}
}

func validateStringMap(path string, values map[string]string, errs *ValidationErrors) {
	if len(values) > 64 {
		errs.add(path, "cannot exceed 64 entries")
	}
	for key, value := range values {
		if problems := utilvalidation.IsQualifiedName(key); len(problems) > 0 {
			errs.add(path+"."+key, strings.Join(problems, ", "))
		}
		if problems := utilvalidation.IsValidLabelValue(value); len(problems) > 0 {
			errs.add(path+"."+key, strings.Join(problems, ", "))
		}
	}
}

func validateLocations(path string, values []string, errs *ValidationErrors) {
	if len(values) > 64 {
		errs.add(path, "cannot exceed 64 entries")
	}
	seen := map[string]struct{}{}
	for i, value := range values {
		if problems := utilvalidation.IsDNS1123Subdomain(value); len(problems) > 0 {
			errs.add(fmt.Sprintf("%s[%d]", path, i), strings.Join(problems, ", "))
		}
		if _, ok := seen[value]; ok {
			errs.add(fmt.Sprintf("%s[%d]", path, i), "duplicate location")
		}
		seen[value] = struct{}{}
	}
}

func ValidateResourceSummary(summary *SpaceDomainResourceSummary, clock Clock) error {
	var errs ValidationErrors
	if summary == nil {
		errs.add("summary", "is required")
		return errs
	}
	if clock == nil {
		errs.add("clock", "is required")
		return errs
	}
	validateDomain("spec.domain", summary.Spec.Domain, &errs)
	validateProvenance("spec.provenance", summary.Spec.Provenance, &errs)
	if summary.Spec.ObservedAt.IsZero() || summary.Spec.ValidUntil.IsZero() || !summary.Spec.ValidUntil.After(summary.Spec.ObservedAt.Time) {
		errs.add("spec.validUntil", "must be after observedAt")
	}
	if !summary.Spec.ValidUntil.After(clock.Now()) {
		errs.add("spec.validUntil", "summary is stale")
	}
	if summary.Spec.ObservedAt.After(clock.Now().Add(time.Duration(MaxClockSkewSecs) * time.Second)) {
		errs.add("spec.observedAt", "is beyond maximum supported clock skew")
	}
	if summary.Spec.ValidUntil.Time.Sub(summary.Spec.ObservedAt.Time) > time.Duration(MaxSnapshotLifetimeSecs)*time.Second {
		errs.addf("spec.validUntil", "snapshot lifetime exceeds %d seconds", MaxSnapshotLifetimeSecs)
	}
	if len(summary.Spec.Devices) > MaxCapabilities {
		errs.addf("spec.devices", "cannot exceed %d entries", MaxCapabilities)
	}
	seen := map[string]struct{}{}
	for i, device := range summary.Spec.Devices {
		path := fmt.Sprintf("spec.devices[%d]", i)
		if device.Class == "" {
			errs.add(path+".class", "is required")
		}
		if _, ok := seen[device.Class]; ok {
			errs.add(path+".class", "duplicate device class")
		}
		seen[device.Class] = struct{}{}
		if device.Count < 0 || device.ComputeMilli < 0 {
			errs.add(path, "count and computeMilli cannot be negative")
		}
		if device.FragmentationMilli < 0 || device.FragmentationMilli > 1000 {
			errs.add(path+".fragmentationMilli", "must be between 0 and 1000")
		}
		for field, values := range map[string][]string{"architectures": device.Architectures, "models": device.Models, "precision": device.Precision} {
			if len(values) > 64 {
				errs.add(path+"."+field, "cannot exceed 64 entries")
			}
			seenValues := map[string]struct{}{}
			for j, value := range values {
				if strings.TrimSpace(value) == "" || len(value) > 128 {
					errs.addf(fmt.Sprintf("%s.%s[%d]", path, field, j), "must be non-empty and at most 128 bytes")
				}
				if _, exists := seenValues[value]; exists {
					errs.addf(fmt.Sprintf("%s.%s[%d]", path, field, j), "duplicate value")
				}
				seenValues[value] = struct{}{}
			}
		}
	}
	validateStringMap("spec.software", summary.Spec.Software, &errs)
	validateLocations("spec.dataLocations", summary.Spec.DataLocations, &errs)
	for field, value := range map[string]int32{"energyHeadroomMilli": summary.Spec.EnergyHeadroomMilli, "thermalHeadroomMilli": summary.Spec.ThermalHeadroomMilli, "resilienceMilli": summary.Spec.ResilienceMilli, "minimumEnergyMilli": summary.Spec.MinimumEnergyMilli, "minimumThermalMilli": summary.Spec.MinimumThermalMilli} {
		if value < 0 || value > 1000 {
			errs.add("spec."+field, "must be between 0 and 1000")
		}
	}
	if summary.Spec.QueueDelaySeconds < 0 || summary.Spec.MaximumSnapshotAgeSecs < 1 {
		errs.add("spec", "queueDelaySeconds cannot be negative and maximumSnapshotAgeSeconds must be positive")
	}
	decoded, err := hex.DecodeString(summary.Spec.ExporterSnapshotDigest)
	if err != nil || len(decoded) != sha256.Size || summary.Spec.ExporterSnapshotDigest != strings.ToLower(summary.Spec.ExporterSnapshotDigest) {
		errs.add("spec.exporterSnapshotDigest", "must be a lowercase hexadecimal SHA-256 digest")
	}
	return errs.errOrNil()
}

func ValidatePlacement(placement *SpacePlacementIntent, mission *SpaceMission) error {
	var errs ValidationErrors
	if placement == nil {
		errs.add("placement", "is required")
		return errs
	}
	if mission == nil {
		errs.add("mission", "is required")
		return errs
	}
	validateDomain("spec.target", placement.Spec.Target, &errs)
	if placement.Spec.MissionRef.Name != mission.Name || placement.Spec.MissionRef.Namespace != mission.Namespace || placement.Spec.MissionRef.UID != mission.UID {
		errs.add("spec.missionRef", "must identify the owning mission by namespace, name and UID")
	}
	if values := utilvalidation.IsDNS1123Label(placement.Spec.PlanID); len(values) > 0 {
		errs.add("spec.planID", strings.Join(values, ", "))
	}
	if placement.Spec.Attempt < 1 || placement.Spec.Attempt > mission.Spec.Retry.MaxAttempts {
		errs.add("spec.attempt", "must be within the mission retry budget")
	}
	if placement.Spec.NotBefore.IsZero() || placement.Spec.ExpiresAt.IsZero() || !placement.Spec.ExpiresAt.After(placement.Spec.NotBefore.Time) {
		errs.add("spec.expiresAt", "must be after notBefore")
	}
	if placement.Spec.ComputeStart.Before(&placement.Spec.NotBefore) || !placement.Spec.ComputeEnd.After(placement.Spec.ComputeStart.Time) {
		errs.add("spec.computeEnd", "compute interval must start after notBefore and have positive duration")
	}
	if placement.Spec.ComputeEnd.After(mission.Spec.Deadline.Time) || placement.Spec.ExpiresAt.After(mission.Spec.Deadline.Time) {
		errs.add("spec", "compute and plan expiry cannot exceed mission deadline")
	}
	if strings.TrimSpace(placement.Spec.MaterialInputDigest) == "" {
		errs.add("spec.materialInputDigest", "is required")
	}
	if mission.Spec.ResultReturnRequired && placement.Spec.ResultTransfer == nil {
		errs.add("spec.resultTransfer", "is required by the mission")
	}
	return errs.errOrNil()
}
