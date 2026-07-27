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
	skew, skewErr := checkedSecondsDurationAPI(snapshot.Spec.MaximumClockSkewSeconds)
	if skewErr != nil {
		errs.add("spec.maximumClockSkewSeconds", "duration conversion overflow")
		skew = 0
	}
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
		minimum, minimumErr := checkedSecondsDurationAPI(snapshot.Spec.MinimumUpdateSeconds)
		if minimumErr != nil {
			errs.add("spec.minimumUpdateSeconds", "duration conversion overflow")
		}
		if minimumErr == nil && observed.Sub(previous.Spec.ObservedAt.Time) < minimum && contactWindowsDigest(snapshot.Spec.Windows) == contactWindowsDigest(previous.Spec.Windows) {
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
		fmt.Fprintf(h, "%s|%d|%d|%d|%d|%d|%d|%d|%d|%t\n", w.ID, w.Start.UnixNano(), w.End.UnixNano(), w.BandwidthBitsPerSec, w.RTTMicroseconds, w.LossPartsPerMillion, w.ErrorPartsPerMillion, w.StabilityMilli, w.ConfidenceMilli, w.Predicted)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// ContactWindowsDigest is the deterministic material digest for contact-window
// content. It is exported for controllers/tests that need to compare material
// link input without duplicating field coverage.
func ContactWindowsDigest(windows []ContactWindow) string {
	return contactWindowsDigest(windows)
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
	if spec.WorkingMemoryBytes < 0 || spec.WorkingMemoryBytes > MaxCapacityBytes {
		errs.addf("spec.workingMemoryBytes", "must be between 0 and %d", MaxCapacityBytes)
	}
	if spec.WorkingStorageBytes < 0 || spec.WorkingStorageBytes > MaxCapacityBytes {
		errs.addf("spec.workingStorageBytes", "must be between 0 and %d", MaxCapacityBytes)
	}
	if spec.MinimumBandwidthBitsPerSecond < 0 || spec.MinimumBandwidthBitsPerSecond > MaxBandwidthBitsPerSecond {
		errs.addf("spec.minimumBandwidthBitsPerSecond", "must be between 0 and %d", MaxBandwidthBitsPerSecond)
	}
	if spec.MaximumRTTMicroseconds < 0 || spec.MaximumRTTMicroseconds > MaxRTTMicroseconds {
		errs.addf("spec.maximumRTTMicroseconds", "must be between 0 and %d", MaxRTTMicroseconds)
	}
	if spec.MaximumLossPartsPerMillion < 0 || spec.MaximumLossPartsPerMillion > 1_000_000 {
		errs.add("spec.maximumLossPartsPerMillion", "must be between 0 and 1000000")
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
		} else if len(set.AllOf) > MaxCapabilities {
			errs.addf(path+".allOf", "cannot exceed %d entries", MaxCapabilities)
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
	totalInputBytes := int64(0)
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
		} else if total, err := checkedAddInt64API(totalInputBytes, input.SizeBytes); err != nil {
			errs.add("spec.inputs", "total input bytes overflow int64")
		} else {
			totalInputBytes = total
		}
		if input.SizeBytes > 0 && len(input.Locations) == 0 {
			errs.add(path+".locations", "is required for non-empty input")
		}
		if input.PayloadDigest != "" {
			validateLowerSHA256(path+".payloadDigest", input.PayloadDigest, &errs)
		}
		validateDataLocations(path+".locations", input.Locations, &errs)
	}
	if spec.OutputSizeBytes < 0 || spec.OutputSizeBytes > MaxDataBytes {
		errs.addf("spec.outputSizeBytes", "must be between 0 and %d", MaxDataBytes)
	}
	if spec.ResultReturnRequired && len(spec.ResultDestinations) == 0 {
		errs.add("spec.resultDestinations", "is required when resultReturnRequired is true")
	}
	validateDataLocations("spec.resultDestinations", spec.ResultDestinations, &errs)
	if spec.Deadline.IsZero() || !spec.Deadline.After(clock.Now()) {
		errs.add("spec.deadline", "must be in the future")
	}
	if spec.ExpectedDurationSeconds < 1 || spec.ExpectedDurationSeconds > MaxMissionDurationSecs {
		errs.addf("spec.expectedDurationSeconds", "must be between 1 and %d", MaxMissionDurationSecs)
	}
	if spec.MaximumDurationSeconds < spec.ExpectedDurationSeconds || spec.MaximumDurationSeconds > MaxMissionDurationSecs {
		errs.addf("spec.maximumDurationSeconds", "must be at least expectedDurationSeconds and at most %d", MaxMissionDurationSecs)
	}
	if spec.DurationUncertaintySecs < 0 {
		errs.add("spec.durationUncertaintySeconds", "must be non-negative and fit within maximumDurationSeconds")
	} else if durationWithUncertainty, err := checkedAddInt64API(spec.ExpectedDurationSeconds, spec.DurationUncertaintySecs); err != nil || durationWithUncertainty > spec.MaximumDurationSeconds {
		errs.add("spec.durationUncertaintySeconds", "must be non-negative, non-overflowing and fit within maximumDurationSeconds")
	}
	if spec.SafetyMarginSeconds < 0 || spec.SafetyMarginSeconds > MaxSafetyMarginSecs {
		errs.addf("spec.safetyMarginSeconds", "must be between 0 and %d", MaxSafetyMarginSecs)
	}
	if spec.MaximumClockSkewSeconds < 0 || spec.MaximumClockSkewSeconds > MaxClockSkewSecs {
		errs.addf("spec.maximumClockSkewSeconds", "must be between 0 and %d", MaxClockSkewSecs)
	}
	minimumSeconds, durationErr := checkedAddInt64API(spec.MaximumDurationSeconds, spec.SafetyMarginSeconds)
	if durationErr == nil {
		minimumSeconds, durationErr = checkedAddInt64API(minimumSeconds, spec.MaximumClockSkewSeconds)
	}
	minimumDuration, conversionErr := checkedSecondsDurationAPI(minimumSeconds)
	if durationErr != nil || conversionErr != nil {
		errs.add("spec.deadline", "duration arithmetic overflow")
	} else {
		minimumFinish := clock.Now().Add(minimumDuration)
		if minimumFinish.Year() < 1 || minimumFinish.Year() > 9999 {
			errs.add("spec.deadline", "minimum finish timestamp is outside RFC3339 range")
		} else if !spec.Deadline.After(minimumFinish) {
			errs.add("spec.deadline", "cannot accommodate maximum duration, safety margin and clock skew")
		}
	}
	if spec.Retry.MaxAttempts < 1 || spec.Retry.MaxAttempts > 100 {
		errs.add("spec.retry.maxAttempts", "must be between 1 and 100")
	}
	if spec.Retry.MaxConcurrentExecutions != 1 {
		errs.add("spec.retry.maxConcurrentExecutions", "must be exactly 1 in the current space-compute API")
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
	if len(values) > MaxSoftwareEntries {
		errs.addf(path, "cannot exceed %d entries", MaxSoftwareEntries)
	}
	for key, value := range values {
		if len(key) > MaxSoftwareKeyBytes {
			errs.addf(path+"."+key, "key cannot exceed %d bytes", MaxSoftwareKeyBytes)
		}
		if len(value) > MaxSoftwareValueBytes {
			errs.addf(path+"."+key, "value cannot exceed %d bytes", MaxSoftwareValueBytes)
		}
		if problems := utilvalidation.IsQualifiedName(key); len(problems) > 0 {
			errs.add(path+"."+key, strings.Join(problems, ", "))
		}
		if problems := utilvalidation.IsValidLabelValue(value); len(problems) > 0 {
			errs.add(path+"."+key, strings.Join(problems, ", "))
		}
	}
}

func validateStringLocations(path string, values []string, errs *ValidationErrors) {
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
		if strings.TrimSpace(device.Class) == "" {
			errs.add(path+".class", "is required")
		}
		identity := deviceCapacityBucketIdentityKey(device)
		if _, ok := seen[identity]; ok {
			errs.add(path, "duplicate device capacity bucket")
		}
		seen[identity] = struct{}{}
		if device.Count < 0 || device.Count > MaxDeviceCapacityCount {
			errs.addf(path+".count", "must be between 0 and %d", MaxDeviceCapacityCount)
		}
		if device.ComputeMilli < 0 || device.ComputeMilli > MaxComputeMilli {
			errs.addf(path+".computeMilli", "must be between 0 and %d", MaxComputeMilli)
		}
		if device.FragmentationMilli < 0 || device.FragmentationMilli > 1000 {
			errs.add(path+".fragmentationMilli", "must be between 0 and 1000")
		}
		for field, values := range map[string][]string{"architectures": device.Architectures, "models": device.Models, "precision": device.Precision} {
			limit := MaxDeviceTopologyValues
			if field == "precision" {
				limit = MaxDevicePrecisionValues
			}
			if len(values) > limit {
				errs.addf(path+"."+field, "cannot exceed %d entries", limit)
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
	validateStringLocations("spec.dataLocations", summary.Spec.DataLocations, &errs)
	for field, value := range map[string]int32{"energyHeadroomMilli": summary.Spec.EnergyHeadroomMilli, "thermalHeadroomMilli": summary.Spec.ThermalHeadroomMilli, "resilienceMilli": summary.Spec.ResilienceMilli, "minimumEnergyMilli": summary.Spec.MinimumEnergyMilli, "minimumThermalMilli": summary.Spec.MinimumThermalMilli} {
		if value < 0 || value > 1000 {
			errs.add("spec."+field, "must be between 0 and 1000")
		}
	}
	if summary.Spec.QueueDelaySeconds < 0 || summary.Spec.QueueDelaySeconds > MaxQueueDelaySecs {
		errs.addf("spec.queueDelaySeconds", "must be between 0 and %d", MaxQueueDelaySecs)
	}
	if summary.Spec.MaximumSnapshotAgeSecs < 1 || summary.Spec.MaximumSnapshotAgeSecs > MaxMaximumSnapshotAgeSecs {
		errs.addf("spec.maximumSnapshotAgeSeconds", "must be between 1 and %d", MaxMaximumSnapshotAgeSecs)
	}
	decoded, err := hex.DecodeString(summary.Spec.ExporterSnapshotDigest)
	if err != nil || len(decoded) != sha256.Size || summary.Spec.ExporterSnapshotDigest != strings.ToLower(summary.Spec.ExporterSnapshotDigest) {
		errs.add("spec.exporterSnapshotDigest", "must be a lowercase hexadecimal SHA-256 digest")
	}
	validateScalarCapacity("spec.cpu", summary.Spec.CPU, MaxCPUMilli, &errs)
	validateScalarCapacity("spec.systemMemoryBytes", summary.Spec.SystemMemoryBytes, MaxCapacityBytes, &errs)
	validateScalarCapacity("spec.ephemeralStorageBytes", summary.Spec.EphemeralStorageBytes, MaxCapacityBytes, &errs)
	if len(summary.Spec.PersistentStorage) > MaxPersistentStorageClasses {
		errs.addf("spec.persistentStorage", "cannot exceed %d entries", MaxPersistentStorageClasses)
	}
	storageClasses := map[string]struct{}{}
	for i, storage := range summary.Spec.PersistentStorage {
		path := fmt.Sprintf("spec.persistentStorage[%d]", i)
		if strings.TrimSpace(storage.Class) == "" || len(storage.Class) > 63 {
			errs.add(path+".class", "must be non-empty and at most 63 bytes")
		}
		if _, exists := storageClasses[storage.Class]; exists {
			errs.add(path+".class", "duplicate storage class")
		}
		storageClasses[storage.Class] = struct{}{}
		validateScalarCapacity(path, ScalarCapacity{Capacity: storage.CapacityBytes, Available: storage.AvailableBytes}, MaxCapacityBytes, &errs)
	}
	if len(summary.Spec.NUMATopology) > MaxNUMANodes {
		errs.addf("spec.numaTopology", "cannot exceed %d entries", MaxNUMANodes)
	}
	numaIDs := map[int32]struct{}{}
	for i, node := range summary.Spec.NUMATopology {
		path := fmt.Sprintf("spec.numaTopology[%d]", i)
		if node.ID < 0 {
			errs.add(path+".id", "must be non-negative")
		}
		if _, exists := numaIDs[node.ID]; exists {
			errs.add(path+".id", "duplicate NUMA ID")
		}
		numaIDs[node.ID] = struct{}{}
		validateScalarCapacity(path+".cpuMilli", ScalarCapacity{Capacity: node.CPUMilliCapacity, Available: node.CPUMilliAvailable}, MaxCPUMilli, &errs)
		validateScalarCapacity(path+".memoryBytes", ScalarCapacity{Capacity: node.MemoryCapacityBytes, Available: node.MemoryAvailableBytes}, MaxCapacityBytes, &errs)
	}
	validateTrustState("spec.trust", summary.Spec.Trust, &errs)
	if summary.Spec.AutonomyDurationSeconds < 0 || summary.Spec.AutonomyDurationSeconds > MaxAutonomyDurationSeconds {
		errs.addf("spec.autonomyDurationSeconds", "must be between 0 and %d", MaxAutonomyDurationSeconds)
	}
	validateEnergyBudget("spec.energy", summary.Spec.Energy, &errs)
	if ref := summary.Spec.PhysicalDeviceInventoryRef; ref != nil {
		if values := utilvalidation.IsDNS1123Subdomain(ref.Name); len(values) > 0 {
			errs.add("spec.physicalDeviceInventoryRef.name", strings.Join(values, ", "))
		}
		validateLowerSHA256("spec.physicalDeviceInventoryRef.digest", ref.Digest, &errs)
		if len(ref.ResourceVersion) > 128 {
			errs.add("spec.physicalDeviceInventoryRef.resourceVersion", "cannot exceed 128 bytes")
		}
	}
	return errs.errOrNil()
}

func validateScalarCapacity(path string, value ScalarCapacity, maximum int64, errs *ValidationErrors) {
	if value.Capacity < 0 || value.Capacity > maximum {
		errs.addf(path+".capacity", "must be between 0 and %d", maximum)
	}
	if value.Available < 0 || value.Available > maximum {
		errs.addf(path+".available", "must be between 0 and %d", maximum)
	}
	if value.Available > value.Capacity {
		errs.add(path+".available", "cannot exceed capacity")
	}
}

func validateTrustState(path string, value TrustAttestationState, errs *ValidationErrors) {
	switch value.State {
	case "", "unknown", "unverified", "verified", "failed":
	default:
		errs.add(path+".state", "must be unknown, unverified, verified, or failed")
	}
	if len(value.Provider) > 128 {
		errs.add(path+".provider", "cannot exceed 128 bytes")
	}
	if value.EvidenceDigest != "" {
		validateLowerSHA256(path+".evidenceDigest", value.EvidenceDigest, errs)
	}
	if !value.ObservedAt.IsZero() && !value.ValidUntil.IsZero() && !value.ValidUntil.After(value.ObservedAt.Time) {
		errs.add(path+".validUntil", "must be after observedAt")
	}
}

func validateEnergyBudget(path string, value EnergyBudget, errs *ValidationErrors) {
	if len(value.Source) > 128 {
		errs.add(path+".source", "cannot exceed 128 bytes")
	}
	validateScalarCapacity(path, ScalarCapacity{Capacity: value.CapacityMilliWattHours, Available: value.AvailableMilliWattHours}, MaxEnergyMilliWattHours, errs)
}

func ValidatePhysicalDeviceInventory(inventory *PhysicalDeviceInventory, clock Clock) error {
	var errs ValidationErrors
	if inventory == nil {
		errs.add("inventory", "is required")
		return errs
	}
	if clock == nil {
		errs.add("clock", "is required")
		return errs
	}
	validateDomain("spec.domain", inventory.Spec.Domain, &errs)
	if problems := utilvalidation.IsDNS1123Subdomain(inventory.Spec.NodeName); len(problems) > 0 {
		errs.add("spec.nodeName", strings.Join(problems, ", "))
	} else if inventory.Name != PhysicalDeviceInventoryName(inventory.Spec.Domain, inventory.Spec.NodeName) {
		errs.addf("metadata.name", "must be %q for domain and nodeName", PhysicalDeviceInventoryName(inventory.Spec.Domain, inventory.Spec.NodeName))
	}
	validateProvenance("spec.provenance", inventory.Spec.Provenance, &errs)
	if inventory.Spec.ObservedAt.IsZero() || inventory.Spec.ValidUntil.IsZero() || !inventory.Spec.ValidUntil.After(inventory.Spec.ObservedAt.Time) {
		errs.add("spec.validUntil", "must be after observedAt")
	}
	if !inventory.Spec.ValidUntil.After(clock.Now()) {
		errs.add("spec.validUntil", "inventory is stale")
	}
	if inventory.Spec.ConfidenceMilli < 0 || inventory.Spec.ConfidenceMilli > 1000 {
		errs.add("spec.confidenceMilli", "must be between 0 and 1000")
	}
	if len(inventory.Spec.Devices) > MaxPhysicalDevices {
		errs.addf("spec.devices", "cannot exceed %d entries", MaxPhysicalDevices)
	}
	seen := map[string]struct{}{}
	for i, device := range inventory.Spec.Devices {
		path := fmt.Sprintf("spec.devices[%d]", i)
		if strings.TrimSpace(device.StableDeviceID) == "" || len(device.StableDeviceID) > 253 {
			errs.add(path+".stableDeviceID", "must be non-empty and at most 253 bytes")
		}
		if _, exists := seen[device.StableDeviceID]; exists {
			errs.add(path+".stableDeviceID", "duplicate stable device ID")
		}
		seen[device.StableDeviceID] = struct{}{}
		if problems := utilvalidation.IsQualifiedName(device.KubernetesResourceName); len(problems) > 0 {
			errs.add(path+".kubernetesResourceName", strings.Join(problems, ", "))
		}
		for field, value := range map[string]string{"class": device.Class, "vendor": device.Vendor, "model": device.Model, "architecture": device.Architecture} {
			if strings.TrimSpace(value) == "" || len(value) > 128 {
				errs.add(path+"."+field, "must be non-empty and at most 128 bytes")
			}
		}
		for field, value := range map[string]string{"allocationID": device.AllocationID, "draAllocationID": device.DRAAllocationID, "vendorAllocationID": device.VendorAllocationID} {
			if len(value) > 512 {
				errs.add(path+"."+field, "cannot exceed 512 bytes")
			}
		}
		if len(device.Topology.SocketID) > 128 || len(device.Topology.PCIAddress) > 32 {
			errs.add(path+".topology", "socketID or pciAddress is too long")
		}
		if len(device.PeerInterconnects) > MaxPeerInterconnects {
			errs.addf(path+".peerInterconnects", "cannot exceed %d entries", MaxPeerInterconnects)
		}
		for j, peer := range device.PeerInterconnects {
			pp := fmt.Sprintf("%s.peerInterconnects[%d]", path, j)
			if strings.TrimSpace(peer.PeerStableDeviceID) == "" || len(peer.PeerStableDeviceID) > 253 {
				errs.add(pp+".peerStableDeviceID", "must be non-empty and at most 253 bytes")
			}
			if strings.TrimSpace(peer.Type) == "" || len(peer.Type) > 64 {
				errs.add(pp+".type", "must be non-empty and at most 64 bytes")
			}
			if peer.BandwidthBitsPerSecond < 0 || peer.BandwidthBitsPerSecond > MaxBandwidthBitsPerSecond {
				errs.addf(pp+".bandwidthBitsPerSecond", "must be between 0 and %d", MaxBandwidthBitsPerSecond)
			}
		}
		if device.TotalMemoryBytes < 0 || device.TotalMemoryBytes > MaxCapacityBytes || device.FreeMemoryBytes < 0 || device.FreeMemoryBytes > device.TotalMemoryBytes {
			errs.add(path+".freeMemoryBytes", "memory must be non-negative and free cannot exceed total")
		}
		for field, value := range map[string]int64{"memoryBandwidthBitsPerSecond": device.MemoryBandwidthBitsPerSecond, "interconnectBandwidthBitsPerSecond": device.InterconnectBandwidthBitsPerSecond} {
			if value < 0 || value > MaxBandwidthBitsPerSecond {
				errs.addf(path+"."+field, "must be between 0 and %d", MaxBandwidthBitsPerSecond)
			}
		}
		if len(device.SupportedPrecision) > MaxDevicePrecisionValues {
			errs.addf(path+".supportedPrecision", "cannot exceed %d entries", MaxDevicePrecisionValues)
		}
		validateStringMap(path+".libraries", device.Libraries, &errs)
		switch device.Health {
		case "healthy", "degraded", "unhealthy", "unknown":
		default:
			errs.add(path+".health", "must be healthy, degraded, unhealthy, or unknown")
		}
		if device.TemperatureMilliCelsius < MinTemperatureMilliCelsius || device.TemperatureMilliCelsius > MaxTemperatureMilliCelsius {
			errs.add(path+".temperatureMilliCelsius", "is outside supported physical range")
		}
		if device.PowerMilliwatts < 0 || device.PowerMilliwatts > MaxPowerMilliwatts {
			errs.add(path+".powerMilliwatts", "is outside supported range")
		}
		if device.ConfidenceMilli < 0 || device.ConfidenceMilli > 1000 {
			errs.add(path+".confidenceMilli", "must be between 0 and 1000")
		}
	}
	for i, device := range inventory.Spec.Devices {
		for j, peer := range device.PeerInterconnects {
			path := fmt.Sprintf("spec.devices[%d].peerInterconnects[%d].peerStableDeviceID", i, j)
			if peer.PeerStableDeviceID == device.StableDeviceID {
				errs.add(path, "cannot refer to the same physical device")
			} else if _, exists := seen[peer.PeerStableDeviceID]; !exists {
				errs.add(path, "must reference a stableDeviceID present in this inventory")
			}
		}
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
	if placement.Spec.PlanningInputDigest != "" {
		decoded, err := hex.DecodeString(placement.Spec.PlanningInputDigest)
		if err != nil || len(decoded) != 32 || strings.ToLower(placement.Spec.PlanningInputDigest) != placement.Spec.PlanningInputDigest {
			errs.add("spec.planningInputDigest", "must be a lowercase hexadecimal SHA-256 digest")
		}
	}
	if len(placement.Spec.CacheResourceVersions) > 2 {
		errs.add("spec.cacheResourceVersions", "cannot contain more than resourceSummaries and linkSnapshots")
	}
	for key, value := range placement.Spec.CacheResourceVersions {
		if key != "resourceSummaries" && key != "linkSnapshots" {
			errs.add("spec.cacheResourceVersions", "contains an unsupported cache key")
		}
		if len(value) > 128 {
			errs.add("spec.cacheResourceVersions."+key, "cannot exceed 128 bytes")
		}
	}
	if mission.Spec.ResultReturnRequired && placement.Spec.ResultTransfer == nil {
		errs.add("spec.resultTransfer", "is required by the mission")
	}
	if len(placement.Spec.InputTransfers) > MaxTransferEpochs {
		errs.addf("spec.inputTransfers", "cannot exceed %d entries", MaxTransferEpochs)
	}
	for i, epoch := range placement.Spec.InputTransfers {
		validateTransferEpoch(fmt.Sprintf("spec.inputTransfers[%d]", i), epoch, &errs)
	}
	if placement.Spec.ResultTransfer != nil {
		validateTransferEpoch("spec.resultTransfer", *placement.Spec.ResultTransfer, &errs)
	}
	if len(placement.Spec.SnapshotSequences) > MaxSnapshotSequenceEntries {
		errs.addf("spec.snapshotSequences", "cannot exceed %d entries", MaxSnapshotSequenceEntries)
	}
	if len(placement.Spec.SelectedCapabilities) > MaxCapabilities {
		errs.addf("spec.selectedCapabilities", "cannot exceed %d entries", MaxCapabilities)
	}
	validateCapabilities("spec.selectedCapabilities", placement.Spec.SelectedCapabilities, &errs)
	if len(placement.Spec.SelectedPhysicalDeviceConstraints) > MaxCapabilities {
		errs.addf("spec.selectedPhysicalDeviceConstraints", "cannot exceed %d entries", MaxCapabilities)
	}
	for i, value := range placement.Spec.SelectedPhysicalDeviceConstraints {
		path := fmt.Sprintf("spec.selectedPhysicalDeviceConstraints[%d]", i)
		if strings.TrimSpace(value.Class) == "" || value.Quantity < 1 || value.Quantity > MaxDeviceCapacityCount {
			errs.add(path, "requires non-empty class and bounded positive quantity")
		}
		if len(value.Precision) > MaxDevicePrecisionValues || len(value.StableDeviceIDs) > MaxPhysicalDevices || len(value.AllocationIDs) > MaxPhysicalDevices {
			errs.add(path, "contains too many precision/device/allocation values")
		}
	}
	if len(placement.Status.TransferReceiptReferences) > MaxReferenceCount {
		errs.addf("status.transferReceiptReferences", "cannot exceed %d entries", MaxReferenceCount)
	}
	for i, value := range placement.Status.TransferReceiptReferences {
		if len(value) > 253 {
			errs.addf(fmt.Sprintf("status.transferReceiptReferences[%d]", i), "cannot exceed 253 bytes")
		}
	}
	for field, value := range map[string]string{"executionLeaseReference": placement.Status.ExecutionLeaseReference, "checkpointReceipt": placement.Status.CheckpointReceipt, "resultReceipt": placement.Status.ResultReceipt} {
		if len(value) > 253 {
			errs.add("status."+field, "cannot exceed 253 bytes")
		}
	}
	if placement.Status.FencingTokenHash != "" {
		validateLowerSHA256("status.fencingTokenHash", placement.Status.FencingTokenHash, &errs)
	}
	if placement.Status.RemoteAcknowledgementSequence < 0 {
		errs.add("status.remoteAcknowledgementSequence", "cannot be negative")
	}
	return errs.errOrNil()
}

func validateTransferEpoch(path string, epoch TransferEpoch, errs *ValidationErrors) {
	if epoch.Bytes < 0 || epoch.Bytes > MaxDataBytes {
		errs.addf(path+".bytes", "must be between 0 and %d", MaxDataBytes)
	}
	if epoch.Start.IsZero() || epoch.End.IsZero() || epoch.End.Before(&epoch.Start) {
		errs.add(path, "start/end must be present and end cannot precede start")
	}
	for field, value := range map[string]string{"sourceURI": epoch.SourceURI, "destinationURI": epoch.DestinationURI} {
		validateDataURI(path+"."+field, value, errs)
	}
}
