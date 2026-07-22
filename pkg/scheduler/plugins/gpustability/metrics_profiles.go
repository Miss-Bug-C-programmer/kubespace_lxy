package gpustability

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strings"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

const defaultMetricProfile = "auto"

const maxMetricProfilesFileBytes = 1 << 20

const (
	metricProfilesAPIVersion = "gpustability.k3s.io/v1alpha1"
	metricProfilesKind       = "MetricProfileList"
)

type deviceMetricField string

const (
	fieldGPUUtilization    deviceMetricField = "gpu_utilization"
	fieldMemoryUtilization deviceMetricField = "memory_utilization"
	fieldMemoryFreeMiB     deviceMetricField = "memory_free_mib"
	fieldMemoryTotalMiB    deviceMetricField = "memory_total_mib"
	fieldMemoryUsedMiB     deviceMetricField = "memory_used_mib"
	fieldTemperatureC      deviceMetricField = "temperature_celsius"
	fieldSMClockMHz        deviceMetricField = "sm_clock_mhz"
	fieldMemClockMHz       deviceMetricField = "memory_clock_mhz"
	fieldPowerUsageW       deviceMetricField = "power_usage_watts"
	fieldHealth            deviceMetricField = "health"
)

type metricUnit string

const (
	unitScalar metricUnit = ""
	unitBytes  metricUnit = "bytes"
	unitKiB    metricUnit = "kib"
	unitMiB    metricUnit = "mib"
	unitRatio  metricUnit = "ratio"
)

type fieldRollup string

const (
	rollupAvg         fieldRollup = "avg"
	rollupMax         fieldRollup = "max"
	rollupMinPositive fieldRollup = "min_positive"
	rollupSum         fieldRollup = "sum"
	rollupMin         fieldRollup = "min"
)

type metricFieldSpec struct {
	Names  []string
	Unit   metricUnit
	Rollup fieldRollup
	Min    *float64
	Max    *float64
}

type metricProfile struct {
	Name           string
	Class          DeviceClass
	MatchNames     []string
	IdentityLabels []string
	NameLabels     []string
	RequiredFields map[deviceMetricField]struct{}
	Health         healthMapping
	Fields         map[deviceMetricField]metricFieldSpec
	// SingleSamplePerDeviceField is used by exporters such as Iluvatar where
	// every ix_* family is a single device gauge. Duplicate series would make a
	// rollup look valid while hiding exporter identity/configuration corruption.
	SingleSamplePerDeviceField bool
}

type metricProfilesConfig struct {
	APIVersion string                `json:"apiVersion,omitempty"`
	Kind       string                `json:"kind,omitempty"`
	Profiles   []metricProfileConfig `json:"profiles"`
}

type metricProfileConfig struct {
	Name           string                           `json:"name"`
	Class          string                           `json:"class"`
	MatchNames     []string                         `json:"matchNames"`
	MatchNamesAlt  []string                         `json:"match_names"`
	IdentityLabels []string                         `json:"identityLabels,omitempty"`
	NameLabels     []string                         `json:"nameLabels,omitempty"`
	RequiredFields []string                         `json:"requiredFields,omitempty"`
	Health         *healthMappingConfig             `json:"health,omitempty"`
	Fields         map[string]metricFieldSpecConfig `json:"fields"`
}

type metricFieldSpecConfig struct {
	Names  []string `json:"names"`
	Unit   string   `json:"unit"`
	Rollup string   `json:"rollup"`
	Min    *float64 `json:"min,omitempty"`
	Max    *float64 `json:"max,omitempty"`
}

type healthMappingConfig struct {
	HealthyValues   []float64 `json:"healthyValues"`
	UnhealthyValues []float64 `json:"unhealthyValues"`
}

type healthMapping struct {
	HealthyValues   map[float64]struct{}
	UnhealthyValues map[float64]struct{}
}

type parserLimits struct {
	MaxMetricFamilies  int
	MaxSamples         int
	MaxLabelsPerSample int
	MaxDevices         int
}

func defaultParserLimits() parserLimits {
	return parserLimits{MaxMetricFamilies: 10_000, MaxSamples: 100_000, MaxLabelsPerSample: 64, MaxDevices: 256}
}

type telemetryAdapter interface {
	Name() string
	Matches(*metricStore) bool
	Normalize(*metricStore) (nodeMetrics, error)
}

type declarativeProfileAdapter struct{ profile metricProfile }

func (a declarativeProfileAdapter) Name() string                    { return a.profile.Name }
func (a declarativeProfileAdapter) Matches(store *metricStore) bool { return a.profile.matches(store) }
func (a declarativeProfileAdapter) Normalize(store *metricStore) (nodeMetrics, error) {
	return a.profile.build(store)
}

type metricSample struct {
	Value  float64
	Labels map[string]string
}

type metricStore struct {
	samples    map[string][]metricSample
	maxDevices int
}

func parseMetricsForProfile(r io.Reader, requestedProfile string) (nodeMetrics, error) {
	return parseMetricsWithProfiles(r, requestedProfile, registeredMetricProfiles())
}

func parseMetricsWithProfiles(r io.Reader, requestedProfile string, profiles []metricProfile) (nodeMetrics, error) {
	return parseMetricsWithProfilesAndLimits(r, requestedProfile, profiles, defaultParserLimits())
}

func parseMetricsWithProfilesAndLimits(r io.Reader, requestedProfile string, profiles []metricProfile, limits parserLimits) (nodeMetrics, error) {
	adapters := make([]telemetryAdapter, 0, len(profiles))
	for _, profile := range profiles {
		adapters = append(adapters, declarativeProfileAdapter{profile: profile})
	}
	return parseMetricsWithAdapters(r, requestedProfile, adapters, limits)
}

func parseMetricsWithAdapters(r io.Reader, requestedProfile string, adapters []telemetryAdapter, limits parserLimits) (nodeMetrics, error) {
	store, err := parsePrometheusMetricsWithLimits(r, limits)
	if err != nil {
		return nodeMetrics{}, err
	}
	requestedProfile = strings.ToLower(strings.TrimSpace(requestedProfile))
	if requestedProfile == "" {
		requestedProfile = defaultMetricProfile
	}
	selected := make([]telemetryAdapter, 0, 1)
	for _, adapter := range adapters {
		if requestedProfile == defaultMetricProfile {
			if adapter.Matches(store) {
				selected = append(selected, adapter)
			}
		} else if strings.EqualFold(adapter.Name(), requestedProfile) {
			selected = append(selected, adapter)
		}
	}
	if len(selected) == 0 {
		return nodeMetrics{}, fmt.Errorf("no GPU metric profile matched exporter metrics")
	}
	if requestedProfile == defaultMetricProfile && len(selected) > 1 {
		names := make([]string, 0, len(selected))
		for _, adapter := range selected {
			names = append(names, adapter.Name())
		}
		sort.Strings(names)
		return nodeMetrics{}, fmt.Errorf("ambiguous metric profile auto-detection matched %s; pin one profile explicitly", strings.Join(names, ", "))
	}
	metrics, err := selected[0].Normalize(store)
	if err != nil {
		return nodeMetrics{}, fmt.Errorf("profile %q: %w", selected[0].Name(), err)
	}
	if metrics.GPUCount == 0 {
		return nodeMetrics{}, fmt.Errorf("profile %q produced no device observations", selected[0].Name())
	}
	return metrics, nil
}

func parsePrometheusMetrics(r io.Reader) (*metricStore, error) {
	return parsePrometheusMetricsWithLimits(r, defaultParserLimits())
}

func parsePrometheusMetricsWithLimits(r io.Reader, limits parserLimits) (*metricStore, error) {
	parser := expfmt.TextParser{}
	families, err := parser.TextToMetricFamilies(r)
	if err != nil {
		return nil, err
	}

	if len(families) > limits.MaxMetricFamilies {
		return nil, fmt.Errorf("metric family count %d exceeds limit %d", len(families), limits.MaxMetricFamilies)
	}
	store := &metricStore{samples: map[string][]metricSample{}, maxDevices: limits.MaxDevices}
	sampleCount := 0
	for name, family := range families {
		for _, metric := range family.GetMetric() {
			sampleCount++
			if sampleCount > limits.MaxSamples {
				return nil, fmt.Errorf("metric sample count exceeds limit %d", limits.MaxSamples)
			}
			value, ok := metricValue(metric)
			if !ok {
				continue
			}
			if math.IsNaN(value) || math.IsInf(value, 0) {
				return nil, fmt.Errorf("metric %q contains a non-finite value", name)
			}
			if len(metric.GetLabel()) > limits.MaxLabelsPerSample {
				return nil, fmt.Errorf("metric %q label count exceeds limit %d", name, limits.MaxLabelsPerSample)
			}
			labels := map[string]string{}
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			store.samples[name] = append(store.samples[name], metricSample{
				Value:  value,
				Labels: labels,
			})
		}
	}
	return store, nil
}

func selectMetricProfiles(store *metricStore, requestedProfile string, profiles []metricProfile) []metricProfile {
	requestedProfile = strings.ToLower(strings.TrimSpace(requestedProfile))
	if requestedProfile == "" {
		requestedProfile = defaultMetricProfile
	}

	if requestedProfile != defaultMetricProfile {
		for _, profile := range profiles {
			if strings.EqualFold(profile.Name, requestedProfile) {
				return []metricProfile{profile}
			}
		}
		return nil
	}

	var matched []metricProfile
	for _, profile := range profiles {
		if profile.matches(store) {
			matched = append(matched, profile)
		}
	}
	return matched
}

func appendMetricProfiles(base, custom []metricProfile) []metricProfile {
	profiles := make([]metricProfile, 0, len(base)+len(custom))
	profiles = append(profiles, base...)
	profiles = append(profiles, custom...)
	return profiles
}

func mergeMetricProfiles(base, custom []metricProfile) ([]metricProfile, error) {
	profiles := make([]metricProfile, 0, len(base)+len(custom))
	seen := map[string]struct{}{}
	for _, profile := range appendMetricProfiles(base, custom) {
		key := strings.ToLower(strings.TrimSpace(profile.Name))
		if _, ok := seen[key]; ok {
			return nil, fmt.Errorf("duplicate metric profile %q conflicts with an already registered profile", profile.Name)
		}
		seen[key] = struct{}{}
		profiles = append(profiles, profile)
	}
	return profiles, nil
}

func metricProfilesFromFile(path string) ([]metricProfile, error) {
	return metricProfilesFromFileLimit(path, maxMetricProfilesFileBytes)
}

func metricProfilesFromFileLimit(path string, maxBytes int64) ([]metricProfile, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, nil
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read GPU metric profiles file %q: %w", path, err)
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read GPU metric profiles file %q: %w", path, err)
	}
	if int64(len(raw)) > maxBytes {
		return nil, fmt.Errorf("GPU metric profiles file %q exceeds %d bytes", path, maxBytes)
	}

	var cfg metricProfilesConfig
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parse GPU metric profiles file %q: %w", path, err)
	}
	if (cfg.APIVersion == "") != (cfg.Kind == "") {
		return nil, fmt.Errorf("parse GPU metric profiles file %q: apiVersion and kind must be set together", path)
	}
	if cfg.APIVersion != "" && (cfg.APIVersion != metricProfilesAPIVersion || cfg.Kind != metricProfilesKind) {
		return nil, fmt.Errorf("parse GPU metric profiles file %q: expected %s %s", path, metricProfilesAPIVersion, metricProfilesKind)
	}
	var trailing interface{}
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("parse GPU metric profiles file %q: multiple JSON values", path)
		}
		return nil, fmt.Errorf("parse GPU metric profiles file %q: trailing data: %w", path, err)
	}

	profiles := make([]metricProfile, 0, len(cfg.Profiles))
	seen := map[string]struct{}{}
	for _, profileCfg := range cfg.Profiles {
		profile, err := metricProfileFromConfig(profileCfg)
		if err != nil {
			return nil, fmt.Errorf("parse GPU metric profiles file %q: %w", path, err)
		}
		key := strings.ToLower(profile.Name)
		if _, ok := seen[key]; ok {
			return nil, fmt.Errorf("duplicate metric profile %q", profile.Name)
		}
		seen[key] = struct{}{}
		profiles = append(profiles, profile)
	}
	return profiles, nil
}

func metricProfileFromConfig(cfg metricProfileConfig) (metricProfile, error) {
	profile := metricProfile{
		Name:           strings.TrimSpace(cfg.Name),
		Class:          DeviceClass(strings.ToLower(strings.TrimSpace(cfg.Class))),
		MatchNames:     nonEmptyStrings(append(cfg.MatchNames, cfg.MatchNamesAlt...)),
		IdentityLabels: nonEmptyStrings(cfg.IdentityLabels),
		NameLabels:     nonEmptyStrings(cfg.NameLabels),
		RequiredFields: map[deviceMetricField]struct{}{},
		Health:         defaultHealthMapping(), Fields: map[deviceMetricField]metricFieldSpec{},
	}
	if profile.Name == "" {
		return metricProfile{}, fmt.Errorf("metric profile name is required")
	}
	if profile.Class == "" {
		profile.Class = DeviceClassAccelerator
	}
	if !validDeviceClass(profile.Class) {
		return metricProfile{}, fmt.Errorf("metric profile %q has invalid class %q", profile.Name, cfg.Class)
	}
	if len(profile.MatchNames) == 0 {
		return metricProfile{}, fmt.Errorf("metric profile %q requires at least one match name", profile.Name)
	}
	if len(cfg.Fields) == 0 {
		return metricProfile{}, fmt.Errorf("metric profile %q requires at least one field", profile.Name)
	}
	if len(profile.IdentityLabels) == 0 {
		profile.IdentityLabels = defaultIdentityLabels()
	}
	if len(profile.NameLabels) == 0 {
		profile.NameLabels = defaultNameLabels()
	}
	for _, labels := range [][]string{profile.IdentityLabels, profile.NameLabels} {
		for _, label := range labels {
			if !model.LabelName(label).IsValid() {
				return metricProfile{}, fmt.Errorf("metric profile %q has invalid label name %q", profile.Name, label)
			}
		}
	}
	if cfg.Health != nil {
		mapping, err := healthMappingFromConfig(profile.Name, *cfg.Health)
		if err != nil {
			return metricProfile{}, err
		}
		profile.Health = mapping
	}

	for rawField, specCfg := range cfg.Fields {
		field := deviceMetricField(strings.TrimSpace(rawField))
		if _, ok := knownMetricFields()[field]; !ok {
			return metricProfile{}, fmt.Errorf("metric profile %q has unknown field %q", profile.Name, rawField)
		}
		spec, err := metricFieldSpecFromConfig(profile.Name, field, specCfg)
		if err != nil {
			return metricProfile{}, err
		}
		profile.Fields[field] = spec
	}
	requiredFields := []deviceMetricField{fieldGPUUtilization, fieldMemoryTotalMiB, fieldTemperatureC}
	for _, rawField := range cfg.RequiredFields {
		field := deviceMetricField(strings.TrimSpace(rawField))
		if _, ok := knownMetricFields()[field]; !ok {
			return metricProfile{}, fmt.Errorf("metric profile %q has unknown required field %q", profile.Name, rawField)
		}
		requiredFields = append(requiredFields, field)
	}
	for _, required := range requiredFields {
		if _, ok := profile.Fields[required]; !ok {
			return metricProfile{}, fmt.Errorf("metric profile %q requires field %q", profile.Name, required)
		}
		profile.RequiredFields[required] = struct{}{}
	}
	if _, free := profile.Fields[fieldMemoryFreeMiB]; !free {
		if _, used := profile.Fields[fieldMemoryUsedMiB]; !used {
			return metricProfile{}, fmt.Errorf("metric profile %q requires field %q or %q", profile.Name, fieldMemoryFreeMiB, fieldMemoryUsedMiB)
		}
	}
	return profile, nil
}

func metricFieldSpecFromConfig(profileName string, field deviceMetricField, cfg metricFieldSpecConfig) (metricFieldSpec, error) {
	spec := metricFieldSpec{
		Names:  nonEmptyStrings(cfg.Names),
		Unit:   metricUnit(strings.TrimSpace(cfg.Unit)),
		Rollup: fieldRollup(strings.TrimSpace(cfg.Rollup)),
		Min:    cfg.Min, Max: cfg.Max,
	}
	if len(spec.Names) == 0 {
		return metricFieldSpec{}, fmt.Errorf("metric profile %q field %q requires at least one metric name", profileName, field)
	}
	if spec.Rollup == "" {
		spec.Rollup = rollupAvg
	}
	if _, ok := knownMetricUnits()[spec.Unit]; !ok {
		return metricFieldSpec{}, fmt.Errorf("metric profile %q field %q has unknown unit %q", profileName, field, cfg.Unit)
	}
	if _, ok := knownMetricRollups()[spec.Rollup]; !ok {
		return metricFieldSpec{}, fmt.Errorf("metric profile %q field %q has unknown rollup %q", profileName, field, cfg.Rollup)
	}
	if spec.Min != nil && (math.IsNaN(*spec.Min) || math.IsInf(*spec.Min, 0)) {
		return metricFieldSpec{}, fmt.Errorf("metric profile %q field %q minimum must be finite", profileName, field)
	}
	if spec.Max != nil && (math.IsNaN(*spec.Max) || math.IsInf(*spec.Max, 0)) {
		return metricFieldSpec{}, fmt.Errorf("metric profile %q field %q maximum must be finite", profileName, field)
	}
	if spec.Min != nil && spec.Max != nil && *spec.Min > *spec.Max {
		return metricFieldSpec{}, fmt.Errorf("metric profile %q field %q minimum exceeds maximum", profileName, field)
	}
	return spec, nil
}

func defaultIdentityLabels() []string {
	return []string{"uuid", "gpu_uuid", "npu_uuid", "accelerator_uuid", "serial", "UUID", "gpu", "npu", "accelerator", "device", "chip", "minor_number", "index", "id", "gpu_id", "npu_id", "accelerator_id", "chip_id", "card"}
}

func defaultNameLabels() []string {
	return []string{"name", "model", "gpu_name", "npu_name", "accelerator_name", "chip_name", "card"}
}

func defaultHealthMapping() healthMapping {
	return healthMapping{HealthyValues: map[float64]struct{}{1: {}}, UnhealthyValues: map[float64]struct{}{0: {}}}
}

func healthMappingFromConfig(profileName string, cfg healthMappingConfig) (healthMapping, error) {
	result := healthMapping{HealthyValues: map[float64]struct{}{}, UnhealthyValues: map[float64]struct{}{}}
	for _, value := range cfg.HealthyValues {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return healthMapping{}, fmt.Errorf("metric profile %q health values must be finite", profileName)
		}
		result.HealthyValues[value] = struct{}{}
	}
	for _, value := range cfg.UnhealthyValues {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return healthMapping{}, fmt.Errorf("metric profile %q health values must be finite", profileName)
		}
		if _, conflict := result.HealthyValues[value]; conflict {
			return healthMapping{}, fmt.Errorf("metric profile %q health value %v is both healthy and unhealthy", profileName, value)
		}
		result.UnhealthyValues[value] = struct{}{}
	}
	if len(result.HealthyValues) == 0 || len(result.UnhealthyValues) == 0 {
		return healthMapping{}, fmt.Errorf("metric profile %q health mapping requires healthy and unhealthy values", profileName)
	}
	return result, nil
}

func knownMetricFields() map[deviceMetricField]struct{} {
	return map[deviceMetricField]struct{}{
		fieldGPUUtilization:    {},
		fieldMemoryUtilization: {},
		fieldMemoryFreeMiB:     {},
		fieldMemoryTotalMiB:    {},
		fieldMemoryUsedMiB:     {},
		fieldTemperatureC:      {},
		fieldSMClockMHz:        {},
		fieldMemClockMHz:       {},
		fieldPowerUsageW:       {},
		fieldHealth:            {},
	}
}

func knownMetricUnits() map[metricUnit]struct{} {
	return map[metricUnit]struct{}{
		unitScalar: {},
		unitBytes:  {},
		unitKiB:    {},
		unitMiB:    {},
		unitRatio:  {},
	}
}

func knownMetricRollups() map[fieldRollup]struct{} {
	return map[fieldRollup]struct{}{
		rollupAvg:         {},
		rollupMax:         {},
		rollupMinPositive: {},
		rollupSum:         {},
		rollupMin:         {},
	}
}

func nonEmptyStrings(values []string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		result = append(result, value)
	}
	return result
}

func registeredMetricProfiles() []metricProfile {
	return []metricProfile{
		{
			Name:                       "iluvatar",
			Class:                      DeviceClassGPU,
			MatchNames:                 []string{"ix_gpu_utilization", "ix_mem_total", "ix_temperature"},
			SingleSamplePerDeviceField: true,
			Fields: map[deviceMetricField]metricFieldSpec{
				fieldGPUUtilization:    {Names: []string{"ix_gpu_utilization"}, Rollup: rollupAvg},
				fieldMemoryUtilization: {Names: []string{"ix_mem_utilization"}, Rollup: rollupAvg},
				fieldMemoryFreeMiB:     {Names: []string{"ix_mem_free"}, Unit: unitMiB, Rollup: rollupMax},
				fieldMemoryTotalMiB:    {Names: []string{"ix_mem_total"}, Unit: unitMiB, Rollup: rollupMax},
				fieldMemoryUsedMiB:     {Names: []string{"ix_mem_used"}, Unit: unitMiB, Rollup: rollupMax},
				fieldTemperatureC:      {Names: []string{"ix_temperature"}, Rollup: rollupMax},
				fieldSMClockMHz:        {Names: []string{"ix_sm_clock"}, Rollup: rollupMinPositive},
				fieldMemClockMHz:       {Names: []string{"ix_mem_clock"}, Rollup: rollupMinPositive},
				fieldPowerUsageW:       {Names: []string{"ix_power_usage"}, Rollup: rollupSum},
			},
		},
		{
			Name:       "dcgm",
			Class:      DeviceClassGPU,
			MatchNames: []string{"DCGM_FI_DEV_GPU_UTIL", "DCGM_FI_DEV_FB_TOTAL", "DCGM_FI_DEV_GPU_TEMP"},
			Fields: map[deviceMetricField]metricFieldSpec{
				fieldGPUUtilization:    {Names: []string{"DCGM_FI_DEV_GPU_UTIL"}, Rollup: rollupAvg},
				fieldMemoryUtilization: {Names: []string{"DCGM_FI_DEV_MEM_COPY_UTIL"}, Rollup: rollupAvg},
				fieldMemoryFreeMiB:     {Names: []string{"DCGM_FI_DEV_FB_FREE"}, Unit: unitMiB, Rollup: rollupMax},
				fieldMemoryTotalMiB:    {Names: []string{"DCGM_FI_DEV_FB_TOTAL"}, Unit: unitMiB, Rollup: rollupMax},
				fieldTemperatureC:      {Names: []string{"DCGM_FI_DEV_GPU_TEMP"}, Rollup: rollupMax},
				fieldSMClockMHz:        {Names: []string{"DCGM_FI_DEV_SM_CLOCK"}, Rollup: rollupMinPositive},
				fieldMemClockMHz:       {Names: []string{"DCGM_FI_DEV_MEM_CLOCK"}, Rollup: rollupMinPositive},
				fieldPowerUsageW:       {Names: []string{"DCGM_FI_DEV_POWER_USAGE"}, Rollup: rollupAvg},
			},
		},
		{
			Name:       "rocm",
			Class:      DeviceClassGPU,
			MatchNames: []string{"rocm_smi_utilization_gpu", "rocm_smi_memory_total_bytes", "rocm_smi_temperature_celsius"},
			Fields: map[deviceMetricField]metricFieldSpec{
				fieldGPUUtilization: {Names: []string{"rocm_smi_utilization_gpu"}, Rollup: rollupAvg},
				fieldMemoryTotalMiB: {Names: []string{"rocm_smi_memory_total_bytes", "rocm_smi_vram_total_bytes"}, Unit: unitBytes, Rollup: rollupMax},
				fieldMemoryUsedMiB:  {Names: []string{"rocm_smi_memory_used_bytes", "rocm_smi_vram_used_bytes"}, Unit: unitBytes, Rollup: rollupMax},
				fieldTemperatureC:   {Names: []string{"rocm_smi_temperature_celsius", "rocm_smi_temp_gpu_edge"}, Rollup: rollupMax},
				fieldSMClockMHz:     {Names: []string{"rocm_smi_sclk_clock_mhz"}, Rollup: rollupMinPositive},
				fieldMemClockMHz:    {Names: []string{"rocm_smi_mclk_clock_mhz"}, Rollup: rollupMinPositive},
				fieldPowerUsageW:    {Names: []string{"rocm_smi_average_socket_power_watts"}, Rollup: rollupAvg},
			},
		},
		{
			Name:       "generic",
			Class:      DeviceClassAccelerator,
			MatchNames: genericAcceleratorMetricNames("memory_total_mib"),
			Fields: map[deviceMetricField]metricFieldSpec{
				fieldGPUUtilization:    {Names: genericAcceleratorMetricNames("utilization_percent"), Rollup: rollupAvg},
				fieldMemoryUtilization: {Names: genericAcceleratorMetricNames("memory_utilization_percent"), Rollup: rollupAvg},
				fieldMemoryFreeMiB:     {Names: genericAcceleratorMetricNames("memory_free_mib"), Unit: unitMiB, Rollup: rollupMax},
				fieldMemoryTotalMiB:    {Names: genericAcceleratorMetricNames("memory_total_mib"), Unit: unitMiB, Rollup: rollupMax},
				fieldMemoryUsedMiB:     {Names: genericAcceleratorMetricNames("memory_used_mib"), Unit: unitMiB, Rollup: rollupMax},
				fieldTemperatureC:      {Names: genericAcceleratorMetricNames("temperature_celsius"), Rollup: rollupMax},
				fieldSMClockMHz:        {Names: appendMetricNames(genericAcceleratorMetricNames("sm_clock_mhz"), genericAcceleratorMetricNames("compute_clock_mhz")), Rollup: rollupMinPositive},
				fieldMemClockMHz:       {Names: genericAcceleratorMetricNames("memory_clock_mhz"), Rollup: rollupMinPositive},
				fieldPowerUsageW:       {Names: genericAcceleratorMetricNames("power_watts"), Rollup: rollupAvg},
				fieldHealth:            {Names: genericAcceleratorMetricNames("health"), Rollup: rollupMin},
			},
		},
	}
}

func (p metricProfile) matches(store *metricStore) bool {
	for _, name := range p.MatchNames {
		if len(store.samples[name]) > 0 {
			return true
		}
	}
	return false
}

func (p metricProfile) build(store *metricStore) (nodeMetrics, error) {
	deviceValues := map[string]map[deviceMetricField][]float64{}
	deviceInfo := map[string]deviceMetrics{}
	deviceAliases := map[string]string{}
	deviceLabelValues := map[string]map[string]string{}
	deviceNames := map[string]string{}
	identityLabels := p.IdentityLabels
	if len(identityLabels) == 0 {
		identityLabels = defaultIdentityLabels()
	}
	nameLabels := p.NameLabels
	if len(nameLabels) == 0 {
		nameLabels = defaultNameLabels()
	}
	health := p.Health
	if len(health.HealthyValues) == 0 || len(health.UnhealthyValues) == 0 {
		health = defaultHealthMapping()
	}

	fields := make([]deviceMetricField, 0, len(p.Fields))
	for field := range p.Fields {
		fields = append(fields, field)
	}
	sort.Slice(fields, func(i, j int) bool { return fields[i] < fields[j] })
	for _, field := range fields {
		spec := p.Fields[field]
		for _, metricName := range spec.Names {
			for _, sample := range store.samples[metricName] {
				class := metricDeviceClass(metricName, p.Class)
				key := string(class) + ":" + firstLabel(sample.Labels, identityLabels...)
				if strings.HasSuffix(key, ":") {
					key += "unlabeled"
				}
				if _, ok := deviceValues[key]; !ok {
					if store.maxDevices > 0 && len(deviceValues) >= store.maxDevices {
						return nodeMetrics{}, fmt.Errorf("normalized device count exceeds limit %d", store.maxDevices)
					}
					deviceValues[key] = map[deviceMetricField][]float64{}
					deviceLabelValues[key] = map[string]string{}
					deviceInfo[key] = deviceMetrics{
						ID:      key,
						Class:   class,
						Name:    firstLabel(sample.Labels, nameLabels...),
						UUID:    firstLabel(sample.Labels, "uuid", "gpu_uuid", "npu_uuid", "accelerator_uuid", "serial", "UUID"),
						Fields:  map[deviceMetricField]struct{}{},
						Healthy: true,
					}
				}
				for _, label := range identityLabels {
					value := strings.TrimSpace(sample.Labels[label])
					if value == "" {
						continue
					}
					alias := string(class) + ":" + label + "=" + value
					if owner, exists := deviceAliases[alias]; exists && owner != key {
						return nodeMetrics{}, fmt.Errorf("identity alias %q maps to both %q and %q", alias, owner, key)
					}
					deviceAliases[alias] = key
					if previous := deviceLabelValues[key][label]; previous != "" && previous != value {
						return nodeMetrics{}, fmt.Errorf("device %q identity label %q changed from %q to %q within one scrape", key, label, previous, value)
					}
					deviceLabelValues[key][label] = value
				}
				name := firstLabel(sample.Labels, nameLabels...)
				if previous := deviceNames[key]; previous != "" && name != "" && previous != name {
					return nodeMetrics{}, fmt.Errorf("device %q model/name changed from %q to %q within one scrape", key, previous, name)
				}
				if name != "" {
					deviceNames[key] = name
				}
				if p.SingleSamplePerDeviceField && len(deviceValues[key][field]) != 0 {
					return nodeMetrics{}, fmt.Errorf("device %q field %q has duplicate samples", key, field)
				}
				deviceValues[key][field] = append(deviceValues[key][field], normalizeMetricValue(sample.Value, spec.Unit))
			}
		}
	}

	metrics := nodeMetrics{
		Profile: p.Name,
		Fields:  map[deviceMetricField]struct{}{},
	}
	for key, values := range deviceValues {
		device := deviceInfo[key]
		for field, fieldValues := range values {
			spec := p.Fields[field]
			value := rollupValues(fieldValues, spec.Rollup)
			if spec.Min != nil && value < *spec.Min {
				return nodeMetrics{}, fmt.Errorf("device %q field %q is below configured minimum", key, field)
			}
			if spec.Max != nil && value > *spec.Max {
				return nodeMetrics{}, fmt.Errorf("device %q field %q exceeds configured maximum", key, field)
			}
			device.setField(field, value)
		}
		for required := range p.RequiredFields {
			if !device.hasField(required) {
				return nodeMetrics{}, fmt.Errorf("device %q is missing profile-required field %q", device.ID, required)
			}
		}
		if device.HealthKnown {
			value := device.HealthValue
			if _, ok := health.HealthyValues[value]; ok {
				device.Healthy = true
				device.HealthValue = 1
			} else if _, ok := health.UnhealthyValues[value]; ok {
				device.Healthy = false
				device.HealthValue = 0
			} else {
				return nodeMetrics{}, fmt.Errorf("device %q health value %v is not mapped", device.ID, value)
			}
		}
		device.deriveMemoryFields()
		if err := validateDeviceMetrics(device); err != nil {
			return nodeMetrics{}, err
		}
		for field := range device.Fields {
			metrics.Fields[field] = struct{}{}
		}
		metrics.Devices = append(metrics.Devices, device)
	}
	sort.Slice(metrics.Devices, func(i, j int) bool {
		if metrics.Devices[i].Class == metrics.Devices[j].Class {
			return metrics.Devices[i].ID < metrics.Devices[j].ID
		}
		return metrics.Devices[i].Class < metrics.Devices[j].Class
	})
	metrics.aggregate()
	return metrics, nil
}

func metricDeviceClass(metricName string, fallback DeviceClass) DeviceClass {
	switch {
	case strings.HasPrefix(metricName, "k3s_gpu_"):
		return DeviceClassGPU
	case strings.HasPrefix(metricName, "k3s_npu_"):
		return DeviceClassNPU
	case strings.HasPrefix(metricName, "k3s_fpga_"):
		return DeviceClassFPGA
	default:
		return fallback
	}
}

func validateDeviceMetrics(device deviceMetrics) error {
	if strings.TrimSpace(device.ID) == "" || strings.HasSuffix(device.ID, ":unlabeled") {
		return fmt.Errorf("device identity label is required")
	}
	required := []deviceMetricField{fieldGPUUtilization, fieldMemoryTotalMiB, fieldTemperatureC}
	for _, field := range required {
		if !device.hasField(field) {
			return fmt.Errorf("device %q is missing required field %q", device.ID, field)
		}
	}
	if !device.hasField(fieldMemoryFreeMiB) && !device.hasField(fieldMemoryUsedMiB) {
		return fmt.Errorf("device %q must report free or used memory", device.ID)
	}
	for field := range device.Fields {
		value := device.fieldValue(field)
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return fmt.Errorf("device %q field %q is non-finite", device.ID, field)
		}
		switch field {
		case fieldGPUUtilization, fieldMemoryUtilization:
			if value < 0 || value > 100 {
				return fmt.Errorf("device %q field %q must be between 0 and 100", device.ID, field)
			}
		case fieldMemoryFreeMiB, fieldMemoryTotalMiB, fieldMemoryUsedMiB, fieldSMClockMHz, fieldMemClockMHz, fieldPowerUsageW:
			if value < 0 {
				return fmt.Errorf("device %q field %q cannot be negative", device.ID, field)
			}
		case fieldTemperatureC:
			if value <= -100 || value > 250 {
				return fmt.Errorf("device %q temperature is outside the supported range", device.ID)
			}
		case fieldHealth:
			if value != 0 && value != 1 {
				return fmt.Errorf("device %q health must be 0 or 1", device.ID)
			}
		}
	}
	if device.MemoryTotalMiB <= 0 {
		return fmt.Errorf("device %q total memory must be positive", device.ID)
	}
	if device.MemoryFreeMiB > device.MemoryTotalMiB || device.MemoryUsedMiB > device.MemoryTotalMiB {
		return fmt.Errorf("device %q memory values exceed total memory", device.ID)
	}
	if device.hasField(fieldMemoryFreeMiB) && device.hasField(fieldMemoryUsedMiB) {
		balance := device.MemoryFreeMiB + device.MemoryUsedMiB
		// Exporter values may be rounded, but a discrepancy larger than 0.1%
		// (or 1 MiB for small devices) is not a trustworthy capacity snapshot.
		tolerance := math.Max(1, device.MemoryTotalMiB*0.001)
		if math.Abs(balance-device.MemoryTotalMiB) > tolerance {
			return fmt.Errorf("device %q free plus used memory is inconsistent with total memory", device.ID)
		}
	}
	if device.hasField(fieldMemoryUtilization) && device.hasMemoryCapacity() {
		calculated := (1 - device.MemoryFreeMiB/device.MemoryTotalMiB) * 100
		if math.Abs(calculated-device.MemoryUtilization) > 2 {
			return fmt.Errorf("device %q reported memory utilization is inconsistent with free and total memory", device.ID)
		}
	}
	return nil
}

func (m *nodeMetrics) aggregate() {
	m.GPUCount = len(m.Devices)
	if m.GPUCount == 0 {
		return
	}

	var utilizationSamples []float64
	var memoryUtilizationSamples []float64
	var temperatures []float64
	var smClocks []float64
	var memClocks []float64

	for _, device := range m.Devices {
		if device.hasField(fieldGPUUtilization) {
			utilizationSamples = append(utilizationSamples, device.GPUUtilization)
		}
		if device.hasField(fieldMemoryUtilization) {
			memoryUtilizationSamples = append(memoryUtilizationSamples, device.MemoryUtilization)
		}
		if device.hasMemoryCapacity() {
			m.MemoryFreeMiB += device.MemoryFreeMiB
			m.MemoryTotalMiB += device.MemoryTotalMiB
		}
		if device.hasField(fieldTemperatureC) {
			temperatures = append(temperatures, device.TemperatureC)
		}
		if device.hasField(fieldSMClockMHz) {
			smClocks = append(smClocks, device.SMClockMHz)
		}
		if device.hasField(fieldMemClockMHz) {
			memClocks = append(memClocks, device.MemClockMHz)
		}
		if device.hasField(fieldPowerUsageW) {
			m.PowerUsageW += device.PowerUsageW
		}
	}

	m.GPUUtilization = avg(utilizationSamples)
	if m.MemoryTotalMiB > 0 {
		m.MemoryUtilization = clampScore((1 - m.MemoryFreeMiB/m.MemoryTotalMiB) * 100)
	} else {
		m.MemoryUtilization = avg(memoryUtilizationSamples)
	}
	m.TemperatureC = max(temperatures)
	m.SMClockMHz = minPositive(smClocks)
	m.MemClockMHz = minPositive(memClocks)
}

func (m nodeMetrics) hasField(field deviceMetricField) bool {
	_, ok := m.Fields[field]
	return ok
}

func (m nodeMetrics) hasMemoryCapacity() bool {
	return m.hasField(fieldMemoryFreeMiB) && m.hasField(fieldMemoryTotalMiB) && m.MemoryTotalMiB > 0
}

func (m nodeMetrics) eligibleDevices(maxTemperature, minFreeMemory, minSMClock, minMemClock float64) []deviceMetrics {
	var devices []deviceMetrics
	for _, device := range m.Devices {
		if !device.eligible(maxTemperature, minFreeMemory, minSMClock, minMemClock) {
			continue
		}
		devices = append(devices, device)
	}
	return devices
}

func (d *deviceMetrics) setField(field deviceMetricField, value float64) {
	d.Fields[field] = struct{}{}
	switch field {
	case fieldGPUUtilization:
		d.GPUUtilization = value
	case fieldMemoryUtilization:
		d.MemoryUtilization = value
	case fieldMemoryFreeMiB:
		d.MemoryFreeMiB = value
	case fieldMemoryTotalMiB:
		d.MemoryTotalMiB = value
	case fieldMemoryUsedMiB:
		d.MemoryUsedMiB = value
	case fieldTemperatureC:
		d.TemperatureC = value
	case fieldSMClockMHz:
		d.SMClockMHz = value
	case fieldMemClockMHz:
		d.MemClockMHz = value
	case fieldPowerUsageW:
		d.PowerUsageW = value
	case fieldHealth:
		d.HealthKnown = true
		d.HealthValue = value
		d.Healthy = value > 0
	}
}

func (d *deviceMetrics) deriveMemoryFields() {
	if d.hasField(fieldMemoryTotalMiB) && d.hasField(fieldMemoryUsedMiB) && !d.hasField(fieldMemoryFreeMiB) {
		d.MemoryFreeMiB = d.MemoryTotalMiB - d.MemoryUsedMiB
		d.Fields[fieldMemoryFreeMiB] = struct{}{}
	}
	if d.hasMemoryCapacity() && !d.hasField(fieldMemoryUtilization) {
		d.MemoryUtilization = clampScore((1 - d.MemoryFreeMiB/d.MemoryTotalMiB) * 100)
		d.Fields[fieldMemoryUtilization] = struct{}{}
	}
}

func (d deviceMetrics) fieldValue(field deviceMetricField) float64 {
	switch field {
	case fieldGPUUtilization:
		return d.GPUUtilization
	case fieldMemoryUtilization:
		return d.MemoryUtilization
	case fieldMemoryFreeMiB:
		return d.MemoryFreeMiB
	case fieldMemoryTotalMiB:
		return d.MemoryTotalMiB
	case fieldMemoryUsedMiB:
		return d.MemoryUsedMiB
	case fieldTemperatureC:
		return d.TemperatureC
	case fieldSMClockMHz:
		return d.SMClockMHz
	case fieldMemClockMHz:
		return d.MemClockMHz
	case fieldPowerUsageW:
		return d.PowerUsageW
	case fieldHealth:
		return d.HealthValue
	default:
		return 0
	}
}

func (d deviceMetrics) hasField(field deviceMetricField) bool {
	_, ok := d.Fields[field]
	return ok
}

func (d deviceMetrics) hasMemoryCapacity() bool {
	return d.hasField(fieldMemoryFreeMiB) && d.hasField(fieldMemoryTotalMiB) && d.MemoryTotalMiB > 0
}

func (d deviceMetrics) eligible(maxTemperature, minFreeMemory, minSMClock, minMemClock float64) bool {
	return d.ineligibilityReason(maxTemperature, minFreeMemory, minSMClock, minMemClock) == ""
}

func (d deviceMetrics) ineligibilityReason(maxTemperature, minFreeMemory, minSMClock, minMemClock float64) string {
	if d.HealthKnown && !d.Healthy {
		return "health"
	}
	if d.hasField(fieldTemperatureC) && d.TemperatureC >= maxTemperature {
		return "temperature"
	}
	if minFreeMemory > 0 {
		if !d.hasMemoryCapacity() || d.MemoryFreeMiB < minFreeMemory {
			return "free_memory"
		}
	}
	if d.hasField(fieldSMClockMHz) && d.SMClockMHz < minSMClock {
		return "sm_clock"
	}
	if d.hasField(fieldMemClockMHz) && d.MemClockMHz < minMemClock {
		return "memory_clock"
	}
	return ""
}

func deviceKey(sample metricSample) string {
	if key := firstLabel(sample.Labels, "uuid", "gpu_uuid", "npu_uuid", "accelerator_uuid", "serial", "UUID", "gpu", "npu", "accelerator", "device", "chip", "minor_number", "index", "id", "gpu_id", "npu_id", "accelerator_id", "chip_id", "card"); key != "" {
		return key
	}
	return "unlabeled"
}

func firstLabel(labels map[string]string, names ...string) string {
	for _, name := range names {
		if value := strings.TrimSpace(labels[name]); value != "" {
			return value
		}
	}
	return ""
}

func normalizeMetricValue(value float64, unit metricUnit) float64 {
	switch unit {
	case unitBytes:
		return value / 1024 / 1024
	case unitKiB:
		return value / 1024
	case unitRatio:
		return value * 100
	default:
		return value
	}
}

func rollupValues(values []float64, rollup fieldRollup) float64 {
	switch rollup {
	case rollupMax:
		return max(values)
	case rollupMinPositive:
		return minPositive(values)
	case rollupSum:
		return sum(values)
	case rollupMin:
		if len(values) == 0 {
			return 0
		}
		result := values[0]
		for _, value := range values[1:] {
			if value < result {
				result = value
			}
		}
		return result
	default:
		return avg(values)
	}
}
