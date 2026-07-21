package gpustability

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/yaml"
)

const (
	argsAPIVersion = "gpustability.k3s.io/v1alpha1"
	argsKind       = "K3SGPUStabilityArgs"
)

type DeviceClass string

const (
	DeviceClassGPU         DeviceClass = "gpu"
	DeviceClassNPU         DeviceClass = "npu"
	DeviceClassFPGA        DeviceClass = "fpga"
	DeviceClassDSP         DeviceClass = "dsp"
	DeviceClassAccelerator DeviceClass = "accelerator"
)

type StatePolicy string

const (
	StatePolicyStrict     StatePolicy = "strict"
	StatePolicyDegraded   StatePolicy = "degraded"
	StatePolicyBestEffort StatePolicy = "best-effort"
)

type ResourceMappingArgs struct {
	Name     string   `json:"name"`
	Class    string   `json:"class"`
	Profiles []string `json:"profiles,omitempty"`
}

type ExporterArgs struct {
	Port               string `json:"port"`
	Path               string `json:"path"`
	Scheme             string `json:"scheme"`
	Profile            string `json:"profile"`
	Timeout            string `json:"timeout"`
	MaxResponseBytes   int64  `json:"maxResponseBytes"`
	MaxMetricFamilies  int    `json:"maxMetricFamilies"`
	MaxSamples         int    `json:"maxSamples"`
	MaxLabelsPerSample int    `json:"maxLabelsPerSample"`
	AllowInsecureHTTP  bool   `json:"allowInsecureHTTP"`
	AllowExternalIP    bool   `json:"allowExternalIP"`
	CAFile             string `json:"caFile,omitempty"`
	CertFile           string `json:"certFile,omitempty"`
	KeyFile            string `json:"keyFile,omitempty"`
	ServerName         string `json:"serverName,omitempty"`
}

type CollectorArgs struct {
	Workers             int     `json:"workers"`
	QueueSize           int     `json:"queueSize"`
	CacheMaxEntries     int     `json:"cacheMaxEntries"`
	SnapshotTTL         string  `json:"snapshotTTL"`
	RefreshInterval     string  `json:"refreshInterval"`
	BackoffBase         string  `json:"backoffBase"`
	BackoffMax          string  `json:"backoffMax"`
	CircuitFailures     int     `json:"circuitFailures"`
	CircuitOpenDuration string  `json:"circuitOpenDuration"`
	JitterFraction      float64 `json:"jitterFraction"`
}

type DiscoveryArgs struct {
	AddressTypes      []string `json:"addressTypes"`
	PreferredIPFamily string   `json:"preferredIPFamily"`
}

type ProfileSourceArgs struct {
	File           string `json:"file,omitempty"`
	ReloadInterval string `json:"reloadInterval"`
	MaxBytes       int64  `json:"maxBytes"`
}

type PolicyArgs struct {
	DefaultStatePolicy string      `json:"defaultStatePolicy"`
	MaxTemperatureC    float64     `json:"maxTemperatureC"`
	TargetTemperatureC float64     `json:"targetTemperatureC"`
	MinSMClockMHz      float64     `json:"minSMClockMHz"`
	MinMemoryClockMHz  float64     `json:"minMemoryClockMHz"`
	DegradedScore      int64       `json:"degradedScore"`
	BestEffortScore    int64       `json:"bestEffortScore"`
	ScoreAllPods       bool        `json:"scoreAllPods"`
	MaxPowerWatts      float64     `json:"maxPowerWatts"`
	Scoring            ScoringArgs `json:"scoring"`
}

// ScoringArgs assigns bounded relative importance to independently explainable
// SLO dimensions. Missing telemetry contributes zero for its dimension.
type ScoringArgs struct {
	Utilization       int64 `json:"utilization"`
	MemoryHeadroom    int64 `json:"memoryHeadroom"`
	ThermalHeadroom   int64 `json:"thermalHeadroom"`
	EnergyHeadroom    int64 `json:"energyHeadroom"`
	ComputeCapability int64 `json:"computeCapability"`
	Health            int64 `json:"health"`
	DataLocality      int64 `json:"dataLocality"`
	Fragmentation     int64 `json:"fragmentation"`
}

type QueueingArgs struct {
	MaxTrackedPods int    `json:"maxTrackedPods"`
	BlockedPodTTL  string `json:"blockedPodTTL"`
}

// GPUStabilityArgs is the versioned scheduler plugin configuration accepted in
// KubeSchedulerConfiguration profiles[].pluginConfig[].args.
type GPUStabilityArgs struct {
	metav1.TypeMeta    `json:",inline"`
	Resources          []ResourceMappingArgs `json:"resources"`
	Exporter           ExporterArgs          `json:"exporter"`
	Collector          CollectorArgs         `json:"collector"`
	Discovery          DiscoveryArgs         `json:"discovery"`
	ProfileSource      ProfileSourceArgs     `json:"profileSource"`
	Policy             PolicyArgs            `json:"policy"`
	Queueing           QueueingArgs          `json:"queueing"`
	MetricProfilesFile string                `json:"metricProfilesFile,omitempty"`
}

func (in *GPUStabilityArgs) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := *in
	out.Resources = append([]ResourceMappingArgs(nil), in.Resources...)
	out.Discovery.AddressTypes = append([]string(nil), in.Discovery.AddressTypes...)
	for i := range out.Resources {
		out.Resources[i].Profiles = append([]string(nil), in.Resources[i].Profiles...)
	}
	return &out
}

type resourceMapping struct {
	Class    DeviceClass
	Profiles map[string]struct{}
}

type Config struct {
	ResourceMappings   map[v1.ResourceName]resourceMapping
	ExporterPort       string
	ExporterPath       string
	Scheme             string
	MetricProfile      string
	MetricProfiles     []metricProfile
	MetricProfilesFile string
	Timeout            time.Duration
	MaxResponseBytes   int64
	MaxMetricFamilies  int
	MaxSamples         int
	MaxLabelsPerSample int
	AllowInsecureHTTP  bool
	AllowExternalIP    bool
	CAFile             string
	CertFile           string
	KeyFile            string
	ServerName         string

	Workers             int
	QueueSize           int
	CacheMaxEntries     int
	SnapshotTTL         time.Duration
	RefreshInterval     time.Duration
	BackoffBase         time.Duration
	BackoffMax          time.Duration
	CircuitFailures     int
	CircuitOpenDuration time.Duration
	JitterFraction      float64

	AddressTypes          []v1.NodeAddressType
	PreferredIPFamily     string
	ProfileReloadInterval time.Duration
	MaxProfileBytes       int64

	DefaultStatePolicy StatePolicy
	MaxTemperatureC    float64
	TargetTempC        float64
	MinSMClockMHz      float64
	MinMemClockMHz     float64
	DegradedScore      int64
	BestEffortScore    int64
	ScoreAllPods       bool
	MaxPowerWatts      float64
	Scoring            ScoringArgs
	MaxTrackedPods     int
	BlockedPodTTL      time.Duration
}

func defaultArgs() GPUStabilityArgs {
	return GPUStabilityArgs{
		TypeMeta: metav1.TypeMeta{APIVersion: argsAPIVersion, Kind: argsKind},
		Resources: []ResourceMappingArgs{
			{Name: "nvidia.com/gpu", Class: string(DeviceClassGPU), Profiles: []string{"dcgm"}},
			{Name: "amd.com/gpu", Class: string(DeviceClassGPU), Profiles: []string{"rocm"}},
			{Name: "iluvatar.com/gpu", Class: string(DeviceClassGPU), Profiles: []string{"iluvatar"}},
			{Name: "ix.com/gpu", Class: string(DeviceClassGPU), Profiles: []string{"iluvatar"}},
			{Name: "corex.ai/gpu", Class: string(DeviceClassGPU), Profiles: []string{"iluvatar"}},
			{Name: "huawei.com/ascend", Class: string(DeviceClassNPU), Profiles: []string{"generic"}},
			{Name: "huawei.com/npu", Class: string(DeviceClassNPU), Profiles: []string{"generic"}},
			{Name: "ascend.com/npu", Class: string(DeviceClassNPU), Profiles: []string{"generic"}},
			{Name: "intel.com/xpu", Class: string(DeviceClassAccelerator), Profiles: []string{"generic"}},
			{Name: "cambricon.com/mlu", Class: string(DeviceClassNPU), Profiles: []string{"generic"}},
			{Name: "hygon.com/dcu", Class: string(DeviceClassGPU), Profiles: []string{"generic"}},
		},
		Exporter: ExporterArgs{
			Port:               defaultExporterPort,
			Path:               defaultExporterPath,
			Scheme:             "https",
			Profile:            defaultMetricProfile,
			Timeout:            defaultTimeout.String(),
			MaxResponseBytes:   4 << 20,
			MaxMetricFamilies:  10_000,
			MaxSamples:         100_000,
			MaxLabelsPerSample: 64,
			AllowInsecureHTTP:  false,
		},
		Collector: CollectorArgs{
			Workers:             4,
			QueueSize:           1024,
			CacheMaxEntries:     defaultCacheMax,
			SnapshotTTL:         defaultCacheTTL.String(),
			RefreshInterval:     (defaultCacheTTL / 2).String(),
			BackoffBase:         "1s",
			BackoffMax:          "1m",
			CircuitFailures:     5,
			CircuitOpenDuration: "2m",
			JitterFraction:      0.20,
		},
		Discovery: DiscoveryArgs{
			AddressTypes:      []string{string(v1.NodeInternalIP)},
			PreferredIPFamily: "ipv4",
		},
		ProfileSource: ProfileSourceArgs{
			ReloadInterval: "30s",
			MaxBytes:       maxMetricProfilesFileBytes,
		},
		Policy: PolicyArgs{
			DefaultStatePolicy: string(StatePolicyStrict),
			MaxTemperatureC:    defaultMaxTemperatureC,
			TargetTemperatureC: defaultTargetTempC,
			MinSMClockMHz:      defaultMinClockMHz,
			MinMemoryClockMHz:  defaultMinClockMHz,
			DegradedScore:      20,
			BestEffortScore:    10,
			MaxPowerWatts:      500,
			Scoring: ScoringArgs{
				Utilization: 15, MemoryHeadroom: 15, ThermalHeadroom: 15,
				EnergyHeadroom: 5, ComputeCapability: 10, Health: 10,
				DataLocality: 10, Fragmentation: 20,
			},
		},
		Queueing: QueueingArgs{MaxTrackedPods: 10_000, BlockedPodTTL: "10m"},
	}
}

func configFromArgs(obj runtime.Object) (Config, error) {
	args := defaultArgs()
	if err := applyLegacyEnv(&args); err != nil {
		return Config{}, err
	}
	if err := decodeArgs(obj, &args); err != nil {
		return Config{}, err
	}
	return validateAndConvertArgs(args)
}

func decodeArgs(obj runtime.Object, into *GPUStabilityArgs) error {
	if obj == nil {
		return nil
	}
	if typed, ok := obj.(*GPUStabilityArgs); ok {
		raw, err := json.Marshal(typed)
		if err != nil {
			return fmt.Errorf("encode %s: %w", argsKind, err)
		}
		return decodeStrictJSON(raw, into)
	}
	unknown, ok := obj.(*runtime.Unknown)
	if !ok {
		return fmt.Errorf("%s args must be runtime.Unknown or *GPUStabilityArgs, got %T", Name, obj)
	}
	if len(unknown.Raw) == 0 {
		return nil
	}
	raw := unknown.Raw
	if unknown.ContentType == runtime.ContentTypeYAML {
		converted, err := yaml.YAMLToJSON(raw)
		if err != nil {
			return fmt.Errorf("decode %s YAML: %w", argsKind, err)
		}
		raw = converted
	} else if unknown.ContentType != "" && unknown.ContentType != runtime.ContentTypeJSON {
		return fmt.Errorf("unsupported %s args content type %q", Name, unknown.ContentType)
	}
	return decodeStrictJSON(raw, into)
}

func decodeStrictJSON(raw []byte, into *GPUStabilityArgs) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(into); err != nil {
		return fmt.Errorf("decode %s args: %w", argsKind, err)
	}
	var trailing interface{}
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("decode %s args: multiple JSON values", argsKind)
		}
		return fmt.Errorf("decode %s args: trailing data: %w", argsKind, err)
	}
	return nil
}

func validateAndConvertArgs(args GPUStabilityArgs) (Config, error) {
	if args.APIVersion != argsAPIVersion {
		return Config{}, fmt.Errorf("apiVersion must be %q, got %q", argsAPIVersion, args.APIVersion)
	}
	if args.Kind != argsKind {
		return Config{}, fmt.Errorf("kind must be %q, got %q", argsKind, args.Kind)
	}
	if len(args.Resources) == 0 {
		return Config{}, fmt.Errorf("at least one explicit resource mapping is required")
	}

	mappings := make(map[v1.ResourceName]resourceMapping, len(args.Resources))
	for i, item := range args.Resources {
		name := v1.ResourceName(strings.TrimSpace(item.Name))
		if errs := utilvalidation.IsQualifiedName(string(name)); len(errs) > 0 || !strings.Contains(string(name), "/") {
			return Config{}, fmt.Errorf("resources[%d].name %q must be a qualified extended resource name", i, name)
		}
		class := DeviceClass(strings.ToLower(strings.TrimSpace(item.Class)))
		if !validDeviceClass(class) {
			return Config{}, fmt.Errorf("resources[%d].class %q is invalid", i, item.Class)
		}
		if _, exists := mappings[name]; exists {
			return Config{}, fmt.Errorf("duplicate resource mapping %q", name)
		}
		profiles := map[string]struct{}{}
		for _, profile := range item.Profiles {
			profile = strings.ToLower(strings.TrimSpace(profile))
			if profile == "" {
				return Config{}, fmt.Errorf("resources[%d] contains an empty profile", i)
			}
			profiles[profile] = struct{}{}
		}
		mappings[name] = resourceMapping{Class: class, Profiles: profiles}
	}

	timeout, err := positiveDuration("exporter.timeout", args.Exporter.Timeout)
	if err != nil {
		return Config{}, err
	}
	ttl, err := positiveDuration("collector.snapshotTTL", args.Collector.SnapshotTTL)
	if err != nil {
		return Config{}, err
	}
	refresh, err := positiveDuration("collector.refreshInterval", args.Collector.RefreshInterval)
	if err != nil {
		return Config{}, err
	}
	backoffBase, err := positiveDuration("collector.backoffBase", args.Collector.BackoffBase)
	if err != nil {
		return Config{}, err
	}
	backoffMax, err := positiveDuration("collector.backoffMax", args.Collector.BackoffMax)
	if err != nil {
		return Config{}, err
	}
	circuitOpen, err := positiveDuration("collector.circuitOpenDuration", args.Collector.CircuitOpenDuration)
	if err != nil {
		return Config{}, err
	}
	profileReloadInterval, err := positiveDuration("profileSource.reloadInterval", args.ProfileSource.ReloadInterval)
	if err != nil {
		return Config{}, err
	}
	blockedPodTTL, err := positiveDuration("queueing.blockedPodTTL", args.Queueing.BlockedPodTTL)
	if err != nil {
		return Config{}, err
	}
	if refresh >= ttl {
		return Config{}, fmt.Errorf("collector.refreshInterval must be less than snapshotTTL")
	}
	if backoffMax < backoffBase {
		return Config{}, fmt.Errorf("collector.backoffMax must be greater than or equal to backoffBase")
	}
	if args.Exporter.MaxResponseBytes < 1024 || args.Exporter.MaxResponseBytes > 64<<20 {
		return Config{}, fmt.Errorf("exporter.maxResponseBytes must be between 1024 and %d", 64<<20)
	}
	if args.Exporter.MaxMetricFamilies < 1 || args.Exporter.MaxMetricFamilies > 100_000 {
		return Config{}, fmt.Errorf("exporter.maxMetricFamilies must be between 1 and 100000")
	}
	if args.Exporter.MaxSamples < 1 || args.Exporter.MaxSamples > 1_000_000 {
		return Config{}, fmt.Errorf("exporter.maxSamples must be between 1 and 1000000")
	}
	if args.Exporter.MaxLabelsPerSample < 1 || args.Exporter.MaxLabelsPerSample > 256 {
		return Config{}, fmt.Errorf("exporter.maxLabelsPerSample must be between 1 and 256")
	}
	if args.ProfileSource.MaxBytes < 1024 || args.ProfileSource.MaxBytes > 16<<20 {
		return Config{}, fmt.Errorf("profileSource.maxBytes must be between 1024 and %d", 16<<20)
	}
	if args.Collector.Workers < 1 || args.Collector.Workers > 128 {
		return Config{}, fmt.Errorf("collector.workers must be between 1 and 128")
	}
	if args.Collector.QueueSize < 1 || args.Collector.QueueSize > 1_000_000 {
		return Config{}, fmt.Errorf("collector.queueSize must be between 1 and 1000000")
	}
	if args.Collector.CacheMaxEntries < 1 || args.Collector.CacheMaxEntries > 1_000_000 {
		return Config{}, fmt.Errorf("collector.cacheMaxEntries must be between 1 and 1000000")
	}
	if args.Collector.CircuitFailures < 1 || args.Collector.CircuitFailures > 1000 {
		return Config{}, fmt.Errorf("collector.circuitFailures must be between 1 and 1000")
	}
	if args.Collector.JitterFraction < 0 || args.Collector.JitterFraction > 0.5 {
		return Config{}, fmt.Errorf("collector.jitterFraction must be between 0 and 0.5")
	}
	addressTypes, err := validateAddressTypes(args.Discovery.AddressTypes, args.Exporter.AllowExternalIP)
	if err != nil {
		return Config{}, err
	}
	preferredIPFamily := strings.ToLower(strings.TrimSpace(args.Discovery.PreferredIPFamily))
	if preferredIPFamily != "any" && preferredIPFamily != "ipv4" && preferredIPFamily != "ipv6" {
		return Config{}, fmt.Errorf("discovery.preferredIPFamily must be any, ipv4, or ipv6")
	}
	scheme := strings.ToLower(strings.TrimSpace(args.Exporter.Scheme))
	if scheme != "https" && scheme != "http" {
		return Config{}, fmt.Errorf("exporter.scheme must be http or https")
	}
	if scheme == "http" && !args.Exporter.AllowInsecureHTTP {
		return Config{}, fmt.Errorf("exporter.allowInsecureHTTP must be true when exporter.scheme is http")
	}
	if err := validatePort(args.Exporter.Port); err != nil {
		return Config{}, fmt.Errorf("exporter.port: %w", err)
	}
	if err := validateMetricsPath(args.Exporter.Path); err != nil {
		return Config{}, fmt.Errorf("exporter.path: %w", err)
	}
	if (args.Exporter.CertFile == "") != (args.Exporter.KeyFile == "") {
		return Config{}, fmt.Errorf("exporter.certFile and exporter.keyFile must be configured together")
	}
	if args.Policy.MaxTemperatureC <= -100 || args.Policy.MaxTemperatureC > 250 {
		return Config{}, fmt.Errorf("policy.maxTemperatureC must be greater than -100 and at most 250")
	}
	if args.Policy.TargetTemperatureC <= -100 || args.Policy.TargetTemperatureC >= args.Policy.MaxTemperatureC {
		return Config{}, fmt.Errorf("policy.targetTemperatureC must be greater than -100 and less than maxTemperatureC")
	}
	if args.Policy.MinSMClockMHz < 0 || args.Policy.MinMemoryClockMHz < 0 {
		return Config{}, fmt.Errorf("minimum clock values cannot be negative")
	}
	if args.Policy.DegradedScore < 0 || args.Policy.DegradedScore > 100 || args.Policy.BestEffortScore < 0 || args.Policy.BestEffortScore > 100 {
		return Config{}, fmt.Errorf("fallback scores must be between 0 and 100")
	}
	if args.Policy.MaxPowerWatts <= 0 || args.Policy.MaxPowerWatts > 100_000 {
		return Config{}, fmt.Errorf("policy.maxPowerWatts must be greater than 0 and at most 100000")
	}
	if err := validateScoringArgs(args.Policy.Scoring); err != nil {
		return Config{}, err
	}
	if args.Queueing.MaxTrackedPods < 1 || args.Queueing.MaxTrackedPods > 1_000_000 {
		return Config{}, fmt.Errorf("queueing.maxTrackedPods must be between 1 and 1000000")
	}
	policy := StatePolicy(strings.ToLower(strings.TrimSpace(args.Policy.DefaultStatePolicy)))
	if !validStatePolicy(policy) {
		return Config{}, fmt.Errorf("policy.defaultStatePolicy %q is invalid", args.Policy.DefaultStatePolicy)
	}

	profileFile := strings.TrimSpace(args.ProfileSource.File)
	if profileFile == "" {
		profileFile = strings.TrimSpace(args.MetricProfilesFile)
	}
	customProfiles, err := metricProfilesFromFileLimit(profileFile, args.ProfileSource.MaxBytes)
	if err != nil {
		return Config{}, err
	}
	profiles, err := mergeMetricProfiles(registeredMetricProfiles(), customProfiles)
	if err != nil {
		return Config{}, err
	}
	profileName := strings.ToLower(strings.TrimSpace(args.Exporter.Profile))
	if profileName == "" {
		profileName = defaultMetricProfile
	}
	if profileName != defaultMetricProfile && !hasMetricProfile(profiles, profileName) {
		return Config{}, fmt.Errorf("exporter.profile %q is not registered", profileName)
	}
	for name, mapping := range mappings {
		for profile := range mapping.Profiles {
			if !hasMetricProfile(profiles, profile) {
				return Config{}, fmt.Errorf("resource mapping %q references unknown profile %q", name, profile)
			}
		}
	}

	return Config{
		ResourceMappings: mappings,
		ExporterPort:     strings.TrimSpace(args.Exporter.Port), ExporterPath: strings.TrimSpace(args.Exporter.Path),
		Scheme: scheme, MetricProfile: profileName, MetricProfiles: profiles, Timeout: timeout,
		MaxResponseBytes: args.Exporter.MaxResponseBytes, MaxMetricFamilies: args.Exporter.MaxMetricFamilies,
		MaxSamples: args.Exporter.MaxSamples, MaxLabelsPerSample: args.Exporter.MaxLabelsPerSample,
		AllowInsecureHTTP: args.Exporter.AllowInsecureHTTP,
		AllowExternalIP:   args.Exporter.AllowExternalIP, CAFile: args.Exporter.CAFile,
		CertFile: args.Exporter.CertFile, KeyFile: args.Exporter.KeyFile, ServerName: args.Exporter.ServerName,
		Workers: args.Collector.Workers, QueueSize: args.Collector.QueueSize, CacheMaxEntries: args.Collector.CacheMaxEntries,
		SnapshotTTL: ttl, RefreshInterval: refresh, BackoffBase: backoffBase, BackoffMax: backoffMax,
		CircuitFailures: args.Collector.CircuitFailures, CircuitOpenDuration: circuitOpen,
		JitterFraction: args.Collector.JitterFraction, AddressTypes: addressTypes,
		PreferredIPFamily: preferredIPFamily, MetricProfilesFile: profileFile,
		ProfileReloadInterval: profileReloadInterval, MaxProfileBytes: args.ProfileSource.MaxBytes,
		DefaultStatePolicy: policy, MaxTemperatureC: args.Policy.MaxTemperatureC,
		TargetTempC: args.Policy.TargetTemperatureC, MinSMClockMHz: args.Policy.MinSMClockMHz,
		MinMemClockMHz: args.Policy.MinMemoryClockMHz, DegradedScore: args.Policy.DegradedScore,
		BestEffortScore: args.Policy.BestEffortScore, ScoreAllPods: args.Policy.ScoreAllPods,
		MaxPowerWatts: args.Policy.MaxPowerWatts, Scoring: args.Policy.Scoring,
		MaxTrackedPods: args.Queueing.MaxTrackedPods, BlockedPodTTL: blockedPodTTL,
	}, nil
}

func validateScoringArgs(weights ScoringArgs) error {
	values := map[string]int64{
		"utilization": weights.Utilization, "memoryHeadroom": weights.MemoryHeadroom,
		"thermalHeadroom": weights.ThermalHeadroom, "energyHeadroom": weights.EnergyHeadroom,
		"computeCapability": weights.ComputeCapability, "health": weights.Health,
		"dataLocality": weights.DataLocality, "fragmentation": weights.Fragmentation,
	}
	var total int64
	for name, value := range values {
		if value < 0 || value > 100 {
			return fmt.Errorf("policy.scoring.%s must be between 0 and 100", name)
		}
		total += value
	}
	if total != 100 {
		return fmt.Errorf("policy.scoring weights must total 100, got %d", total)
	}
	return nil
}

func validateAddressTypes(raw []string, allowExternalIP bool) ([]v1.NodeAddressType, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("discovery.addressTypes must contain at least one address type")
	}
	allowed := map[v1.NodeAddressType]struct{}{
		v1.NodeInternalIP: {}, v1.NodeExternalIP: {}, v1.NodeHostName: {},
	}
	seen := map[v1.NodeAddressType]struct{}{}
	result := make([]v1.NodeAddressType, 0, len(raw)+1)
	for i, value := range raw {
		addressType := v1.NodeAddressType(strings.TrimSpace(value))
		if _, ok := allowed[addressType]; !ok {
			return nil, fmt.Errorf("discovery.addressTypes[%d] %q is not InternalIP, ExternalIP, or Hostname", i, value)
		}
		if _, ok := seen[addressType]; ok {
			return nil, fmt.Errorf("discovery.addressTypes contains duplicate %q", addressType)
		}
		seen[addressType] = struct{}{}
		result = append(result, addressType)
	}
	if allowExternalIP {
		if _, ok := seen[v1.NodeExternalIP]; !ok {
			result = append(result, v1.NodeExternalIP)
		}
	}
	return result, nil
}

func applyLegacyEnv(args *GPUStabilityArgs) error {
	setString := func(name string, target *string) {
		if value, ok := os.LookupEnv(name); ok {
			*target = strings.TrimSpace(value)
		}
	}
	setString("K3S_GPU_EXPORTER_PORT", &args.Exporter.Port)
	setString("K3S_GPU_EXPORTER_PATH", &args.Exporter.Path)
	setString("K3S_GPU_EXPORTER_SCHEME", &args.Exporter.Scheme)
	setString("K3S_GPU_METRIC_PROFILE", &args.Exporter.Profile)
	setString("K3S_GPU_EXPORTER_TIMEOUT", &args.Exporter.Timeout)
	setString("K3S_GPU_EXPORTER_CACHE_TTL", &args.Collector.SnapshotTTL)
	setString("K3S_GPU_EXPORTER_CACHE_CLEANUP_INTERVAL", &args.Collector.RefreshInterval)
	setString("K3S_GPU_METRIC_PROFILES_FILE", &args.MetricProfilesFile)

	if value, ok := os.LookupEnv("K3S_GPU_RESOURCE_NAMES"); ok {
		var resources []ResourceMappingArgs
		for _, name := range strings.Split(value, ",") {
			name = strings.TrimSpace(name)
			if name == "" {
				continue
			}
			resources = append(resources, ResourceMappingArgs{Name: name, Class: string(DeviceClassAccelerator), Profiles: []string{"generic"}})
		}
		args.Resources = resources
	}
	if err := envBoolStrict("K3S_GPU_ALLOW_INSECURE_HTTP", &args.Exporter.AllowInsecureHTTP); err != nil {
		return err
	}
	if err := envBoolStrict("K3S_GPU_SCORE_ALL_PODS", &args.Policy.ScoreAllPods); err != nil {
		return err
	}
	if err := envIntStrict("K3S_GPU_EXPORTER_CACHE_MAX_ENTRIES", &args.Collector.CacheMaxEntries); err != nil {
		return err
	}
	if err := envFloatStrict("K3S_GPU_MAX_TEMPERATURE_C", &args.Policy.MaxTemperatureC); err != nil {
		return err
	}
	if err := envFloatStrict("K3S_GPU_TARGET_TEMPERATURE_C", &args.Policy.TargetTemperatureC); err != nil {
		return err
	}
	if err := envFloatStrict("K3S_GPU_MIN_SM_CLOCK_MHZ", &args.Policy.MinSMClockMHz); err != nil {
		return err
	}
	if err := envFloatStrict("K3S_GPU_MIN_MEM_CLOCK_MHZ", &args.Policy.MinMemoryClockMHz); err != nil {
		return err
	}
	return nil
}

func envBoolStrict(name string, target *bool) error {
	value, ok := os.LookupEnv(name)
	if !ok {
		return nil
	}
	parsed, err := strconv.ParseBool(strings.TrimSpace(value))
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	*target = parsed
	return nil
}

func envIntStrict(name string, target *int) error {
	value, ok := os.LookupEnv(name)
	if !ok {
		return nil
	}
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	*target = parsed
	return nil
}

func envFloatStrict(name string, target *float64) error {
	value, ok := os.LookupEnv(name)
	if !ok {
		return nil
	}
	parsed, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	*target = parsed
	return nil
}

func positiveDuration(name, raw string) (time.Duration, error) {
	value, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration, got %q", name, raw)
	}
	return value, nil
}

func validDeviceClass(class DeviceClass) bool {
	switch class {
	case DeviceClassGPU, DeviceClassNPU, DeviceClassFPGA, DeviceClassDSP, DeviceClassAccelerator:
		return true
	default:
		return false
	}
}

func validStatePolicy(policy StatePolicy) bool {
	return policy == StatePolicyStrict || policy == StatePolicyDegraded || policy == StatePolicyBestEffort
}

func hasMetricProfile(profiles []metricProfile, name string) bool {
	for _, profile := range profiles {
		if strings.EqualFold(profile.Name, name) {
			return true
		}
	}
	return false
}
