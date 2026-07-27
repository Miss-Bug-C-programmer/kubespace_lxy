package v1alpha1

import (
	"fmt"
	"math"
	"net/url"
	"sort"
	"strings"
	"time"
)

const (
	MaxDeviceCapacityCount      = int64(1_000_000)
	MaxComputeMilli             = int64(1_000_000_000)
	MaxQueueDelaySecs           = MaxMissionDurationSecs
	MaxMaximumSnapshotAgeSecs   = MaxSnapshotLifetimeSecs
	MaxDataLocations            = 64
	MaxDataLocationURIBytes     = 2048
	MaxDeviceTopologyValues     = 64
	MaxDevicePrecisionValues    = 32
	MaxSoftwareEntries          = 64
	MaxSoftwareKeyBytes         = 253
	MaxSoftwareValueBytes       = 128
	MaxTransferEpochs           = 128
	MaxPlannerTopologyEntries   = 20_000
	MaxSnapshotSequenceEntries  = 128
	MaxPhysicalDevices          = 4096
	MaxPeerInterconnects        = 256
	MaxPersistentStorageClasses = 64
	MaxNUMANodes                = 1024
	MaxCapacityBytes            = int64(1 << 60)
	MaxCPUMilli                 = int64(1_000_000_000)
	MaxBandwidthBitsPerSecond   = int64(1_000_000_000_000_000)
	MaxRTTMicroseconds          = int64(86_400_000_000)
	MaxAutonomyDurationSeconds  = int64(365 * 24 * 60 * 60)
	MaxEnergyMilliWattHours     = int64(1 << 60)
	MaxTemperatureMilliCelsius  = int64(500_000)
	MinTemperatureMilliCelsius  = int64(-273_150)
	MaxPowerMilliwatts          = int64(100_000_000)
	MaxReferenceCount           = 256
)

func checkedAddInt64API(a, b int64) (int64, error) {
	if b > 0 && a > math.MaxInt64-b {
		return 0, fmt.Errorf("integer addition overflow")
	}
	if b < 0 && a < math.MinInt64-b {
		return 0, fmt.Errorf("integer addition underflow")
	}
	return a + b, nil
}

func checkedMulInt64API(a, b int64) (int64, error) {
	if a == 0 || b == 0 {
		return 0, nil
	}
	if a == -1 && b == math.MinInt64 || b == -1 && a == math.MinInt64 {
		return 0, fmt.Errorf("integer multiplication overflow")
	}
	result := a * b
	if result/b != a {
		return 0, fmt.Errorf("integer multiplication overflow")
	}
	return result, nil
}

func checkedSecondsDurationAPI(seconds int64) (time.Duration, error) {
	if seconds < 0 {
		return 0, fmt.Errorf("duration cannot be negative")
	}
	value, err := checkedMulInt64API(seconds, int64(time.Second))
	if err != nil {
		return 0, err
	}
	return time.Duration(value), nil
}

func validateDataLocations(path string, values []DataLocation, errs *ValidationErrors) {
	if len(values) > MaxDataLocations {
		errs.addf(path, "cannot exceed %d entries", MaxDataLocations)
	}
	seen := map[string]struct{}{}
	for i, value := range values {
		item := fmt.Sprintf("%s[%d]", path, i)
		validateDomain(item+".domain", value.Domain, errs)
		validateDataURI(item+".uri", value.URI, errs)
		key := dataLocationIdentityKey(value)
		if _, ok := seen[key]; ok {
			errs.add(item, "duplicate data location")
		}
		seen[key] = struct{}{}
	}
}

func validateDataURI(path, value string, errs *ValidationErrors) {
	if len(value) > MaxDataLocationURIBytes || strings.ContainsAny(value, "\r\n\x00") {
		errs.addf(path, "must be at most %d bytes without control separators", MaxDataLocationURIBytes)
		return
	}
	if value == "" {
		return
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme == "" {
		errs.add(path, "must be an absolute URI when set")
	}
}

func dataLocationIdentityKey(value DataLocation) string {
	return domainIdentityKey(value.Domain) + "\x00" + value.URI
}

func domainIdentityKey(value DomainReference) string {
	return value.ClusterID + "/" + value.Name + "/" + string(value.OrbitClass)
}

func sortedUniqueStringSet(values []string) []string {
	out := append([]string(nil), values...)
	sort.Strings(out)
	unique := out[:0]
	for _, value := range out {
		if len(unique) == 0 || unique[len(unique)-1] != value {
			unique = append(unique, value)
		}
	}
	return unique
}

func deviceCapacityCompatibilityKey(value DeviceCapacity) string {
	return strings.Join([]string{
		strings.TrimSpace(value.Class),
		strings.Join(sortedUniqueStringSet(value.Architectures), "\x1f"),
		strings.Join(sortedUniqueStringSet(value.Models), "\x1f"),
		strings.Join(sortedUniqueStringSet(value.Precision), "\x1f"),
	}, "\x00")
}

func deviceCapacityBucketIdentityKey(value DeviceCapacity) string {
	return fmt.Sprintf("%s\x00%020d\x00%010d", deviceCapacityCompatibilityKey(value), value.ComputeMilli, value.FragmentationMilli)
}

func deviceCapacityCanonicalKey(value DeviceCapacity) string {
	return fmt.Sprintf("%s\x00%020d", deviceCapacityBucketIdentityKey(value), value.Count)
}
