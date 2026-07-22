package gpustability

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	v1 "k8s.io/api/core/v1"
)

type profileRegistrySnapshot struct {
	Version  uint64
	Digest   [sha256.Size]byte
	Profiles []metricProfile
	Adapters []telemetryAdapter
}

type profileRegistry struct {
	file     string
	maxBytes int64
	mappings map[v1.ResourceName]resourceMapping
	current  atomic.Pointer[profileRegistrySnapshot]
	reloadMu sync.Mutex
}

func newProfileRegistry(cfg Config) (*profileRegistry, error) {
	registry := &profileRegistry{file: cfg.MetricProfilesFile, maxBytes: cfg.MaxProfileBytes, mappings: cfg.ResourceMappings}
	digest, err := metricProfilesFileDigest(registry.file, registry.maxBytes)
	if err != nil {
		return nil, err
	}
	if err := validateProfileReferences(cfg.MetricProfiles, registry.mappings); err != nil {
		return nil, err
	}
	registry.current.Store(newProfileRegistrySnapshot(1, digest, cfg.MetricProfiles))
	return registry, nil
}

func newProfileRegistrySnapshot(version uint64, digest [sha256.Size]byte, profiles []metricProfile) *profileRegistrySnapshot {
	profileCopy := append([]metricProfile(nil), profiles...)
	adapters := make([]telemetryAdapter, 0, len(profileCopy))
	for _, profile := range profileCopy {
		adapters = append(adapters, declarativeProfileAdapter{profile: profile})
	}
	return &profileRegistrySnapshot{Version: version, Digest: digest, Profiles: profileCopy, Adapters: adapters}
}

func (r *profileRegistry) snapshot() *profileRegistrySnapshot {
	return r.current.Load()
}

func (r *profileRegistry) has(name string) bool {
	name = strings.ToLower(strings.TrimSpace(name))
	for _, adapter := range r.snapshot().Adapters {
		if strings.EqualFold(adapter.Name(), name) {
			return true
		}
	}
	return false
}

func (r *profileRegistry) parse(reader io.Reader, requested string, limits parserLimits) (nodeMetrics, error) {
	return parseMetricsWithAdapters(reader, requested, r.snapshot().Adapters, limits)
}

func (r *profileRegistry) parseVersion(reader io.Reader, requested string, version uint64, limits parserLimits) (nodeMetrics, error) {
	snapshot := r.snapshot()
	if snapshot.Version != version {
		return nodeMetrics{}, fmt.Errorf("metric profile registry generation changed")
	}
	return parseMetricsWithAdapters(reader, requested, snapshot.Adapters, limits)
}

// reload atomically installs a complete valid declarative registry. Invalid or
// unchanged input leaves the last-known-good snapshot active.
func (r *profileRegistry) reload() (bool, error) {
	r.reloadMu.Lock()
	defer r.reloadMu.Unlock()
	if strings.TrimSpace(r.file) == "" {
		return false, nil
	}
	digest, err := metricProfilesFileDigest(r.file, r.maxBytes)
	if err != nil {
		return false, err
	}
	old := r.snapshot()
	if digest == old.Digest {
		return false, nil
	}
	custom, err := metricProfilesFromFileLimit(r.file, r.maxBytes)
	if err != nil {
		return false, err
	}
	profiles, err := mergeMetricProfiles(registeredMetricProfiles(), custom)
	if err != nil {
		return false, err
	}
	if err := validateProfileReferences(profiles, r.mappings); err != nil {
		return false, err
	}
	r.current.Store(newProfileRegistrySnapshot(old.Version+1, digest, profiles))
	return true, nil
}

func validateProfileReferences(profiles []metricProfile, mappings map[v1.ResourceName]resourceMapping) error {
	available := make(map[string]struct{}, len(profiles))
	for _, profile := range profiles {
		key := strings.ToLower(strings.TrimSpace(profile.Name))
		if key == "" {
			return fmt.Errorf("metric profile name cannot be empty")
		}
		available[key] = struct{}{}
	}
	resourceNames := make([]string, 0, len(mappings))
	for resourceName := range mappings {
		resourceNames = append(resourceNames, string(resourceName))
	}
	sort.Strings(resourceNames)
	for _, rawName := range resourceNames {
		mapping := mappings[v1.ResourceName(rawName)]
		profileNames := make([]string, 0, len(mapping.Profiles))
		for profileName := range mapping.Profiles {
			profileNames = append(profileNames, profileName)
		}
		sort.Strings(profileNames)
		for _, profileName := range profileNames {
			if _, ok := available[strings.ToLower(profileName)]; !ok {
				return fmt.Errorf("resource mapping %q references unavailable profile %q", rawName, profileName)
			}
		}
	}
	return nil
}

func metricProfilesFileDigest(path string, maxBytes int64) ([sha256.Size]byte, error) {
	if strings.TrimSpace(path) == "" {
		return sha256.Sum256(nil), nil
	}
	file, err := os.Open(path)
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("read GPU metric profiles file %q: %w", path, err)
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("read GPU metric profiles file %q: %w", path, err)
	}
	if int64(len(raw)) > maxBytes {
		return [sha256.Size]byte{}, fmt.Errorf("GPU metric profiles file %q exceeds %d bytes", path, maxBytes)
	}
	return sha256.Sum256(raw), nil
}
