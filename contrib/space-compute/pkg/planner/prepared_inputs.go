package planner

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"sort"
	"strconv"
	"sync"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
)

// PreparedPlanningInputs is an immutable, informer-generation-scoped view of
// resource/link state. Controllers build it when informer objects change and
// may safely reuse it across mission reconciles. The contained API objects must
// never be mutated by planning code.
const materialDigestMemoSlots = 64

type materialDigestMemo struct {
	mu     sync.Mutex
	key    [sha256.Size]byte
	digest string
	set    bool
}

type PreparedPlanningInputs struct {
	resourceSummaries       []*spacev1.SpaceDomainResourceSummary
	linkSnapshots           []*spacev1.SpaceLinkSnapshot
	linkIndex               map[string][]*spacev1.SpaceLinkSnapshot
	materialResources       [][]byte
	materialLinks           [][]byte
	materialCanonicalDigest [sha256.Size]byte
	materialMemo            [materialDigestMemoSlots]materialDigestMemo
	inputDigest             string
}

func PreparePlanningInputs(summaries []*spacev1.SpaceDomainResourceSummary, links []*spacev1.SpaceLinkSnapshot) (*PreparedPlanningInputs, error) {
	if err := validatePlannerTopology(summaries, links); err != nil {
		return nil, err
	}
	prepared := &PreparedPlanningInputs{}
	prepared.resourceSummaries = append([]*spacev1.SpaceDomainResourceSummary(nil), summaries...)
	sort.SliceStable(prepared.resourceSummaries, func(i, j int) bool {
		if prepared.resourceSummaries[i] == nil {
			return false
		}
		if prepared.resourceSummaries[j] == nil {
			return true
		}
		return domainKey(prepared.resourceSummaries[i].Spec.Domain) < domainKey(prepared.resourceSummaries[j].Spec.Domain)
	})
	prepared.linkSnapshots = append([]*spacev1.SpaceLinkSnapshot(nil), links...)
	prepared.linkIndex = indexLinksWithoutFreshness(prepared.linkSnapshots)

	resourceMaterial := make([]materialFragment, 0, len(prepared.resourceSummaries))
	for _, summary := range prepared.resourceSummaries {
		if summary == nil {
			continue
		}
		raw, err := json.Marshal(normalizedResourceSummarySpecForDigest(summary.Spec))
		if err != nil {
			return nil, fmt.Errorf("encode canonical resource summary %q: %w", summary.Name, err)
		}
		resourceMaterial = append(resourceMaterial, materialFragment{key: fullDomainKey(summary.Spec.Domain), raw: raw})
	}
	sort.SliceStable(resourceMaterial, func(i, j int) bool { return resourceMaterial[i].key < resourceMaterial[j].key })
	prepared.materialResources = fragmentBytes(resourceMaterial)

	linkMaterial := make([]materialFragment, 0, len(prepared.linkSnapshots))
	for _, link := range prepared.linkSnapshots {
		if link == nil {
			continue
		}
		raw, err := json.Marshal(normalizedLinkSpecForDigest(link.Spec))
		if err != nil {
			return nil, fmt.Errorf("encode canonical link snapshot %q: %w", link.Name, err)
		}
		key := directedDomainKey(link.Spec.Source, link.Spec.Destination) + "\x00" + link.Name
		linkMaterial = append(linkMaterial, materialFragment{key: key, raw: raw})
	}
	sort.SliceStable(linkMaterial, func(i, j int) bool { return linkMaterial[i].key < linkMaterial[j].key })
	prepared.materialLinks = fragmentBytes(linkMaterial)
	canonical := sha256.New()
	_, _ = canonical.Write([]byte(`{"resources":`))
	writeJSONArray(canonical, prepared.materialResources, true)
	_, _ = canonical.Write([]byte(`,"links":`))
	writeJSONArray(canonical, prepared.materialLinks, true)
	_, _ = canonical.Write([]byte(`}`))
	copy(prepared.materialCanonicalDigest[:], canonical.Sum(nil))

	inputResources := make([]materialFragment, 0, len(summaries))
	for _, summary := range summaries {
		if summary == nil {
			continue
		}
		raw, err := json.Marshal(summary)
		if err != nil {
			return nil, fmt.Errorf("encode resource summary snapshot %q: %w", summary.Name, err)
		}
		inputResources = append(inputResources, materialFragment{key: summary.Name, raw: raw})
	}
	sort.SliceStable(inputResources, func(i, j int) bool { return inputResources[i].key < inputResources[j].key })
	inputLinks := make([]materialFragment, 0, len(links))
	for _, link := range links {
		if link == nil {
			continue
		}
		raw, err := json.Marshal(link)
		if err != nil {
			return nil, fmt.Errorf("encode link snapshot %q: %w", link.Name, err)
		}
		inputLinks = append(inputLinks, materialFragment{key: link.Name, raw: raw})
	}
	sort.SliceStable(inputLinks, func(i, j int) bool { return inputLinks[i].key < inputLinks[j].key })
	h := sha256.New()
	_, _ = h.Write([]byte(`{"resources":`))
	writeJSONArray(h, fragmentBytes(inputResources), false)
	_, _ = h.Write([]byte(`,"links":`))
	writeJSONArray(h, fragmentBytes(inputLinks), false)
	_, _ = h.Write([]byte(`}`))
	prepared.inputDigest = hex.EncodeToString(h.Sum(nil))
	return prepared, nil
}

type materialFragment struct {
	key string
	raw []byte
}

func fragmentBytes(values []materialFragment) [][]byte {
	out := make([][]byte, len(values))
	for i := range values {
		out[i] = values[i].raw
	}
	return out
}

func (p *PreparedPlanningInputs) ResourceSummaries() []*spacev1.SpaceDomainResourceSummary {
	if p == nil {
		return nil
	}
	return p.resourceSummaries
}

func (p *PreparedPlanningInputs) LinkSnapshots() []*spacev1.SpaceLinkSnapshot {
	if p == nil {
		return nil
	}
	return p.linkSnapshots
}

func (p *PreparedPlanningInputs) InputDigest() string {
	if p == nil {
		return ""
	}
	return p.inputDigest
}

func indexLinksWithoutFreshness(links []*spacev1.SpaceLinkSnapshot) map[string][]*spacev1.SpaceLinkSnapshot {
	result := make(map[string][]*spacev1.SpaceLinkSnapshot)
	for _, link := range links {
		if link == nil {
			continue
		}
		key := directedDomainKey(link.Spec.Source, link.Spec.Destination)
		result[key] = append(result[key], link)
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

func usablePreparedLinkIndex(prepared *PreparedPlanningInputs, clock spacev1.Clock) map[string][]*spacev1.SpaceLinkSnapshot {
	if prepared == nil || len(prepared.linkIndex) == 0 {
		return nil
	}
	// Reuse the immutable per-direction slices when every member is currently
	// usable. Only directions containing stale/rejected data allocate a filtered
	// replacement; this keeps freshness time-dependent without rebuilding the
	// complete global index each reconcile.
	result := make(map[string][]*spacev1.SpaceLinkSnapshot, len(prepared.linkIndex))
	for key, values := range prepared.linkIndex {
		allUsable := true
		for _, link := range values {
			if !linkSnapshotAccepted(link) || spacev1.ValidateLinkSnapshot(link, nil, clock) != nil {
				allUsable = false
				break
			}
		}
		if allUsable {
			result[key] = values
			continue
		}
		for _, link := range values {
			if linkSnapshotAccepted(link) && spacev1.ValidateLinkSnapshot(link, nil, clock) == nil {
				result[key] = append(result[key], link)
			}
		}
	}
	return result
}

func materialDigestPrepared(mission *spacev1.SpaceMission, prepared *PreparedPlanningInputs) (string, error) {
	normalizedMission, err := normalizedMissionSpecForDigest(mission.Spec)
	if err != nil {
		return "", err
	}
	missionRaw, err := json.Marshal(normalizedMission)
	if err != nil {
		return "", err
	}
	// The full resource/link canonical product is invariant for one prepared
	// informer generation. Hash it once, then use that precomputed digest plus
	// Mission content/generation as a bounded memo key. The returned material
	// digest remains byte-for-byte compatible with the legacy encoding.
	keyHash := sha256.New()
	_, _ = keyHash.Write(missionRaw)
	_, _ = keyHash.Write([]byte{0})
	_, _ = keyHash.Write([]byte(strconv.FormatInt(mission.Generation, 10)))
	_, _ = keyHash.Write(prepared.materialCanonicalDigest[:])
	var key [sha256.Size]byte
	copy(key[:], keyHash.Sum(nil))
	slot := &prepared.materialMemo[int(key[0])%len(prepared.materialMemo)]
	slot.mu.Lock()
	if slot.set && slot.key == key {
		digest := slot.digest
		slot.mu.Unlock()
		return digest, nil
	}
	slot.mu.Unlock()

	h := sha256.New()
	_, _ = h.Write([]byte(`{"mission":`))
	_, _ = h.Write(missionRaw)
	_, _ = h.Write([]byte(`,"missionGeneration":`))
	_, _ = h.Write([]byte(strconv.FormatInt(mission.Generation, 10)))
	_, _ = h.Write([]byte(`,"resources":`))
	writeJSONArray(h, prepared.materialResources, true)
	_, _ = h.Write([]byte(`,"links":`))
	writeJSONArray(h, prepared.materialLinks, true)
	_, _ = h.Write([]byte(`}`))
	digest := hex.EncodeToString(h.Sum(nil))
	slot.mu.Lock()
	slot.key = key
	slot.digest = digest
	slot.set = true
	slot.mu.Unlock()
	return digest, nil
}

func writeJSONArray(h hash.Hash, fragments [][]byte, nilWhenEmpty bool) {
	if len(fragments) == 0 && nilWhenEmpty {
		_, _ = h.Write([]byte("null"))
		return
	}
	_, _ = h.Write([]byte("["))
	for i, raw := range fragments {
		if i > 0 {
			_, _ = h.Write([]byte(","))
		}
		_, _ = h.Write(raw)
	}
	_, _ = h.Write([]byte("]"))
}
