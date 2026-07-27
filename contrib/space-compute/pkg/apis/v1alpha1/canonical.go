package v1alpha1

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
)

const (
	canonicalReporterFormat   = "spacecompute-canonical-v1"
	canonicalReporterFormatV2 = "spacecompute-canonical-v2"
)

type canonicalWriter struct {
	buffer bytes.Buffer
}

func newCanonicalWriter(kind, name string) *canonicalWriter {
	return newCanonicalWriterWithFormat(canonicalReporterFormat, kind, name)
}

func newCanonicalWriterWithFormat(format, kind, name string) *canonicalWriter {
	w := &canonicalWriter{}
	w.string("format", format)
	// This logical wire identifier intentionally remains v1alpha1 across served
	// versions. Reporter signatures therefore survive lossless alpha<->beta
	// conversion and stored-version migration. The format field versions the
	// signed payload when canonical fields are added.
	w.string("apiVersion", SchemeGroupVersion.String())
	w.string("kind", kind)
	w.string("name", name)
	return w
}

func (w *canonicalWriter) string(key, value string) {
	fmt.Fprintf(&w.buffer, "%s=s:%s\n", key, strconv.Quote(value))
}

func (w *canonicalWriter) integer(key string, value int64) {
	fmt.Fprintf(&w.buffer, "%s=i:%d\n", key, value)
}

func (w *canonicalWriter) boolean(key string, value bool) {
	fmt.Fprintf(&w.buffer, "%s=b:%t\n", key, value)
}

func (w *canonicalWriter) timestamp(key string, value time.Time) {
	// metav1.Time is persisted by the Kubernetes JSON codec at whole-second
	// precision. Normalize before formatting so a reporter signs exactly the
	// canonical value that admission will reconstruct from the stored object.
	w.string(key, value.UTC().Truncate(time.Second).Format(time.RFC3339Nano))
}

func (w *canonicalWriter) domain(prefix string, value DomainReference) {
	w.string(prefix+".name", strings.ToLower(strings.TrimSpace(value.Name)))
	w.string(prefix+".clusterID", strings.ToLower(strings.TrimSpace(value.ClusterID)))
	w.string(prefix+".orbitClass", strings.ToLower(strings.TrimSpace(string(value.OrbitClass))))
}

func (w *canonicalWriter) provenance(value Provenance) {
	w.string("provenance.reporterID", value.ReporterID)
	w.string("provenance.source", value.Source)
	w.integer("provenance.sequence", value.Sequence)
	w.string("provenance.previousDigest", value.PreviousDigest)
	// Digest and Signature are intentionally excluded. The digest would be
	// self-referential and the signature authenticates the resulting digest.
}

func (w *canonicalWriter) bytes() []byte {
	return append([]byte(nil), w.buffer.Bytes()...)
}

// CanonicalReporterBytes returns the versioned, deterministic serialization used
// by reporter digests. It deliberately accepts only signed reporter-owned API
// objects; adding a new kind requires an explicit canonical field order here.
func CanonicalReporterBytes(value runtime.Object) ([]byte, error) {
	switch object := value.(type) {
	case *SpaceLinkSnapshot:
		return canonicalLinkSnapshot(object)
	case *SpaceDomainResourceSummary:
		return canonicalResourceSummary(object)
	case *PhysicalDeviceInventory:
		return canonicalPhysicalDeviceInventory(object)
	case *SpaceTransferReceipt:
		return canonicalTransferReceipt(object)
	case *SpaceExecutionLease:
		return canonicalExecutionLease(object)
	case *SpaceExecutionObservation:
		return canonicalExecutionObservation(object)
	case *SpaceResultReceipt:
		return canonicalResultReceipt(object)
	default:
		return nil, fmt.Errorf("unsupported canonical reporter object %T", value)
	}
}

// ReporterDigest calculates lowercase SHA-256 over CanonicalReporterBytes.
func ReporterDigest(value runtime.Object) (string, error) {
	raw, err := CanonicalReporterBytes(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func canonicalLinkSnapshot(snapshot *SpaceLinkSnapshot) ([]byte, error) {
	if snapshot == nil {
		return nil, fmt.Errorf("link snapshot is required")
	}
	w := newCanonicalWriter("SpaceLinkSnapshot", snapshot.Name)
	w.domain("source", snapshot.Spec.Source)
	w.domain("destination", snapshot.Spec.Destination)
	w.timestamp("observedAt", snapshot.Spec.ObservedAt.Time)
	w.timestamp("validUntil", snapshot.Spec.ValidUntil.Time)
	w.integer("maximumClockSkewSeconds", snapshot.Spec.MaximumClockSkewSeconds)
	w.integer("minimumUpdateSeconds", snapshot.Spec.MinimumUpdateSeconds)
	w.integer("historyLimit", int64(snapshot.Spec.HistoryLimit))
	w.provenance(snapshot.Spec.Provenance)

	windows := append([]ContactWindow(nil), snapshot.Spec.Windows...)
	sort.Slice(windows, func(i, j int) bool { return windows[i].ID < windows[j].ID })
	w.integer("windows.count", int64(len(windows)))
	for index, window := range windows {
		prefix := fmt.Sprintf("windows.%d", index)
		w.string(prefix+".id", window.ID)
		w.timestamp(prefix+".start", window.Start.Time)
		w.timestamp(prefix+".end", window.End.Time)
		w.integer(prefix+".bandwidthBitsPerSecond", window.BandwidthBitsPerSec)
		w.integer(prefix+".rttMicroseconds", window.RTTMicroseconds)
		w.integer(prefix+".lossPartsPerMillion", int64(window.LossPartsPerMillion))
		w.integer(prefix+".errorPartsPerMillion", int64(window.ErrorPartsPerMillion))
		w.integer(prefix+".stabilityMilli", int64(window.StabilityMilli))
		w.integer(prefix+".confidenceMilli", int64(window.ConfidenceMilli))
		w.boolean(prefix+".predicted", window.Predicted)
	}
	return w.bytes(), nil
}

func canonicalResourceSummary(summary *SpaceDomainResourceSummary) ([]byte, error) {
	if summary == nil {
		return nil, fmt.Errorf("resource summary is required")
	}
	w := newCanonicalWriter("SpaceDomainResourceSummary", summary.Name)
	w.domain("domain", summary.Spec.Domain)
	w.timestamp("observedAt", summary.Spec.ObservedAt.Time)
	w.timestamp("validUntil", summary.Spec.ValidUntil.Time)
	w.provenance(summary.Spec.Provenance)

	devices := append([]DeviceCapacity(nil), summary.Spec.Devices...)
	sort.Slice(devices, func(i, j int) bool {
		return deviceCapacityCanonicalKey(devices[i]) < deviceCapacityCanonicalKey(devices[j])
	})
	w.integer("devices.count", int64(len(devices)))
	for index, device := range devices {
		prefix := fmt.Sprintf("devices.%d", index)
		w.string(prefix+".class", device.Class)
		w.integer(prefix+".count", device.Count)
		writeSortedStrings(w, prefix+".architectures", device.Architectures)
		writeSortedStrings(w, prefix+".models", device.Models)
		writeSortedStrings(w, prefix+".precision", device.Precision)
		w.integer(prefix+".computeMilli", device.ComputeMilli)
		w.integer(prefix+".fragmentationMilli", int64(device.FragmentationMilli))
	}

	keys := make([]string, 0, len(summary.Spec.Software))
	for key := range summary.Spec.Software {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	w.integer("software.count", int64(len(keys)))
	for index, key := range keys {
		prefix := fmt.Sprintf("software.%d", index)
		w.string(prefix+".key", key)
		w.string(prefix+".value", summary.Spec.Software[key])
	}
	writeSortedStrings(w, "dataLocations", summary.Spec.DataLocations)
	w.integer("queueDelaySeconds", summary.Spec.QueueDelaySeconds)
	w.integer("energyHeadroomMilli", int64(summary.Spec.EnergyHeadroomMilli))
	w.integer("thermalHeadroomMilli", int64(summary.Spec.ThermalHeadroomMilli))
	w.integer("resilienceMilli", int64(summary.Spec.ResilienceMilli))
	w.integer("minimumEnergyMilli", int64(summary.Spec.MinimumEnergyMilli))
	w.integer("minimumThermalMilli", int64(summary.Spec.MinimumThermalMilli))
	w.integer("maximumSnapshotAgeSeconds", summary.Spec.MaximumSnapshotAgeSecs)
	w.string("exporterSnapshotDigest", summary.Spec.ExporterSnapshotDigest)
	if resourceSummaryUsesCanonicalV2(summary.Spec) {
		// Re-emit all fields with a v2 format header. Keeping v1 untouched when the
		// new fields are absent preserves already-signed v1alpha1 objects.
		w = newCanonicalWriterWithFormat(canonicalReporterFormatV2, "SpaceDomainResourceSummary", summary.Name)
		w.domain("domain", summary.Spec.Domain)
		w.timestamp("observedAt", summary.Spec.ObservedAt.Time)
		w.timestamp("validUntil", summary.Spec.ValidUntil.Time)
		w.provenance(summary.Spec.Provenance)
		devices := append([]DeviceCapacity(nil), summary.Spec.Devices...)
		sort.Slice(devices, func(i, j int) bool {
			return deviceCapacityCanonicalKey(devices[i]) < deviceCapacityCanonicalKey(devices[j])
		})
		w.integer("devices.count", int64(len(devices)))
		for index, device := range devices {
			prefix := fmt.Sprintf("devices.%d", index)
			w.string(prefix+".class", device.Class)
			w.integer(prefix+".count", device.Count)
			writeSortedStrings(w, prefix+".architectures", device.Architectures)
			writeSortedStrings(w, prefix+".models", device.Models)
			writeSortedStrings(w, prefix+".precision", device.Precision)
			w.integer(prefix+".computeMilli", device.ComputeMilli)
			w.integer(prefix+".fragmentationMilli", int64(device.FragmentationMilli))
		}
		writeSortedStringMap(w, "software", summary.Spec.Software)
		writeSortedStrings(w, "dataLocations", summary.Spec.DataLocations)
		w.integer("queueDelaySeconds", summary.Spec.QueueDelaySeconds)
		w.integer("energyHeadroomMilli", int64(summary.Spec.EnergyHeadroomMilli))
		w.integer("thermalHeadroomMilli", int64(summary.Spec.ThermalHeadroomMilli))
		w.integer("resilienceMilli", int64(summary.Spec.ResilienceMilli))
		w.integer("minimumEnergyMilli", int64(summary.Spec.MinimumEnergyMilli))
		w.integer("minimumThermalMilli", int64(summary.Spec.MinimumThermalMilli))
		w.integer("maximumSnapshotAgeSeconds", summary.Spec.MaximumSnapshotAgeSecs)
		w.string("exporterSnapshotDigest", summary.Spec.ExporterSnapshotDigest)
		writeScalarCapacity(w, "cpu", summary.Spec.CPU)
		writeScalarCapacity(w, "systemMemoryBytes", summary.Spec.SystemMemoryBytes)
		writeScalarCapacity(w, "ephemeralStorageBytes", summary.Spec.EphemeralStorageBytes)
		storage := append([]StorageCapacity(nil), summary.Spec.PersistentStorage...)
		sort.Slice(storage, func(i, j int) bool { return storage[i].Class < storage[j].Class })
		w.integer("persistentStorage.count", int64(len(storage)))
		for i, v := range storage {
			p := fmt.Sprintf("persistentStorage.%d", i)
			w.string(p+".class", v.Class)
			w.integer(p+".capacityBytes", v.CapacityBytes)
			w.integer(p+".availableBytes", v.AvailableBytes)
		}
		numa := append([]NUMAResource(nil), summary.Spec.NUMATopology...)
		sort.Slice(numa, func(i, j int) bool { return numa[i].ID < numa[j].ID })
		w.integer("numaTopology.count", int64(len(numa)))
		for i, v := range numa {
			p := fmt.Sprintf("numaTopology.%d", i)
			w.integer(p+".id", int64(v.ID))
			w.integer(p+".cpuMilliCapacity", v.CPUMilliCapacity)
			w.integer(p+".cpuMilliAvailable", v.CPUMilliAvailable)
			w.integer(p+".memoryCapacityBytes", v.MemoryCapacityBytes)
			w.integer(p+".memoryAvailableBytes", v.MemoryAvailableBytes)
		}
		w.string("trust.state", summary.Spec.Trust.State)
		w.string("trust.provider", summary.Spec.Trust.Provider)
		w.string("trust.evidenceDigest", summary.Spec.Trust.EvidenceDigest)
		w.timestamp("trust.observedAt", summary.Spec.Trust.ObservedAt.Time)
		w.timestamp("trust.validUntil", summary.Spec.Trust.ValidUntil.Time)
		w.integer("autonomyDurationSeconds", summary.Spec.AutonomyDurationSeconds)
		w.string("energy.source", summary.Spec.Energy.Source)
		w.integer("energy.capacityMilliWattHours", summary.Spec.Energy.CapacityMilliWattHours)
		w.integer("energy.availableMilliWattHours", summary.Spec.Energy.AvailableMilliWattHours)
		if summary.Spec.PhysicalDeviceInventoryRef == nil {
			w.boolean("physicalDeviceInventoryRef.present", false)
		} else {
			ref := summary.Spec.PhysicalDeviceInventoryRef
			w.boolean("physicalDeviceInventoryRef.present", true)
			w.string("physicalDeviceInventoryRef.name", ref.Name)
			w.string("physicalDeviceInventoryRef.digest", ref.Digest)
			w.string("physicalDeviceInventoryRef.resourceVersion", ref.ResourceVersion)
		}
	}
	return w.bytes(), nil
}

func resourceSummaryUsesCanonicalV2(spec SpaceDomainResourceSummarySpec) bool {
	return spec.CPU.Capacity != 0 || spec.CPU.Available != 0 || spec.SystemMemoryBytes.Capacity != 0 || spec.SystemMemoryBytes.Available != 0 || spec.EphemeralStorageBytes.Capacity != 0 || spec.EphemeralStorageBytes.Available != 0 || len(spec.PersistentStorage) != 0 || len(spec.NUMATopology) != 0 || spec.Trust.State != "" || spec.Trust.Provider != "" || spec.Trust.EvidenceDigest != "" || !spec.Trust.ObservedAt.IsZero() || !spec.Trust.ValidUntil.IsZero() || spec.AutonomyDurationSeconds != 0 || spec.Energy.Source != "" || spec.Energy.CapacityMilliWattHours != 0 || spec.Energy.AvailableMilliWattHours != 0 || spec.PhysicalDeviceInventoryRef != nil
}

func writeScalarCapacity(w *canonicalWriter, prefix string, value ScalarCapacity) {
	w.integer(prefix+".capacity", value.Capacity)
	w.integer(prefix+".available", value.Available)
}

func writeSortedStringMap(w *canonicalWriter, prefix string, values map[string]string) {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	w.integer(prefix+".count", int64(len(keys)))
	for i, key := range keys {
		p := fmt.Sprintf("%s.%d", prefix, i)
		w.string(p+".key", key)
		w.string(p+".value", values[key])
	}
}

func canonicalPhysicalDeviceInventory(inventory *PhysicalDeviceInventory) ([]byte, error) {
	if inventory == nil {
		return nil, fmt.Errorf("physical device inventory is required")
	}
	w := newCanonicalWriterWithFormat(canonicalReporterFormatV2, "PhysicalDeviceInventory", inventory.Name)
	w.domain("domain", inventory.Spec.Domain)
	w.string("nodeName", inventory.Spec.NodeName)
	w.timestamp("observedAt", inventory.Spec.ObservedAt.Time)
	w.timestamp("validUntil", inventory.Spec.ValidUntil.Time)
	w.integer("confidenceMilli", int64(inventory.Spec.ConfidenceMilli))
	w.provenance(inventory.Spec.Provenance)
	devices := append([]PhysicalDevice(nil), inventory.Spec.Devices...)
	sort.Slice(devices, func(i, j int) bool { return devices[i].StableDeviceID < devices[j].StableDeviceID })
	w.integer("devices.count", int64(len(devices)))
	for i, d := range devices {
		p := fmt.Sprintf("devices.%d", i)
		w.string(p+".stableDeviceID", d.StableDeviceID)
		w.string(p+".kubernetesResourceName", d.KubernetesResourceName)
		w.string(p+".allocationID", d.AllocationID)
		w.string(p+".draAllocationID", d.DRAAllocationID)
		w.string(p+".vendorAllocationID", d.VendorAllocationID)
		w.string(p+".class", d.Class)
		w.string(p+".vendor", d.Vendor)
		w.string(p+".model", d.Model)
		w.string(p+".architecture", d.Architecture)
		w.integer(p+".topology.numaNode", int64(d.Topology.NUMANode))
		w.string(p+".topology.socketID", d.Topology.SocketID)
		w.string(p+".topology.pciAddress", d.Topology.PCIAddress)
		peers := append([]DevicePeerInterconnect(nil), d.PeerInterconnects...)
		sort.Slice(peers, func(a, b int) bool {
			if peers[a].PeerStableDeviceID != peers[b].PeerStableDeviceID {
				return peers[a].PeerStableDeviceID < peers[b].PeerStableDeviceID
			}
			return peers[a].Type < peers[b].Type
		})
		w.integer(p+".peerInterconnects.count", int64(len(peers)))
		for j, peer := range peers {
			pp := fmt.Sprintf("%s.peerInterconnects.%d", p, j)
			w.string(pp+".peerStableDeviceID", peer.PeerStableDeviceID)
			w.string(pp+".type", peer.Type)
			w.integer(pp+".bandwidthBitsPerSecond", peer.BandwidthBitsPerSecond)
		}
		w.integer(p+".totalMemoryBytes", d.TotalMemoryBytes)
		w.integer(p+".freeMemoryBytes", d.FreeMemoryBytes)
		w.integer(p+".memoryBandwidthBitsPerSecond", d.MemoryBandwidthBitsPerSecond)
		w.integer(p+".interconnectBandwidthBitsPerSecond", d.InterconnectBandwidthBitsPerSecond)
		writeSortedStrings(w, p+".supportedPrecision", d.SupportedPrecision)
		w.string(p+".firmware", d.Firmware)
		w.string(p+".driver", d.Driver)
		w.string(p+".runtime", d.Runtime)
		writeSortedStringMap(w, p+".libraries", d.Libraries)
		w.string(p+".health", d.Health)
		w.integer(p+".temperatureMilliCelsius", d.TemperatureMilliCelsius)
		w.integer(p+".powerMilliwatts", d.PowerMilliwatts)
		w.integer(p+".confidenceMilli", int64(d.ConfidenceMilli))
	}
	return w.bytes(), nil
}

func canonicalTransferReceipt(receipt *SpaceTransferReceipt) ([]byte, error) {
	if receipt == nil {
		return nil, fmt.Errorf("transfer receipt is required")
	}
	w := newCanonicalWriter("SpaceTransferReceipt", receipt.Name)
	w.string("transferID", receipt.Spec.TransferID)
	w.string("missionUID", receipt.Spec.MissionUID)
	w.string("planID", receipt.Spec.PlanID)
	w.integer("attempt", int64(receipt.Spec.Attempt))
	w.domain("source", receipt.Spec.Source)
	w.domain("destination", receipt.Spec.Destination)
	w.string("dataID", receipt.Spec.DataID)
	w.integer("bytes", receipt.Spec.Bytes)
	w.string("payloadDigest", receipt.Spec.PayloadDigest)
	w.timestamp("startedAt", receipt.Spec.StartedAt.Time)
	w.timestamp("completedAt", receipt.Spec.CompletedAt.Time)
	w.provenance(receipt.Spec.Provenance)
	return w.bytes(), nil
}

func canonicalExecutionLease(lease *SpaceExecutionLease) ([]byte, error) {
	if lease == nil {
		return nil, fmt.Errorf("execution lease is required")
	}
	w := newCanonicalWriter("SpaceExecutionLease", lease.Name)
	w.domain("source", lease.Spec.Source)
	w.domain("destination", lease.Spec.Destination)
	w.string("fence.missionUID", lease.Spec.Fence.MissionUID)
	w.string("fence.planID", lease.Spec.Fence.PlanID)
	w.integer("fence.attempt", int64(lease.Spec.Fence.Attempt))
	w.integer("fence.leaseEpoch", lease.Spec.Fence.LeaseEpoch)
	w.string("fence.tokenHash", lease.Spec.Fence.TokenHash)
	w.timestamp("fence.expiresAt", lease.Spec.Fence.ExpiresAt.Time)
	w.timestamp("heartbeatAt", lease.Spec.HeartbeatAt.Time)
	w.integer("maximumClockSkewSeconds", lease.Spec.MaximumClockSkewSeconds)
	w.provenance(lease.Spec.Provenance)
	return w.bytes(), nil
}

func canonicalExecutionObservation(observation *SpaceExecutionObservation) ([]byte, error) {
	if observation == nil {
		return nil, fmt.Errorf("execution observation is required")
	}
	w := newCanonicalWriter("SpaceExecutionObservation", observation.Name)
	w.string("observationID", observation.Spec.ObservationID)
	w.string("missionUID", observation.Spec.MissionUID)
	w.string("planID", observation.Spec.PlanID)
	w.integer("attempt", int64(observation.Spec.Attempt))
	w.integer("leaseEpoch", observation.Spec.LeaseEpoch)
	w.string("tokenHash", observation.Spec.TokenHash)
	w.domain("source", observation.Spec.Source)
	w.domain("destination", observation.Spec.Destination)
	w.string("phase", string(observation.Spec.Phase))
	w.string("checkpointID", observation.Spec.CheckpointID)
	w.timestamp("observedAt", observation.Spec.ObservedAt.Time)
	w.provenance(observation.Spec.Provenance)
	return w.bytes(), nil
}

func canonicalResultReceipt(receipt *SpaceResultReceipt) ([]byte, error) {
	if receipt == nil {
		return nil, fmt.Errorf("result receipt is required")
	}
	w := newCanonicalWriter("SpaceResultReceipt", receipt.Name)
	w.string("resultID", receipt.Spec.ResultID)
	w.string("missionUID", receipt.Spec.MissionUID)
	w.string("planID", receipt.Spec.PlanID)
	w.integer("attempt", int64(receipt.Spec.Attempt))
	w.domain("source", receipt.Spec.Source)
	w.domain("destination", receipt.Spec.Destination)
	w.integer("bytes", receipt.Spec.Bytes)
	w.string("payloadDigest", receipt.Spec.PayloadDigest)
	w.integer("leaseEpoch", receipt.Spec.LeaseEpoch)
	w.string("tokenHash", receipt.Spec.TokenHash)
	w.timestamp("completedAt", receipt.Spec.CompletedAt.Time)
	w.provenance(receipt.Spec.Provenance)
	return w.bytes(), nil
}

func writeSortedStrings(w *canonicalWriter, key string, values []string) {
	sorted := append([]string(nil), values...)
	sort.Strings(sorted)
	w.integer(key+".count", int64(len(sorted)))
	for index, value := range sorted {
		w.string(fmt.Sprintf("%s.%d", key, index), value)
	}
}

func normalizedDomainIdentity(value DomainReference) string {
	return strings.ToLower(strings.TrimSpace(string(value.OrbitClass))) + "|" +
		strings.ToLower(strings.TrimSpace(value.ClusterID)) + "|" +
		strings.ToLower(strings.TrimSpace(value.Name))
}

func derivedObjectName(prefix, identity string) string {
	sum := sha256.Sum256([]byte(identity))
	return prefix + "-" + hex.EncodeToString(sum[:16])
}

// ReporterBindingName is deterministic so admission can fetch one binding with a
// single GET instead of granting list access to all cluster-scoped bindings.
func ReporterBindingName(principal string) string {
	return derivedObjectName("reporter", strings.TrimSpace(principal))
}

func DomainResourceSummaryName(domain DomainReference) string {
	return derivedObjectName("domain", normalizedDomainIdentity(domain))
}

func PhysicalDeviceInventoryName(domain DomainReference, nodeName string) string {
	return derivedObjectName("device-inventory", normalizedDomainIdentity(domain)+"|"+strings.ToLower(strings.TrimSpace(nodeName)))
}

func LinkSnapshotName(source, destination DomainReference) string {
	return derivedObjectName("link", normalizedDomainIdentity(source)+"->"+normalizedDomainIdentity(destination))
}

func TransferReceiptName(source, destination DomainReference, missionUID, planID, transferID string) string {
	identity := normalizedDomainIdentity(source) + "->" + normalizedDomainIdentity(destination) + "|" +
		strings.TrimSpace(missionUID) + "|" + strings.ToLower(strings.TrimSpace(planID)) + "|" + strings.ToLower(strings.TrimSpace(transferID))
	return derivedObjectName("transfer", identity)
}

func ResultReceiptName(source, destination DomainReference, missionUID, planID, resultID string) string {
	identity := normalizedDomainIdentity(source) + "->" + normalizedDomainIdentity(destination) + "|" +
		strings.TrimSpace(missionUID) + "|" + strings.ToLower(strings.TrimSpace(planID)) + "|" + strings.ToLower(strings.TrimSpace(resultID))
	return derivedObjectName("result", identity)
}
