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

const canonicalReporterFormat = "spacecompute-canonical-v1"

type canonicalWriter struct {
	buffer bytes.Buffer
}

func newCanonicalWriter(kind, name string) *canonicalWriter {
	w := &canonicalWriter{}
	w.string("format", canonicalReporterFormat)
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
