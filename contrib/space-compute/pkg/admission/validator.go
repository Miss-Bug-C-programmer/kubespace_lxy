package admission

import (
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"strings"

	admissionv1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
)

var reporterBindingGVR = schema.GroupVersionResource{
	Group: spacev1.GroupName, Version: "v1alpha1", Resource: "spacedomainreporterbindings",
}

type TrustSource interface {
	Binding(context.Context, string) (*spacev1.SpaceDomainReporterBinding, error)
	PublicKey(context.Context, spacev1.SecretReference) (ed25519.PublicKey, error)
}

type KubernetesTrustSource struct {
	dynamic         dynamic.Interface
	core            kubernetes.Interface
	secretNamespace string
	secretName      string
}

func NewKubernetesTrustSource(dynamicClient dynamic.Interface, coreClient kubernetes.Interface, secretNamespace, secretName string) (*KubernetesTrustSource, error) {
	if dynamicClient == nil || coreClient == nil {
		return nil, fmt.Errorf("dynamic and core Kubernetes clients are required")
	}
	secretNamespace = strings.TrimSpace(secretNamespace)
	secretName = strings.TrimSpace(secretName)
	if secretNamespace == "" || secretName == "" {
		return nil, fmt.Errorf("reporter public-key Secret namespace and name are required")
	}
	return &KubernetesTrustSource{dynamic: dynamicClient, core: coreClient, secretNamespace: secretNamespace, secretName: secretName}, nil
}

func (s *KubernetesTrustSource) Binding(ctx context.Context, principal string) (*spacev1.SpaceDomainReporterBinding, error) {
	name := spacev1.ReporterBindingName(principal)
	object, err := s.dynamic.Resource(reporterBindingGVR).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get reporter binding %q: %w", name, err)
	}
	result := &spacev1.SpaceDomainReporterBinding{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.Object, result); err != nil {
		return nil, fmt.Errorf("decode reporter binding %q: %w", name, err)
	}
	if err := spacev1.ValidateReporterBinding(result, nil); err != nil {
		return nil, fmt.Errorf("reporter binding %q is invalid: %w", name, err)
	}
	return result, nil
}

func (s *KubernetesTrustSource) PublicKey(ctx context.Context, ref spacev1.SecretReference) (ed25519.PublicKey, error) {
	if ref.Namespace != s.secretNamespace || ref.Name != s.secretName {
		return nil, fmt.Errorf("public key reference %s/%s is outside configured trust Secret %s/%s", ref.Namespace, ref.Name, s.secretNamespace, s.secretName)
	}
	secret, err := s.core.CoreV1().Secrets(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get reporter public-key Secret: %w", err)
	}
	raw, ok := secret.Data[ref.Key]
	if !ok || len(raw) == 0 {
		return nil, fmt.Errorf("reporter public key %q is missing from Secret %s/%s", ref.Key, ref.Namespace, ref.Name)
	}
	key, err := parseEd25519PublicKey(raw)
	if err != nil {
		return nil, fmt.Errorf("parse reporter public key %q: %w", ref.Key, err)
	}
	return key, nil
}

func parseEd25519PublicKey(raw []byte) (ed25519.PublicKey, error) {
	if len(raw) == ed25519.PublicKeySize {
		return append(ed25519.PublicKey(nil), raw...), nil
	}
	block, _ := pem.Decode(raw)
	if block == nil {
		return nil, fmt.Errorf("expected raw 32-byte Ed25519 key or PEM PUBLIC KEY")
	}
	parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	key, ok := parsed.(ed25519.PublicKey)
	if !ok || len(key) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("PEM key is not Ed25519")
	}
	return append(ed25519.PublicKey(nil), key...), nil
}

type Validator struct {
	trust TrustSource
	clock spacev1.Clock
}

func NewValidator(trust TrustSource, clock spacev1.Clock) (*Validator, error) {
	if trust == nil {
		return nil, fmt.Errorf("trust source is required")
	}
	if clock == nil {
		clock = spacev1.RealClock{}
	}
	return &Validator{trust: trust, clock: clock}, nil
}

func (v *Validator) Validate(ctx context.Context, request *admissionv1.AdmissionRequest) error {
	if request == nil {
		return fmt.Errorf("admission request is required")
	}
	switch request.Operation {
	case admissionv1.Create, admissionv1.Update:
	default:
		return nil
	}

	if request.Resource.Group != spacev1.GroupName || request.Resource.Version != "v1alpha1" {
		return fmt.Errorf("unsupported admission resource %s/%s/%s", request.Resource.Group, request.Resource.Version, request.Resource.Resource)
	}
	if request.Resource.Resource == "spacedomainreporterbindings" {
		return v.validateBindingRequest(request)
	}
	return v.validateReporterRequest(ctx, request)
}

func (v *Validator) validateBindingRequest(request *admissionv1.AdmissionRequest) error {
	current := &spacev1.SpaceDomainReporterBinding{}
	if err := decodeRaw(request.Object.Raw, current); err != nil {
		return fmt.Errorf("decode reporter binding: %w", err)
	}
	var previous *spacev1.SpaceDomainReporterBinding
	if request.Operation == admissionv1.Update {
		previous = &spacev1.SpaceDomainReporterBinding{}
		if err := decodeRaw(request.OldObject.Raw, previous); err != nil {
			return fmt.Errorf("decode previous reporter binding: %w", err)
		}
	}
	return spacev1.ValidateReporterBinding(current, previous)
}

type reporterEnvelope struct {
	kind           string
	name           string
	provenance     *spacev1.Provenance
	source         spacev1.DomainReference
	destination    *spacev1.DomainReference
	digestObject   runtime.Object
	observedAtNano int64
	identity       string
}

func (v *Validator) validateReporterRequest(ctx context.Context, request *admissionv1.AdmissionRequest) error {
	current, err := v.decodeReporterEnvelope(request.Resource.Resource, request.Object.Raw)
	if err != nil {
		return err
	}
	var previous *reporterEnvelope
	if request.Operation == admissionv1.Update {
		previous, err = v.decodeReporterEnvelope(request.Resource.Resource, request.OldObject.Raw)
		if err != nil {
			return fmt.Errorf("decode previous reporter object: %w", err)
		}
		// Stable identity and the exact provenance chain are checked before
		// current binding/peer policy so an update can never masquerade as a
		// create in another domain or directed link.
		if err := validateImmutableAndChain(current, previous); err != nil {
			return err
		}
		if err := validateUpdateStructural(current, previous, v.clock); err != nil {
			return err
		}
	}
	principal := request.UserInfo.Username
	if principal == "" {
		return fmt.Errorf("authenticated principal is required")
	}
	reporterPrincipal := current.provenance.ReporterID
	if reporterPrincipal == "" {
		return fmt.Errorf("spec.provenance.reporterID is required")
	}
	binding, err := v.trust.Binding(ctx, reporterPrincipal)
	if err != nil {
		return err
	}
	if binding.Spec.ReporterPrincipal != reporterPrincipal {
		return fmt.Errorf("reporter binding principal does not match signed reporter identity")
	}
	if principal != reporterPrincipal && !principalAllowed(binding.Spec.AllowedGateways, principal) {
		return fmt.Errorf("authenticated principal is neither the signed reporter nor an explicitly allowed transport gateway")
	}
	if binding.Spec.Domain != current.source {
		return fmt.Errorf("reporter object source/domain does not match bound domain")
	}
	if !kindAllowed(binding.Spec.AllowedKinds, current.kind) {
		return fmt.Errorf("reporter binding does not allow kind %s", current.kind)
	}
	if current.destination != nil && !peerAllowed(binding.Spec.AllowedPeers, *current.destination) {
		return fmt.Errorf("destination domain is not an explicitly allowed peer")
	}
	if current.name != expectedObjectName(current) {
		return fmt.Errorf("metadata.name %q is not derived from normalized reporter object identity; expected %q", current.name, expectedObjectName(current))
	}

	if request.Operation != admissionv1.Update {
		if current.provenance.Sequence != 1 {
			return fmt.Errorf("new reporter object sequence must be exactly 1")
		}
		if current.provenance.PreviousDigest != "" {
			return fmt.Errorf("new reporter object previousDigest must be empty")
		}
	}

	digest, err := spacev1.ReporterDigest(current.digestObject)
	if err != nil {
		return fmt.Errorf("canonical reporter digest: %w", err)
	}
	if current.provenance.Digest != digest {
		return fmt.Errorf("spec.provenance.digest mismatch: canonical payload digest is %s", digest)
	}
	if previous != nil {
		oldDigest, err := spacev1.ReporterDigest(previous.digestObject)
		if err != nil {
			return fmt.Errorf("canonical previous reporter digest: %w", err)
		}
		if previous.provenance.Digest != oldDigest {
			return fmt.Errorf("stored previous object digest does not match its canonical payload")
		}
	}
	publicKey, err := v.trust.PublicKey(ctx, binding.Spec.PublicKeyRef)
	if err != nil {
		return err
	}
	if err := verifySignature(publicKey, current.provenance); err != nil {
		return err
	}
	return nil
}

func validateUpdateStructural(current, previous *reporterEnvelope, clock spacev1.Clock) error {
	switch value := current.digestObject.(type) {
	case *spacev1.SpaceLinkSnapshot:
		old, ok := previous.digestObject.(*spacev1.SpaceLinkSnapshot)
		if !ok {
			return fmt.Errorf("previous reporter object kind does not match")
		}
		return spacev1.ValidateLinkSnapshot(value, old, clock)
	}
	return nil
}

func validateImmutableAndChain(current, previous *reporterEnvelope) error {
	if current.kind != previous.kind || current.name != previous.name || current.identity != previous.identity {
		return fmt.Errorf("reporter object stable identity is immutable")
	}
	if current.provenance.ReporterID != previous.provenance.ReporterID {
		return fmt.Errorf("spec.provenance.reporterID is immutable")
	}
	if current.provenance.Source != previous.provenance.Source {
		return fmt.Errorf("spec.provenance.source is immutable")
	}
	if current.provenance.Sequence != previous.provenance.Sequence+1 {
		return fmt.Errorf("spec.provenance.sequence must increase by exactly one")
	}
	if current.provenance.PreviousDigest != previous.provenance.Digest {
		return fmt.Errorf("spec.provenance.previousDigest must equal the previous canonical digest")
	}
	if current.observedAtNano <= previous.observedAtNano {
		return fmt.Errorf("reporter timestamp must increase on every update")
	}
	return nil
}

func verifySignature(publicKey ed25519.PublicKey, provenance *spacev1.Provenance) error {
	digestBytes, err := hex.DecodeString(provenance.Digest)
	if err != nil || len(digestBytes) != 32 {
		return fmt.Errorf("spec.provenance.digest is not a SHA-256 digest")
	}
	signature, err := base64.StdEncoding.DecodeString(provenance.Signature)
	if err != nil || len(signature) != ed25519.SignatureSize {
		return fmt.Errorf("spec.provenance.signature must be standard-base64 encoded Ed25519 signature")
	}
	if !ed25519.Verify(publicKey, digestBytes, signature) {
		return fmt.Errorf("spec.provenance.signature verification failed")
	}
	return nil
}

func (v *Validator) decodeReporterEnvelope(resource string, raw []byte) (*reporterEnvelope, error) {
	switch resource {
	case "spacelinksnapshots":
		value := &spacev1.SpaceLinkSnapshot{}
		if err := decodeRaw(raw, value); err != nil {
			return nil, fmt.Errorf("decode SpaceLinkSnapshot: %w", err)
		}
		var previous *spacev1.SpaceLinkSnapshot
		if err := spacev1.ValidateLinkSnapshot(value, previous, v.clock); err != nil {
			return nil, err
		}
		destination := value.Spec.Destination
		return &reporterEnvelope{
			kind: "SpaceLinkSnapshot", name: value.Name, provenance: &value.Spec.Provenance,
			source: value.Spec.Source, destination: &destination, digestObject: value,
			observedAtNano: value.Spec.ObservedAt.UnixNano(),
			identity:       normalizedEnvelopeIdentity(value.Spec.Source, &value.Spec.Destination, ""),
		}, nil
	case "spacedomainresourcesummaries":
		value := &spacev1.SpaceDomainResourceSummary{}
		if err := decodeRaw(raw, value); err != nil {
			return nil, fmt.Errorf("decode SpaceDomainResourceSummary: %w", err)
		}
		if err := spacev1.ValidateResourceSummary(value, v.clock); err != nil {
			return nil, err
		}
		return &reporterEnvelope{
			kind: "SpaceDomainResourceSummary", name: value.Name, provenance: &value.Spec.Provenance,
			source: value.Spec.Domain, digestObject: value, observedAtNano: value.Spec.ObservedAt.UnixNano(),
			identity: normalizedEnvelopeIdentity(value.Spec.Domain, nil, ""),
		}, nil
	case "spacetransferreceipts":
		value := &spacev1.SpaceTransferReceipt{}
		if err := decodeRaw(raw, value); err != nil {
			return nil, fmt.Errorf("decode SpaceTransferReceipt: %w", err)
		}
		if err := spacev1.ValidateTransferReceipt(value, v.clock); err != nil {
			return nil, err
		}
		destination := value.Spec.Destination
		return &reporterEnvelope{
			kind: "SpaceTransferReceipt", name: value.Name, provenance: &value.Spec.Provenance,
			source: value.Spec.Source, destination: &destination, digestObject: value,
			observedAtNano: value.Spec.CompletedAt.UnixNano(),
			identity:       normalizedEnvelopeIdentity(value.Spec.Source, &value.Spec.Destination, fmt.Sprintf("%s|%s|%d|%s|%s", value.Spec.MissionUID, value.Spec.PlanID, value.Spec.Attempt, value.Spec.TransferID, value.Spec.DataID)),
		}, nil
	case "spaceexecutionleases":
		value := &spacev1.SpaceExecutionLease{}
		if err := decodeRaw(raw, value); err != nil {
			return nil, fmt.Errorf("decode SpaceExecutionLease: %w", err)
		}
		if err := spacev1.ValidateExecutionLease(value, v.clock); err != nil {
			return nil, err
		}
		destination := value.Spec.Destination
		var peer *spacev1.DomainReference
		if destination != value.Spec.Source {
			peer = &destination
		}
		return &reporterEnvelope{kind: "SpaceExecutionLease", name: value.Name, provenance: &value.Spec.Provenance, source: value.Spec.Source, destination: peer, digestObject: value, observedAtNano: value.Spec.HeartbeatAt.UnixNano(), identity: normalizedEnvelopeIdentity(value.Spec.Source, &value.Spec.Destination, fmt.Sprintf("%s|%s|%d|%d", value.Spec.Fence.MissionUID, value.Spec.Fence.PlanID, value.Spec.Fence.Attempt, value.Spec.Fence.LeaseEpoch))}, nil
	case "spaceexecutionobservations":
		value := &spacev1.SpaceExecutionObservation{}
		if err := decodeRaw(raw, value); err != nil {
			return nil, fmt.Errorf("decode SpaceExecutionObservation: %w", err)
		}
		if err := spacev1.ValidateExecutionObservation(value, v.clock); err != nil {
			return nil, err
		}
		destination := value.Spec.Destination
		var peer *spacev1.DomainReference
		if destination != value.Spec.Source {
			peer = &destination
		}
		return &reporterEnvelope{kind: "SpaceExecutionObservation", name: value.Name, provenance: &value.Spec.Provenance, source: value.Spec.Source, destination: peer, digestObject: value, observedAtNano: value.Spec.ObservedAt.UnixNano(), identity: normalizedEnvelopeIdentity(value.Spec.Source, &value.Spec.Destination, fmt.Sprintf("%s|%s|%d|%d|%s", value.Spec.MissionUID, value.Spec.PlanID, value.Spec.Attempt, value.Spec.LeaseEpoch, value.Spec.ObservationID))}, nil
	case "spaceresultreceipts":
		value := &spacev1.SpaceResultReceipt{}
		if err := decodeRaw(raw, value); err != nil {
			return nil, fmt.Errorf("decode SpaceResultReceipt: %w", err)
		}
		if err := spacev1.ValidateResultReceipt(value, v.clock); err != nil {
			return nil, err
		}
		destination := value.Spec.Destination
		return &reporterEnvelope{
			kind: "SpaceResultReceipt", name: value.Name, provenance: &value.Spec.Provenance,
			source: value.Spec.Source, destination: &destination, digestObject: value,
			observedAtNano: value.Spec.CompletedAt.UnixNano(),
			identity:       normalizedEnvelopeIdentity(value.Spec.Source, &value.Spec.Destination, fmt.Sprintf("%s|%s|%d|%s", value.Spec.MissionUID, value.Spec.PlanID, value.Spec.Attempt, value.Spec.ResultID)),
		}, nil
	default:
		return nil, fmt.Errorf("unsupported reporter resource %q", resource)
	}
}

func expectedObjectName(value *reporterEnvelope) string {
	switch object := value.digestObject.(type) {
	case *spacev1.SpaceLinkSnapshot:
		return spacev1.LinkSnapshotName(object.Spec.Source, object.Spec.Destination)
	case *spacev1.SpaceDomainResourceSummary:
		return spacev1.DomainResourceSummaryName(object.Spec.Domain)
	case *spacev1.SpaceTransferReceipt:
		return spacev1.TransferReceiptName(object.Spec.Source, object.Spec.Destination, object.Spec.MissionUID, object.Spec.PlanID, object.Spec.TransferID)
	case *spacev1.SpaceExecutionLease:
		return spacev1.ExecutionLeaseName(object.Spec.Fence.MissionUID, object.Spec.Fence.PlanID, object.Spec.Fence.Attempt, object.Spec.Fence.LeaseEpoch)
	case *spacev1.SpaceExecutionObservation:
		return spacev1.ExecutionObservationName(object.Spec.Source, object.Spec.Destination, object.Spec.MissionUID, object.Spec.PlanID, object.Spec.ObservationID)
	case *spacev1.SpaceResultReceipt:
		return spacev1.ResultReceiptName(object.Spec.Source, object.Spec.Destination, object.Spec.MissionUID, object.Spec.PlanID, object.Spec.ResultID)
	default:
		return ""
	}
}

func normalizedEnvelopeIdentity(source spacev1.DomainReference, destination *spacev1.DomainReference, id string) string {
	value := string(source.OrbitClass) + "|" + source.ClusterID + "|" + source.Name
	if destination != nil {
		value += "->" + string(destination.OrbitClass) + "|" + destination.ClusterID + "|" + destination.Name
	}
	if id != "" {
		value += "|" + id
	}
	return strings.ToLower(value)
}

func kindAllowed(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func principalAllowed(values []string, target string) bool {
	for _, value := range values {
		if strings.TrimSpace(value) == target {
			return true
		}
	}
	return false
}

func peerAllowed(values []spacev1.DomainReference, target spacev1.DomainReference) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func decodeRaw(raw []byte, out interface{}) error {
	if len(raw) == 0 {
		return fmt.Errorf("object payload is empty")
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	return nil
}
