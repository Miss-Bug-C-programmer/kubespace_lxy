package v1alpha1

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
)

var reporterKinds = map[string]struct{}{
	"SpaceLinkSnapshot":          {},
	"SpaceDomainResourceSummary": {},
	"SpaceTransferReceipt":       {},
	"SpaceExecutionLease":        {},
	"SpaceExecutionObservation":  {},
	"SpaceResultReceipt":         {},
}

func ValidateReporterBinding(binding *SpaceDomainReporterBinding, previous *SpaceDomainReporterBinding) error {
	var errs ValidationErrors
	if binding == nil {
		errs.add("binding", "is required")
		return errs
	}
	principal := strings.TrimSpace(binding.Spec.ReporterPrincipal)
	if principal == "" || len(principal) > 253 || strings.ContainsAny(principal, "\r\n\x00") {
		errs.add("spec.reporterPrincipal", "must be a non-empty authenticated principal of at most 253 bytes without control separators")
	}
	validateDomain("spec.domain", binding.Spec.Domain, &errs)
	expectedName := ReporterBindingName(principal)
	if binding.Name != expectedName {
		errs.addf("metadata.name", "must be %q for reporterPrincipal", expectedName)
	}
	if len(binding.Spec.AllowedKinds) == 0 || len(binding.Spec.AllowedKinds) > len(reporterKinds) {
		errs.addf("spec.allowedKinds", "must contain between 1 and %d reporter kinds", len(reporterKinds))
	}
	seenKinds := map[string]struct{}{}
	for i, kind := range binding.Spec.AllowedKinds {
		if _, ok := reporterKinds[kind]; !ok {
			errs.addf(fmt.Sprintf("spec.allowedKinds[%d]", i), "unsupported reporter kind %q", kind)
		}
		if _, duplicate := seenKinds[kind]; duplicate {
			errs.addf(fmt.Sprintf("spec.allowedKinds[%d]", i), "duplicate reporter kind %q", kind)
		}
		seenKinds[kind] = struct{}{}
	}
	if len(binding.Spec.AllowedPeers) > 64 {
		errs.add("spec.allowedPeers", "cannot exceed 64 domains")
	}
	if len(binding.Spec.AllowedGateways) > 16 {
		errs.add("spec.allowedGateways", "cannot exceed 16 principals")
	}
	seenGateways := map[string]struct{}{}
	for i, gateway := range binding.Spec.AllowedGateways {
		gateway = strings.TrimSpace(gateway)
		if gateway == "" || len(gateway) > 253 || strings.ContainsAny(gateway, "\r\n\x00") {
			errs.addf(fmt.Sprintf("spec.allowedGateways[%d]", i), "must be a non-empty principal of at most 253 bytes")
		}
		if gateway == principal {
			errs.addf(fmt.Sprintf("spec.allowedGateways[%d]", i), "must differ from reporterPrincipal")
		}
		if _, ok := seenGateways[gateway]; ok {
			errs.addf(fmt.Sprintf("spec.allowedGateways[%d]", i), "duplicate gateway principal")
		}
		seenGateways[gateway] = struct{}{}
	}
	seenPeers := map[string]struct{}{}
	for i, peer := range binding.Spec.AllowedPeers {
		path := fmt.Sprintf("spec.allowedPeers[%d]", i)
		validateDomain(path, peer, &errs)
		key := normalizedDomainIdentity(peer)
		if key == normalizedDomainIdentity(binding.Spec.Domain) {
			errs.add(path, "must differ from the reporter domain")
		}
		if _, duplicate := seenPeers[key]; duplicate {
			errs.add(path, "duplicate peer domain")
		}
		seenPeers[key] = struct{}{}
	}
	ref := binding.Spec.PublicKeyRef
	if problems := utilvalidation.IsDNS1123Subdomain(ref.Namespace); len(problems) > 0 {
		errs.add("spec.publicKeyRef.namespace", strings.Join(problems, ", "))
	}
	if problems := utilvalidation.IsDNS1123Subdomain(ref.Name); len(problems) > 0 {
		errs.add("spec.publicKeyRef.name", strings.Join(problems, ", "))
	}
	if problems := utilvalidation.IsConfigMapKey(ref.Key); len(problems) > 0 {
		errs.add("spec.publicKeyRef.key", strings.Join(problems, ", "))
	}
	if previous != nil {
		if previous.Spec.ReporterPrincipal != binding.Spec.ReporterPrincipal {
			errs.add("spec.reporterPrincipal", "is immutable")
		}
		if previous.Spec.Domain != binding.Spec.Domain {
			errs.add("spec.domain", "is immutable; delete and recreate the binding to move a principal")
		}
	}
	return errs.errOrNil()
}

func ValidateTransferReceipt(receipt *SpaceTransferReceipt, clock Clock) error {
	var errs ValidationErrors
	if receipt == nil {
		errs.add("receipt", "is required")
		return errs
	}
	if clock == nil {
		errs.add("clock", "is required")
		return errs
	}
	validateReceiptIdentity("spec.transferID", receipt.Spec.TransferID, &errs)
	validateReceiptCommon(receipt.Spec.MissionUID, receipt.Spec.PlanID, receipt.Spec.Attempt, receipt.Spec.Source, receipt.Spec.Destination, receipt.Spec.Bytes, receipt.Spec.PayloadDigest, receipt.Spec.Provenance, &errs)
	if receipt.Spec.DataID == "" || len(receipt.Spec.DataID) > 253 || strings.ContainsAny(receipt.Spec.DataID, "\r\n\x00") {
		errs.add("spec.dataID", "must be non-empty and at most 253 bytes without control separators")
	}
	if receipt.Spec.StartedAt.IsZero() || receipt.Spec.CompletedAt.IsZero() || receipt.Spec.CompletedAt.Before(&receipt.Spec.StartedAt) {
		errs.add("spec.completedAt", "must be at or after startedAt")
	}
	if receipt.Spec.CompletedAt.After(clock.Now().Add(time.Duration(MaxClockSkewSecs) * time.Second)) {
		errs.add("spec.completedAt", "is beyond maximum supported clock skew")
	}
	return errs.errOrNil()
}

func ValidateResultReceipt(receipt *SpaceResultReceipt, clock Clock) error {
	var errs ValidationErrors
	if receipt == nil {
		errs.add("receipt", "is required")
		return errs
	}
	if clock == nil {
		errs.add("clock", "is required")
		return errs
	}
	validateReceiptIdentity("spec.resultID", receipt.Spec.ResultID, &errs)
	validateReceiptCommon(receipt.Spec.MissionUID, receipt.Spec.PlanID, receipt.Spec.Attempt, receipt.Spec.Source, receipt.Spec.Destination, receipt.Spec.Bytes, receipt.Spec.PayloadDigest, receipt.Spec.Provenance, &errs)
	if receipt.Spec.LeaseEpoch < 0 {
		errs.add("spec.leaseEpoch", "cannot be negative")
	}
	if receipt.Spec.TokenHash != "" {
		validateLowerSHA256("spec.tokenHash", receipt.Spec.TokenHash, &errs)
	}
	if (receipt.Spec.LeaseEpoch == 0) != (receipt.Spec.TokenHash == "") {
		errs.add("spec", "leaseEpoch and tokenHash must either both be set or both be absent")
	}
	if receipt.Spec.CompletedAt.IsZero() {
		errs.add("spec.completedAt", "is required")
	} else if receipt.Spec.CompletedAt.After(clock.Now().Add(time.Duration(MaxClockSkewSecs) * time.Second)) {
		errs.add("spec.completedAt", "is beyond maximum supported clock skew")
	}
	return errs.errOrNil()
}

func validateReceiptIdentity(path, value string, errs *ValidationErrors) {
	if problems := utilvalidation.IsDNS1123Label(value); len(problems) > 0 {
		errs.add(path, strings.Join(problems, ", "))
	}
}

func validateReceiptCommon(missionUID, planID string, attempt int32, source, destination DomainReference, bytes int64, payloadDigest string, provenance Provenance, errs *ValidationErrors) {
	if missionUID == "" || len(missionUID) > 128 || strings.ContainsAny(missionUID, "\r\n\x00") {
		errs.add("spec.missionUID", "must be non-empty and at most 128 bytes without control separators")
	}
	if problems := utilvalidation.IsDNS1123Label(planID); len(problems) > 0 {
		errs.add("spec.planID", strings.Join(problems, ", "))
	}
	if attempt < 1 || attempt > 100 {
		errs.add("spec.attempt", "must be between 1 and 100")
	}
	validateDomain("spec.source", source, errs)
	validateDomain("spec.destination", destination, errs)
	if source == destination {
		errs.add("spec.destination", "must differ from source")
	}
	if bytes < 0 || bytes > MaxDataBytes {
		errs.addf("spec.bytes", "must be between 0 and %d", MaxDataBytes)
	}
	validateLowerSHA256("spec.payloadDigest", payloadDigest, errs)
	validateProvenance("spec.provenance", provenance, errs)
	if provenance.PreviousDigest != "" {
		validateLowerSHA256("spec.provenance.previousDigest", provenance.PreviousDigest, errs)
	}
	if provenance.Signature != "" {
		signature, err := base64.StdEncoding.DecodeString(provenance.Signature)
		if err != nil || len(signature) != 64 {
			errs.add("spec.provenance.signature", "must be standard-base64 encoded 64-byte Ed25519 signature")
		}
	}
}

func validateLowerSHA256(path, value string, errs *ValidationErrors) {
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != 32 || value != strings.ToLower(value) {
		errs.add(path, "must be a lowercase hexadecimal SHA-256 digest")
	}
}
