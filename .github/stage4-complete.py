#!/usr/bin/env python3
from pathlib import Path

source = Path('.github/stage4-recovered.py').read_text()
marker = 'append_once("docs/space-compute/PHASE4_API_AND_OPERATIONS.md", "Trusted reporter-domain admission"'
if marker not in source:
    raise SystemExit('recovered patch tail marker is missing')
head = source.split(marker, 1)[0]
doc = """## Trusted reporter-domain admission

Reporter-owned cross-domain objects are admitted through the fail-closed `space-compute-reporter-webhook`. Administrators create one cluster-scoped `SpaceDomainReporterBinding` whose deterministic name is derived from the authenticated principal. The binding fixes the reporter domain, allowed kinds, explicit peers and one Ed25519 public-key reference.

Reporter object names are derived from normalized domain or directed-link identity. Canonical provenance uses `spacecompute-canonical-v1`, fixed field order, UTC RFC3339Nano timestamps after Kubernetes API persistence normalization, explicitly sorted map/set-like values, and excludes only digest and signature. Reporters submit lowercase SHA-256 plus a base64 Ed25519 signature. CREATE requires sequence 1 and empty previous digest; UPDATE requires exact sequence increment, exact previous digest, increasing timestamp and immutable reporter/domain/source/destination/stable identity.

Provision the webhook TLS Secret and CA bundle before granting reporter writes. Reporter roles receive CREATE plus exact `resourceNames` for GET/UPDATE/PATCH; no shared reporter role receives unrestricted mutation of all cluster-scoped summaries or snapshots."""
tail = (
    '\nappend_once("docs/space-compute/PHASE4_API_AND_OPERATIONS.md", '
    '"Trusted reporter-domain admission", ' + repr(doc) + ')\n\n'
    'for path, content in NEW_FILES.items():\n'
    '    write(path, content)\n'
)
completed = compile(head + tail, '<stage4-recovered-complete>', 'exec')
exec(completed, {'__name__': '__main__'})

canonical_path = Path('contrib/space-compute/pkg/apis/v1alpha1/canonical.go')
canonical = canonical_path.read_text()
old_timestamp = '\tw.string(key, value.UTC().Format(time.RFC3339Nano))'
new_timestamp = '''\t// metav1.Time is persisted by the Kubernetes JSON codec at whole-second\n\t// precision. Normalize before formatting so a reporter signs exactly the\n\t// canonical value that admission will reconstruct from the stored object.\n\tw.string(key, value.UTC().Truncate(time.Second).Format(time.RFC3339Nano))'''
if canonical.count(old_timestamp) != 1:
    raise SystemExit('canonical timestamp marker mismatch')
canonical_path.write_text(canonical.replace(old_timestamp, new_timestamp, 1))

validator_path = Path('contrib/space-compute/pkg/admission/validator.go')
validator = validator_path.read_text()
current_marker = '''\tcurrent, err := v.decodeReporterEnvelope(request.Resource.Resource, request.Object.Raw)\n\tif err != nil {\n\t\treturn err\n\t}\n'''
update_first = current_marker + '''\tvar previous *reporterEnvelope\n\tif request.Operation == admissionv1.Update {\n\t\tprevious, err = v.decodeReporterEnvelope(request.Resource.Resource, request.OldObject.Raw)\n\t\tif err != nil {\n\t\t\treturn fmt.Errorf("decode previous reporter object: %w", err)\n\t\t}\n\t\t// Stable identity and the exact provenance chain are checked before\n\t\t// current binding/peer policy so an update can never masquerade as a\n\t\t// create in another domain or directed link.\n\t\tif err := validateImmutableAndChain(current, previous); err != nil {\n\t\t\treturn err\n\t\t}\n\t\tif err := validateUpdateStructural(current, previous, v.clock); err != nil {\n\t\t\treturn err\n\t\t}\n\t}\n'''
if validator.count(current_marker) != 1:
    raise SystemExit('validator current-object marker mismatch')
validator = validator.replace(current_marker, update_first, 1)
old_late_update = '''\tvar previous *reporterEnvelope\n\tif request.Operation == admissionv1.Update {\n\t\tprevious, err = v.decodeReporterEnvelope(request.Resource.Resource, request.OldObject.Raw)\n\t\tif err != nil {\n\t\t\treturn fmt.Errorf("decode previous reporter object: %w", err)\n\t\t}\n\t\tif err := validateUpdateStructural(current, previous, v.clock); err != nil {\n\t\t\treturn err\n\t\t}\n\t\tif err := validateImmutableAndChain(current, previous); err != nil {\n\t\t\treturn err\n\t\t}\n\t} else {\n\t\tif current.provenance.Sequence != 1 {\n\t\t\treturn fmt.Errorf("new reporter object sequence must be exactly 1")\n\t\t}\n\t\tif current.provenance.PreviousDigest != "" {\n\t\t\treturn fmt.Errorf("new reporter object previousDigest must be empty")\n\t\t}\n\t}\n'''
create_only = '''\tif request.Operation != admissionv1.Update {\n\t\tif current.provenance.Sequence != 1 {\n\t\t\treturn fmt.Errorf("new reporter object sequence must be exactly 1")\n\t\t}\n\t\tif current.provenance.PreviousDigest != "" {\n\t\t\treturn fmt.Errorf("new reporter object previousDigest must be empty")\n\t\t}\n\t}\n'''
if validator.count(old_late_update) != 1:
    raise SystemExit('validator late-update marker mismatch')
validator_path.write_text(validator.replace(old_late_update, create_only, 1))
