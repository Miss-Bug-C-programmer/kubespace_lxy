#!/usr/bin/env python3
from pathlib import Path

source = Path('.github/stage4-recovered.py').read_text()
marker = 'append_once("docs/space-compute/PHASE4_API_AND_OPERATIONS.md", "Trusted reporter-domain admission"'
if marker not in source:
    raise SystemExit('recovered patch tail marker is missing')
head = source.split(marker, 1)[0]
doc = """## Trusted reporter-domain admission

Reporter-owned cross-domain objects are admitted through the fail-closed `space-compute-reporter-webhook`. Administrators create one cluster-scoped `SpaceDomainReporterBinding` whose deterministic name is derived from the authenticated principal. The binding fixes the reporter domain, allowed kinds, explicit peers and one Ed25519 public-key reference.

Reporter object names are derived from normalized domain or directed-link identity. Canonical provenance uses `spacecompute-canonical-v1`, fixed field order, UTC RFC3339Nano timestamps, explicitly sorted map/set-like values, and excludes only digest and signature. Reporters submit lowercase SHA-256 plus a base64 Ed25519 signature. CREATE requires sequence 1 and empty previous digest; UPDATE requires exact sequence increment, exact previous digest, increasing timestamp and immutable reporter/domain/source/destination/stable identity.

Provision the webhook TLS Secret and CA bundle before granting reporter writes. Reporter roles receive CREATE plus exact `resourceNames` for GET/UPDATE/PATCH; no shared reporter role receives unrestricted mutation of all cluster-scoped summaries or snapshots."""
tail = (
    '\nappend_once("docs/space-compute/PHASE4_API_AND_OPERATIONS.md", '
    '"Trusted reporter-domain admission", ' + repr(doc) + ')\n\n'
    'for path, content in NEW_FILES.items():\n'
    '    write(path, content)\n'
)
completed = compile(head + tail, '<stage4-recovered-complete>', 'exec')
exec(completed, {'__name__': '__main__'})
