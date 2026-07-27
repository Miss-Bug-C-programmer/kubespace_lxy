import os
from pathlib import Path

p = Path("docs/space-compute/IMPLEMENTATION_STATUS.md")
text = p.read_text()
marker = "## Phase 9 canonical API upgrade\n"
if marker in text:
    raise SystemExit("Phase 9 status section already exists")
section = f"""
## Phase 9 canonical API upgrade

- Executed on 2026-07-27 by GitHub Actions run `{os.environ['GITHUB_RUN_ID']}` from validation baseline `{os.environ['GITHUB_SHA']}` before non-force fast-forward publication.
- Added `spacecompute.k3s.io/v1beta1` as canonical/storage while retaining `v1alpha1` served, with webhook conversion and identical canonical schemas for lossless downgrade/rollback views. Conversion preserves raw schema-admitted JSON and exact fixed-width integers.
- Added signed `PhysicalDeviceInventory`, expanded domain CPU/memory/storage/NUMA/trust/attestation/autonomy/energy/inventory state, and added Mission working-memory/storage/bandwidth/RTT/loss hard constraints. Present hard-constraint violations reject in strict/degraded/best-effort; the modes differ only for missing trusted state.
- Placement now records selected capability/physical-device constraints plus transfer receipts/state, execution lease, fencing hash, checkpoint/result receipts and remote acknowledgement sequence.
- Added deployable conversion-webhook and default-suspended stored-version migrator. Forward migration rewrites objects through `v1beta1` before narrowing `storedVersions`; rollback rewrites through `v1alpha1` before beta removal. Migrator admission permits semantic no-op rewrites only.
- Successful gates: focused unit/race, unchanged `scripts/space-compute all`, `go mod verify`, `go mod tidy -diff`, YAML/schema/isolation checks and `go test ./pkg/executor/embed -count=1`. Tests cover alpha->beta, beta->alpha, round trip, schema-admitted unknown/new data and large integers, stored-version migration, rollback and hard-field preservation.
- Default K3s scheduler source/profile/manifest stayed outside the Phase 9 production diff. Full-agent K3s patch-upgrade and physical-hardware qualification remain separate release gates.

"""
i = text.find("## Verified plugin catalogue")
if i < 0:
    raise SystemExit("status insertion marker missing")
p.write_text(text[:i] + section + text[i:])
