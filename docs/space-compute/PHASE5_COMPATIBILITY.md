# Phase 5 K3s compatibility and migration report

## Fork surface

The verified upstream source tag `v1.33.7+k3s1` was cloned read-only under
`/tmp`. After Phase 5, `pkg/executor/embed/embed.go` has no content difference
from upstream; only the pre-existing local executable mode differs. Space
components are additive commands/packages/manifests. Existing unrelated
Makefile/module/build/package-script differences were preserved and not reset.

The current-tree K3s binary is byte-identical to the previously verified K3s
binary (SHA-256 `484a962e3207132161161c8c089c9d34347aaaa9b755168146e83e1af240e05b`).
The default scheduler neither imports nor activates the space plugin.

## Lifecycle matrix

| State | Evidence | Status |
| --- | --- | --- |
| Feature absent | Fresh K3s started without CRDs/components; ordinary/default scheduler and startup passed | PASS |
| Installed, components disabled/absent | Production CRDs/admission/RBAC installed before external processes; ordinary scheduling remained independent | PASS |
| Enabled | Production planner SA, controller, exporter snapshot and standalone scheduler completed the real Binding flow | PASS (agentless) |
| Restart | Same datastore and objects survived K3s restart; repeat e2e and duplicate apply passed | PASS |
| Uninstalled | Admission, RBAC and CRDs deleted; API disappeared; ordinary/independent scheduler test passed 15.37s | PASS |
| Full K3s agent/kubelet/CNI/CRI | No privileged full-agent environment | NOT RUN |
| K3s patch upgrade/rollback | No prior supported patch binary was qualified | NOT RUN |

Server-side dry-run passed for the Phase-5 production scheduler, planner, CRD and admission manifests. That historical baseline stored only `v1alpha1`. Phase 9 now adds the previously required dual-served `v1alpha1`/`v1beta1` conversion webhook and explicit stored-version migrator before changing canonical storage to beta. The Phase-4/5 CRD files are retained as historical rollback baselines; `phase9-canonical-crds.yaml` is the current storage schema. See `PHASE9_CANONICAL_API.md` for forward migration and rollback ordering.

The DRA coexistence unit test proves a ResourceClaim-only Pod is skipped by the
telemetry plugin and remains owned by upstream DynamicResources. Extended
resources remain owned by NodeResourcesFit/vendor device plugins; strict mode
does not claim physical identity without full exporter-to-allocation linkage.

## Installation, upgrade, rollback and cleanup

For the Phase-5 historical baseline, install CRDs and wait for Established, then admission, planner and the independent scheduler. For Phase 9, deploy and trust the conversion webhook first, apply `phase9-canonical-crds.yaml`, verify both served versions, then roll out the Phase-9 controller/reporter/domain-agent images that use canonical `v1beta1` GVRs. Only after representative alpha/beta reads pass should the suspended storage-migrator Job be enabled. Verify every admission policy has `observedGeneration == generation`, no expression warning, and every migrated CRD has `status.storedVersions: [v1beta1]` before accepting the storage upgrade as complete. Ordinary/default scheduling stays independent throughout.

Rollback uses the preceding standalone images/config, never an embedded
profile. Scale planner/scheduler Deployments to zero before rollback. Preserve
missions, placements, Events and audit logs before CRD deletion. Delete
admission bindings/policies, planner/scheduler resources, then CRDs; verify the
API group disappears. Restore from Kubernetes datastore backup plus external
audit/source snapshots, then reconcile reporters before unsuspending missions.

Compatibility gate is incomplete because full-agent and supported patch
upgrade/rollback were not run. Release status remains `Not ready`.

## Stage 4 reporter-authenticity compatibility note

The v1alpha1 API adds three cluster-scoped kinds: `SpaceDomainReporterBinding`, `SpaceTransferReceipt` and `SpaceResultReceipt`. `Provenance` adds optional `previousDigest` and `signature` fields so stored pre-hardening objects remain decodable. Once the reporter webhook is enabled, CREATE requires sequence 1 with an empty previous digest and a valid signature; UPDATE requires an exact chain. Unsigned legacy reporter objects therefore cannot be mutated through the hardened reporter path and should be deleted/recreated by the owning reporter after a binding/key is provisioned.

The new webhook is an optional space-compute component and does not register anything into the stock K3s scheduler. The standalone mission planner and scheduler process contracts are unchanged.
