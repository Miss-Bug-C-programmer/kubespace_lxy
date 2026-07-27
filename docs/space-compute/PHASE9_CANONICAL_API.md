# Phase 9 canonical API upgrade

Phase 9 introduces `spacecompute.k3s.io/v1beta1` as the canonical storage API while keeping `v1alpha1` served. The upgrade is additive and remains outside the embedded K3s scheduler. The standalone scheduler continues to consume Pod/Node projections and exporter snapshots exactly as before.

## Version and conversion contract

Every Phase 9 CRD in `manifests/phase9-canonical-crds.yaml` serves both `v1alpha1` and `v1beta1`. `v1beta1` is the storage version. The conversion webhook performs a lossless structural conversion for schema-admitted data: it validates the source/target group-version and kind, changes only `apiVersion`, and preserves the complete raw JSON object it receives, including fields unknown to the current typed Go client. Arbitrary fields that are unknown to the structural CRD schema are not an extension mechanism and remain subject to normal Kubernetes pruning/validation; Phase 9 preserves new canonical fields by declaring the same fields in both served version schemas.

Phase 9 deliberately keeps all canonical fields representable in the served `v1alpha1` compatibility view. This makes `v1beta1 -> v1alpha1 -> v1beta1` lossless and provides a real rollback path rather than encoding beta-only state into annotations. Reporter canonical signing remains version-conversion stable: existing pre-Phase-9 objects retain the v1 canonical payload, while resource summaries that use Phase-9 fields and PhysicalDeviceInventory use the v2 canonical payload. Conversion itself never changes a signed material field.

## Canonical physical inventory

`PhysicalDeviceInventory` is cluster-scoped and reporter-owned. An inventory is bound to one full domain identity and one Node name and contains bounded physical-device records. Each device carries a stable ID, Kubernetes extended-resource name, generic/DRA/vendor allocation identifiers, class/vendor/model/architecture, NUMA/socket/PCIe topology, peer interconnects, total/free memory, memory/interconnect bandwidth, precision, firmware/driver/runtime/libraries, health, temperature, power, and confidence. Inventory-level provenance, observation/expiry and confidence are signed and validated.

`SpaceDomainResourceSummary` adds canonical CPU capacity/availability, system memory, ephemeral and persistent storage, NUMA resources, trust/attestation evidence, autonomy duration, energy source/budget and an exact physical-inventory reference (name, digest and optional resourceVersion). The summary remains the bounded planner aggregate; the inventory is the auditable physical truth record rather than a second allocator.

## Mission hard constraints and state policies

`SpaceMission` adds working-memory bytes, working-storage bytes, minimum link bandwidth, maximum RTT and maximum loss PPM. A zero value means the corresponding optional constraint is not declared; positive values are bounded at admission.

These five fields are hard constraints in every state policy:

- `strict`: the declared memory/storage/link constraints are non-relaxable, and configured energy/thermal minimum failures also reject a candidate.
- `degraded`: the declared hard constraints are still non-relaxable. Only soft energy/thermal degradation can remain eligible with a score penalty.
- `best-effort`: the declared hard constraints are still non-relaxable. Missing/degraded soft telemetry may receive the existing best-effort treatment but can never satisfy a declared memory/storage/bandwidth/RTT/loss requirement by assumption.

For remote input or result transfer, the selected contact window must satisfy timing plus minimum bandwidth, maximum RTT and maximum loss simultaneously. Local data paths do not require a network contact window.

## Placement audit state

`SpacePlacementIntent.spec` records the selected alternative capability-set name, the complete selected capability requirements, and deterministic physical-device constraints derived from the planner capacity allocation. It does not claim an exact physical device unless an upstream DRA/vendor allocation identity exists; Kubernetes device plugins/DRA remain the allocator.

Placement status records transfer state, transfer receipt references, execution lease reference, fencing token hash, checkpoint receipt, result receipt and the highest trusted remote acknowledgement sequence. Status conflict reconciliation merges these fields monotonically so a stale controller write cannot erase newer fencing/receipt evidence.

## Upgrade and stored-version migration

Use this order. Do not skip the readiness or storage checks.

1. Build and deploy `space-compute-conversion-webhook`; provision `space-compute-conversion-webhook-tls` for `space-compute-conversion-webhook.kube-system.svc`.
2. Inject the issuing CA into every `spec.conversion.webhook.clientConfig.caBundle` in `phase9-canonical-crds.yaml`.
3. Wait for both conversion-webhook replicas to be Ready.
4. Apply `phase9-canonical-crds.yaml`. Confirm every CRD serves alpha and beta and reports `v1beta1` as `storage: true`.
5. Read representative existing objects through both API versions and verify material fields/digests are unchanged.
6. Roll out the Phase-9 planner, dispatcher, reporter webhook and domain-agent images. These writers use the canonical `v1beta1` GVR directly; the conversion webhook remains a compatibility path for `v1alpha1` clients rather than the normal reconciliation hot path.
7. Apply `storage-version-migrator.yaml`, review the target, then unsuspend `Job/space-compute-storage-migrate-v1beta1`.
8. The migrator rewrites every object through the target API version with conflict retry and then updates CRD `status.storedVersions`. Verify every managed CRD reports exactly `[v1beta1]` before considering storage migration complete.
9. Keep the conversion webhook running while `v1alpha1` remains served.

The migrator ServiceAccount receives a narrow admission exception only for a semantic no-op UPDATE: kind/name/namespace/UID, labels, annotations, finalizers, ownerReferences, spec and status must match the old object. Server-owned resourceVersion/managedFields may advance, but migration credentials cannot change Mission, reporter or placement business or authorization metadata.

## Rollback

Rollback must occur before removing beta or the conversion webhook:

1. Stop/hold Phase-9 writers and preserve an API/datastore backup.
2. Run the same migrator image with `--target-version=v1alpha1` while both versions remain served and conversion is healthy.
3. Verify every relevant CRD has `storage: true` on alpha and `status.storedVersions: [v1alpha1]`.
4. Verify representative alpha reads, signed reporter canonical digests and Mission/Placement state.
5. Only then roll back standalone component images/manifests and, where required, the historical Phase-4/5 CRD manifests.
6. Remove the conversion webhook last. `PhysicalDeviceInventory` objects must be exported/deleted before removing its Phase-9-only CRD.

Do not change `storage: true` in YAML and assume old etcd objects migrated. The object rewrite plus `storedVersions` verification is mandatory.

## Qualification boundary

Phase 9 closes the typed canonical API and deterministic CPU-only conversion/planning gaps. It does not claim physical accelerator allocation identity, telemetry accuracy, full-agent K3s upgrade qualification or vendor hardware qualification. Those require the existing hardware/full-cluster gates.
