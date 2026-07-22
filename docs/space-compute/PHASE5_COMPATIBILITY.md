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

Server-side dry-run passed for all production scheduler, planner, CRD and
admission manifests. All CRDs store only `v1alpha1`; no conversion webhook is
needed for this single served/storage version. API additions must remain
backward compatible within v1alpha1. A future version must add conversion and
stored-version migration tests before changing storage.

The DRA coexistence unit test proves a ResourceClaim-only Pod is skipped by the
telemetry plugin and remains owned by upstream DynamicResources. Extended
resources remain owned by NodeResourcesFit/vendor device plugins; strict mode
does not claim physical identity without full exporter-to-allocation linkage.

## Installation, upgrade, rollback and cleanup

Install in this order: CRDs and Established wait; the four admission policies;
planner; independent scheduler. Verify every admission policy has
`observedGeneration == generation` and no expression warning before accepting
reports. Upgrade standalone images/config together, one replica at a time, and
keep ordinary scheduling independent.

Rollback uses the preceding standalone images/config, never an embedded
profile. Scale planner/scheduler Deployments to zero before rollback. Preserve
missions, placements, Events and audit logs before CRD deletion. Delete
admission bindings/policies, planner/scheduler resources, then CRDs; verify the
API group disappears. Restore from Kubernetes datastore backup plus external
audit/source snapshots, then reconcile reporters before unsuspending missions.

Compatibility gate is incomplete because full-agent and supported patch
upgrade/rollback were not run. Release status remains `Not ready`.
