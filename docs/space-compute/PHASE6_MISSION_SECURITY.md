# Phase 6 Mission-to-Pod Authorization Boundary

Status: implemented; verification evidence is recorded in `IMPLEMENTATION_STATUS.md` by the Stage 6 gate.

## Threat model

A `SpaceMission` contains a user-supplied `PodTemplateSpec`, while the space-compute control plane holds broader Kubernetes credentials than a normal Mission submitter. Treating that template as trusted would create a confused-deputy path: a user who can create a Mission could indirectly ask the controller to create a Pod that the user could not create directly.

Phase 6 makes the original AdmissionRequest identity, not the planner ServiceAccount, the authorization boundary. The admission webhook fails closed and performs `SubjectAccessReview` for Pod creation, ServiceAccount use, RuntimeClass use and every referenced PVC, Secret and ConfigMap. Updates rerun these checks whenever the Pod template materially changes.

`use` on `serviceaccounts` and `runtimeclasses` is an intentional admission-only RBAC verb. Mission submitters must be granted it explicitly for the exact names that policy permits; granting `get` alone is not treated as permission to execute under that identity/runtime.

## Default-deny template policy

The administrator-owned `MissionSecurityPolicy` JSON is mounted read-only into the webhook. Empty allowlists deny the feature. Unless explicitly enabled, Mission templates cannot use host namespaces, privileged or privilege-escalating containers, hostPath/hostPort, added Linux capabilities, arbitrary RuntimeClasses or ServiceAccounts, service-account token automount/projection, controller-reserved metadata, owner references/finalizers, preselected Nodes, nodeSelector/nodeAffinity/tolerations, unapproved image registries, or containers without positive CPU/memory requests and limits.

The shipped example deliberately permits only the `default` ServiceAccount, `registry.k8s.io`, and ordinary `app.kubernetes.io/*` labels. Operators must replace those values with their own explicit policy before accepting user Missions.

## Attempt Pod construction

`BuildAttemptPodWithLease` no longer copies `ObjectMeta` wholesale. It constructs a fresh Pod, copies only the safe application-label allowlist, drops user annotations and server-owned metadata, installs the Mission controller owner reference, forces `space-compute-scheduler`, clears direct Node-placement fields, disables ServiceAccount token automount, enforces Restricted-style container security (non-root, RuntimeDefault seccomp, no privilege escalation, no privileged mode, drop ALL capabilities), rejects non-Restricted volume types, and creates canonical SHA-256 Mission/Placement digest annotations.

The Mission webhook also validates controlled attempt Pod CREATE/UPDATE. Its Pod rule uses admission `matchConditions`: ordinary Pod requests from unrelated identities never call the webhook, while approved dispatcher identities and Pods carrying space-compute attempt labels do. Therefore an outage fails closed for the controlled path without coupling ordinary/default-scheduler Pods to this webhook. Approved dispatcher identities are also denied if they try to create an unlabeled/uncontrolled Pod. Controlled Pod spec, labels, annotations, digests, scheduler identity, finalizers and owner reference are immutable after creation.

## Controller identity split

The mission-planner image runs four mutually exclusive controller roles under separate ServiceAccounts:

- `planner`: reads Mission/link/resource snapshots and writes placement/status only. It has no Pod create/delete and no Node patch/update permission.
- `workload-dispatcher`: reads planning/evidence state. Pod create/delete and placement/status writes are granted only through a namespace-scoped RoleBinding to `system:space-compute-workload-dispatcher-namespace`; no global Pod-write binding is shipped.
- `node-projector`: reads validated summaries and Nodes and has Node `patch`, never `update`. It applies only `spacecompute.k3s.io/*` metadata via Server-Side Apply with field manager `space-compute-node-projector` and never submits a full Node object.
- `transport-agent`: reads Mission/Placement intent and is the only planner-side role that mutates transfer/lease/receipt APIs. It has no Pod, Node, or Secret permission. It materializes input transfer intents from immutable placement state; the dispatcher only consumes receipts/evidence. The separately deployed cross-domain execution agent remains a distinct Phase 5 execution boundary and is not the planner identity.

Each active controller role uses a distinct leader Lease. The old all-powerful planner deployment is not retained as a compatibility mode.

The node projector uses Kubernetes Server-Side Apply with field manager `space-compute-node-projector`. The apply object contains only `metadata.name` plus `spacecompute.k3s.io/*` labels and annotations; it never submits a full Node and has no `update` verb on Nodes.

## Authorized workload namespaces

The dispatcher has cluster-wide read-only informers so it can observe Mission state, but its write ClusterRole is intentionally not ClusterRoleBound. An administrator authorizes a namespace by creating a RoleBinding in that namespace to `system:space-compute-workload-dispatcher-namespace` for ServiceAccount `kube-system/space-compute-workload-dispatcher`. This scopes Pod create/delete and placement/status writes to the authorized namespace at the API server.

## Compatibility

No code under `cmd/space-compute-scheduler`, the default K3s scheduler, or `pkg/scheduler/plugins/gpustability` production implementation is changed by Phase 6. Space-compute attempt Pods still opt in explicitly with `schedulerName: space-compute-scheduler`.
