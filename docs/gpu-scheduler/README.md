# K3s GPU Stability Scheduler Plugin

This repository builds a separate `space-compute-scheduler` executable from the
upstream Kubernetes 1.33 kube-scheduler command and registers the
`K3SGPUStability` framework plugin in that executable. Its configuration
contains exactly one profile, also named `space-compute-scheduler`. Normal Pods
remain owned by the independently running K3s `default-scheduler`; its leader
lease, queue, callbacks, and availability do not depend on accelerator
telemetry or this Deployment.

Build the version-pinned executable with:

```shell
go build -buildvcs=false -o bin/space-compute-scheduler ./cmd/space-compute-scheduler
docker build -f Dockerfile.space-compute-scheduler -t space-compute-scheduler:v1.33.7-k3s1 .
```

Build an image containing that executable, create the exporter TLS Secret named
`space-compute-exporter-tls`, review the resource/profile mappings, and apply
`manifests/space-compute-scheduler.yaml`. The manifest supplies dedicated RBAC,
a separately scoped leader Lease, two replicas, authenticated secure serving,
health/readiness probes, configuration, and a metrics Service. A static-Pod
alternative is intentionally not shipped: mounting the K3s administrator
kubeconfig into a host-networked static Pod violates the least-privilege
boundary. Use the dedicated ServiceAccount Deployment.

```shell
kubectl apply -f docs/gpu-scheduler/manifests/space-compute-scheduler.yaml
kubectl -n kube-system rollout status deploy/space-compute-scheduler
# Uninstall only this component; existing space Pods become Pending.
kubectl delete -f docs/gpu-scheduler/manifests/space-compute-scheduler.yaml
```

Pods select the opt-in profile with:

```yaml
spec:
  schedulerName: space-compute-scheduler
```

Do not add this profile to the embedded K3s scheduler configuration. Phase 5
removed the in-process registration hook, so the upstream K3s default scheduler
has no compile-time or runtime dependency on this plugin.

## Upgrade and migration

Older experimental deployments that configured an embedded profile must remove
that profile before installing this release; it is no longer a supported
rollback target. Migrate in this order:

1. Save the old scheduler configuration and confirm ordinary Pods use
   `default-scheduler`.
2. Remove only the `space-compute-scheduler` profile from the old K3s config and
   restart servers one at a time. Space Pods pause; ordinary scheduling does not.
3. Deploy the version-matched standalone binary and wait for `/readyz`, informer
   synchronization, and the `space-compute-scheduler` Lease holder.
4. Submit a canary with the explicit scheduler name, then move production work.

Rollback uses the preceding standalone image/config version; it does not
restore an embedded profile. Scale the Deployment to zero before rollback.
Uninstalling this component never edits the default scheduler. Upgrade the binary and configuration together with the
repository's Kubernetes `v1.33.7-k3s1` replacement; other framework versions
are unsupported.

## CPU-only cluster qualification

`scripts/space-compute cluster-e2e` runs the real second-scheduler qualification
against an operator-supplied, isolated Kubernetes 1.33 K3s API. It creates a
temporary namespace, a Ready fixture Node, and ordinary/space Pods; serves the
recorded Iluvatar `ix_*` Prometheus fixture from a Go `httptest.Server`; starts
two copies of the production scheduler binary; verifies default-scheduler
independence, exporter-backed binding, HTTPS health/readiness, Lease failover,
restart recovery, and extended-resource no-overcommit; and deletes every test
Node/namespace it creates. It does not create a device or orbit simulator and
does not require accelerator hardware.

Build all binaries from the same worktree, start a disposable K3s control
plane and provide its kubeconfig and both standalone component paths. The e2e
installs the production CRDs, split type-safe admission policies and controller
RBAC itself, waits for admission activation, and uses a real planner
ServiceAccount token:

```shell
go build -buildvcs=false -trimpath -o /tmp/k3s-space-e2e ./cmd/k3s
go build -buildvcs=false -trimpath -o /tmp/space-compute-scheduler-e2e ./cmd/space-compute-scheduler
go build -buildvcs=false -trimpath -o /tmp/space-compute-mission-planner-e2e ./cmd/space-compute-mission-planner

# Example for an agentless qualification host. Choose unused ports/CIDRs.
/tmp/k3s-space-e2e server --rootless --disable-agent \
  --node-ip=192.168.0.100 --https-listen-port=16443 \
  --data-dir=/tmp/k3s-space-e2e-data \
  --write-kubeconfig=/tmp/k3s-space-e2e.yaml \
  --kube-scheduler-arg=secure-port=11259 \
  --kube-controller-manager-arg=secure-port=11257 \
  --kube-controller-manager-arg=allocate-node-cidrs=false \
  --cluster-cidr=10.242.0.0/16 --service-cidr=10.243.0.0/16 \
  --cluster-dns=10.243.0.10 --flannel-backend=none \
  --egress-selector-mode=disabled --disable=coredns --disable=servicelb \
  --disable=traefik --disable=local-storage --disable=metrics-server \
  --disable-network-policy --disable-kube-proxy --disable-cloud-controller \
  --disable-helm-controller

SPACE_COMPUTE_E2E_KUBECONFIG=/tmp/k3s-space-e2e.yaml \
SPACE_COMPUTE_E2E_SCHEDULER_BINARY=/tmp/space-compute-scheduler-e2e \
SPACE_COMPUTE_PHASE4_E2E_PLANNER_BINARY=/tmp/space-compute-mission-planner-e2e \
scripts/space-compute cluster-e2e
```

The agentless mode validates API scheduling and Binding behavior but does not
claim image execution or device-plugin conformance. Run the same command
against a disposable full K3s cluster for kubelet/CRI qualification. Never point
the test at a production cluster: it intentionally creates and deletes the
fixed `space-compute-phase3-e2e` namespace and
`space-compute-fixture-node` Node.

Phase 4 adds versioned mission/placement annotations and four fixed scheduling
dimensions—predicted completion, data locality, link risk and resilience—to the
existing accelerator scores. Their global planning semantics, API units,
controller ownership and runbook are documented in
[`../space-compute/PHASE4_API_AND_OPERATIONS.md`](../space-compute/PHASE4_API_AND_OPERATIONS.md).

## Allocation boundary

The plugin is scheduling policy, not a device plugin, DRA driver, or vGPU
allocator. For enforceable capacity, a Pod must request mapped Kubernetes
extended resources (or, in a later phase, use ResourceClaims) and the cluster
must run the corresponding device plugin/driver. Kubernetes `NodeResourcesFit`
owns allocatable-versus-requested accounting.

`gpustability.k3s.io/enabled: "true"` without an extended-resource request is
observational best effort: it may influence scoring, but never hard-filters a
node or claims guaranteed capacity. Until DRA or a vendor result links telemetry
identity to the device selected for the Pod, strict thresholds use a
conservative node-wide rule: exporter telemetry must cover every device in
Kubernetes allocatable and every covered device must pass. A coverage mismatch
is a hard rejection in strict mode and a capped soft signal in degraded or
best-effort mode. The plugin deliberately does not implement Reserve/Unreserve
or maintain a second allocator.

## Collection and failure behavior

Exporter collection runs in bounded background workers. Node informer
add/update/delete events reconcile stable Node UID/name identities to endpoint
generations. Address, profile, port, path, scheme, or UID changes immediately
invalidate the old generation; a late old-endpoint response cannot overwrite the
replacement. `Filter` reads the local snapshot once per Node and pins a deep,
immutable copy into scheduler cycle state. `PreScore` and `Score` consume only
that Filter-pinned generation, freshness, profile, confidence, timestamps and
resource context; they never re-read the collector. A cache miss may enqueue a
non-blocking refresh for the already discovered generation. Global snapshots
persist allocatable capacity but never cycle-specific `NodeInfo.Requested` data.
Queue size, workers, cache entries, timeout, retry backoff, and circuit breaking
are bounded. The exporter observation time determines freshness; reading or
pinning a snapshot never extends it.

Background discovery scrapes only Nodes that advertise a configured positive
extended resource or explicit exporter metadata. Thus ordinary K3s agents and a
schedulable server/master add no exporter queue/cache load. Every accelerator
Node resolves its own watched address and endpoint generation; duplicate
endpoints claimed by different Node identities fail closed. `maxDevicesPerNode`
(default 256, maximum 4096), `cacheMaxEntries`, worker and queue limits must be
sized for the intended accelerator-node population. Adding more nodes requires
no scheduler restart and no hard-coded IP address.

The state policy is selected by `gpustability.k3s.io/state-policy` or the typed
default:

- `strict`: missing, failed, or stale telemetry rejects enforceable workloads.
- `degraded`: static type/profile compatibility remains mandatory, while an
  unavailable/dynamically ineligible snapshot is admitted with a capped score.
- `best-effort`: uses a separately configured low fallback score and never
  treats missing dimensions as perfect.

Telemetry is untrusted. Responses are size-limited, redirects are rejected,
scheme/port/path and Node-derived endpoints are validated, parser family/sample/
label work is bounded, non-finite and
out-of-range samples are rejected, and profile/device identity and memory
consistency are checked. Endpoint hosts come from watched Node `InternalIP`
addresses by default; the legacy arbitrary endpoint annotation is ignored.

`discovery.addressTypes` is an ordered allowlist (`InternalIP`, `ExternalIP`, or
`Hostname`). The first type with a valid address wins. Within that type,
`preferredIPFamily` (`ipv4`, `ipv6`, or `any`) selects the family; multiple
addresses in the selected family are rejected as ambiguous instead of being
chosen nondeterministically. If the preferred family is absent, exactly one
validated fallback address is accepted. IPv6 endpoints are bracketed correctly.
The deprecated `exporter.allowExternalIP` compatibility flag appends
`ExternalIP` after the typed order when it is not already present. Ports, paths,
schemes, and pinned profiles may be overridden by validated Node annotations,
but the host is always re-resolved from the current Node object.

Authenticated HTTPS with system or configured CA trust is the production
default. Optional client certificate/key settings enable mTLS. Plain HTTP is
available only when `allowInsecureHTTP: true` is explicit; startup logs a
compatibility warning because HTTP provides neither server authentication nor
transport confidentiality.

## Typed plugin arguments

Plugin arguments are strict, versioned `gpustability.k3s.io/v1alpha1`
`K3SGPUStabilityArgs`. Unknown fields, conflicting profile names, invalid ranges,
unsafe transport choices, and incomplete custom profile schemas fail scheduler
startup with an actionable error. See the example YAML for a complete practical
configuration.

Built-in profiles are `iluvatar` (`ix_*`), `dcgm` (NVIDIA), `rocm` (AMD), and
`generic` (`k3s_accelerator_*`, `k3s_gpu_*`, `k3s_npu_*`, or `k3s_fpga_*`).
Resource names map explicitly to a device class and compatible profiles; names
are never classified by substring.

Custom profiles use the versioned, size-limited strict JSON schema shown below.
Set `profileSource.file` to the file and add the new profile to an explicit
resource mapping. The file is polled at `profileSource.reloadInterval`; a whole
valid registry is installed atomically, while invalid/oversized/conflicting
updates retain the last-known-good registry and increment a bounded reload-error
metric. Built-in names cannot be overridden. Auto-detection rejects exporters
that match multiple profiles; heterogeneous nodes can pin the intended profile
with `gpustability.k3s.io/exporter-profile`. Each profile requires utilization,
total memory, temperature, and either free or used memory:

```json
{
  "apiVersion": "gpustability.k3s.io/v1alpha1",
  "kind": "MetricProfileList",
  "profiles": [
    {
      "name": "vendorx",
      "class": "fpga",
      "matchNames": ["vendorx_mem_total_bytes"],
      "identityLabels": ["serial"],
      "nameLabels": ["chip"],
      "requiredFields": ["gpu_utilization", "memory_total_mib", "temperature_celsius"],
      "fields": {
        "gpu_utilization": {"names": ["vendorx_busy"], "rollup": "avg", "min": 0, "max": 100},
        "memory_free_mib": {"names": ["vendorx_mem_free_bytes"], "unit": "bytes", "rollup": "max", "min": 0},
        "memory_total_mib": {"names": ["vendorx_mem_total_bytes"], "unit": "bytes", "rollup": "max", "min": 1},
        "temperature_celsius": {"names": ["vendorx_temp_c"], "rollup": "max", "min": -99, "max": 250}
      }
    }
  ]
}
```

The corresponding typed resource mapping is explicit; the scheduler policy
does not infer it from the resource or metric name:

```yaml
resources:
- name: example.com/fpga
  class: fpga
  profiles: [vendorx]
profileSource:
  file: /var/lib/rancher/k3s/server/gpustability-profiles.json
  reloadInterval: 30s
  maxBytes: 1048576
```

Typed args take precedence over the deprecated environment compatibility layer.
The retained variables are `K3S_GPU_EXPORTER_PORT`,
`K3S_GPU_EXPORTER_PATH`, `K3S_GPU_EXPORTER_SCHEME`,
`K3S_GPU_METRIC_PROFILE`, `K3S_GPU_METRIC_PROFILES_FILE`,
`K3S_GPU_EXPORTER_TIMEOUT`, `K3S_GPU_EXPORTER_CACHE_TTL`,
`K3S_GPU_EXPORTER_CACHE_CLEANUP_INTERVAL`,
`K3S_GPU_EXPORTER_CACHE_MAX_ENTRIES`, `K3S_GPU_RESOURCE_NAMES`,
`K3S_GPU_SCORE_ALL_PODS`, `K3S_GPU_ALLOW_INSECURE_HTTP`,
`K3S_GPU_MAX_TEMPERATURE_C`, `K3S_GPU_TARGET_TEMPERATURE_C`,
`K3S_GPU_MIN_SM_CLOCK_MHZ`, and `K3S_GPU_MIN_MEM_CLOCK_MHZ`.

Node annotations may override only the validated exporter port, path, scheme,
and profile. Pod annotations support enabled, state-policy,
min-eligible-devices, max-temperature-celsius, and min-free-memory-mib. The
minimum eligible annotation is rejected for mixed device classes because a
single value would be ambiguous.

The preferred Phase 3 workload interface is the strict JSON annotation
`gpustability.k3s.io/workload-intent`, with apiVersion
`gpustability.k3s.io/v1alpha1` and kind `SpaceComputeWorkloadIntent`. It supports
`statePolicy`, `minFreeMemoryMiB`, `maxTemperatureC`, `minEligibleDevices`,
`requiredProfiles`, `requiredNodeLabels`, and `preferredNodeLabels`. Required
labels express hard software/trust compatibility for workloads with enforceable
resource requests; for annotation-only observational workloads they are safely
downgraded to preference. Preferred labels add bounded data-locality scoring.
Unknown fields, invalid labels, duplicates, non-finite
values, and ambiguous mixed-class minimums reject the workload in PreFilter.
Resource names and quantities remain exclusively in Pod requests. Typed intent
values take precedence over retained legacy scalar annotations.

PreFilter stores immutable intent and upstream Kubernetes Pod accounting once.
Fixed SLO scoring reports utilization, memory, thermal, energy, compute-clock,
health/resilience, data-locality, and scarce-resource fragmentation dimensions.
Missing telemetry contributes zero for its dimension. Every weight must be in
`[0,100]` and all weights must total 100. Candidate-relative NormalizeScore is
not enabled because each dimension already has a fixed explainable scale.

The plugin exports bounded-cardinality scheduler metrics for discovered target
count, queue depth, active workers, collection and parse latency/failures,
backoff/circuit state, refresh suppression, stale-generation discards, profile
reload results, snapshot state/age, filter reasons, and score evaluation. No
metric label contains node, endpoint, device, or secret identity.

Snapshot publication uses Kubernetes 1.33's local `PodActivator` to wake only
Pods recorded as blocked on that Node. Node allocatable, required-label, and
exporter-metadata changes also use `EnqueueExtensions` queue hints. Dependency
tracking is bounded by count and TTL; unrelated changes do not create a hot
loop, and ordinary Pods are skipped.

## Failure boundaries and operations

| Failure | Impact | Operator signal |
| --- | --- | --- |
| standalone scheduler down or leaderless | only explicitly selected space Pods remain Pending | readiness, Lease age, scheduling Events |
| exporter slow or down | strict space Pods remain Pending; degraded policies use capped scores | collection, backoff/circuit, snapshot-age metrics |
| invalid profile reload | last valid registry stays active | reload error gauge and counter |
| bounded scrape queue full | refresh is suppressed; callbacks remain non-blocking | queue depth and suppression reason |
| space component uninstalled | space Pods remain Pending; ordinary K3s scheduling is unchanged | absent Deployment/Lease |

A minimal dashboard should show target count, workers, queue depth, p95 scrape
and parse latency, bounded failure reasons, backoff/open-circuit targets,
snapshot age versus TTL, blocked Pods and activations, filter reasons, score
distribution, readiness, and leader Lease age. Alert on readiness loss for two
retry periods, no Lease renewal within its duration, sustained queue depth over
80%, rising strict missing/stale decisions, or snapshot age nearing TTL.
Framework-generated FailedScheduling Events carry the deterministic rejection
text. The plugin does not write Events from callbacks because API writes would
violate the hot-path I/O boundary.
