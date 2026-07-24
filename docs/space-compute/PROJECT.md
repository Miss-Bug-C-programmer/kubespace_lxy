# Heterogeneous Space Compute Management and Scheduling Project

Status: architecture contract  
Applies to: K3s `v1.33.7+k3s1` workspace and subsequent compatible upgrades  
Primary language: Go

## 1. Mission

Build a production-grade, opt-in heterogeneous compute management and scheduling
stack for high-orbit, low-orbit, and ground/edge environments while preserving
normal K3s behavior.

The system shall:

- discover and normalize physical CPU, GPU, NPU, FPGA, DSP, AI accelerator,
  memory, storage, software-stack, energy, thermal, and link capabilities;
- maintain a unified, time-aware resource inventory with provenance, freshness,
  confidence, and failure state;
- retain node-local Prometheus exporters as the primary dynamic telemetry source,
  including TianShu ZhiXin/Iluvatar `ix_*` utilization, memory, temperature,
  clock and power metrics;
- make deterministic scheduling decisions from workload requirements, physical
  capacity, device compatibility, data locality, contact windows, link quality,
  energy/thermal margins, deadlines, and resource-fragmentation cost;
- keep global cross-orbit planning separate from local Kubernetes node placement;
- integrate with Kubernetes extended resources, Dynamic Resource Allocation
  (DRA), or vendor device plugins for physical-device accounting and assignment;
- remain distinct from vGPU/device-slicing products such as HAMi. Virtual device
  partitioning, memory/core isolation, and runtime injection are outside this
  project's core scope.

## 2. Product boundary

### In scope

- Hardware and software capability discovery and normalization.
- Unified resource inventory and validated dynamic snapshots.
- Scheduling policy, feasibility checks, scoring, explanation, and observability.
- Independent `space-compute-scheduler` operation alongside K3s's
  `default-scheduler`.
- Physical-device requests and claims through existing Kubernetes mechanisms.
- Link/contact-window, deadline, energy, thermal, trust, and data-locality-aware
  placement.
- Disconnected and degraded-mode behavior with explicit state and policy.
- Exporter text fixtures and HTTP-level integration tests that do not require
  physical accelerator hardware.

### Out of scope unless separately approved

- vGPU or NPU slicing and overcommit.
- CUDA/CANN/ROCm runtime injection into application containers.
- Replacing kubelet, containerd, device plugins, or DRA drivers.
- Stretching one etcd quorum across unreliable high/low-orbit links.
- Hiding missing telemetry by fabricating healthy capacity.
- A second, simplified scheduling implementation used only in tests.
- A standalone resource/device/orbit simulator product. Tests should replay
  exporter responses and Kubernetes objects, not create an alternative runtime.

## 3. Architectural principles

### 3.1 Isolation from core K3s behavior

Normal Pods continue to use `default-scheduler`. Space-compute workloads opt in
with `spec.schedulerName: space-compute-scheduler` or a deliberate admission
policy. Failure, overload, or absence of the space-compute stack must not prevent
ordinary K3s workloads from being scheduled.

Any temporary embedded-plugin compatibility path must remain disabled by
default, documented, and covered by regression tests. The long-term target is a
separately built and deployed scheduler component with Kubernetes-version
compatibility pinned to the K3s release.

### 3.2 Separation of control loops

The architecture has these cooperating layers:

1. **Exporter target discovery** — watches Kubernetes Nodes and derives targets
   from current `InternalIP`/approved address types plus validated exporter
   port/path/scheme/profile metadata. No node IP is compiled into code or stored
   as an immutable endpoint.
2. **Asynchronous scrape and normalization manager** — uses bounded workers,
   jitter, per-target timeouts, backoff and profile adapters to scrape TianShu
   ZhiXin/Iluvatar and other exporters outside the scheduling cycle.
3. **Unified snapshot store** — combines Kubernetes Node allocatable resources
   with normalized exporter telemetry and retains observation time, expiry, source
   profile and failure state. Per-cycle `NodeInfo` requested resources remain
   scheduler cycle-local and are never persisted into the global snapshot store.
4. **Space compute scheduler** — performs in-cluster Pod-to-Node feasibility and
   ranking using only local informer/cache snapshots.
5. **Optional global mission planner** — later chooses cluster/orbit domain and
   execution epoch for future contact-window workflows. It does not replace the
   exporter collection path or the local scheduler.

### 3.3 No network I/O in the scheduler hot path

Scheduler framework callbacks must not scrape Prometheus exporters, query remote
APIs, or wait for satellite links. They read immutable or concurrency-safe local
snapshots populated by the asynchronous exporter scrape manager.

High-frequency raw telemetry must not be written directly to the Kubernetes API
at unbounded frequency. The scrape manager retains bounded local snapshots with
`observedAt`, `validUntil`, provenance, and confidence. Static capabilities and
dynamic conditions have separate refresh policies.

### 3.4 Exporter extensibility and discovery

TianShu ZhiXin/Iluvatar support is retained as a built-in profile. DCGM, ROCm,
Ascend/NPU and future exporters plug into the same normalized model.

The normal extension path is declarative and versioned: metric names, match
rules, units, rollups, required fields, device identity labels, resource class
mapping and range validation. Scheduling Filter/Score code must never add
vendor-specific branches. When declarative mapping is insufficient, a small
adapter implementing the common parser interface may be registered without
changing scheduling policy.

Target discovery watches Node add/update/delete events. Address/profile changes
invalidate the old target and snapshot safely. Per-node metadata may override
port, path, scheme and profile after validation, but must not provide an
arbitrary control-plane URL. Large clusters require bounded worker pools,
refresh jitter, coalescing/singleflight, backoff, circuit breaking, cache limits,
and scrape/parse observability.

### 3.5 Hard constraints versus soft signals

Hard feasibility checks may include:

- resource type and quantity;
- physical-device compatibility and assignability;
- architecture, model, precision, driver/runtime and software-stack constraints;
- required memory/storage capacity;
- trust/security level;
- snapshot freshness required by a strict workload;
- contact-window feasibility for transfer, execution, and return before deadline.

Soft scoring signals may include:

- utilization and queue estimate;
- thermal and energy headroom;
- predicted completion time;
- bandwidth, delay, loss, and link-risk margin;
- data locality and transfer cost;
- capability slack and scarce-resource fragmentation;
- resilience and checkpoint/migration cost.

Dynamic per-device signals must not be advertised as enforceable hard guarantees
unless the final allocation mechanism can select or constrain the same physical
device. Without that link, dynamic device health is a Node-level soft signal or
the feasibility rule must conservatively require all allocatable candidates to
meet it.

### 3.6 Explicit degraded behavior

Each policy/workload class selects one of these documented modes:

- **strict** — stale, incomplete, untrusted, or incompatible required state is
  unschedulable with an actionable reason;
- **degraded** — fall back to trusted static capacity while applying a visible
  score penalty and condition/event;
- **best-effort** — use available signals but never fabricate missing data.

No mode may silently treat missing metrics as perfect health.

## 4. Canonical resource model

The implementation may initially use versioned CRDs and later map appropriate
parts to DRA. API types must be versioned and conversion-friendly.

### 4.1 Device inventory

Each physical device record needs at least:

- stable device ID and node identity;
- class: CPU/GPU/NPU/FPGA/DSP/other accelerator;
- vendor, model, architecture, topology/NUMA or interconnect information;
- capacity and assignable quantity;
- memory and relevant bandwidth/throughput capabilities;
- supported precision/instruction features;
- driver, firmware, runtime and library compatibility;
- health state, provenance, `observedAt`, `validUntil`, and confidence.

Resource names must map explicitly to device classes. Substring heuristics such
as checking whether a resource name contains `gpu` are not acceptable in the
production path.

### 4.2 Node/platform profile

The node/platform model includes:

- satellite/ground identity, orbit class and orbit plane/domain;
- CPU architecture, memory, storage, device topology and software stack;
- trust/security domain;
- energy state, power budget, thermal state and operating constraints;
- current autonomy/connectivity condition and last validated report.

### 4.3 Link/contact snapshot

The link model includes source, destination/domain, bandwidth, RTT/latency,
loss/error rate, stability/confidence, contact-window start/end, observation
time, expiry, and provenance.

### 4.4 Workload profile

Workload intent includes:

- required and alternative resource classes and quantities;
- model/architecture/precision/software-stack constraints;
- working memory/storage and input/output sizes;
- data location and locality requirements;
- deadline, expected or bounded duration, priority and mission class;
- minimum bandwidth/maximum latency where applicable;
- checkpointability, migration/retry constraints and result-return requirement;
- strict/degraded/best-effort state policy.

Annotations may remain as a compatibility input, but production policy must have
a validated, typed representation. Invalid values are rejected rather than
silently replaced with permissive defaults.

## 5. Scheduling decision model

The scheduler first applies hard constraints, then ranks feasible nodes. A target
cost model is:

```text
predicted total cost =
    queue wait
  + input transfer time
  + compute time
  + result transfer time
  + contact/link risk penalty
  + energy/thermal risk penalty
  + scarce-capability fragmentation cost
  - data-locality/resilience benefits
```

For deadline-bound work, feasibility must account for input transfer, execution,
result transfer, contact-window boundaries, and a configurable safety margin.

Scores must be deterministic for a fixed Pod and snapshot. Every decision shall
be explainable through structured reasons and per-dimension scores. Missing
dimensions must use the configured degraded-state rule, not a hidden perfect
score.

## 6. Required failure and security behavior

- Treat exporters, Nodes, CRDs, annotations, and metrics as untrusted.
- Authenticate reporters and apply least-privilege RBAC.
- Validate message/body size, schema, units, ranges, timestamps, monotonicity
  where relevant, and node/device identity.
- Reject NaN, positive/negative infinity, invalid negative capacity, impossible
  percentages, and inconsistent totals.
- Bound concurrency, memory, cache entries, queue sizes, retry rate and logging.
- Use backoff and circuit breakers in asynchronous collectors.
- Never follow arbitrary redirects or node-controlled URLs from control-plane
  components.
- State expiry must be explicit. Old reports cannot become fresh merely because
  a cache was read recently.
- Leader election, restart recovery and duplicate reports must be tested.

## 7. Testing strategy without physical accelerators

The absence of real GPU/NPU/FPGA hardware must not reduce functional scope.

### 7.1 Mandatory hardware-independent tests

- Pure unit tests for parsing, normalization, demand calculation, compatibility,
  feasibility, scoring and stale/degraded modes.
- Golden Prometheus text fixtures for representative NVIDIA/DCGM, AMD/ROCm,
  TianShu ZhiXin/Iluvatar, Ascend, generic NPU, FPGA and malformed/untrusted
  exporter responses.
- Go `httptest.Server` endpoints that return those fixtures to the production HTTP
  client, parser, profile registry, asynchronous collector and snapshot store.
- Scheduler-framework tests with realistic `NodeInfo`, Pods, init containers,
  restartable init containers, extended resources and ResourceClaims where used.
- Informer/controller tests using Kubernetes fake clients and, where API watch or
  status behavior matters, envtest or an ephemeral API server.
- End-to-end tests on CPU-only K3s-compatible environments using ordinary Node
  objects plus exporter HTTP fixtures.
- Race, fuzz, malformed-input, timeout, cancellation and cache-stampede tests.
- Scale and chaos tests for hundreds/thousands of nodes, stale reports, packet
  delay/loss, partitions, controller restarts and leader changes.
- Regression tests proving that ordinary `default-scheduler` Pods are unaffected.

HTTP test servers may replace the physical exporter process only. They must not
bypass the production target resolver, HTTP client, profile parser, snapshot
store, policy or scheduler logic. There must be no `if testing` production path
that grants resources or returns success.

### 7.2 Optional but required-before-hardware-release tests

- Vendor hardware discovery and health-report conformance.
- Physical-device allocation identity consistency.
- Driver/runtime compatibility and real workload execution.
- Thermal/power signal accuracy and failure injection.

These tests are allowed to skip only when the required hardware is absent, must
report an explicit skip reason, and must not be counted as core functional test
success.

## 8. Global quality gates

Every completed implementation phase must satisfy all applicable gates:

1. `go test` passes for changed and dependent packages.
2. `go test -race` passes for new concurrent components and scheduler/resource
   packages on supported platforms.
3. Formatting, vet/static analysis and generated-code verification pass where
   configured by the repository.
4. New external inputs have malformed, oversized, timeout and cancellation tests.
5. No test is deleted, skipped, weakened, or rewritten to accommodate a defect
   without an explicitly documented product decision.
6. No production feature is replaced by a constant decision, fixture-only
   implementation, or test-only bypass. Exporter fixtures remain test inputs,
   not an alternate implementation.
7. Plugin-disabled/default-scheduler behavior remains compatible with upstream
   K3s.
8. APIs/configuration have validation, versioning and upgrade/migration notes.
9. Metrics, events/logs and actionable scheduling reasons exist for failure paths.
10. `IMPLEMENTATION_STATUS.md` records exact commands and results.

## 9. Target repository/component layout

Prefer a separately buildable module/component boundary to minimize K3s fork
surface, for example:

```text
contrib/space-compute/
  cmd/space-scheduler/
  cmd/mission-planner/
  pkg/apis/
  pkg/exporters/
  pkg/targets/
  pkg/scrape/
  pkg/inventory/
  pkg/snapshots/
  pkg/policy/
  pkg/scheduler/
  pkg/planner/
  test/fixtures/
  test/integration/
  test/e2e/
  manifests/
```

Exact placement may change through an ADR, but the K3s core must not accumulate
vendor collectors, mission-planning logic, or high-frequency telemetry state.

## 10. Staged roadmap

- **Phase 0:** reproducible baseline, test harness, architectural guardrails.
- **Phase 1:** correctness, security and concurrency hardening of the existing
  `gpustability` plugin.
- **Phase 2:** extensible exporter registry, dynamic target discovery, scalable
  asynchronous scrape manager and unified snapshot model.
- **Phase 3:** independent `space-compute-scheduler`, informer-only hot path, and
  migration away from modifying `default-scheduler`.
- **Phase 4:** link/contact-window, deadline, data-locality, energy/thermal and
  hierarchical mission-planning features.
- **Phase 5:** scale, chaos, security, upgrade, migration and release
  qualification.

No phase may redefine the final architecture downward merely because a later
component is not yet implemented. Temporary limitations must be explicit,
backward-compatible, and accompanied by the next-phase interface or migration
plan.

### Resource inventory and allocation-identity contract

Accelerator resource names are inventory partitions, not aliases for a DeviceClass. A physical device may satisfy at most one simultaneous resource demand unless an upstream allocation result proves distinct identities. The plugin may consume DRA/vendor allocation identity but never becomes an allocator. Per-device hard guarantees are asserted only when exporter stable identity is linked to the already-selected physical device; otherwise strict mode uses conservative node-wide enforcement and hardware release remains blocked.
