# Codex Staged Implementation Prompts

These prompts are intended to be executed in order. Copy one complete prompt into
a Codex task. Do not combine phases merely to reduce the number of tasks.

Each phase assumes that Codex can run Go commands in the development environment.
Physical GPU/NPU/FPGA hardware is not assumed. Tests replay representative real
Prometheus exporter text through Go HTTP test servers and production parsers.
Do not build a standalone resource/device/orbit simulator.

## How to use these prompts

1. Start with the **Continuation/orientation prompt** when opening a fresh Codex
   task or when the prior task's completion state is uncertain.
2. Run Phase 0 once and record a trustworthy baseline.
3. Run only the next phase marked as permitted in
   `IMPLEMENTATION_STATUS.md`.
4. If a phase is too large for one context window, Codex must split it into
   production-complete milestones inside the same phase and update the status
   file after each milestone. It must not reduce phase acceptance criteria.
5. A phase that is partially implemented remains `In progress`, not `Complete`.

---

## Continuation/orientation prompt

```text
You are continuing the heterogeneous space-compute management and scheduling
work in this K3s repository.

Before doing anything else:
1. Read AGENTS.md completely.
2. Read docs/space-compute/PROJECT.md completely.
3. Read docs/space-compute/IMPLEMENTATION_STATUS.md completely.
4. Inspect the current worktree, relevant source files, tests, and recent changes.
5. Do not assume the status document is correct if it conflicts with code or test
   evidence; reconcile the difference explicitly.

Determine the current phase and the smallest production-complete next milestone
that advances that phase without reducing the target architecture.

Non-negotiable constraints:
- Do not switch, create, reset, rebase, merge, or discard Git branches/changes.
- Preserve all unrelated user changes.
- Do not weaken requirements, tests, validation, or failure semantics to obtain a
  green build.
- Do not add test-only success paths to production code.
- The lack of physical accelerators is handled through recorded exporter metric
  fixtures served by Go HTTP test servers, not by deleting device behavior or
  building an alternate simulator runtime.
- Normal K3s/default-scheduler behavior must remain independent of this feature.
- Scheduler hot-path callbacks must not perform remote I/O.
- Keep scheduling policy separate from vGPU/device slicing.

First report your evidence-based understanding and a short execution plan. Then
implement and test the next permitted milestone. Run all applicable tests; do
not claim unexecuted tests as passed. At the end, update
docs/space-compute/IMPLEMENTATION_STATUS.md with exact commands/results, files
changed, limitations, decisions, and the next action.

If a product decision would materially change PROJECT.md, stop and ask for that
decision instead of silently changing the architecture.
```

---

## Phase 0 prompt — reproducible baseline and quality guardrails

```text
Execute Phase 0 of the heterogeneous space-compute project: establish a
reproducible baseline, trustworthy test harness, and anti-regression guardrails.

Mandatory preparation:
- Read AGENTS.md, docs/space-compute/PROJECT.md, and
  docs/space-compute/IMPLEMENTATION_STATUS.md completely.
- Inspect the worktree before editing. Do not change branches or discard any
  existing change.
- Identify the exact Go version, K3s/Kubernetes module replacements, build tags,
  and the custom files relative to the best locally available upstream baseline.
  If Git history is unavailable, record that fact and use file/module evidence;
  do not invent provenance.

Objectives:
1. Run and record the existing gpustability unit tests and race tests before
   changing production code.
2. Establish repeatable commands or narrowly scoped Make targets/scripts for:
   - unit tests;
   - race tests for concurrent space-compute packages;
   - formatting/static checks applicable to changed Go packages;
   - CPU-only integration tests;
   - optional hardware conformance tests with explicit skip reasons.
3. Catalogue the existing plugin entry points, configuration, build-tag behavior,
   dependencies, and its impact when disabled.
4. Add Prometheus text fixtures and Go HTTP test-server helpers only where needed
   to support later phases. They must feed the production HTTP client, parser,
   profile registry and scheduler cache; do not add a simplified scheduler or
   standalone simulator.
5. Add regression coverage that proves a normal Pod not opting into accelerator
   scheduling is ignored by the plugin and that plugin registration alone does
   not alter its scheduling result.
6. Add targeted characterization tests for known defects without prematurely
   changing their expected production behavior. Tests may initially demonstrate
   a defect and be marked clearly as Phase 1 work, but do not disable the package
   test suite or add unconditional skips.

Required baseline/characterization cases:
- one GPU request;
- one NPU request;
- mixed GPU+NPU request;
- multiple application containers;
- multiple init containers with different resource types;
- restartable init container semantics for the repository's Kubernetes version;
- request and limit not being double-counted;
- minimum free-memory consistency across Filter/PreScore/Score;
- missing metrics, stale metrics, NaN, +Inf, -Inf, negative capacity;
- exporter timeout, redirect, oversized body, cancellation and concurrent cache
  miss behavior;
- plugin disabled and non-accelerator Pod behavior.

Do not fix production defects in this phase unless a tiny change is strictly
required to make the baseline harness executable and is documented separately.
Do not lower existing assertions to make characterization tests pass. When a
known defect cannot be represented as a passing test without encoding the wrong
behavior, document it with a precise proposed Phase 1 test instead of committing
an expected-wrong assertion.

Minimum commands to run, adapting package paths only after inspecting the repo:
- go version
- go env
- go test ./pkg/scheduler/plugins/gpustability -count=1
- go test -race ./pkg/scheduler/plugins/gpustability -count=1
- go test on every package changed during this phase
- repository formatting/static checks that apply to those packages

Acceptance criteria:
- Baseline commands and their real results are recorded.
- Existing failures are separated from failures introduced by this phase.
- The test strategy works without physical accelerators and retains full API and
  scheduling behavior.
- No branch change, test weakening, production mock, or scope reduction occurred.
- The disabled/default K3s path has explicit regression evidence.
- IMPLEMENTATION_STATUS.md contains the verified baseline, exact commands,
  environmental limitations, changed files, known defects, and whether Phase 1
  is permitted.

If baseline tests fail, diagnose them. Fix only test-infrastructure/environment
issues in scope; record genuine production defects for Phase 1. Do not declare
Phase 0 complete until the baseline is reproducible or a concrete blocker is
recorded with evidence.
```

---

## Phase 1 prompt — correctness, security, and concurrency hardening

```text
Execute Phase 1 of the heterogeneous space-compute project: make the existing
gpustability scheduling path correct, secure, deterministic, and non-blocking,
without shrinking its production requirements.

Preparation:
- Read AGENTS.md, PROJECT.md, IMPLEMENTATION_STATUS.md, and the Phase 0 evidence.
- Inspect all current changes and preserve unrelated work.
- Confirm Phase 0 permits Phase 1. If not, finish the missing Phase 0 evidence
  first.

Required production outcomes:

1. Correct Pod resource demand
- Represent demand per explicit ResourceName/device class; never collapse mixed
  GPU/NPU/FPGA resources into one eligible-device count.
- Reuse the exact Kubernetes 1.33 Pod request/accounting helpers where possible.
- Match Kubernetes semantics for application containers, ordinary init
  containers, restartable init containers/sidecars, requests, limits, and
  applicable Pod overhead.
- Avoid duplicating NodeResourcesFit behavior. Keep only compatibility/telemetry
  checks this plugin uniquely owns.
- Replace substring resource-name guessing with validated explicit mappings.

2. Consistent feasibility and scoring
- Parse and validate workload requirements once in PreFilter or equivalent cycle
  state and reuse them in Filter, PreScore, and Score.
- Apply the same minimum-memory, temperature, health, type, and compatibility
  definitions throughout the cycle.
- Missing dimensions must follow explicit strict/degraded/best-effort policy and
  must never receive an implicit perfect score.
- Make scores deterministic and expose per-dimension reasons suitable for tests,
  events, logs, and metrics.
- Protect scarce/high-capability devices from small-workload fragmentation; do
  not blindly prefer the richest node for every workload.

3. Remove remote I/O from scheduler callbacks
- Filter, PreScore, Score and binding-path callbacks must not issue HTTP or other
  remote requests.
- Move exporter collection to bounded asynchronous workers that populate
  validated snapshots. Exporters remain the production telemetry source;
  scheduler callbacks
  read only local snapshots and may enqueue a non-blocking refresh request.
- Prevent duplicate refreshes with singleflight/coalescing, bound worker count,
  queue size, cache size and retry rate, and implement cancellation, backoff and
  circuit-breaking behavior.
- Preserve source observation time and expiry; cache-read time must not refresh
  data freshness.

4. Harden untrusted telemetry
- Limit response size and parsing resource consumption.
- Disable arbitrary redirects and validate scheme, port/path and endpoint source.
- Add authenticated TLS/configurable trust for production; plain HTTP may exist
  only as an explicit compatibility mode with a warning and documented risk.
- Validate profile schema, required fields, units, identity, NaN/Inf, ranges,
  negative values and inconsistent memory totals.
- Resolve custom/built-in profile precedence deterministically and reject
  conflicting duplicate names.

5. Typed configuration and observability
- Use versioned typed scheduler plugin arguments instead of ignoring the supplied
  runtime.Object. Validate all ranges and reject invalid configuration with an
  actionable startup error.
- Retain environment variables only through a documented compatibility layer if
  required; define precedence and deprecation behavior.
- Add bounded-cardinality metrics for collection success/failure/latency,
  snapshot age, refresh suppression, filter reasons and score evaluation.
- Ensure logs/events never expose secrets or create unbounded per-device labels.

6. Honest device-allocation semantics
- Annotation-only workloads must not claim guaranteed capacity. Either require
  extended resources/ResourceClaims for enforceable allocation or mark such
  policy as explicitly observational/best-effort.
- Do not claim that a per-device hard constraint is guaranteed unless allocation
  is tied to that same physical device. Until Phase 2/3 supplies that linkage,
  use conservative node feasibility or soft scoring and document the behavior.

Mandatory CPU-only tests:
- all Phase 0 characterization cases now assert correct behavior;
- mixed device types are satisfied separately and wrong-type devices are rejected;
- Kubernetes init/restartable-init accounting matches upstream helpers;
- min-memory behavior is identical across scheduling stages;
- simultaneous scheduling/cache access is race-free;
- slow/unavailable collectors never block scheduler callbacks;
- request coalescing, bounded queues, backoff, expiry and stale-mode transitions;
- NaN/Inf/negative/oversized/redirect/invalid-identity inputs;
- strict/degraded/best-effort behavior;
- invalid typed configuration fails startup;
- non-space/default-scheduler Pods remain unaffected;
- fuzz tests for metric/profile parsing and annotation/config parsing where useful.

Run at minimum:
- go test for gpustability and every new/changed package
- go test -race for all concurrent collector/cache/scheduler packages
- relevant fuzz seeds as ordinary tests; run a bounded fuzz session if the
  environment supports it
- formatting, vet/static analysis applicable to changed packages
- the Phase 0 regression command set

Forbidden shortcuts:
- no synchronous collection hidden behind a helper called from Filter/Score;
- no constant score or always-success fallback;
- no deleting mixed-resource support;
- no skipping malformed-input or race tests;
- no reducing exporter/profile coverage because hardware is absent; use recorded
  Prometheus text and HTTP test endpoints;
- no branch changes, build-tag exclusions, or test-only production bypasses.

Acceptance criteria:
- Known P0/P1 defects in IMPLEMENTATION_STATUS.md are fixed or explicitly carried
  with evidence and a reason that does not misrepresent completeness.
- Scheduler callbacks are demonstrably free of remote I/O.
- Resource calculation is proven equivalent to Kubernetes semantics by tests.
- All malformed/stale modes produce deterministic, observable results.
- Disabled/default K3s behavior remains compatible.
- Documentation and example configuration describe typed args and failure modes.
- IMPLEMENTATION_STATUS.md contains exact test results and permits Phase 2 only
  when these gates are met.
```

---

## Phase 2 prompt — extensible exporters and scalable asynchronous collection

```text
Execute Phase 2: retain Prometheus exporters as the production telemetry source
and build an extensible, dynamically discovered, large-cluster-capable collection
and snapshot layer for scheduling.

Preparation:
- Read AGENTS.md, PROJECT.md, IMPLEMENTATION_STATUS.md, and all Phase 1 APIs/tests.
- Confirm Phase 1 completion evidence. Preserve unrelated changes and do not
  change branches.
- Treat the current TianShu ZhiXin/Iluvatar `ix_*` exporter behavior as a required
  compatibility case, not temporary demo code.

Required architecture:

1. Dynamic exporter target discovery
- Watch Kubernetes Node add/update/delete events through the scheduler/client
  informer APIs available in this K3s version.
- Resolve each target from the Node's current InternalIP or an explicitly allowed
  fallback address type plus validated per-node/global port, path, scheme and
  profile settings.
- Never hard-code a node IP and never depend on an immutable endpoint annotation.
- On Node address/profile changes, stop using the old target, invalidate the old
  snapshot safely, and schedule a refresh for the new target without restart.
- Reject arbitrary URLs, invalid schemes, ports, paths and ambiguous address
  selection. Document precedence and IPv4/IPv6 behavior.

2. Extensible exporter/profile registry
- Preserve a built-in TianShu ZhiXin/Iluvatar profile for `ix_*` metrics.
- Preserve and test DCGM, ROCm and generic accelerator profiles already present.
- Define a versioned declarative profile schema covering match rules, metric
  names, units, rollups, required fields, identity labels, health mapping,
  resource/device class and numeric validation.
- A new exporter that fits this schema must be addable through configuration and
  test fixtures without modifying Filter/Score policy code.
- When declarative mapping is insufficient, define a narrow parser/adapter
  interface and registry. Vendor adapters may normalize telemetry but may not
  contain scheduling policy.
- Detect duplicate/conflicting profile names, ambiguous auto-detection and unsafe
  reloads. Support atomic configuration reload from the selected K3s-compatible
  source, retaining the last valid configuration on invalid updates and exposing
  an error condition/metric.

3. Large-cluster asynchronous scrape manager
- Use bounded worker pools, per-target scheduling, jitter, timeouts,
  coalescing/singleflight, exponential backoff and circuit breaking.
- Scheduler callbacks only read snapshots and never wait for a scrape.
- Separate scrape interval, snapshot TTL and failure/backoff state.
- Prevent one slow or malicious exporter from starving other nodes.
- Bound request/response size, parser work, queue size, cache entries,
  goroutines, retries and metric label cardinality.
- Make shutdown/cancellation race-free and ensure deleted nodes do not leak
  targets, timers, cache entries or goroutines.
- Design for at least 100 and 1,000 target benchmark cases; do not create one
  ticker or unbounded goroutine per scheduling attempt.

4. Unified snapshot store
- Key state by stable Node UID/name plus resolved target generation, not IP alone.
- Store normalized device observations, Kubernetes allocatable/requested resource
  context, profile, endpoint generation, observedAt, validUntil, collection error
  and confidence/degraded condition.
- Make reads cheap and concurrency-safe for Filter/PreScore/Score.
- Ensure a cache read never changes freshness and an old endpoint response cannot
  overwrite a newer target generation.
- Provide deterministic strict/degraded/best-effort lookup behavior.

5. Configuration and observability
- Typed configuration for discovery, worker count, intervals, timeouts, TTL,
  backoff, allowed schemes/address types, profile sources and size limits.
- Metrics for target count, queue depth, worker utilization, scrape/parse
  duration, failures by bounded reason, backoff/circuit state, snapshot age,
  reload success and discarded stale-generation responses.
- Operator documentation showing how to add a new exporter profile without code
  changes and how to pin a profile for a heterogeneous node.

Mandatory tests without accelerator hardware:
- use recorded Prometheus text for TianShu ZhiXin/Iluvatar `ix_*`, DCGM, ROCm,
  NPU/generic and at least one configuration-only new exporter;
- serve fixtures through Go `httptest.Server` so the production resolver, HTTP
  client, parser, registry, scrape manager and snapshot store are exercised;
- Node InternalIP change, IPv4/IPv6, node deletion/recreation with a new UID,
  profile change and invalid metadata;
- no hard-coded IP dependency and no use of stale endpoint data after change;
- successful declarative addition of a new exporter without scheduler policy
  modifications;
- ambiguous auto-detection, duplicate profile, invalid reload and last-known-good
  configuration behavior;
- timeout, cancellation, oversized body, redirect, malformed metrics, NaN/Inf,
  HTTP errors and recovery/backoff;
- concurrent cache misses are coalesced and scheduler reads remain non-blocking;
- node deletion and shutdown leave no leaked goroutines/timers/cache entries;
- race tests for target registry, workers, reload and snapshot store;
- generated 100/1,000 Node target benchmark datasets using Node objects and a
  controlled HTTP fixture endpoint; this is a load test, not a simulator product;
- default-scheduler/non-space Pod regression while collectors are slow or down.

Run all affected unit, race, integration, benchmark, formatting and static checks.
Record benchmark environment and results honestly. Do not meet scale targets by
dropping required nodes, fields or validation.

Acceptance criteria:
- TianShu ZhiXin/Iluvatar exporter scheduling behavior remains first-class and is
  covered end to end.
- Node addresses are dynamically resolved and address changes are tested.
- A schema-compatible new exporter is added through configuration plus fixtures,
  with no scheduling policy code change.
- Filter/PreScore/Score perform no remote I/O and snapshot reads remain bounded.
- 100/1,000-node tests show bounded goroutines, queues, cache and failure impact.
- Missing/stale exporters have explicit policy behavior and cannot block normal
  K3s/default-scheduler operation.
- IMPLEMENTATION_STATUS.md records exact tests, benchmark results, limits and
  whether Phase 3 may start.
```

---

## Phase 3 prompt — independent production scheduler and K3s isolation

```text
Execute Phase 3: deliver a separately deployable `space-compute-scheduler` that
uses the exporter-backed snapshot layer and leaves K3s `default-scheduler` behavior
independent.

Preparation:
- Read AGENTS.md, PROJECT.md, IMPLEMENTATION_STATUS.md, the exporter collection
  and snapshot design,
  and all current scheduler/resource APIs.
- Verify Phase 2 evidence and the exact scheduler framework interfaces in the
  repository's Kubernetes 1.33 dependency.
- Preserve the worktree and current branch.

Required implementation:

1. Component and deployment isolation
- Build a separately invokable scheduler binary/component compatible with this
  K3s/Kubernetes version.
- Use scheduler profile name `space-compute-scheduler`.
- Supply RBAC, ServiceAccount, leader election, health/readiness endpoints,
  configuration, deployment/static-manifest options and upgrade notes.
- Ordinary Pods without the explicit schedulerName remain owned by
  `default-scheduler`.
- Plan and implement a backward-compatible migration from the embedded
  gpustability registration. Do not abruptly delete compatibility without a
  tested migration path; do not leave default-scheduler examples that encourage
  production coupling.

2. Scheduler plugin architecture
- PreFilter parses the typed workload intent once and stores immutable cycle
  state.
- Filter evaluates explicit resource/device/software compatibility, assignable
  capacity, trust and snapshot freshness using informer-local data only.
- PreScore prepares candidate-wide normalization inputs without remote calls.
- Score evaluates deterministic device fit, predicted compute capability,
  utilization, thermal/energy headroom, data locality available at this phase,
  resilience and scarce-resource fragmentation.
- NormalizeScore is used where candidate-relative normalization is truly needed;
  fixed SLO-based normalization is preferred for explainability.
- Implement EnqueueExtension/queueing behavior supported by the exact Kubernetes
  version so relevant inventory/snapshot changes requeue Pods without hot loops.
- Use Reserve/Unreserve only for state not already atomically owned by Kubernetes
  extended-resource or DRA accounting. Do not create a second inconsistent
  allocator.
- Produce structured decision explanations, Events and bounded metrics.

3. Physical-device consistency
- Integrate ResourceClaims/DRA where selected by the Phase 2 ADR, or provide a
  clearly tested extended-resource compatibility path.
- Prove that hard per-device constraints correspond to devices eligible for final
  allocation. If the selected allocation API cannot guarantee this, downgrade
  that signal to soft scoring or apply the documented conservative rule; never
  claim false enforcement.
- Support alternative device classes without collapsing quantities or types.

4. Configuration and policy
- Versioned typed scheduler configuration with validation and safe defaults.
- Explicit strict/degraded/best-effort state policy.
- Configurable scoring weights with range checks and documented rationale.
- Invalid workload intent results in a clear rejection/condition, not silent
  permissive fallback.

Mandatory CPU-only integration/e2e scenarios:
- default-scheduler and space-compute-scheduler operate concurrently with unique
  identities and leader election;
- ordinary Pods are scheduled when the space scheduler is down;
- space Pods remain pending with actionable reasons when their scheduler or
  required state is unavailable;
- heterogeneous Kubernetes Node objects backed by exporter HTTP fixtures and
  alternative GPU/NPU/FPGA workload choices;
- mixed resource quantities, init/restartable-init accounting, and concurrent
  scheduling without overcommit;
- snapshot update causes precise requeue; unrelated updates do not create a hot
  loop;
- strict/degraded/best-effort stale-state behavior;
- scheduler restart during scheduling and controller restart/recovery;
- allocation failure triggers correct unreserve/retry behavior where applicable;
- deterministic score and explanation golden tests;
- no network calls from framework callbacks, verified by injected transports or
  forbidden-I/O test guards;
- install/uninstall/disable flow leaves normal K3s functionality intact.

Performance tests:
- benchmark scheduling on representative generated 100, 1,000 and, where
  practical, larger NodeInfo/exporter-snapshot datasets;
- report callback latency, allocations, informer lookup cost, queue depth and
  end-to-end scheduling latency;
- establish evidence-based budgets and fail tests on gross regression. Do not
  meet a target by evaluating fewer required constraints or sampling away scarce
  device classes.

Forbidden shortcuts:
- no replacing kube-scheduler behavior with a hand-written toy scheduler;
- no binding Pods directly to Nodes outside the scheduler framework to simplify
  tests;
- no default-scheduler dependency on space telemetry;
- no remote I/O hidden in snapshot getters;
- no fixture-driven fake-success allocation;
- no removal of mixed-device or degraded-mode behavior.

Acceptance criteria:
- A CPU-only ephemeral cluster demonstrates the real second-scheduler flow from
  exporter-backed snapshots to Pod binding.
- Failure of every new component has a documented and tested impact boundary.
- Default K3s scheduling remains operational and independently testable.
- Device constraints and allocation semantics are honest and tested.
- Deployment, RBAC, metrics, dashboards/runbook basics and migration docs exist.
- IMPLEMENTATION_STATUS.md records exact commands/results and permits Phase 4.
```

---

## Phase 4 prompt — high/low-orbit aware policy and hierarchical orchestration

```text
Execute Phase 4: add high-orbit/low-orbit/ground-aware workload policy, link and
contact-window modeling, and a hierarchical mission planner while keeping local
Pod-to-Node scheduling separate and production-grade.

Preparation:
- Read all project governance/status files and Phase 2/3 ADRs.
- Verify the independent scheduler and exporter collection/snapshot layer are
  stable.
- Preserve branches and unrelated changes.
- Do not place future-window waiting, cross-cluster transfer orchestration, or
  mission retry logic inside a scheduler Score callback.

Required APIs and control loops:

1. Versioned link/contact model
- source/destination or domain identity;
- bandwidth, latency/RTT, loss/error rate and stability/confidence;
- contact-window start/end, observedAt, validUntil and provenance;
- validation for overlapping/impossible windows, clock skew and stale data;
- bounded update frequency and history required for prediction/audit.

2. Versioned workload/mission intent
- required and alternative device/software capabilities;
- input/output size and data location;
- deadline, expected/bounded compute duration and safety margin;
- priority/mission class, checkpointability, retry/migration constraints;
- result-return requirement and strict/degraded/best-effort policy;
- validation/admission that rejects impossible or contradictory intent.

3. Local scheduler policy additions
- hard feasibility for current node/domain, snapshot freshness and any contact
  window that must cover immediate transfer/execution/return;
- deterministic scoring for predicted completion time, data locality, link risk,
  energy/thermal headroom, resilience and fragmentation;
- fixed units and documented normalization;
- structured explanation of every rejected constraint and score component;
- no remote calls and no long waits in scheduler callbacks.

4. Global mission planner
- select target cluster/orbit domain and an execution epoch/window before local
  Pod scheduling;
- represent placement intent durably and idempotently;
- support store-and-forward, disconnected operation, retries, checkpoint policy
  and result-return tracking at the orchestration level;
- tolerate planner/controller restart and duplicate/out-of-order observations;
- never require a single K3s/etcd quorum to span intermittent orbital links;
- define clear ownership between planner, resource controller, scheduler and
  workload controller through an ADR/state-machine document.

5. Time and uncertainty
- use injectable clocks in logic tests without adding production test bypasses;
- include clock skew, uncertain duration and configurable safety margins;
- never fabricate future link availability when prediction is missing;
- explicitly expire plans and replan when material inputs change.

Mandatory CPU-only fixture-driven/e2e scenarios:
- LEO, GEO/high-orbit and ground domains with changing contact windows;
- input transfer fits/does not fit before a window closes;
- execution fits but result return misses deadline;
- high compute capability loses to a nearer/data-local node when total completion
  time is lower;
- low energy/thermal margin rejects or penalizes placement as policy specifies;
- stale link prediction in strict/degraded/best-effort modes;
- clock skew around window boundaries;
- link partition after plan, checkpointable recovery and non-checkpointable
  failure behavior;
- planner and controller restart, duplicate intent, idempotent reconciliation;
- cross-domain replan without duplicate Pod execution beyond declared policy;
- ordinary K3s workloads unaffected throughout;
- deterministic replay: the same workload and recorded snapshots produce the
  same decision/explanation.

Test-fixture requirements:
- Use production CRDs/APIs, controller reconciliation, exporter snapshot
  providers and scheduler plugins.
- Use an injectable clock plus recorded link/contact snapshot objects to test
  boundary conditions deterministically. Do not build a separate orbit or link
  simulator runtime and do not bypass production validation/state transitions.
- Store golden workload, Node, exporter and link inputs with expected decision
  explanations.

Operational deliverables:
- API and state-machine documentation;
- metrics for planning latency, replan reasons, deadline slack, snapshot age,
  link-risk class and reconciliation errors with bounded labels;
- Events/conditions and a troubleshooting runbook;
- security/RBAC review for cross-domain resource summaries and placement intent;
- data-retention and clock-synchronization assumptions.

Acceptance criteria:
- The full fixture-driven flow runs on CPU-only infrastructure: exporter
  snapshots -> link snapshots -> mission plan -> independent scheduler ->
  binding/status.
- Future-window and cross-cluster behavior resides in controllers/planner, not
  scheduler hot-path callbacks.
- Deadline/contact/energy decisions are deterministic, explainable and tested at
  boundary conditions.
- Disconnection and restart behavior are explicit and idempotent.
- No K3s core or default-scheduler dependency on the planner is introduced.
- All applicable unit, race, integration, e2e, fuzz and scenario tests pass, and
  IMPLEMENTATION_STATUS.md records the evidence before permitting Phase 5.
```

---

## Phase 5 prompt — production qualification, migration, and release gates

```text
Execute Phase 5: qualify the heterogeneous space-compute stack for production
use, close migration/security/scale gaps, and produce an evidence-based release
decision. Do not redefine missing functionality as out of scope merely to call
the project complete.

Preparation:
- Read AGENTS.md, PROJECT.md, IMPLEMENTATION_STATUS.md, all ADRs, APIs, runbooks
  and prior test evidence.
- Inspect the complete diff against the verified K3s baseline without switching
  or rewriting branches.
- Build a traceability matrix from every PROJECT.md requirement and known defect
  to production code, tests and documentation.

Qualification workstreams:

1. Functional completeness
- Verify every resource type, policy mode, controller state transition,
  scheduler extension point, mission-planning state and failure mode.
- Add missing tests rather than narrowing documentation.
- Verify DRA/extended-resource/device-plugin interoperability and physical-device
  constraint honesty.

2. K3s compatibility and upgrade
- Test feature absent, installed-disabled, enabled, upgraded and uninstalled.
- Verify normal K3s server/agent startup, default scheduling and representative
  repository conformance/integration tests.
- Test supported K3s patch upgrade and API conversion/migration paths.
- Minimize and document the remaining K3s fork surface.

3. Scale and performance
- Execute reproducible 100/1,000-node and larger practical benchmark datasets
  with mixed exporter snapshots and workload classes.
- Measure controller throughput, API write rate, informer lag, memory, goroutines,
  queue depth, scheduling latency, planning latency and recovery time.
- Set evidence-based SLOs/budgets and add regression thresholds with appropriate
  environmental tolerance. Do not meet them by disabling constraints, reducing
  resource classes, or returning cached constant decisions.

4. Chaos and resilience
- exporter/collector loss, malformed responses, node churn, API throttling,
  controller
  leader failure, scheduler restart, planner restart, clock skew, stale state,
  packet delay/loss and long partitions;
- duplicate/out-of-order updates and partial cross-domain synchronization;
- verify bounded retries, no hot loops, no unbounded memory/cardinality and
  correct recovery/idempotency.

5. Security
- threat-model exporter endpoints, collection workers, telemetry, CRDs/DRA
  objects, scheduler, planner and
  cross-domain summaries;
- verify mTLS/identity, least-privilege RBAC, admission/validation, size/rate
  limits, secret handling, redirect/SSRF defenses and auditability;
- fuzz all untrusted parsers and API conversion/defaulting paths;
- run repository-supported dependency/static/vulnerability checks and triage
  findings without suppressing them solely to get green output.

6. Hardware-independent and hardware qualification
- Run the full CPU-only suite; it is the mandatory functional gate.
- Provide opt-in vendor hardware conformance suites for discovery, health,
  allocation identity and representative workload execution.
- If hardware is unavailable, record those tests as explicitly not run. Do not
  mark hardware qualification complete, but do not treat it as a failure of the
  CPU-only functional implementation.

7. Release and operations
- installation, configuration, upgrade, rollback and uninstall documentation;
- dashboards/alerts, troubleshooting and degraded-mode runbooks;
- API compatibility policy and supported-version matrix;
- disaster recovery and state cleanup procedures;
- known limitations stated without contradicting implemented guarantees.

Release decision rules:
- `Ready for CPU-only functional release` requires every mandatory functional,
  integration, race, security, chaos, scale and K3s-isolation gate to pass.
- `Ready for vendor hardware release` additionally requires the applicable real
  hardware conformance suite.
- `Not ready` is the correct result if required evidence is missing. Do not lower
  the release category or silently rename requirements to achieve completion.

Required final outputs:
- traceability matrix;
- exact test command/result report;
- performance/scale report;
- threat model and security findings/status;
- compatibility and migration report;
- open-risk register with severity and owner/next action;
- final update to IMPLEMENTATION_STATUS.md with an honest release classification.

Forbidden shortcuts:
- no branch replacement/reset to hide failures;
- no skipped core suites;
- no assertions changed to encode broken behavior;
- no fixture-only implementation that bypasses production paths;
- no constant scheduler or planner decisions;
- no claiming physical-device verification without hardware evidence;
- no marking the project complete when PROJECT.md traceability has gaps.
```

---

## Focused defect-fix prompt template

Use this only for a bounded defect discovered inside the active phase. It does
not replace the phase acceptance criteria.

```text
Fix the following defect within the currently active space-compute phase:

<describe the observed defect, reproduction, and expected behavior>

Before editing, read AGENTS.md, PROJECT.md, IMPLEMENTATION_STATUS.md, and the
active phase prompt. Inspect related production code and existing tests. Preserve
the current branch and unrelated changes.

Requirements:
- determine and explain the root cause;
- add a failing regression test that uses production interfaces/data paths;
- implement the complete production fix, including concurrency, validation,
  stale-state, security and observability consequences where applicable;
- do not weaken the requirement or change unrelated public behavior merely to
  pass the test;
- do not use physical-hardware absence as a reason to omit functionality; use
  recorded Prometheus text through a Go HTTP test server at the exporter boundary;
- run the focused test, affected package tests, race tests for concurrent code,
  and applicable integration/regression checks;
- update IMPLEMENTATION_STATUS.md with exact results and whether the active phase
  remains blocked or can continue.

Do not call the defect fixed based only on compilation or a mocked-out code path.
```
