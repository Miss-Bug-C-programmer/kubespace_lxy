# Space Compute Implementation Status

This is the cross-session handoff record. Update it at the end of every Codex
implementation phase. Do not mark an item complete based only on source
inspection or compilation.

## Current baseline

- Workspace source identifies as K3s `v1.33.7+k3s1`: `go.mod` requires
  `k8s.io/kubernetes v1.33.7` and replaces Kubernetes modules with
  `github.com/k3s-io/kubernetes` or its staging modules at `v1.33.7-k3s1`.
- The verified security-qualified toolchain is `go1.25.12 linux/amd64`; the
  module minimum is pinned to `go 1.25.12` so vulnerable earlier 1.25 patches
  cannot build the release.
  The pre-remediation Phase 5 baseline was Go 1.24.11. `CGO_ENABLED=1`.
- Git status is usable and remained on branch `master`; no branch, reset,
  rebase, merge or checkout operation was performed. The pre-Phase-5 short
  status was clean; unrelated upstream-baseline build/module differences were
  preserved rather than reset.
- Existing custom implementation:
  - `pkg/scheduler/plugins/gpustability/gpu_stability.go`
  - `pkg/scheduler/plugins/gpustability/metrics_profiles.go`
  - `pkg/scheduler/plugins/gpustability/config.go`
  - `pkg/scheduler/plugins/gpustability/collector.go`
  - `pkg/scheduler/plugins/gpustability/snapshot_store.go`
  - `pkg/scheduler/plugins/gpustability/profile_registry.go`
  - `pkg/scheduler/plugins/gpustability/observability.go`
  - `pkg/scheduler/plugins/gpustability/gpu_stability_test.go`
  - `pkg/scheduler/plugins/gpustability/phase0_test.go`
  - `pkg/scheduler/plugins/gpustability/phase1_test.go`
  - `pkg/scheduler/plugins/gpustability/phase2_test.go`
  - `pkg/scheduler/plugins/gpustability/phase3_test.go`
  - `pkg/scheduler/plugins/gpustability/phase3_integration_test.go`
  - `pkg/scheduler/plugins/gpustability/queueing.go`
  - `pkg/scheduler/plugins/gpustability/workload_intent.go`
  - `cmd/space-compute-scheduler/main.go`
  - standalone registration in `cmd/space-compute-scheduler/main.go`
  - configuration/documentation in `docs/gpu-scheduler/`
- Current behavior is an opt-in scheduler-framework `PreFilter`, `Filter`,
  `PreScore`, and `Score` plugin. A bounded asynchronous collector watches Node
  add/update/delete data, resolves ordered validated Node-address targets, and
  publishes generation-safe unified local snapshots keyed by Node name/UID and
  target generation; scheduler callbacks perform no remote I/O. Iluvatar, DCGM,
  ROCm, and generic GPU/NPU/FPGA profiles remain built in. Versioned declarative
  profiles can be atomically reloaded from a bounded file source without policy
  changes, retaining last-known-good configuration after invalid updates.
- Phase 0 established CPU-only HTTP fixture tests and a repeatable verification
  script. Phase 1 corrected resource accounting, snapshot-only scheduling,
  telemetry validation, typed configuration, state policy, deterministic
  fragmentation-aware scoring, transport security, and observability. Phase 2
  added dynamic discovery, atomic profiles, per-target refresh scheduling,
  unified snapshots, generation rejection/cancellation, and measured 100/1,000
  target coverage.
- Phase 3 implementation adds a separately linked upstream Kubernetes 1.33
  scheduler command, an isolated one-profile configuration and leader lease,
  deployment/static-manifest/RBAC assets, immutable versioned workload intent,
  fixed weighted decision dimensions, conservative extended-resource physical
  coverage, and precise bounded snapshot publication requeue. The real
  framework/fake-API integration and a repository-built version-matched
  ephemeral K3s API/control-plane e2e now pass, including two standalone
  scheduler processes, real Lease failover, exporter-backed Pod Binding,
  probes, RBAC, and manifest install/uninstall. Phase 3 is complete.
- Phase 4 adds versioned link/resource/mission/placement APIs, strict
  validation and admission, a restart-safe global mission planner and workload
  controller, informer-local link/domain projections, deterministic local
  policy dimensions, an ownership/state-machine ADR, bounded metrics/Events,
  and a CPU-only real-K3s full-flow e2e. Future waiting, transfer/retry and
  result-return state remain above the scheduler. Phase 4 is complete.
- Phase 5 qualifies and hardens Iluvatar multi-device parsing, multiple Agent
  Nodes and schedulable-master isolation, controller bounds, typed admission,
  5,000-node datasets and K3s lifecycle behavior. It also identifies release
  blockers; Phase 5 is executed but the stack is not release-ready.

## Verified plugin catalogue

- Entry points are `New`, `PreFilter`, `Filter`, `PreScore`, `Score`, `Close`,
  `EventsToRegister`, and their scheduler-framework extension methods. `New`
  strictly decodes versioned
  `gpustability.k3s.io/v1alpha1` `K3SGPUStabilityArgs`; deprecated environment
  values are applied first and typed values take precedence.
- Phase 5 removed the earlier `pkg/executor/embed/embed.go` registration. Its
  source content now matches verified upstream K3s, so the default scheduler has
  no import or compiled dependency on the plugin. `cmd/space-compute-scheduler`
  uses the real upstream
  `kube-scheduler/app.NewSchedulerCommand` with the plugin factory registered.
  Its production example contains only the `space-compute-scheduler` profile
  and lease; no current example adds a space profile to default K3s.
- With `K3S_GPU_SCORE_ALL_PODS=false`, a normal Pod without configured accelerator
  resources or the opt-in annotation returns Filter success, PreScore `Skip`, a
  zero/minimum score contribution, and performs no exporter request. This is now
  regression-tested even when a `Plugin` instance exists.
- Direct dependencies specific to this path include Kubernetes scheduler
  framework/`NodeInfo` and cycle state, the Kubernetes 1.33
  `component-helpers/resource.PodRequests` accounting helper, Node informers,
  Prometheus `expfmt`/client-model parsing, `net/http`, TLS/x509, component-base
  metrics, an atomic declarative profile registry, and concurrency-safe bounded
  target/snapshot stores. There is no DRA allocation linkage or vGPU slicing.

## Known limitations after Phase 4 implementation

- Physical-device selection is still owned by the configured device plugin.
  Because telemetry identity is not linked to the allocated device yet, strict
  dynamic constraints use conservative node feasibility and are not advertised
  as a per-device allocation guarantee.
- Annotation-only/`scoreAllPods` evaluation is explicitly observational best
  effort and never claims capacity or hard-filters a node. Enforceable capacity
  requires mapped extended-resource requests; future DRA linkage remains later
  work and was not introduced into scheduling policy.
- Declarative profiles reload from the selected local file source. Operators
  should replace that file atomically; a partial or invalid read is rejected and
  the last-known-good registry remains active. A ConfigMap-specific informer
  source is not implemented.
- The 100/1,000-target evidence is a CPU-only, one-iteration controlled HTTP
  fixture benchmark on one host. It validates bounded target/queue/cache/worker
  behavior, not production network throughput, multi-hour soak, or chaos under
  1,000 distinct physical exporters.
- `cacheMaxEntries` is an explicit hard bound. Deployments with more discovered
  nodes than that value receive deterministic eviction and missing-state policy;
  production sizing must cover the intended accelerator-node population.
- The version-matched CPU-only K3s qualification uses an agentless control
  plane, a real API server/default scheduler/Binding API, a fixture Node, and
  separately launched scheduler processes. It validates scheduling and control
  plane isolation, not kubelet image execution, CNI, a vendor device plugin, or
  physical accelerator allocation; those remain Phase 5/hardware qualification.
- Cross-domain summary delivery is an authenticated at-least-once integration
  boundary; this phase does not ship a WAN transport or stretch one etcd quorum
  across orbital links. Each domain must deploy its own reporter credentials,
  audit sink and time-synchronization monitoring.
- The agentless CPU-only K3s Phase 4 e2e validates real CRDs/admission,
  controller persistence, contact planning, exporter collection, independent
  scheduler Binding and terminal status. It does not claim kubelet image
  execution, CNI, physical contact hardware, a vendor device plugin or result
  transfer-agent qualification. Those remain Phase 5/hardware work.

## Phase state

| Phase | State | Evidence |
|---|---|---|
| 0 — Baseline and guardrails | Complete | Unit/race/static/HTTP integration harness and opt-out regression passed on 2026-07-20 |
| 1 — Existing plugin hardening | Complete | Unit/race/static/integration/fuzz gates passed on 2026-07-20; callbacks are snapshot-only |
| 2 — Exporter collection and snapshots | Complete | Dynamic Node discovery, atomic declarative reload, unified generation-safe snapshots, race tests, and 100/1,000-target fixture benchmarks passed on 2026-07-20 |
| 3 — Independent scheduler | Complete | Version-matched K3s API e2e, two-process Lease failover, probes/RBAC/install-uninstall, exporter-backed binding, unit/race/static/fuzz/scale gates passed on 2026-07-20 |
| 4 — Space-aware orchestration | Complete | Versioned CRDs/admission, accepted-generation link/resource control, deterministic planner/local policy, restart-safe workload state, CPU-only fixtures and real K3s full-flow/race e2e passed on 2026-07-21 |
| 5 — Production qualification | Executed; Not ready | CPU gates, 5,000-node scale, K3s lifecycle, admission/security review and vulnerability remediation completed on 2026-07-21; full-agent, upgrade, hardware, API and transport gates remain open |

## Latest Phase 3 work

- Implemented the Phase 3 production milestone without branch operations, test
  weakening, production mocks, simulator code, vGPU/device allocation, or
  default-scheduler coupling. The current repository has a usable `master`
  branch; Phase 4 likewise performed no branch-changing operation.
- Added `cmd/space-compute-scheduler`, which invokes the exact upstream
  Kubernetes `v1.33.7-k3s1` scheduler command and registers only the external
  plugin factory. The reproducible command uses `-buildvcs=false` so workspace
  state is not embedded as a fabricated release revision. JSON log
  registration adds the canonical indirect `github.com/go-logr/zapr v1.3.0`
  module requirement; `go mod tidy -diff` reports no remaining delta.
- Added a strict typed `SpaceComputeWorkloadIntent` annotation for state policy,
  telemetry thresholds, accepted profiles, required software/trust labels, and
  preferred data-locality labels. PreFilter stores cloned immutable maps;
  annotation-only intent remains observational and downgrades required labels
  to preferences instead of hard-filtering capacity it cannot claim.
- Added validated scoring weights for utilization, memory, thermal, energy,
  compute, health/resilience, locality, and fragmentation. All weights are
  bounded and must total 100; missing dimensions score zero and fixed SLO
  normalization keeps `ScoreExtensions` nil.
- Added bounded/TTL blocked-Pod dependency tracking, Kubernetes 1.33 Node queue
  hints, and local `PodActivator` notification on a published snapshot. Filter,
  PreScore, Score, and the binding path still issue no remote I/O.
- Retained extended-resource compatibility. Kubernetes NodeResourcesFit/device
  plugins own capacity and allocation; this plugin implements no Reserve or
  Unreserve. Strict hard device telemetry requires exporter coverage to equal
  Kubernetes allocatable and all possible devices to pass. Coverage mismatch is
  capped soft evidence outside strict mode and never claims physical pinning.
- Added dedicated ServiceAccount, version-pinned RBAC including DRA informer
  reads used by Kubernetes 1.33 defaults, scoped leader-election Role/Lease,
  two-replica Deployment, secure health/readiness, Service, static-Pod template,
  container build, failure runbook/dashboard basics, and safe embedded-profile
  migration/rollback ordering.
- Added ordered dynamic Node address resolution with explicit address-type and
  IP-family policy, validated per-node/global scheme/port/path/profile metadata,
  Node UID plus monotonically generated target identity, immediate old-snapshot
  invalidation, in-flight old-endpoint cancellation, and late-response rejection.
- Added a unified concurrency-safe snapshot store containing normalized devices,
  Node allocatable/requested extended-resource context, profile, endpoint
  generation, observation/expiry, bounded error text, and explicit
  validated/degraded/stale/missing/failed confidence. Reads do not refresh time.
- Added the versioned `MetricProfileList` declarative schema, finite range/unit/
  rollup/identity/health/required-field validation, deterministic duplicate and
  ambiguous-auto-detection rejection, a narrow normalization-adapter seam, and
  atomic polling reload with last-known-good retention.
- Added bounded metric family/sample/label parsing, per-target deterministic
  jitter and refresh due-times in one manager loop, bounded workers/queue/cache,
  coalescing, backoff/circuit state, deletion/shutdown cleanup, and metrics for
  targets, queue, workers, parsing, reload state/errors, backoff/circuit state,
  and stale-generation discards.
- Added recorded DCGM and ROCm fixtures, retained Iluvatar `ix_*` and generic NPU
  fixtures, and added a configuration-only VendorX profile/fixture that reaches
  production collection and snapshots without scheduler-policy modifications.
- Added dynamic address/profile/UID lifecycle, IPv4/IPv6, invalid metadata,
  generation discard/cancellation, reload/last-known-good, parser bounds,
  concurrency, resource-context, cleanup, and 100/1,000-target tests and
  benchmarks. The repeatable harness now has a `scale` mode.

- Completed Phase 1 without changing branches, weakening tests, adding a
  simulator/runtime bypass, or adding device-allocation/vGPU behavior. The local
  `.git` remains unusable, so preservation was based on visible-file inspection.
- Replaced approximate accelerator counting with explicit resource-to-class
  mappings and exact Kubernetes 1.33 `PodRequests` semantics for application,
  ordinary init, restartable init, request/limit, and overhead accounting.
- Added immutable workload cycle state and deterministic PreScore node state;
  Filter and Score share compatibility, type, health, temperature, memory, and
  clock definitions. Scoring penalizes missing optional dimensions and combines
  device quality with tight-fit fragmentation protection.
- Moved all exporter HTTP work into bounded background workers with coalescing,
  bounded queue/cache/workers, timeouts, exponential backoff, circuit breaking,
  cancellation, endpoint-generation rejection, and observation-time expiry.
- Hardened HTTPS/mTLS trust configuration, explicit insecure-HTTP compatibility,
  redirects, body size, endpoint metadata, strict typed args/profile JSON,
  duplicate profile precedence, identity, required fields, units, finite/range
  checks, health, and memory consistency.
- Added bounded-cardinality metrics for collection, failures, refresh
  suppression, snapshots, filter reasons, and scores. No node/device/endpoint or
  secret labels are used.
- Corrected the scheduler example to retain an unchanged `default-scheduler`
  profile and document typed args, failure policies, the allocation boundary,
  TLS risk, and Phase 3 isolation limitation.
- Expanded `scripts/space-compute` CPU integration coverage and added bounded
  parser/config fuzz modes.
- Added recorded Iluvatar and normalized NPU Prometheus text fixtures under
  `pkg/scheduler/plugins/gpustability/testdata/fixtures/`.
- Added `phase0_test.go` coverage that serves those fixtures through real Go HTTP
  test servers and exercises the production target resolver, HTTP client, parser,
  profile registry, cache, Filter, PreScore, and Score path.
- Added regression/characterization coverage for opt-out Pods, one GPU, one NPU,
  mixed GPU+NPU resource preservation, multiple application containers, exporter
  timeout, cancellation, and cache reuse. Existing tests cover request/limit
  non-double-counting and stale cache pruning.
- Added executable `scripts/space-compute` modes: `unit`, `race`, `static`,
  `integration`, `fuzz`, `hardware`, and `all`. The hardware mode uses the existing
  production-parser conformance path and reports an explicit skip when
  `K3S_GPU_TEST_METRICS_FILE` is unset.
- Revalidated the complete Phase 0 command set on 2026-07-20 after a fresh read
  and tree inspection. Results matched the original Phase 0 evidence; no harness
  or production correction was required.
- No build tag, plugin registration point, vGPU behavior, device allocation, or
  default enablement changed. Typed args are the primary API; legacy environment
  variables remain as a documented deprecated compatibility layer.

## Test evidence

Record commands exactly, including working directory, result, duration when
material, and any explicit skip/environment reason.

| Date | Commit/worktree | Command | Result | Notes |
|---|---|---|---|---|
| 2026-07-20 | Empty `.git`; unmodified baseline | `go version` | Passed | `go version go1.24.11 linux/amd64` |
| 2026-07-20 | Empty `.git`; unmodified baseline | `go env` | Passed | `GOMOD` is this workspace; `GOOS=linux`, `GOARCH=amd64`, `CGO_ENABLED=1`; configured `GOCACHE` is read-only here |
| 2026-07-20 | Empty `.git`; unmodified baseline | `go test ./pkg/scheduler/plugins/gpustability -count=1` | Environment failure | Go could not read/write `/home/user/.cache/go-build`; no tests executed |
| 2026-07-20 | Empty `.git`; unmodified baseline | `go test -race ./pkg/scheduler/plugins/gpustability -count=1` | Environment failure | Same read-only configured-cache failure |
| 2026-07-20 | Empty `.git`; unmodified baseline | `env GOCACHE=/tmp/space-compute-go-build go test ./pkg/scheduler/plugins/gpustability -count=1` | Passed | `ok`, package test time `0.020s` after initial dependency compilation |
| 2026-07-20 | Empty `.git`; unmodified baseline | `env GOCACHE=/tmp/space-compute-go-build-race go test -race ./pkg/scheduler/plugins/gpustability -count=1` | Passed | `ok`, package test time `1.117s` after initial dependency compilation |
| 2026-07-20 | Phase 0 files present | `env GOCACHE=/tmp/space-compute-go-build go test ./pkg/scheduler/plugins/gpustability -count=1` | Environment failure in restricted sandbox | Required `httptest.Server` could not bind `[::1]:0`: `socket: operation not permitted`; suite was rerun with loopback permission |
| 2026-07-20 | Phase 0 files present | `env GOCACHE=/tmp/space-compute-go-build go test ./pkg/scheduler/plugins/gpustability -count=1` | Passed with local-loopback permission | `ok`, package test time `0.041s` |
| 2026-07-20 | Phase 0 files present | `gofmt -w pkg/scheduler/plugins/gpustability/phase0_test.go && bash -n scripts/space-compute && gofmt -s -l pkg/scheduler/plugins/gpustability/*.go` | Passed | Script syntax valid; formatting check produced no paths |
| 2026-07-20 | Phase 0 files present | `env GOCACHE=/tmp/space-compute-go-build scripts/space-compute all` | Passed with local-loopback permission | Unit `0.044s`; race `1.132s`; `gofmt` clean; `go vet` clean; CPU-only HTTP integration `0.043s`; hardware conformance explicitly skipped because `K3S_GPU_TEST_METRICS_FILE` was unset, command otherwise passed in `0.017s` |
| 2026-07-20 | Phase 0 files present | `env GOCACHE=/tmp/space-compute-go-build go test ./pkg/executor/embed -count=1` | Passed | Registration boundary compiled; package reports `[no test files]` |
| 2026-07-20 | Phase 0 revalidation; empty `.git` | `go version` | Passed | `go version go1.24.11 linux/amd64` |
| 2026-07-20 | Phase 0 revalidation; empty `.git` | `go env` | Passed | Confirmed `GOOS=linux`, `GOARCH=amd64`, `CGO_ENABLED=1`, workspace `GOMOD`, and configured read-only home-cache path |
| 2026-07-20 | Phase 0 revalidation; empty `.git` | `env GOCACHE=/tmp/space-compute-go-build go test ./pkg/scheduler/plugins/gpustability -count=1` | Passed with local-loopback permission | `ok`, package test time `0.047s`; run before this revalidation's documentation-only edit |
| 2026-07-20 | Phase 0 revalidation; empty `.git` | `env GOCACHE=/tmp/space-compute-go-build go test -race ./pkg/scheduler/plugins/gpustability -count=1` | Passed with local-loopback permission | `ok`, package test time `1.125s`; run before this revalidation's documentation-only edit |
| 2026-07-20 | Phase 0 revalidation; empty `.git` | `env GOCACHE=/tmp/space-compute-go-build scripts/space-compute all` | Passed with local-loopback permission | Unit `0.042s`; race `1.157s`; `gofmt` clean; `go vet` clean; CPU-only HTTP integration `0.042s`; hardware conformance explicitly skipped because `K3S_GPU_TEST_METRICS_FILE` was unset, command otherwise passed in `0.021s` |
| 2026-07-20 | Phase 0 revalidation; empty `.git` | `env GOCACHE=/tmp/space-compute-go-build go test ./pkg/executor/embed -count=1` | Passed | Registration boundary compiled; package reports `[no test files]` |
| 2026-07-20 | Phase 1 worktree; empty `.git` | `gofmt -w pkg/scheduler/plugins/gpustability/config.go pkg/scheduler/plugins/gpustability/collector.go pkg/scheduler/plugins/gpustability/observability.go pkg/scheduler/plugins/gpustability/gpu_stability.go pkg/scheduler/plugins/gpustability/metrics_profiles.go pkg/scheduler/plugins/gpustability/gpu_stability_test.go pkg/scheduler/plugins/gpustability/phase0_test.go pkg/scheduler/plugins/gpustability/phase1_test.go && go test ./pkg/scheduler/plugins/gpustability -run '^$' -count=1` | Environment failure before source compile | Restricted sandbox could not write `/home/user/.cache/go-build` or use the downloaded Go 1.24.11 standard library; rerun with the required cache/toolchain permission |
| 2026-07-20 | Phase 1 worktree; empty `.git` | `go test ./pkg/scheduler/plugins/gpustability -run '^$' -count=1` | Introduced test compile failure, then fixed | Three test expressions called pointer method `Quantity.Value` on non-addressable map indexes; quantities were assigned to variables. Production code was unaffected. Corrected compile-only run passed: `ok`, `0.018s [no tests to run]` |
| 2026-07-20 | Phase 1 worktree; empty `.git` | `go test ./pkg/scheduler/plugins/gpustability -count=1` | Passed | First complete Phase 1 behavior run `ok`, `0.073s`; final post-hardening run is recorded below |
| 2026-07-20 | Phase 1 worktree; empty `.git` | `go test -v ./pkg/scheduler/plugins/gpustability -run '^$' -fuzz '^FuzzParseMetricsRejectsUntrustedInput$' -fuzztime=5s` | Passed | `17,440` executions, `44` new interesting inputs, package `6.064s` |
| 2026-07-20 | Phase 1 worktree; empty `.git` | `go test -v ./pkg/scheduler/plugins/gpustability -run '^$' -fuzz '^FuzzTypedArgsDecoder$' -fuzztime=5s` | Passed | `28,439` executions, `20` new interesting inputs, package `5.614s` |
| 2026-07-20 | Final Phase 1 worktree; empty `.git` | `gofmt -w pkg/scheduler/plugins/gpustability/collector.go pkg/scheduler/plugins/gpustability/gpu_stability.go pkg/scheduler/plugins/gpustability/metrics_profiles.go pkg/scheduler/plugins/gpustability/observability.go pkg/scheduler/plugins/gpustability/phase1_test.go && go test ./pkg/scheduler/plugins/gpustability -count=1 && go test -race ./pkg/scheduler/plugins/gpustability -count=1` | Passed with local-loopback/toolchain permission | Unit `ok`, `0.106s`; race `ok`, `1.368s` |
| 2026-07-20 | Final Phase 1 worktree; empty `.git` | `go test ./pkg/executor/embed -count=1` | Passed | Embed registration boundary compiled; package reports `[no test files]` |
| 2026-07-20 | Final Phase 1 worktree; empty `.git` | `scripts/space-compute all` | Passed with local-loopback/toolchain permission | Unit `0.073s`; race `1.327s`; `gofmt -s -l` clean; `go vet` clean; HTTP/TLS integration `0.060s`; parser fuzz `17,384` executions/`6.088s`; args fuzz `20,731` executions/`6.118s`; hardware conformance invoked and explicitly skipped because `K3S_GPU_TEST_METRICS_FILE` was unset (`0.018s`) |
| 2026-07-20 | Pre-Phase 2 worktree; empty `.git` | `go test ./pkg/scheduler/plugins/gpustability -count=1 && go test -race ./pkg/scheduler/plugins/gpustability -count=1` | Passed with local-loopback/toolchain permission | Required before-production-edit baseline: unit `ok`, `0.071s`; race `ok`, `1.315s` |
| 2026-07-20 | Phase 2 in progress; empty `.git` | `go test ./pkg/scheduler/plugins/gpustability -run '^$' -count=1` | Introduced test compile failure, diagnosed and fixed | Old Phase 1 tests referenced removed `snapshots`/`snapshotEntry` cache internals; they were migrated without changing expiry/eviction assertions. Corrected compile-only result: `ok`, `0.016s [no tests to run]` |
| 2026-07-20 | Phase 2 in progress; empty `.git` | `go test ./pkg/scheduler/plugins/gpustability -count=1` | Introduced characterization harness failure, diagnosed and fixed | Stale-policy setup had removed its generation record before publishing; the test now transitions the production snapshot store first. The first full Phase 2 behavior run then passed: `ok`, `0.162s` |
| 2026-07-20 | Phase 2 cancellation hardening; empty `.git` | `go test ./pkg/scheduler/plugins/gpustability -count=1` | Introduced harness hang, isolated and fixed; not counted as pass | A Phase 1 bounded-queue test constructed targets without the new UID/generation/profile-version identity, so workers correctly discarded them before its blocking transport signaled. The target fixtures were updated to valid production identities; assertions were unchanged |
| 2026-07-20 | Phase 2 cancellation-hardened worktree; empty `.git` | `go test ./pkg/scheduler/plugins/gpustability -count=1 -timeout=30s` | Passed | `ok`, package test time `0.180s`; includes the 100/1,000-target lifecycle tests and all Phase 0/1 regressions; the final post-observability run is recorded by `scripts/space-compute all` below |
| 2026-07-20 | Phase 2 cancellation-hardened worktree; empty `.git` | `go test -race ./pkg/scheduler/plugins/gpustability -count=1 -timeout=60s` | Passed | `ok`, package test time `1.709s`; includes concurrent target, worker/store, scheduler-read, and profile reload/parse coverage; the final post-observability race run is recorded below |
| 2026-07-20 | Final Phase 2 worktree; empty `.git` | `gofmt -s -l pkg/scheduler/plugins/gpustability/*.go && go vet ./pkg/scheduler/plugins/gpustability && bash -n scripts/space-compute && go test ./pkg/executor/embed -count=1` | Passed | Final post-edit run: formatting produced no paths; vet and script syntax were silent; embed registration boundary compiled with `[no test files]`; the source audit command below then found the sole production HTTP call in collector worker code and no scheduler callback I/O |
| 2026-07-20 | Final Phase 2 worktree; empty `.git` | `scripts/space-compute all` | Passed with local-loopback/toolchain permission | Final post-edit run: unit `0.172s`; race `1.711s`; formatting/vet clean; HTTP/TLS/profile integration `0.041s`; parser fuzz `17,936` executions/`6.067s`; args fuzz `19,498` executions/`5.850s`; optional hardware conformance explicitly skipped because `K3S_GPU_TEST_METRICS_FILE` was unset (`0.018s`) |
| 2026-07-20 | Final Phase 2 worktree; empty `.git` | `scripts/space-compute scale` | Passed with local-loopback/toolchain permission | Final post-edit lifecycle test `ok`, `0.084s`. Benchmark: linux/amd64, Go 1.24.11, Intel Core i7-6700 @ 3.40GHz, 8 logical benchmark CPUs; 100 targets `7,047,192 ns/op`, `4,853,968 B/op`, `67,231 allocs/op`; 1,000 targets `64,547,696 ns/op`, `45,836,152 B/op`, `663,334 allocs/op`; `-benchtime=1x` controlled shared `httptest.Server` fixture |

The source-level hot-path audit command was also run successfully:

```sh
rg -n 'client\.Do|http\.(Get|Post|Do)|collectTarget|refreshNode' pkg/scheduler/plugins/gpustability/*.go
```

It found the sole production HTTP invocation in `collector.go`; all direct
`refreshNode` callers were tests, and none were scheduler callbacks.

## Phase 1 changed files

- Production: `pkg/scheduler/plugins/gpustability/gpu_stability.go`,
  `metrics_profiles.go`, plus new `config.go`, `collector.go`, and
  `observability.go` in the same package.
- Tests: `gpu_stability_test.go`, `phase0_test.go`, and new `phase1_test.go` in
  the plugin package. Existing recorded `.prom` fixtures were retained.
- Harness: `scripts/space-compute`.
- Operator/API documentation:
  `docs/gpu-scheduler/kube-scheduler-gpu-stability.yaml` and
  `docs/gpu-scheduler/README.md`.
- Handoff: this `docs/space-compute/IMPLEMENTATION_STATUS.md`.
- No other visible file was intentionally changed. Git cannot verify a diff
  because the workspace `.git` directory is empty.

## Phase 2 changed files

- Production: `config.go`, `collector.go`, `gpu_stability.go`,
  `metrics_profiles.go`, and `observability.go`; new `profile_registry.go` and
  `snapshot_store.go` in `pkg/scheduler/plugins/gpustability/`.
- Tests: new `phase2_test.go`; migrated cache setup in `gpu_stability_test.go`
  and `phase1_test.go`; new recorded `dcgm.prom`, `rocm.prom`, and
  `vendorx.prom` fixtures plus `testdata/profiles/vendorx.json`.
- Harness: `scripts/space-compute` covers the standalone command in unit/race/
  static/integration modes, builds the binary, fuzzes workload intent, and adds
  100/1,000/5,000-Node callback benchmarks with a 500 microsecond gross
  regression budget per full node callback cycle.
- Operator/API documentation: `docs/gpu-scheduler/README.md` and
  `docs/gpu-scheduler/kube-scheduler-gpu-stability.yaml`.
- Handoff: this `docs/space-compute/IMPLEMENTATION_STATUS.md`.
- No branch operation or unrelated visible-file edit was made. Git still cannot
  verify a diff because `.git` is empty.

## Decisions and ADRs

- The final production path uses a dedicated `space-compute-scheduler`; normal
  K3s Pods remain on `default-scheduler`.
- Resource scheduling is separate from vGPU slicing.
- Scheduler hot-path callbacks consume asynchronous local snapshots only.
- Production telemetry remains exporter-based, with TianShu ZhiXin/Iluvatar
  `ix_*` as a first-class profile.
- CPU-only tests use recorded Prometheus exporter text through `httptest.Server`;
  no standalone device/orbit simulator is required.
- Node scrape addresses are dynamically resolved from watched Kubernetes Node
  data and validated exporter metadata; hard-coded node IPs are prohibited.
- Phase 3 retains Kubernetes extended resources as the compatibility allocation
  API because Phase 2 selected no DRA linkage. NodeResourcesFit/vendor plugins
  own allocation; gpustability does not implement Reserve or a second allocator.
- Fixed SLO-based normalization is used instead of NormalizeScore. Locality is
  expressed by typed preferred Node labels; existing Kubernetes NodeAffinity
  continues to own hard general placement constraints.
- Embedded-profile migration is ordered rather than concurrent: stop/remove the
  embedded space profile before starting the standalone process, because two
  process leader-election domains must not consume the same schedulerName.

Add future decisions here with links to ADRs. Do not silently reverse decisions
from `PROJECT.md`.

## Open risks/blockers

- Git/upstream provenance and any pre-existing local patch set cannot be
  established because `.git` is empty. This is an evidence limitation, not a
  code blocker; future handoffs must continue to preserve all visible
  files and avoid claiming a branch/diff state.
- Kubernetes 1.33 DRA integration details and enabled feature gates must be
  verified against the exact K3s build before implementation.
- This execution sandbox requires elevated local-loopback permission for Go
  `httptest.Server`; ordinary development/CI hosts that permit loopback do not
  need that exception.
- The host remains UID/GID `1000`; its installed system K3s is
  `v1.27.7+k3s2`/Go 1.20.10 and was not modified. Phase 3 instead built the
  repository source into an isolated `v1.33.7+k3s-` binary and used the
  supported `--rootless --disable-agent` control-plane path with separate
  ports/CIDRs/data. No supplied sudo password was used. Full kubelet/CRI image
  execution is intentionally not inferred from this agentless evidence.
- The Go module download cache is readable but partly owned by another user;
  some successful Go commands warn that a temporary `zapr` stat-cache file
  cannot be written. Builds/tests still exit zero and no dependency download is
  missing, but CI should use a writable module cache.

## Phase 1 characterization disposition

- All Phase 0 Phase 1-backlog cases now have passing correct-behavior coverage:
  Kubernetes 1.33 application/init/restartable-init/overhead demand, mixed class
  separation, request/limit handling, shared minimum-memory candidate sets,
  malformed/non-finite/range/identity telemetry, stale observation time,
  redirects/body bounds/timeout/cancellation, coalescing/queue/backoff/circuit,
  explicit strict/degraded/best-effort behavior, and missing-dimension penalties.
- Additional coverage verifies explicit resource mapping, wrong-type rejection,
  slow collectors not blocking callbacks, TLS trust, strict profile schemas,
  typed-config startup validation/precedence, the real Kubernetes scheduler YAML
  decoder, callback/cache concurrency, fragmentation-aware tight fit, and
  default/non-accelerator Pod isolation.
- No known P0/P1 production defect from the previous status is carried as an
  unacknowledged Phase 1 completion gap. Remaining limitations are the declared
  Phase 3/5 architecture and qualification items above.

## Phase 2 gate disposition

- Iluvatar `ix_*`, DCGM, ROCm, generic NPU, and configuration-only VendorX
  fixtures pass through `httptest.Server`, the production resolver/HTTP client,
  bounded parser/profile registry, collection manager, and unified store.
- InternalIP/family selection, endpoint/profile generation changes, UID
  deletion/recreation, invalid metadata cleanup, old-response discard, and
  in-flight cancellation have passing coverage. Auto-detection ambiguity,
  duplicate profiles, invalid reload/last-known-good, parser work bounds, and
  concurrent reload/store/worker paths are passing, including under `-race`.
- The 100/1,000 Node datasets retain every required target and validated
  snapshot with bounded configured workers, queues, cache entries, and a single
  manager ticker; deletion and shutdown empty manager/store state.
- The Phase 0 opt-out/default-profile regression and Phase 1 callback timing
  regression remain green while unavailable/slow collectors operate. The
  source audit still finds no HTTP invocation in `Filter`, `PreScore`, `Score`,
  or another scheduling/binding callback.
- No known Phase 2 production defect is hidden as a completion exception. The
  benchmark/qualification and allocation boundaries listed above remain honest
  limitations for Phase 3/5, not failed Phase 2 acceptance gates.

## Phase 3 gate disposition

- The component is a real upstream kube-scheduler command, not a custom binding
  loop. The standalone decoded configuration and Deployment ConfigMap contain
  only `space-compute-scheduler`; command tests retain the upstream config,
  leader-election, secure-serving, authentication, and authorization flags.
- A CPU-only integration constructs real Kubernetes scheduler instances over a
  shared fake API and Node informer. With only the default scheduler running, an
  ordinary Pod binds, a space Pod remains unbound, and the exporter sees zero
  requests. Starting the space scheduler causes production collection/parsing/
  snapshot publication and real framework binding. Stopping and recreating the
  space scheduler rebuilds informers and snapshots; two simultaneously pending
  one-device workloads produce one binding and no overcommit.
- The snapshot worker notifies only locally tracked Pods for its Node through
  `PodActivator`; unrelated snapshots and Node metadata return `QueueSkip`.
  Tracking has a validated count bound and TTL and passes `-race`. Framework
  callbacks remain snapshot-only, and the benchmark's forbidden transport sees
  zero calls across 100/1,000/5,000 Nodes.
- Strict physical-device policy is conservative and allocation-honest: hard
  thresholds require complete allocatable coverage because extended resources
  do not expose the vendor-selected physical identity. Degraded/best-effort
  policy caps this signal. No Reserve interface or fixture-success allocator is
  present.
- A source-built `v1.33.7+k3s-` ephemeral K3s control plane reached `/readyz`.
  A durable CPU-only test then served recorded Iluvatar `ix_*` text from a Go
  `httptest.Server`, proved that the embedded default scheduler bound ordinary
  Pods while the space component was absent/down, launched two production
  `space-compute-scheduler` processes, observed a real Lease holder and standby
  takeover, exercised HTTPS `/livez` and leader `/readyz`, bound space Pods
  through the real API, and rejected a second one-device workload through
  Kubernetes extended-resource accounting. The manifest applied and deleted
  cleanly; impersonation checks allowed binding, Node reads and Lease creation
  while denying Secret reads. Phase 3 is complete and Phase 4 is permitted.

## Phase 3 commands and exact results (2026-07-20)

Pre-change evidence:

| Command | Result |
|---|---|
| `go test ./pkg/scheduler/plugins/gpustability -count=1` | Passed, package `0.199s` |
| `go test -race ./pkg/scheduler/plugins/gpustability -count=1` | Passed, package `1.718s` |

Final repeatable evidence:

| Command | Result |
|---|---|
| `scripts/space-compute unit` | Final pass: plugin `1.037s`; command `0.035s` |
| `scripts/space-compute race` | Final pass: plugin `3.162s`; command `1.180s` |
| `scripts/space-compute static` | Passed: gofmt produced no paths and `go vet` exited 0; non-fatal read-only module stat-cache warning occurred |
| `scripts/space-compute integration` | Passed: production-pipeline and real scheduler integration `0.898s`; command/config/manifest tests `0.033s` |
| `scripts/space-compute fuzz` | Passed: metric parser 2,705 executions/5 new interesting/130 total; typed args 5,633/2/117; workload intent 1,996/1/4 |
| `scripts/space-compute hardware` | Passed with explicit skip: `K3S_GPU_TEST_METRICS_FILE is not set`; package `0.023s` |
| `scripts/space-compute scale` | Passed. Collector: 100 `6.286483ms`, 4,918,760 B, 67,480 allocs; 1,000 `61.581581ms`, 45,846,608 B, 663,738 allocs. Scheduler callback cycle: 100 `2.296608ms`/22,966 ns per Node, 1,198,816 B/9,807 allocs; 1,000 `26.969376ms`/26,969 ns, 12,194,536 B/98,035; 5,000 `120.998391ms`/24,200 ns, 60,129,304 B/490,043 |
| `go build -buildvcs=false -trimpath -o /tmp/space-compute-scheduler ./cmd/space-compute-scheduler && /tmp/space-compute-scheduler --version` | Passed: `Kubernetes v1.33.7-k3s1`; binary 105,752,451 bytes, SHA-256 `a4cd9b244dcb12f636f77ddc31c072f097ef976317c638b3383499752a4e0ba9` |
| `go mod tidy -diff` | Passed with no diff |
| `bash -n scripts/space-compute` and final gofmt listing | Passed; no output |
| `id && /usr/local/bin/k3s --version` | UID/GID 1000; installed K3s `v1.27.7+k3s2`, Go 1.20.10 (version-incompatible for this e2e) |
| `sudo -n true` | Failed as environmental evidence: exit 1, password required; no cluster mutation attempted |

Phase 3 completion qualification on the same date:

| Command | Result |
|---|---|
| `go build -buildvcs=false -trimpath -o /tmp/k3s-phase3 ./cmd/k3s` and `/tmp/k3s-phase3 --version` | Passed; source binary reports `dev (HEAD)`, Go 1.24.11 because usable Git metadata is absent. The running API reported `v1.33.7+k3s-`, empty Git commit, Go 1.24.11. Binary: 1,227,768,176 bytes, SHA-256 `484a962e3207132161161c8c089c9d34347aaaa9b755168146e83e1af240e05b` |
| `go build -buildvcs=false -trimpath -o /tmp/space-compute-scheduler-phase3 ./cmd/space-compute-scheduler` and `--version` | Passed: `Kubernetes v1.33.7-k3s1`; 105,750,707 bytes; SHA-256 `d80ef0251466fa980f9b50d047209ce4b04d40917e996ba762e29a30461e497b` |
| `/tmp/k3s-phase3 server --rootless --disable-agent --node-ip=192.168.0.100 --data-dir=/tmp/space-compute-phase3-e2e.thdh7X/data8 --write-kubeconfig=/tmp/space-compute-phase3-e2e.thdh7X/kubeconfig-final-clean.yaml --write-kubeconfig-mode=600 --bind-address=0.0.0.0 --advertise-address=192.168.0.100 --https-listen-port=16443 --cluster-cidr=10.242.0.0/16 --service-cidr=10.243.0.0/16 --cluster-dns=10.243.0.10 --flannel-backend=none --egress-selector-mode=disabled --kube-scheduler-arg=secure-port=11259 --kube-controller-manager-arg=secure-port=11257 --kube-controller-manager-arg=allocate-node-cidrs=false --disable=coredns --disable=servicelb --disable=traefik --disable=local-storage --disable=metrics-server --disable-network-policy --disable-kube-proxy --disable-cloud-controller --disable-helm-controller` | Passed with local socket/bind permission. `/readyz` returned `ok`; embedded default scheduler used isolated port 11259. Agentless warning about aggregation/webhooks was expected because this test does not exercise those APIs |
| `SPACE_COMPUTE_E2E_KUBECONFIG=/tmp/space-compute-phase3-e2e.thdh7X/kubeconfig-final-clean.yaml SPACE_COMPUTE_E2E_SCHEDULER_BINARY=/tmp/space-compute-scheduler-phase3 scripts/space-compute cluster-e2e` | Final pass: `TestIndependentSchedulerAgainstK3s`, 19.81s; package 19.825s. Real API/default scheduler, two external processes, real Lease failover, probes, exporter-backed binding, outage behavior and no-overcommit all passed |
| Same external-cluster test via `go test -race ./tests/integration/spacecomputescheduler -run '^TestIndependentSchedulerAgainstK3s$' -count=1 -v` | Passed: test 18.95s, package 19.999s; no race reported |
| `kubectl apply -f docs/gpu-scheduler/manifests/space-compute-scheduler.yaml`, wait 2s, impersonated `kubectl auth can-i`, then `kubectl delete -f ... --wait=true --timeout=30s` | Passed. ServiceAccount/ClusterRole/ClusterRoleBinding/Role/RoleBinding/ConfigMap/Deployment/Service created and deleted. ServiceAccount could create `pods/binding`, get Nodes and create the leader Lease; it could not get Secrets. `/readyz` remained `ok` after uninstall |
| `scripts/space-compute unit` | Passed: plugin 0.950s; command 0.038s; external-cluster harness compile/explicit-env skip 0.013s |
| `scripts/space-compute race` | Passed: plugin 3.532s; command 1.161s; external-cluster harness compile/explicit-env skip 1.052s |
| `scripts/space-compute static` | Passed: gofmt produced no paths and vet exited 0 for plugin, command and cluster harness; the known non-fatal read-only `zapr` stat-cache warning occurred |
| `scripts/space-compute integration` | Passed: production pipeline/framework 0.917s; command/config/manifests 0.031s; external-cluster harness compile/explicit-env skip 0.010s |
| `scripts/space-compute scale` | Passed. Lifecycle 0.148s. Collector: 100 `6.721202ms`, 4,976,856 B, 67,673 allocs; 1,000 `62.204118ms`, 45,857,024 B, 663,516 allocs. Callback cycle: 100 `2.466896ms`/24,669 ns per Node, 1,198,912 B/9,809; 1,000 `24.261099ms`/24,261 ns, 12,193,232 B/98,029; 5,000 `118.652080ms`/23,730 ns, 60,092,168 B/490,039. Host: linux/amd64, Intel i7-6700 @ 3.40GHz, 8 benchmark CPUs, `-benchtime=1x` |
| `scripts/space-compute fuzz` | Passed: parser 4,356 executions/4 new/134 total; typed args 5,452/1/118; workload intent 7,113/18/22 |
| `scripts/space-compute hardware` | Passed with explicit skip: `K3S_GPU_TEST_METRICS_FILE is not set`; package 0.024s. CPU-only recorded exporter coverage did run in unit/integration/e2e |
| `go mod tidy -diff` and `bash -n scripts/space-compute` | Passed with no diff/output |

Qualification diagnostics were kept separate from final passes. An unprivileged
sandboxed K3s attempt could not create/chmod the Kine Unix socket; the same
isolated binary was therefore run with approved local bind/socket permission.
The first new e2e run failed before scheduler startup on a Node status
resourceVersion conflict; the harness adopted client-go's standard
`RetryOnConflict` and then passed without changing scheduling assertions. Early
control-plane trials exposed a loopback advertise-address warning and agentless
node-IPAM noise; the documented final topology uses the validated host address
and disables node CIDR allocation only for the no-kubelet fixture. No production
scheduler defect was hidden or carried. The user-provided sudo password was not
used or persisted.

The pre-change suite was green. Interim failures introduced during implementation
(a map-value `Quantity.Cmp` compile error, old fixture allocatable/telemetry
inconsistency exposed by conservative coverage, and a VCS-stamping build failure
caused by empty `.git`) were diagnosed and fixed without weakening assertions or
production behavior. No pre-existing production test failure is being carried.

## Phase 3 files changed

- Component/build: `cmd/space-compute-scheduler/main.go`, its tests,
  `Dockerfile.space-compute-scheduler`, `Makefile`, `scripts/space-compute`, and
  `go.mod` (the JSON logging path's indirect `zapr` requirement). `go.sum` did
  not change.
- Plugin production: `collector.go`, `config.go`, `gpu_stability.go`,
  `observability.go`, `queueing.go`, and `workload_intent.go`.
- Tests: `gpu_stability_test.go`, `phase1_test.go`, `phase3_test.go`,
  `phase3_integration_test.go`, and the real external-cluster harness
  `tests/integration/spacecomputescheduler/space_compute_scheduler_int_test.go`.
- Operator assets: `docs/gpu-scheduler/README.md`, the standalone
  `kube-scheduler-gpu-stability.yaml`, Deployment/RBAC/Service configuration and
  static-Pod template under `docs/gpu-scheduler/manifests/`.
- Handoff: this status file. No generated binary remains in the repository.

## Phase 4 outcome

Phase 4 is complete. It was implemented without branch operations, default-
scheduler coupling, vGPU allocation, a link/orbit simulator, scheduler-path
remote I/O, future waiting in callbacks, test weakening or unrelated-file
discard.

- Added `spacecompute.k3s.io/v1alpha1` Go APIs and structural CRDs for directed
  link/contact snapshots, exporter-derived domain resource summaries,
  namespaced mission intent and durable placement intent. Fixed-unit validation
  covers identity, SHA-256 provenance, sequence, observation/validity, clock
  skew, overlapping/impossible windows, bounds, capability alternatives,
  deadline/duration/safety contradictions, migration/checkpoint constraints,
  workload templates and one-active-execution policy.
- Added fail-closed admission for contradictory mission intent and authenticated
  immutable reporter identity. Cross-domain reporter RBAC is intentionally
  unbound. Planner RBAC cannot read Secrets or bind Pods; the independent
  scheduler retains binding ownership.
- Added the resource-controller acceptance boundary: link observations record a
  capped acceptance/rejection history and reject replay/too-fast unchanged
  updates. Real API generations enter planning only when matching
  resource-controller conditions and accepted link sequence are true. Rejected
  current generations cannot bypass status by being syntactically valid.
- Added a deterministic global planner that selects domain and epoch before Pod
  scheduling, fits input/compute/return against guarded windows, expires plans,
  hashes all material inputs, and ranks fixed scores for completion, locality,
  link risk, energy/thermal, resilience and fragmentation. It consumes no
  fabricated future link and persists structured rejection/score explanations.
- Added dynamic-CRD optimistic persistence, bounded work queues/workers,
  informer watches, leader election, health/readiness and restart/idempotency.
  Placement uses mission UID, plan ID, attempt and material digest. Duplicate or
  out-of-order observations cannot regress state.
- Added a workload controller for store-and-forward waiting, deterministic
  attempt Pod creation, UID/plan fencing, checkpoint/retry/migration policy and
  explicit result-return tracking. Checkpointable partition recovery replans
  only after fencing/checkpoint; non-checkpointable execution fails without a
  duplicate Pod.
- Added informer-local Node projection and Phase 4 scheduler feasibility for
  planned domain, current epoch, expiry, exact link sequence/window and snapshot
  freshness. Strict/degraded/best-effort stale behavior is explicit. Scheduler
  callbacks clone immutable cycle state and issue no API, exporter or remote
  calls and perform no long wait.
- Extended fixed scheduler scoring with predicted completion, link risk and
  resilience while retaining backward-compatible explicit old scoring configs.
  Shipped configs use eleven weights totaling 100. Queue hints wake only Pods
  affected by domain/projection/placement changes.
- Added bounded planner metrics for latency, active reconciliations, replan
  reason, deadline slack, snapshot age, link-risk class and error stage;
  conditions/Events; security, retention and clock assumptions; troubleshooting
  runbook; and the accepted ownership/state-machine ADR.
- Added golden workload, Kubernetes Node, recorded link and expected decision
  inputs. The first-class Iluvatar `ix_*` exporter fixture continues through the
  production parser/collector. CPU-only tests use the production CRDs,
  controllers, projections and scheduler plugin with injectable clocks, not a
  separate simulator or validation bypass.

## Phase 4 scenario coverage

- LEO, GEO/high-orbit and ground candidates with changing directed windows;
  higher GEO compute loses to earlier/data-near LEO total completion.
- Input transfer fit and window-close failure; execution fit with required
  result-return deadline failure.
- Strict energy/thermal rejection and degraded penalty.
- Strict/degraded/best-effort stale projection and missing exact-window
  behavior; no future availability is inferred.
- Inclusive/exclusive boundary and configured clock-skew guards.
- Checkpointable partition/replan and non-checkpointable terminal failure.
- Planner/workload restart, duplicate intent, deterministic attempt naming,
  duplicate/out-of-order observations and one-active-Pod cross-domain fence.
- Ordinary K3s workload independence and deterministic replay of reversed input
  ordering with identical plan, score and explanation order.
- Full CPU-only flow on a real K3s 1.33 API: production exporter snapshot ->
  accepted link/resource CRDs -> durable plan -> deterministic attempt Pod ->
  independent scheduler Binding -> terminal placement status.

## Phase 4 commands and exact results (2026-07-21)

Pre-change stability evidence:

| Command | Result |
| --- | --- |
| `scripts/space-compute unit` | Passed with approved loopback permission: plugin `1.034s`, scheduler command `0.038s`, existing external harness compile/skip `0.012s` |
| `scripts/space-compute race` | Passed: plugin `6.245s`, scheduler command `1.162s`, external harness compile/skip `1.054s` |

Final repeatable evidence:

| Command | Result |
| --- | --- |
| `scripts/space-compute unit` | Passed all nine affected packages: plugin `1.158s`, scheduler command `0.052s`, planner command `0.037s`, API `0.012s`, kube adapter `0.033s`, planner `0.015s`, local policy `0.013s`, workload `0.013s`, external harness compile/skip `0.015s` |
| `scripts/space-compute race` | Passed all nine packages: plugin `3.364s`, scheduler command `1.204s`, planner command `1.130s`, API `1.044s`, kube adapter `1.098s`, planner `1.085s`, local policy `1.046s`, workload `1.070s`, external harness compile/skip `1.063s` |
| `scripts/space-compute static` | Passed: gofmt listed no paths and `go vet` exited 0 |
| `scripts/space-compute integration` | Passed: plugin production/framework flow `0.984s`; both commands and all four Phase 4 packages passed; external harness compile/skip `0.015s` |
| `scripts/space-compute scenarios` | Passed: API `0.022s`, kube `0.018s`, planner `0.010s`, policy `0.005s`, workload `0.010s`, scheduler Phase 4 flow `0.222s` |
| `scripts/space-compute fuzz` | Passed six 5-second targets: metric parser 3,138 executions/3 new/137 total; typed args 5,595/16/134; workload intent 6,154/26/48; link validation 15/0/1; mission validation 18/0/1; Pod mission projection 15/0/1 |
| `scripts/space-compute scale` | Passed. Collector: 100 `6.398192ms`, 4,862,368 B, 67,181 allocs; 1,000 `61.750772ms`, 45,931,880 B, 663,730 allocs. Scheduler callbacks: 100 `2.223319ms`/22,233 ns per Node, 1,216,416 B/9,807 allocs; 1,000 `24.254854ms`/24,255 ns, 12,406,272 B/98,033; 5,000 `119.791074ms`/23,958 ns, 61,157,368 B/490,054 |
| `scripts/space-compute hardware` | Passed with explicit skip: `K3S_GPU_TEST_METRICS_FILE is not set`; package `0.022s`. Recorded exporter coverage did run in unit/integration/e2e |
| `go mod tidy -diff` and `bash -n scripts/space-compute` | Passed with no diff/output |
| Scheduler/planner builds in `/tmp` | Scheduler reports `Kubernetes v1.33.7-k3s1`, 106,075,611 bytes, SHA-256 `cb60d81ee79496564055138b1a23a878962fcf07564c2eec5c37a2d7757331d1`. Planner 73,963,419 bytes, SHA-256 `10c0d27509f6c97dfd80771bf44bff46785bc3bfdec9534e9a11bc61266fd7e9` |

Real API/control-plane qualification used the source-built K3s 1.33 binary from
Phase 3 with an isolated `/tmp/space-compute-phase4-e2e.wpRRBe` datastore,
port 17443, separate service/cluster CIDRs and no agent. The system-installed
K3s 1.27 server was not modified. The isolated process was stopped cleanly;
its datastore remains in `/tmp` for audit/replay.

| Command or check | Result |
| --- | --- |
| K3s `/readyz`, CRD apply and `Established` wait | Passed for all four structural CRDs on API `v1.33.7+k3s-` |
| Admission positive/negative objects | Valid link and mission accepted; forged reporter denied; contradictory duration mission denied by CRD CEL; admission policy type checking reported no errors |
| Planner manifest server dry-run and real apply | Passed for ServiceAccount, least-privilege roles/bindings, two-replica Deployment and Service. Impersonation denied Secret reads and allowed placement creation/Node projection |
| Production planner manual reconciliation and restart | Created one durable plan and one `valid-mission-attempt-1` Pod, projected 1,112-byte Node state, recorded accepted link history and Events; restart retained exactly one plan/Pod and the same plan ID |
| `scripts/space-compute cluster-e2e` | Passed both tests together after scoped zero-grace agentless cleanup: Phase 4 full flow `25.84s`, Phase 3 independence/failover regression `15.41s`, package `41.276s` |
| Final accepted-generation Phase 4 e2e | Passed `25.65s`; production controller/scheduler binaries and real CRDs/API bound and completed the mission |
| Final accepted-generation Phase 4 e2e with `-race` | Passed test `27.67s`, package `28.739s`; no race reported |
| Both external tests with `-race` before the final conservative acceptance check | Passed Phase 4 `25.41s`, Phase 3 `14.98s`, package `41.441s`; the final planner package race gate above also includes the accepted-generation code |

The non-fatal read-only module `zapr` stat-cache warning recurred in some Go
commands; build/test exit status remained 0 and `go mod tidy -diff` remained
empty.

## Phase 4 diagnostic disposition

- An unprivileged `httptest`/K3s run could not bind local sockets. Required
  commands were rerun with approved local bind permission and passed. One later
  unprivileged scenario invocation reproduced that same environmental panic;
  the immediately repeated elevated command passed all scenarios.
- The first isolated server used `--disable=cloud-controller`, which did not
  disable K3s's built-in cloud controller and collided with the host's port
  10258. It exited before manifest mutation. The documented
  `--disable-cloud-controller` agentless command then passed.
- First real API validation rejected explicit `additionalProperties: false`
  combined with named schema properties. The structural CRDs were corrected and
  a manifest regression guard was added. All four then became Established.
- The first admission condition treated absent `request.subResource` as a
  string and failed closed on valid creates. Base-resource rules already exclude
  status subresources, so the redundant condition was removed; positive and
  negative admission cases then passed.
- Sequential external tests initially reused a Terminating namespace while
  no kubelet was available to complete normal Pod grace periods. Teardown now
  zero-grace deletes only fixture Pods, waits for actual namespace/Node removal,
  and both external tests pass together. No production behavior or assertion
  was weakened.
- Final audit found planner input needed the resource-controller accepted
  generation/sequence, not only fresh syntactic validation. Conservative
  generation/status gating and rejection tests were added, then unit/race/live
  e2e gates were rerun successfully.

## Phase 4 files changed

- API/controllers: `contrib/space-compute/pkg/apis/v1alpha1`, `pkg/planner`,
  `pkg/policy`, `pkg/workload` and `pkg/kube`.
- Components/build: `cmd/space-compute-mission-planner`,
  `Dockerfile.space-compute-mission-planner`, `Makefile` and
  `scripts/space-compute`.
- Scheduler: `config.go`, `gpu_stability.go`, `queueing.go`, Phase 4 unit/full
  integration tests, and shipped standalone scoring configurations.
- E2E/fixtures: real external Phase 4 harness, safer agentless cleanup, golden
  workload/Node/link/decision data and retained Iluvatar exporter text.
- Operations: Phase 4 CRDs, admission, planner RBAC/Deployment/Service,
  ownership ADR, API/state/operations/runbook documentation and this handoff.
  The security remediation updates `go.mod`/`go.sum`; no generated binary was
  written into the repository.

## Phase 5 qualification outcome (2026-07-21)

**Release classification: Not ready.** Neither `Ready for CPU-only functional
release` nor `Ready for vendor hardware release` is permitted. The scoped
CPU-only implementation gates and the remediated vulnerability gate pass, but
required compatibility/transport/full-agent, API and hardware evidence is
missing. Hardware qualification was explicitly not run.

### Phase 5 security-remediation evidence (2026-07-21)

- Root cause: explicit K3s replace directives selected `golang.org/x/net`
  v0.38.0, `x/crypto` v0.36.0 and `x/sys` v0.31.0 despite newer requirements;
  OTel core/SDK modules were split across 1.37/1.38; the Go 1.24.11 runtime
  exposed the standard-library findings. These were dependency/toolchain
  defects, not scheduler policy behavior.
- Production fix: `go.mod`/`go.sum` now require Go 1.25.12, x/net v0.55.0,
  x/crypto v0.53.0, x/sys v0.45.0, gRPC-Go v1.79.3 and
  the OTel 1.40.0 release line. Collector Node host validation performs an
  IDNA round-trip and rejects ASCII-only Punycode labels.
- Regression: `go test ./pkg/scheduler/plugins/gpustability -run TestExporterTargetRejectsASCIIOnlyPunycodeHost -count=1` passed (`0.024s`) through production target resolution.
- Affected unit tests passed for all nine packages; affected race tests passed
  for all nine packages; integration harness, gofmt, go vet, module verification
  and tidy-diff passed under official Go 1.25.12.
- Post-remediation `scripts/space-compute all` under the same toolchain passed
  unit, race, static, integration, scenarios, fuzz, scale and component builds;
  the full K3s binary also built successfully (`/tmp/k3s-phase5-go125`, Go
  1.25.12, SHA-256 `249255e99431cb4710dae57fce2441914b5d29cf70ae6d74f55e0facd10887cb`).
- Patched govulncheck exited 0 with `No vulnerabilities found` (zero
  imported-package and one module-only non-reachable OpenPGP advisory was
  informational). The former 22 reachable findings are closed; this does not
  close the independent release blockers below.

### Production changes

- Hardened the Iluvatar `ix_*` adapter for deterministic multi-device identity,
  label/model consistency, duplicate fields, memory arithmetic and bounded
  devices (`maxDevicesPerNode`, default 256). Added an exact two-GPU fixture.
- Background collection now targets only Nodes with a mapped positive physical
  resource or explicit exporter metadata. Multiple Agent endpoints are
  generation-isolated, duplicate Node endpoint ownership fails closed, and an
  ordinary or schedulable K3s master consumes no collector slot unless it
  actually advertises the accelerator/exporter.
- Added O(1) resource-event coalescing, named rate-limited queues, a 15-retry
  budget, API-write/queue/retry metrics, and local old-attempt Pod deletion and
  UID/owner fencing before retry.
- Bounded mission templates at 64 KiB; strengthened capability, resource
  summary, provenance and digest validation; added fuzz targets.
- Split admission into four type-safe policies. Live K3s evidence confirms all
  observed generations have zero type warnings, forged reporters are rejected,
  and only the planner SA writes placements.
- Removed the embedded K3s plugin hook and unsafe administrator-kubeconfig
  static-Pod example. Hardened production Deployments with fixed non-root
  UID/GID, RuntimeDefault seccomp, read-only filesystems, dropped capabilities
  and CPU/memory bounds.
- Added the opt-in physical Iluvatar suite, 100/1,000/5,000 scale datasets,
  production-manifest self-installing e2e and all Phase 5 reports.
- Remediated the 22 reachable govulncheck findings: the K3s module graph now
  uses `x/net` v0.55.0, `x/crypto` v0.53.0, `x/sys` v0.45.0, gRPC-Go v1.79.3
  and the OTel 1.40.0
  line, while `go.mod` pins the official Go 1.25.12 toolchain. Exporter Node
  host validation rejects ASCII-only Punycode labels through an IDNA round-trip.
  The production-path regression and the rerun govulncheck both pass.

### Exact final evidence

- `env GOCACHE=/tmp/space-compute-phase5-gocache scripts/space-compute all`:
  PASS. Unit, race, gofmt/go vet, integration, scenarios, eight fuzz targets,
  scale and builds all exited 0. Final key times: plugin unit `1.270s`, race
  `3.667s`; scheduler 5,000-node callbacks `122.117ms` (`24.423us/node`);
  planner 5,000-domain worst `62.424ms`; collector 5,000 targets `916.426ms`,
  233.92 MB temporary allocations.
- Current-tree K3s build: PASS, 1,227,768,176 bytes, SHA-256
  `484a962e3207132161161c8c089c9d34347aaaa9b755168146e83e1af240e05b`,
  byte-identical to the verified K3s baseline binary.
- Final isolated real K3s e2e: PASS, Phase 4 production API/admission/RBAC ->
  planner -> exporter -> independent scheduler -> Binding `32.00s`; ordinary
  scheduling/failover `15.28s`; package `47.308s`. Same datastore restart and
  duplicate install also passed (`46.284s`).
- Server-side manifest dry-runs and uninstall passed. After custom API removal,
  ordinary/independent scheduling passed `15.37s`. All four CRDs stored only
  `v1alpha1`; all admission policies were observed with no warnings. Planner SA
  can create placements and cannot read Secrets.
- `go mod verify` and `go mod tidy -diff`: PASS.
- Security remediation: official Go 1.25.12 `govulncheck` over all affected
  packages exited 0 with `No vulnerabilities found`; the focused Punycode
  regression, affected-package unit tests, race tests, integration harness,
  `go vet`, module verification and tidy-diff all passed under the patched
  graph.
- `scripts/space-compute hardware`: two explicit skips because no live metrics,
  physical K3s/device-plugin cluster, representative Pod or expected device ID.
- The pre-remediation govulncheck failure (22 reachable findings in Go 1.24.11,
  x/net v0.38.0 and OTel SDK v1.38.0) is closed by the dependency/toolchain
  update; no finding was suppressed.
- Targeted Trivy source secret scans passed. Manifest review retained only
  intentional system-namespace and scheduler/planner Pod ownership warnings.
  Repository golangci-lint is incompatible with current Go export data and is
  recorded as not passed.

### Remaining blockers

Critical blocker is the absence of a shipped, qualified authenticated
cross-domain transfer/result/fencing agent. High blockers are full-agent
K3s/kubelet/CNI/CRI, supported patch upgrade/rollback, physical Iluvatar/device-
plugin execution and PROJECT canonical resource/mission API gaps. Multi-hour
soak, external chaos and supply-chain image evidence are also missing. See
`PHASE5_TRACEABILITY.md`, `PHASE5_TEST_REPORT.md`,
`PHASE5_PERFORMANCE.md`, `PHASE5_SECURITY.md`,
`PHASE5_COMPATIBILITY.md`, `PHASE5_OPERATIONS.md` and
`PHASE5_CHAOS.md` and `PHASE5_RISK_REGISTER.md`.

## Next action

Phase 5 remains the active release-qualification phase. The Go/dependency
security baseline is patched and verified. Implement/qualify the cross-domain
transport and remote execution fence, close the API traceability gaps, and run
full-agent patch-upgrade plus physical Iluvatar suites. Phase 6 or a production
release is not permitted until every mandatory gate passes.

## Handoff template

At the end of each phase, replace or append the following:

```text
Phase:
Outcome:
Production files changed:
APIs/configuration changed:
Tests added:
Commands actually run and results:
Hardware-dependent tests skipped and why:
Known failures or limitations:
Security/concurrency considerations:
Compatibility evidence:
Decisions/ADRs:
Next permitted phase or task:
```
