# PROJECT.md traceability matrix

Status meanings: **Pass** has production code plus executed evidence; **Partial**
has useful implementation but not the entire requirement; **Gap/Not run** blocks
completion. Paths are repository-relative.

| PROJECT requirement | Production implementation | Test/evidence | Documentation | Status |
| --- | --- | --- | --- | --- |
| 1: discover/normalize CPU, GPU, NPU, FPGA, DSP/AI | profile registry and normalized `deviceMetrics`; built-ins Iluvatar/DCGM/ROCm/generic | fixture/parser tests | GPU README | Partial: canonical adapters/fields incomplete |
| 1: unified time-aware inventory/provenance/freshness | snapshot store; link/resource CRDs and validation | Phase 2/4/5 tests, real CRDs | Phase 4 API | Pass for implemented summaries; canonical inventory partial |
| 1: exporters primary, K8s APIs static | collector + Node/resource accounting | production HTTP fixture tests | GPU README | Pass |
| 1: deterministic scheduling from physical/dynamic constraints | scheduler plugin/policy projection | golden scores, deterministic replay | Phase 4 API | Pass within declared fields |
| 1: global planning separate from local placement | planner/workload controllers vs standalone scheduler | Phase 4 scenarios/e2e | ADR 0001 | Pass |
| 1: extended resource/DRA/device-plugin integration | upstream NodeResourcesFit/DynamicResources ownership | no-overcommit + DRA Skip test | allocation boundary | Partial: no physical identity evidence |
| 1: no vGPU slicing | no allocator/Reserve implementation | interface assertion | ADR/README | Pass |
| 2 in-scope discovery/inventory/policy/failure modes/fixtures | corresponding packages and CRDs | unit/integration/scenario | Phase 4 API | Partial where gaps below apply |
| 2: independent scheduler beside default | standalone command/profile/lease; no K3s embed import | repeated real K3s e2e | compatibility report | Pass |
| 2: physical requests via Kubernetes | extended resources; DRA left upstream | no-overcommit/DRA tests | README | Pass honestly; hardware Not run |
| 2: link/deadline/energy/data policy | Phase 4 API/planner/local policy | mandatory scenarios | Phase 4 API | Pass |
| 2: disconnected/degraded explicit state | phases, policies, retry/checkpoint state | partition/restart/stale tests | ADR/runbook | Partial: no WAN transport |
| 2 out-of-scope prohibitions (slicing/runtime/kubelet/etcd fabrication/simulator) | none introduced | code/diff/e2e inspection | ADR/reports | Pass |
| 3.1 K3s opt-in isolation | no embed hook; schedulerName only | absent/enabled/uninstall real e2e | compatibility | Pass |
| 3.2 control-loop ownership | planner/resource/workload/scheduler boundaries | restart/retry/e2e tests | ADR 0001 | Pass locally; remote fence gap |
| 3.3 no hot-path I/O/wait | immutable cache/projection reads | callback/HTTP request tests | README/ADR | Pass |
| 3.4 exporter registry/discovery/no hard-coded IP | atomic profiles; watched Node addresses/metadata | address/lifecycle/multi-node tests | README | Pass; adapters incomplete |
| 3.5 hard resource/type/assignability | resource mapping + conservative coverage | strict coverage/no-overcommit | README | Partial: no allocation linkage |
| 3.5 architecture/model/precision/software | mission capabilities/resource summary | API/planner tests | Phase 4 API | Pass for summary model |
| 3.5 memory/storage capacity | GPU free memory only | parser/filter tests | README | Gap for workload working storage/memory |
| 3.5 trust/security constraint | Node labels/software/provenance admission | policy/admission e2e | security report | Partial: no canonical node trust field |
| 3.5 snapshot/contact/deadline feasibility | planner and local policy | boundary/scenario tests | Phase 4 API | Pass |
| 3.5 soft utilization/queue/thermal/energy/completion/link/locality/fragmentation/resilience | fixed score components | golden and scale tests | score tables | Pass |
| 3.6 strict/degraded/best-effort | typed policies, never fabricate missing windows | stale/link scenario tests | Phase 4 API | Pass |
| 4.1 device stable ID/class/vendor/model/arch | exporter-normalized ID/model/class/profile | two-GPU accuracy tests | README | Partial: vendor not canonical, only selected profiles |
| 4.1 topology/capacity/memory/bandwidth/precision/software/health/provenance/time/confidence | split across Node, exporter snapshots and domain summary | parser/API tests | API docs | Gap: no full canonical physical-device record |
| 4.2 domain/orbit identity | DomainReference and Node labels | planner/e2e | Phase 4 API | Pass |
| 4.2 CPU/memory/storage/topology/software/trust/energy/autonomy | resource summary + Node static data | selected planner tests | Phase 4 API | Partial; several canonical fields absent |
| 4.3 versioned link/contact model | SpaceLinkSnapshot | validation/fuzz/scenarios/real CRD | Phase 4 API | Pass |
| 4.4 required/alternative capability + software | SpaceMission | validation/planner tests | Phase 4 API | Pass |
| 4.4 working memory/storage | none explicit | none | risk R7 | Gap |
| 4.4 I/O size/location/deadline/duration/priority/class | SpaceMission | scenario/e2e | Phase 4 API | Pass |
| 4.4 minimum bandwidth/maximum latency | no mission fields | link model only | risk R7 | Gap |
| 4.4 checkpoint/migration/retry/return/policy | SpaceMission and controllers | state/restart/partition tests | ADR/runbook | Pass locally; remote transport gap |
| 5 scheduling decision/explanations/units | deterministic planner + scheduler explanations | golden/replay/scale | Phase 4 API | Pass |
| 6 untrusted validation and identity | parser/API/CEL/RBAC | fuzz, forged reporter e2e | security report | Pass for implemented inputs |
| 6 size/range/time/monotonic/NaN/windows | limits and validation | unit/fuzz/boundaries | Phase 4 API | Pass |
| 6 bounded concurrency/cache/queue/retry/log labels | collector/controller bounds | race/scale/retry tests | performance/security | Pass; API quotas assumed |
| 6 backoff/circuit/redirect/SSRF | collector | integration tests | README/security | Pass |
| 6 explicit expiry/restart/duplicates | planner/controller state | scenario + live restart | ADR/API | Pass |
| 7.1 pure unit/golden/HTTP/framework/controller/K3s CPU tests | production paths and fixtures | `scripts/space-compute all`, cluster-e2e | test report | Pass for implemented functionality |
| 7.1 representative NVIDIA/AMD/Huawei/FPGA/CPU fixtures | DCGM/ROCm/generic NPU/Iluvatar exist | parser registry tests | status | Partial: Huawei/FPGA/CPU-specific gaps |
| 7.1 race/fuzz/malformed/timeout/cache stampede | tests present | final all PASS | test report | Pass |
| 7.1 scale/chaos hundreds/thousands/packet faults | deterministic 5k and controller fault scenarios | final scale/scenario PASS | performance | Partial: no external packet/API/soak campaign |
| 7.1 ordinary default scheduler unaffected | standalone ownership | repeated real K3s e2e/uninstall | compatibility | Pass |
| 7.2 vendor discovery/allocation/runtime/thermal | opt-in Iluvatar suite | compiled but skipped | test report | Not run |
| 8 build/unit/race/static/integration/e2e/fuzz | scripts and harness | final executed gates | test report | Pass for scoped CPU suite |
| 8 security/vulnerability | controls implemented; patched Go 1.25.12, x/net v0.55.0 and OTel 1.40.0 | govulncheck exit 0, no reachable vulnerabilities; Punycode regression pass | security report | Pass (remaining supply-chain qualification open) |
| 8 K3s upgrade/rollback | docs only | not run | compatibility | Not run |
| 8 release documentation/status honesty | Phase 5 reports/status | document checks | this matrix | Pass |

## Known-defect closure

| Defect | Disposition |
| --- | --- |
| Iluvatar duplicate/contradictory per-device samples could be ambiguous | Closed: deterministic field order, alias/model consistency, duplicate rejection, memory arithmetic checks and exact two-GPU fixture. |
| Multiple Nodes could claim one endpoint | Closed fail-closed; distinct Node endpoint generations tested. |
| Ordinary/schedulable Master could consume collector capacity | Closed: background targets require mapped positive resource or explicit metadata; multi-Agent/Master test passes. |
| Collector newest target could exceed bound | Closed: prune arithmetic fixed and tested. |
| Controller could retry forever or fan out resource events | Closed: named rate-limited queues, 15 retries, coalesced resource key and metrics. |
| Retry could create replacement before deleting old local Pod | Closed: UID/attempt fence, delete/wait, owner reference and test. Remote-domain fence remains R2. |
| One heterogeneous admission policy never became active | Closed: four typed policies, activation wait, forged reporter rejection and zero-warning live evidence. |
| K3s core imported the plugin | Closed: hook removed; current K3s binary matches verified baseline. |
| Unsafe static-Pod required admin kubeconfig | Closed: unsupported manifest removed; dedicated-SA Deployment only. |

Because the matrix contains gaps, failed security evidence and required Not-run
items, PROJECT.md is not fully traceable to a production release.
