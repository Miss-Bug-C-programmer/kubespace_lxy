# Phase 5 open-risk register

| ID | Severity | Risk/evidence | Owner | Next action |
| --- | --- | --- | --- | --- |
| R1 | Resolved | Original govulncheck scan found 22 reachable vulnerabilities in Go 1.24.11, x/net v0.38.0 and OTel v1.38.0 | K3s platform/security | Patched Go 1.25.12, x/net v0.55.0, x/crypto v0.53.0, gRPC-Go v1.79.3 and OTel 1.40.0; rerun exit 0 with no reachable vulnerabilities. One non-reachable OpenPGP module advisory remains informational. Keep the scan in release CI. |
| R2 | Critical | No shipped authenticated cross-domain summary/transfer/result agent; remote old-Pod fencing cannot be enforced by a single local controller | Mission orchestration | Implement/qualify bounded at-least-once transport, remote execution lease/fence and result acknowledgement. |
| R3 | High | Full-agent K3s/kubelet/CNI/CRI and representative image execution not run | K3s integration | Run disposable privileged server+agents and repository conformance/integration suites. |
| R4 | High | No physical Iluvatar exporter/device-plugin allocation, health, power/thermal or workload evidence | Hardware/vendor integration | Run `scripts/space-compute hardware` with all required inputs and retain signed evidence. |
| R5 | High | Supported K3s patch upgrade/rollback not run | Release engineering | Test prior supported 1.33 patch -> this build -> rollback with CRD data. |
| R6 | High | Canonical resource model lacks full per-device topology/NUMA/interconnect, memory/bandwidth, firmware/runtime/library and node storage/trust/autonomy fields | API owners | Design additive API version, conversion and migration; do not claim full PROJECT traceability. |
| R7 | High | Mission API lacks explicit working memory/storage and min-bandwidth/max-latency constraints | API/planner | Add versioned fields, admission, planning/scheduler tests and conversion plan. |
| R8 | High | NPU/FPGA/DSP/CPU canonical production adapters and vendor fixtures are incomplete; Ascend was intentionally not researched per user direction | Exporter adapters | Add through data-driven registry when metrics/contracts are supplied; no policy forks. |
| R9 | Medium | 5,000-target collector uses about 234 MB temporary allocations; no multi-hour soak or API throttling campaign | Performance | Profile allocations, execute soak with 1k+ live endpoints and set percentile SLOs. |
| R10 | Medium | Workqueue entries are API-cardinality bounded but not a separate hard-cap; Kubernetes API quotas are assumed | Controllers/operations | Document/enforce mission/link/resource quotas and alert queue depth/retry exhaustion. |
| R11 | Medium | CRD deletion caused transient upstream garbage-collector reflector errors until restart/backoff | K3s operations | Document maintenance-window uninstall and verify upstream behavior in full-agent/upgrade lab. |
| R12 | Medium | Repository golangci-lint v1.55.2 is incompatible with current Go export data | Build tooling | Upgrade pinned linter and rerun without suppressions. |
| R13 | Medium | Image signing, SBOM and registry admission not qualified | Supply-chain security | Produce/sign SBOMs and verify admission policy in release pipeline. |
| R14 | Low | Container Dockerfiles lack Docker HEALTHCHECK; Kubernetes probes are present | Operations | Retain Kubernetes probes; add image-level check only if runtime policy requires it. |

No risk is reclassified as out of scope to obtain a release. Critical/high open
items mean the evidence-based decision is **Not ready**.
