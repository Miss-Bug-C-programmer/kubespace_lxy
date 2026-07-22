# Space Compute Development Instructions

This repository contains an upstream K3s codebase plus an evolving heterogeneous
space-compute resource-management and scheduling capability.

Before changing code related to accelerators, resource discovery, scheduling,
satellite topology, telemetry, or workload orchestration, read these files in
full:

1. `docs/space-compute/PROJECT.md` — authoritative architecture, scope, and
   quality contract.
2. `docs/space-compute/IMPLEMENTATION_STATUS.md` — current implementation state,
   verified tests, open risks, and the next permitted phase.
3. The applicable phase in `docs/space-compute/CODEX_PHASE_PROMPTS.md` when the
   task is part of the staged implementation.

## Non-negotiable rules

- Preserve normal K3s behavior. Space-compute workloads must be opt-in and must
  not make the default scheduler depend on accelerator telemetry or satellite
  links.
- Keep scheduling policy separate from vGPU slicing. This project may consume
  Kubernetes extended resources, DRA, or vendor device-plugin inventory, but it
  must not silently become a HAMi-like virtual-device allocator.
- Never perform remote exporter or satellite-link I/O from scheduler `Filter`,
  `PreScore`, `Score`, `Reserve`, or binding-path callbacks. Those callbacks may
  consume only validated local cache/informer snapshots.
- Do not weaken requirements, delete behavior, lower assertions, skip tests,
  add production test bypasses, or replace production logic with mocks merely
  to make code compile or tests pass.
- Do not turn the implementation into a toy or demo. Interfaces, error handling,
  concurrency, stale-state behavior, security boundaries, observability, and
  upgrade compatibility are part of the deliverable.
- A machine without a physical GPU/NPU/FPGA must still be able to execute the
  full deterministic functional and integration test suite by replaying real
  exporter text fixtures through Go HTTP test servers and production parsers.
  Do not build a separate resource, device, or orbital simulator subsystem.
  Hardware/exporter tests are an additional qualification layer, not a reason to
  omit behavior.
- Preserve the exporter-based telemetry design. TianShu ZhiXin/Iluvatar `ix_*`
  metrics remain a first-class built-in profile. New exporters must be added
  through a data-driven profile/adapter registry without changing scheduling
  policy code.
- Never hard-code node IP addresses. Resolve scrape targets from watched
  Kubernetes Node addresses and validated per-node exporter metadata, and react
  to address changes without restarting K3s.
- Do not switch, create, reset, rebase, merge, or otherwise change Git branches
  unless the user explicitly asks. Do not discard or overwrite unrelated user
  changes.
- Reuse Kubernetes resource-accounting helpers and scheduler semantics instead
  of reimplementing Pod request calculation approximately.
- Treat all external telemetry as untrusted input. Validate identity, size,
  units, numeric ranges, timestamps, freshness, and provenance.
- Every implementation phase must update
  `docs/space-compute/IMPLEMENTATION_STATUS.md` with exact changes, tests run,
  failures or environmental limitations, remaining risks, and the next step.
- Never claim a test passed unless its command was actually run successfully.
  Record unavailable tools or hardware explicitly.

## Definition of done for any phase

A phase is complete only when its production code, API/configuration, tests,
documentation, observability, failure semantics, and backward-compatibility
checks meet the acceptance criteria in `PROJECT.md` and the phase prompt. A
successful compile alone is never sufficient.
