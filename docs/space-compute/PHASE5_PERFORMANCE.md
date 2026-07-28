# Phase 5 scale and performance report

The reproducible gate is `scripts/space-compute scale`. It uses production
parsers, collector target/snapshot stores, scheduler callbacks and planner code;
inputs are deterministic recorded fixtures. No constraint or resource class is
disabled. Measurements are one-iteration qualification samples on an Intel
i7-6700 CPU, Linux amd64, Go 1.24.11, so they are regression budgets rather than
universal capacity promises.

| Dataset | Collector target lifecycle | Scheduler decision | Planner decision |
| ---: | ---: | ---: | ---: |
| 100 | 8.155 ms; 5.01 MB; 69,727 allocs | 2.646 ms; 26.462 us/node; 1.22 MB | strict 2.426 ms; degraded 0.892 ms; best-effort 1.367 ms |
| 1,000 | 81.364 ms; 47.75 MB; 690,661 allocs | 25.192 ms; 25.192 us/node; 12.33 MB | strict 11.991 ms; degraded 9.733 ms; best-effort 9.928 ms |
| 5,000 | 916.426 ms; 233.92 MB; 3,436,213 allocs | 122.117 ms; 24.423 us/node; 60.94 MB | strict 62.424 ms; degraded 59.146 ms; best-effort 61.154 ms |

The separate first run measured collector 865.632 ms/236.70 MB, scheduler
119.012 ms/23.802 us per node and planner 59.121--60.622 ms at 5,000. The final
values remain within environmental tolerance and show approximately linear
growth.

## Qualification budgets

- 1,000-node scheduler cycle: p50-style one-shot budget 50 ms; observed 25.2 ms.
- 5,000-node scheduler cycle: budget 250 ms; observed 122.1 ms.
- 1,000-domain planning: budget 50 ms; observed at most 12.0 ms.
- 5,000-domain planning: budget 150 ms; observed at most 62.4 ms.
- Collector reconciliation of 1,000/5,000 unique targets: 250 ms/1.5 s;
  observed 81.4 ms/916.4 ms.
- Regression failure threshold is 2x the listed budget on comparable hardware;
  production SLOs require repeated percentile/load testing on deployment-class
  hardware.

Controller observability now exposes bounded-label planning duration/active,
queue depth, retries exhausted, API writes, reconciliation errors, snapshot
age, deadline slack, replan reason and link-risk class. Collector cache, target,
queue, worker, response/sample/label and per-node device counts are bounded.

## Phase 10 post-optimization evidence

Phase 10 was re-profiled on GitHub Actions run `30335789459` using Go 1.25.12,
Linux amd64 and an AMD EPYC 7763 runner after the complete repository gates had
passed. The production commit is `19609892ada3e46ba0ec93f1b0d128e330263e2c`.
The benchmark command used exactly one 5,000-target Collector iteration and
three 5,000-domain prepared-planning iterations with `-benchmem`, plus CPU and
allocation profiles.

| Workload | Phase 10 result | Change from Phase 5 baseline |
| --- | ---: | ---: |
| Collector target lifecycle, 5,000 targets | 474.468 ms; 191.332 MB; 3,311,584 allocs | latency -48.2%; allocation bytes -18.2%; alloc count -3.6% |
| Prepared mission planning, 5,000 domains | 55.754 ms; 28.527 MB; 366,584 allocs | remains below the 150 ms Phase 5 planning budget; prepared inputs are reused across unchanged informer generations |

The Collector allocation profile sampled 206.48 MB total allocation space. Its
largest flat contributors are `bufio.NewReaderSize`, the Prometheus text parser,
metric-profile construction and HTTP/parser support. The former
`strings.NewReader(string(raw))` response-copy path is absent; production now
parses directly from `bytes.NewReader(raw)`. Scheduling due work is deadline
driven rather than a periodic all-target scan, snapshot eviction is sharded and
access-weighted without changing freshness, and explicit refreshes remain behind
the common generation/singleflight/backoff queue path.

Planner reconciliation now consumes an immutable informer-backed
`PlanningIndex`, reuses prepared canonical inputs and their digest across
unchanged generations, and suppresses no-op status writes. Phase 10 tests cover
5,000-domain snapshot reuse and repeated planning without linear API list/write
amplification.

## Evidence limitations

This is not a multi-hour soak and does not measure API-server informer lag,
physical network transfer, actual exporter response latency, leader recovery
under load or a 10,000-node cluster. The Phase 10 profile materially reduces the
5,000-target temporary allocation observed in Phase 5, but production capacity
qualification still requires deployment-class soak and physical-hardware tests.
