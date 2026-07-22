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

## Evidence limitations

This is not a multi-hour soak and does not measure API-server informer lag,
physical network transfer, actual exporter response latency, leader recovery
under load or a 10,000-node cluster. Collector 5,000-node temporary allocation
is material and requires production sizing/soak. Consequently the scale code
gate passes, but production capacity qualification remains incomplete.
