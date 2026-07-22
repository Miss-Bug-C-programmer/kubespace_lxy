# Phase 5 qualification test report

Date: 2026-07-21. Host: Linux amd64, Intel i7-6700, CPU-only. Source K3s:
`v1.33.7-k3s1`, patched toolchain and module minimum Go `1.25.12`. No result
below is called passed unless its
command exited zero.

## Mandatory code gates

| Exact command | Result |
| --- | --- |
| `env GOCACHE=/tmp/space-compute-phase5-gocache scripts/space-compute all` | PASS. Unit, race, gofmt/go vet, integration, scenarios, eight fuzz targets, scale and both component builds exited 0. |
| `scripts/space-compute unit` within final `all` | PASS: plugin `1.270s`; scheduler command `0.060s`; planner command `0.041s`; API `0.010s`; kube `0.028s`; planner `0.333s`; policy `0.014s`; workload `0.016s`; external harness `0.015s`. |
| `scripts/space-compute race` within final `all` | PASS: plugin `3.667s`; scheduler `1.217s`; planner command `1.141s`; API `1.048s`; kube `1.089s`; planner `2.984s`; policy `1.045s`; workload `1.063s`; external harness `1.065s`. |
| `scripts/space-compute static` | PASS: gofmt produced no paths and scoped `go vet` exited 0. |
| `env PATH=/tmp/go1.25.12/bin:$PATH GOROOT=/tmp/go1.25.12 GOTOOLCHAIN=local GOCACHE=/tmp/space-compute-phase5-go125-all-cache GOMODCACHE=/tmp/space-compute-gomodcache ... scripts/space-compute all` | PASS after remediation. Unit/race/static/integration/scenarios/fuzz/scale/build all exited 0; plugin unit `1.682s`, race `7.271s`; collector 5,000 `923.422ms`, scheduler callbacks 5,000 `122.738ms`, planner 5,000 strict `60.740ms`. |
| `scripts/space-compute integration` | PASS: production collector/framework `0.933s`; scheduler `0.032s`; planner command `0.026s`; API `0.009s`; kube `0.023s`; planner `0.175s`; policy `0.010s`; workload `0.011s`; external harness compile/skip `0.015s`. |
| `scripts/space-compute scenarios` | PASS: API `0.007s`, kube `0.019s`, planner `0.010s`, policy `0.005s`, workload `0.010s`, scheduler Phase 4 `0.223s`. |
| `scripts/space-compute fuzz` | PASS. Parser 10,979 executions; typed args 8,871; workload intent 4,572; link 72,922; mission 4,756; resource summary 39,868; placement 57,744; projection 14. Each target ran about 6s and did not panic. |
| `go mod verify` | PASS: `all modules verified`. |
| `env GOCACHE=... go mod tidy -diff` | PASS: no output/diff. |
| `govulncheck -format=text ...affected packages...` under Go 1.25.12 | PASS: exit 0, `No vulnerabilities found`; zero imported-package and one module-only non-reachable OpenPGP advisory remained informational. |
| `go test ./pkg/scheduler/plugins/gpustability -run TestExporterTargetRejectsASCIIOnlyPunycodeHost -count=1` | PASS: production collector target-resolution regression, `0.024s`. |
| affected-package `go test ... -count=1` | PASS: all nine affected packages, plugin `1.616s`. |
| affected-package `go test -race ... -count=1` under the final dependency graph | PASS outside the restricted sandbox: all nine affected packages; plugin `7.093s`, planner `2.542s`. The sandbox-only attempt failed because `httptest` could not bind IPv6 loopback (`operation not permitted`); the same production-path command passed with network permission. |
| current component builds | PASS. Scheduler: 106,104,311 bytes, SHA-256 `40330cd2495218df0a9949f537e405c75a4e1c0209f5093e729db5bbd7008a4b`. Planner: 73,998,793 bytes, SHA-256 `99640c8d1f515a71a9f6f9a206bf277f9a114fcbec1b1b1b8c743c9106e1e9b5`. |
| `go build -buildvcs=false -trimpath -o /tmp/k3s-phase5-go125-final ./cmd/k3s` under final dependency graph | PASS. SHA-256 `dd85196efdeb7a216ec462648c487c7da442f2362357fbf43cab51397a92982d`. |

The recurring `go: writing stat cache ... permission denied` warning concerns
the read-only module download cache. It did not change command exit status;
`GOCACHE` was writable and module verification/tidy checks passed.

## Real K3s API/control-plane evidence

An isolated rootless, agentless current-tree K3s used port 18443, datastore
`/tmp/space-compute-phase5-e2e.zBGZ8w`, separate CIDRs, no system K3s mutation,
and the production standalone binaries.

| Exact command/check | Result |
| --- | --- |
| `/tmp/k3s-phase5 server --rootless --disable-agent ... --https-listen-port=18443 ...` then `kubectl get --raw=/readyz` | PASS, `ok`. |
| `SPACE_COMPUTE_E2E_KUBECONFIG=... SPACE_COMPUTE_E2E_SCHEDULER_BINARY=... SPACE_COMPUTE_PHASE4_E2E_PLANNER_BINARY=... scripts/space-compute cluster-e2e` | Final current-tree PASS: production CRDs/admission/RBAC, planner and independent scheduler flow `32.00s`; ordinary/default scheduler isolation/failover `15.28s`; package `47.308s`. |
| same e2e after K3s datastore restart | PASS: `31.66s` and `14.61s`, package `46.284s`. Duplicate manifest application and controller state were idempotent. |
| CRD storage inspection | PASS: all four CRDs reported `storedVersions: [v1alpha1]`. All four admission policies reported generation=observedGeneration and no type warnings. |
| planner impersonation checks | PASS: planner SA can create placement intents and cannot get Secrets. |
| server-side dry-run of scheduler, CRD, admission and planner manifests | PASS. |
| uninstall manifests, wait for API removal, run `TestIndependentSchedulerAgainstK3s` | PASS: custom API disappeared and normal/independent scheduling passed in `15.37s`. |

An initial clean-cluster run exposed that the harness assumed preinstalled CRDs.
The harness now installs production objects. A subsequent run exposed a real
CEL type-check defect in a policy spanning heterogeneous CRDs. It was split into
four typed policies; forged reporter admission then failed closed and the final
runs passed. Assertions were not weakened.

## Security and hardware gates

| Command | Result |
| --- | --- |
| `scripts/space-compute hardware` | NOT RUN/SKIP: no live Iluvatar metrics file, physical cluster kubeconfig, representative Pod or expected device ID. Both opt-in tests compiled and reported their required inputs. |
| original pre-fix `govulncheck -format=text ...affected packages...` | Historical FAIL (exit 3): 22 reachable vulnerabilities in Go 1.24.11/x/net v0.38.0/OTel v1.38.0; remediated and rerun successfully above. |
| Trivy 0.59 targeted secret scans | PASS for plugin, planner, scheduler and contrib source. |
| Trivy targeted manifest scan | REVIEW REQUIRED: only expected system-namespace and Pod create/delete ownership findings remain after removing the unsafe static-Pod manifest and hardening Deployment security contexts. |
| repository `golangci-lint` | TOOL INCOMPATIBLE: v1.55.2 built with Go 1.21 cannot read the current toolchain export-data v2. `go vet` passed, but this is not recorded as a lint pass. |

## Not executed

- Real full-agent K3s/kubelet/CNI/CRI and vendor device-plugin execution.
- A fresh real-K3s API e2e using the post-remediation Go 1.25.12 binary was not
  rerun in this correction; the existing current-tree e2e evidence uses the
  previously qualified binary, while `/tmp/k3s-phase5-go125` full K3s build and
  all CPU integration/regression packages passed after the update.
- Supported K3s 1.33 patch upgrade from an older release binary and rollback.
- Multi-hour soak, external API throttling/leader-kill campaign, and real packet
  impairment over multiple clusters.
- Physical Iluvatar thermal/power accuracy and allocation identity.

These missing gates keep the release decision `Not ready`; the vulnerability
gate itself is now passing after the dependency/toolchain remediation.
