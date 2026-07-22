# Phase 5 threat model and security status

## Trust boundaries and controls

| Boundary/threat | Implemented control | Residual status |
| --- | --- | --- |
| Exporter spoofing/tampering | HTTPS TLS 1.2+, optional mTLS, configured CA/name, Node-derived address, no redirects, response/sample/label/device limits, strict units/ranges/identity consistency | TLS identities and Iluvatar hardware endpoints not qualified in this environment. |
| SSRF/redirect through Node metadata | Address is selected from watched Node addresses; external IP disabled by default; path/scheme/port validated; redirects rejected; duplicate endpoint ownership fails closed | Operators with Node-update permission remain trusted and must be audited. |
| Malformed `ix_*` metrics | Production Prometheus parser rejects NaN/Inf/range errors, contradictory aliases/models, duplicate per-device fields, memory arithmetic mismatch and >`maxDevicesPerNode` | Fixture evidence passes; physical signal accuracy not measured. |
| CRD/reporter forgery | Four kind-specific fail-closed ValidatingAdmissionPolicies; reporter ID equals authenticated principal; monotonic sequence; immutable identity; size/time/digest validation | Cross-domain credential provisioning/rotation is an operator integration. |
| Planner privilege abuse | Dedicated SA; cannot read Secrets or bind Pods; exact placement writer admission; bounded retries/labels; Pod owner references | Pod create/delete is powerful. Namespace admission/PodSecurity and mission submitter RBAC must restrict who may request execution. |
| Scheduler privilege abuse | Dedicated SA and lease; only standalone opt-in profile; default K3s has no plugin import; exporter access is asynchronous | Upstream scheduler requires Pod delete for preemption and Binding create; protect its token. |
| Cross-domain replay/reorder | sequence/generation checks, provenance/digests, material input digest, attempt/UID fencing and monotonic observations | No shipped WAN transport, key exchange or result agent; cannot be production-qualified. |
| Memory/CPU/cardinality DoS | bounded body/families/samples/labels/devices, target/cache/queue/pod history, rate-limited queues, 15 retries, bounded metric labels | Workqueue key count follows API object count; enforce API quotas. 5,000-target collector allocation needs soak. |
| Secret/image leakage | no secret read RBAC, no bearer tokens/endpoints in annotations, non-root distroless images, read-only root FS, seccomp, dropped capabilities | Image provenance/SBOM/signing was not qualified. |

The former host-networked static-Pod example was removed because it required
mounting the K3s administrator kubeconfig. Production manifests now fix UID/GID
65532, use RuntimeDefault seccomp, drop all capabilities, disallow privilege
escalation, use read-only root filesystems and set CPU/memory budgets.

## Parser/API fuzz and static evidence

Eight fuzz targets passed: exporter parser, typed scheduler args, workload
intent, link, mission, resource summary, placement and Pod projection. Targeted
Trivy secret scans reported no finding in the Phase 5 source. `go vet`, module
verification and tidy-diff passed. Trivy's remaining manifest findings are:

- system components in `kube-system` (accepted: intentional system ownership);
- scheduler Pod delete (accepted: upstream scheduler preemption contract);
- planner Pod create/delete (accepted but high-impact: workload-controller
  ownership, protected by SA and placement admission).

The repository golangci-lint is too old for the current Go export format; this
is a toolchain gap, not a pass.

## Vulnerability remediation evidence

The original Phase 5 scan (Go 1.24.11, `x/net` v0.38.0 and OTel SDK v1.38.0)
reported 22 reachable vulnerabilities. The root cause was the K3s module graph's
explicit replace pins, combined with an unpatched Go toolchain; the OTel modules
were also split across incompatible release lines.

The production fix updates the module graph to `x/net` v0.55.0,
`x/crypto` v0.53.0, `x/sys` v0.45.0, gRPC-Go v1.79.3 and the OTel 1.40.0 release line, and pins
the build toolchain to Go 1.25.12. Node exporter host validation now performs an
IDNA round-trip and rejects ASCII-only Punycode labels before endpoint creation.

Exact verification command (official Go 1.25.12, patched module cache):

```text
env PATH=/tmp/go1.25.12/bin:$PATH GOROOT=/tmp/go1.25.12 GOTOOLCHAIN=local \
  GOCACHE=/tmp/space-compute-govuln-cache GOMODCACHE=/tmp/space-compute-gomodcache \
  /tmp/space-compute-phase5-bin/govulncheck -format=text \
  ./pkg/scheduler/plugins/gpustability ./cmd/space-compute-scheduler \
  ./cmd/space-compute-mission-planner ./contrib/space-compute/pkg/apis/v1alpha1 \
  ./contrib/space-compute/pkg/kube ./contrib/space-compute/pkg/planner \
  ./contrib/space-compute/pkg/policy ./contrib/space-compute/pkg/workload \
  ./tests/integration/spacecomputescheduler
```

Result: exit 0, `No vulnerabilities found`; the tool additionally reported 0
non-reachable imported-package vulnerabilities and 1 non-reachable module
vulnerability (the x/crypto OpenPGP package is unsafe by design). No finding
was suppressed. The focused Punycode regression,
affected-package tests, race tests, integration harness, `go vet`, `go mod
verify`, and `go mod tidy -diff` also passed under Go 1.25.12.

The security gate is now **PASS**. Release remains blocked by the independent
cross-domain transport/fence, full-agent/upgrade, hardware and API evidence
listed in the risk register.
