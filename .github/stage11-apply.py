from pathlib import Path

ROOT = Path('.')
BOOKWORM = 'golang:1.25.12-bookworm@sha256:ea341baa9bd5ba6784f6d7161ace70544349a6242d54d34a0fbfd2c4d51c9d58'
ALPINE = 'golang:1.25.12-alpine@sha256:56961d79ea8129efddcc0b8643fd8a5416b4e6228cfd477e3fd61deb2672c587'
DISTROLESS = 'gcr.io/distroless/static-debian12:nonroot@sha256:f5b485ea962d9bd1186b2f6b3a061191539b905b82ec395de78cbfae51f20e35'


def read(path):
    return (ROOT / path).read_text()


def write(path, text):
    p = ROOT / path
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text)


def replace_once(text, old, new, label):
    count = text.count(old)
    if count != 1:
        raise SystemExit(f'{label}: expected one marker, found {count}')
    return text.replace(old, new, 1)


# Secure and split planner health/metrics servers.
path = 'cmd/space-compute-mission-planner/main.go'
text = read(path)
text = replace_once(text, '\t"k8s.io/component-base/metrics/legacyregistry"\n', '', 'remove legacy registry import')
text = replace_once(
    text,
    'type options struct {\n\tkubeconfig, master, metricsAddress, leaderNamespace, leaderName, role string\n\tworkers                                                               int\n',
    'type options struct {\n\tkubeconfig, master, metricsAddress, healthAddress, metricsTLSCertFile, metricsTLSPrivateKeyFile, leaderNamespace, leaderName, role string\n\tworkers                                                                                                               int\n',
    'options fields',
)
text = replace_once(
    text,
    '\tflag.StringVar(&opt.metricsAddress, "metrics-bind-address", ":10261", "Health and metrics listen address")\n',
    '\tflag.StringVar(&opt.metricsAddress, "metrics-bind-address", ":10261", "HTTPS metrics listen address; empty disables metrics serving")\n'
    '\tflag.StringVar(&opt.healthAddress, "health-bind-address", ":10271", "HTTP health listen address for kubelet probes")\n'
    '\tflag.StringVar(&opt.metricsTLSCertFile, "metrics-tls-cert-file", "/var/run/space-compute-metrics/tls.crt", "TLS certificate for the HTTPS metrics endpoint")\n'
    '\tflag.StringVar(&opt.metricsTLSPrivateKeyFile, "metrics-tls-private-key-file", "/var/run/space-compute-metrics/tls.key", "TLS private key for the HTTPS metrics endpoint")\n',
    'metrics flags',
)
old_server = '''\trecorder := eventRecorder(client)\n\tvar ready atomic.Bool\n\tserver := healthServer(opt.metricsAddress, &ready)\n\tgo func() {\n\t\tif err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {\n\t\t\tklog.ErrorS(err, "health server stopped")\n\t\t}\n\t}()\n\tdefer server.Shutdown(context.Background())\n'''
new_server = '''\trecorder := eventRecorder(client)\n\tvar ready atomic.Bool\n\thealth := healthServer(opt.healthAddress, &ready)\n\tgo func() {\n\t\tif err := health.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {\n\t\t\tklog.ErrorS(err, "health server stopped")\n\t\t}\n\t}()\n\tdefer health.Shutdown(context.Background())\n\n\tif opt.metricsAddress != "" {\n\t\tif err := validateMetricsTLSFiles(opt.metricsTLSCertFile, opt.metricsTLSPrivateKeyFile); err != nil {\n\t\t\treturn err\n\t\t}\n\t\tmetrics := metricsServer(opt.metricsAddress, client)\n\t\tgo func() {\n\t\t\tif err := metrics.ListenAndServeTLS(opt.metricsTLSCertFile, opt.metricsTLSPrivateKeyFile); err != nil && !errors.Is(err, http.ErrServerClosed) {\n\t\t\t\tklog.ErrorS(err, "HTTPS metrics server stopped")\n\t\t\t}\n\t\t}()\n\t\tdefer metrics.Shutdown(context.Background())\n\t}\n'''
text = replace_once(text, old_server, new_server, 'split server startup')
old_health = '''func healthServer(address string, ready *atomic.Bool) *http.Server {\n\tmux := http.NewServeMux()\n\tmux.Handle("/metrics", legacyregistry.Handler())\n\tmux.HandleFunc("/livez", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\\n")) })\n\tmux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {\n\t\tif !ready.Load() {\n\t\t\thttp.Error(w, "not leader or caches not synchronized", http.StatusServiceUnavailable)\n\t\t\treturn\n\t\t}\n\t\t_, _ = w.Write([]byte("ok\\n"))\n\t})\n\treturn &http.Server{Addr: address, Handler: mux, ReadHeaderTimeout: 5 * time.Second}\n}\n'''
text = replace_once(text, old_health, '', 'remove old combined health server')
write(path, text)

write('cmd/space-compute-mission-planner/metrics_server.go', r'''package main

import (
    "context"
    "crypto/tls"
    "fmt"
    "net/http"
    "strings"
    "sync/atomic"
    "time"

    authenticationv1 "k8s.io/api/authentication/v1"
    authorizationv1 "k8s.io/api/authorization/v1"
    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
    "k8s.io/client-go/kubernetes"
    "k8s.io/component-base/metrics/legacyregistry"
)

const (
    serverReadTimeout  = 5 * time.Second
    serverWriteTimeout = 15 * time.Second
    serverIdleTimeout  = 60 * time.Second
    serverMaxHeaders   = 1 << 20
)

func hardenedHTTPServer(address string, handler http.Handler) *http.Server {
    return &http.Server{
        Addr:              address,
        Handler:           handler,
        ReadTimeout:       serverReadTimeout,
        ReadHeaderTimeout: serverReadTimeout,
        WriteTimeout:      serverWriteTimeout,
        IdleTimeout:       serverIdleTimeout,
        MaxHeaderBytes:    serverMaxHeaders,
    }
}

func healthServer(address string, ready *atomic.Bool) *http.Server {
    mux := http.NewServeMux()
    mux.HandleFunc("/livez", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
    mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
        if !ready.Load() {
            http.Error(w, "not leader or caches not synchronized", http.StatusServiceUnavailable)
            return
        }
        _, _ = w.Write([]byte("ok\n"))
    })
    return hardenedHTTPServer(address, mux)
}

func metricsServer(address string, client kubernetes.Interface) *http.Server {
    mux := http.NewServeMux()
    mux.Handle("/metrics", delegatedMetricsAuth(client, legacyregistry.Handler()))
    server := hardenedHTTPServer(address, mux)
    server.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
    return server
}

func validateMetricsTLSFiles(certFile, keyFile string) error {
    if strings.TrimSpace(certFile) == "" || strings.TrimSpace(keyFile) == "" {
        return fmt.Errorf("metrics TLS certificate and private key are required when metrics serving is enabled")
    }
    if _, err := tls.LoadX509KeyPair(certFile, keyFile); err != nil {
        return fmt.Errorf("load metrics TLS keypair: %w", err)
    }
    return nil
}

func delegatedMetricsAuth(client kubernetes.Interface, next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        if r.Method != http.MethodGet {
            w.Header().Set("Allow", http.MethodGet)
            http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
            return
        }
        token, ok := bearerToken(r.Header.Get("Authorization"))
        if !ok {
            w.Header().Set("WWW-Authenticate", "Bearer")
            http.Error(w, "authentication required", http.StatusUnauthorized)
            return
        }
        if client == nil {
            http.Error(w, "authentication service unavailable", http.StatusServiceUnavailable)
            return
        }
        ctx, cancel := context.WithTimeout(r.Context(), serverReadTimeout)
        defer cancel()
        review, err := client.AuthenticationV1().TokenReviews().Create(ctx, &authenticationv1.TokenReview{
            Spec: authenticationv1.TokenReviewSpec{Token: token},
        }, metav1.CreateOptions{})
        if err != nil {
            http.Error(w, "authentication service unavailable", http.StatusServiceUnavailable)
            return
        }
        if !review.Status.Authenticated || strings.TrimSpace(review.Status.User.Username) == "" {
            w.Header().Set("WWW-Authenticate", "Bearer")
            http.Error(w, "invalid credentials", http.StatusUnauthorized)
            return
        }
        extra := make(map[string]authorizationv1.ExtraValue, len(review.Status.User.Extra))
        for key, values := range review.Status.User.Extra {
            extra[key] = authorizationv1.ExtraValue(append([]string(nil), values...))
        }
        access, err := client.AuthorizationV1().SubjectAccessReviews().Create(ctx, &authorizationv1.SubjectAccessReview{
            Spec: authorizationv1.SubjectAccessReviewSpec{
                User:   review.Status.User.Username,
                UID:    review.Status.User.UID,
                Groups: append([]string(nil), review.Status.User.Groups...),
                Extra:  extra,
                NonResourceAttributes: &authorizationv1.NonResourceAttributes{
                    Path: "/metrics",
                    Verb: "get",
                },
            },
        }, metav1.CreateOptions{})
        if err != nil {
            http.Error(w, "authorization service unavailable", http.StatusServiceUnavailable)
            return
        }
        if !access.Status.Allowed {
            http.Error(w, "forbidden", http.StatusForbidden)
            return
        }
        w.Header().Set("Cache-Control", "no-store")
        next.ServeHTTP(w, r)
    })
}

func bearerToken(header string) (string, bool) {
    fields := strings.Fields(header)
    if len(fields) != 2 || !strings.EqualFold(fields[0], "Bearer") || fields[1] == "" {
        return "", false
    }
    return fields[1], true
}
''')

write('cmd/space-compute-mission-planner/phase11_security_test.go', r'''package main

import (
    "net/http"
    "net/http/httptest"
    "strings"
    "sync/atomic"
    "testing"

    authenticationv1 "k8s.io/api/authentication/v1"
    authorizationv1 "k8s.io/api/authorization/v1"
    "k8s.io/apimachinery/pkg/runtime"
    ktesting "k8s.io/client-go/testing"
    "k8s.io/client-go/kubernetes/fake"
    "k8s.io/component-base/metrics/legacyregistry"
)

func TestPhase11HealthAndMetricsAreSeparatedAndBounded(t *testing.T) {
    var ready atomic.Bool
    health := healthServer(":0", &ready)
    response := httptest.NewRecorder()
    health.Handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
    if response.Code != http.StatusNotFound {
        t.Fatalf("health /metrics=%d, want 404", response.Code)
    }
    for name, server := range map[string]*http.Server{"health": health, "metrics": metricsServer(":0", fake.NewSimpleClientset())} {
        if server.ReadTimeout != serverReadTimeout || server.ReadHeaderTimeout != serverReadTimeout || server.WriteTimeout != serverWriteTimeout || server.IdleTimeout != serverIdleTimeout || server.MaxHeaderBytes != serverMaxHeaders {
            t.Fatalf("%s server limits are not hardened: %#v", name, server)
        }
    }
    metrics := metricsServer(":0", fake.NewSimpleClientset())
    if metrics.TLSConfig == nil || metrics.TLSConfig.MinVersion == 0 {
        t.Fatal("metrics server has no minimum TLS version")
    }
}

func TestPhase11MetricsUsesDelegatedAuthenticationAndAuthorization(t *testing.T) {
    client := fake.NewSimpleClientset()
    client.Fake.PrependReactor("create", "tokenreviews", func(action ktesting.Action) (bool, runtime.Object, error) {
        review := action.(ktesting.CreateAction).GetObject().(*authenticationv1.TokenReview).DeepCopy()
        if review.Spec.Token != "monitor-token" {
            return true, &authenticationv1.TokenReview{Status: authenticationv1.TokenReviewStatus{Authenticated: false}}, nil
        }
        review.Status.Authenticated = true
        review.Status.User.Username = "system:serviceaccount:space-compute-monitoring:space-compute-monitoring"
        review.Status.User.UID = "monitor-uid"
        review.Status.User.Groups = []string{"system:serviceaccounts", "system:serviceaccounts:space-compute-monitoring"}
        return true, review, nil
    })
    client.Fake.PrependReactor("create", "subjectaccessreviews", func(action ktesting.Action) (bool, runtime.Object, error) {
        review := action.(ktesting.CreateAction).GetObject().(*authorizationv1.SubjectAccessReview)
        allowed := review.Spec.User == "system:serviceaccount:space-compute-monitoring:space-compute-monitoring" && review.Spec.NonResourceAttributes != nil && review.Spec.NonResourceAttributes.Path == "/metrics" && review.Spec.NonResourceAttributes.Verb == "get"
        return true, &authorizationv1.SubjectAccessReview{Status: authorizationv1.SubjectAccessReviewStatus{Allowed: allowed}}, nil
    })
    handler := delegatedMetricsAuth(client, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))

    response := httptest.NewRecorder()
    handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
    if response.Code != http.StatusUnauthorized {
        t.Fatalf("anonymous metrics=%d, want 401", response.Code)
    }

    request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
    request.Header.Set("Authorization", "Bearer monitor-token")
    response = httptest.NewRecorder()
    handler.ServeHTTP(response, request)
    if response.Code != http.StatusNoContent {
        t.Fatalf("authorized metrics=%d, body=%q", response.Code, response.Body.String())
    }

    request = httptest.NewRequest(http.MethodPost, "/metrics", nil)
    request.Header.Set("Authorization", "Bearer monitor-token")
    response = httptest.NewRecorder()
    handler.ServeHTTP(response, request)
    if response.Code != http.StatusMethodNotAllowed {
        t.Fatalf("POST metrics=%d, want 405", response.Code)
    }
}

func TestPhase11PlannerMetricsExposeNoHighCardinalityIdentityLabels(t *testing.T) {
    families, err := legacyregistry.DefaultGatherer.Gather()
    if err != nil {
        t.Fatal(err)
    }
    forbidden := []string{"mission", "node", "endpoint", "reporter", "device", "token", "secret", "kubeconfig"}
    for _, family := range families {
        for _, metric := range family.Metric {
            for _, pair := range metric.Label {
                name := strings.ToLower(pair.GetName())
                for _, fragment := range forbidden {
                    if strings.Contains(name, fragment) {
                        t.Fatalf("metric %q exposes forbidden high-cardinality label %q", family.GetName(), pair.GetName())
                    }
                }
            }
        }
    }
}
''')

# External K3s e2e explicitly disables metrics; there is no insecure fallback.
path = 'tests/integration/spacecomputescheduler/phase4_external_int_test.go'
text = read(path)
text = replace_once(
    text,
    'process.cmd = exec.Command(binary, "--kubeconfig="+kubeconfig, "--leader-elect=false", "--workers=2", "--metrics-bind-address=127.0.0.1:13361")',
    'process.cmd = exec.Command(binary, "--kubeconfig="+kubeconfig, "--leader-elect=false", "--workers=2", "--metrics-bind-address=", "--health-bind-address=127.0.0.1:13361")',
    'phase4 e2e planner metrics opt-out',
)
write(path, text)

# Secure all four controller deployments.
path = 'docs/space-compute/manifests/mission-planner.yaml'
text = read(path)
for app, role in [
    ('space-compute-workload-dispatcher', 'workload-dispatcher'),
    ('space-compute-node-projector', 'node-projector'),
    ('space-compute-transport-agent', 'transport-agent'),
]:
    text = text.replace(
        f'matchLabels: {{app.kubernetes.io/name: {app}}}',
        f'matchLabels: {{app.kubernetes.io/name: {app}, spacecompute.k3s.io/controller-role: {role}}}',
    )
    text = text.replace(
        f'labels: {{app.kubernetes.io/name: {app}}}',
        f'labels: {{app.kubernetes.io/name: {app}, spacecompute.k3s.io/controller-role: {role}}}',
    )
for metrics_port, health_port in [(10261, 10271), (10262, 10272), (10263, 10273)]:
    old = f'''        - --metrics-bind-address=:{metrics_port}\n        ports: [{{name: health, containerPort: {metrics_port}}}]\n        readinessProbe: {{httpGet: {{path: /readyz, port: health}}, initialDelaySeconds: 3}}\n        livenessProbe: {{httpGet: {{path: /livez, port: health}}, initialDelaySeconds: 10}}\n'''
    new = f'''        - --metrics-bind-address=:{metrics_port}\n        - --health-bind-address=:{health_port}\n        - --metrics-tls-cert-file=/var/run/space-compute-metrics/tls.crt\n        - --metrics-tls-private-key-file=/var/run/space-compute-metrics/tls.key\n        ports:\n        - {{name: metrics, containerPort: {metrics_port}, protocol: TCP}}\n        - {{name: health, containerPort: {health_port}, protocol: TCP}}\n        readinessProbe: {{httpGet: {{scheme: HTTP, path: /readyz, port: health}}, initialDelaySeconds: 3}}\n        livenessProbe: {{httpGet: {{scheme: HTTP, path: /livez, port: health}}, initialDelaySeconds: 10}}\n'''
    if old not in text:
        raise SystemExit(f'manifest metrics block {metrics_port} missing')
    text = text.replace(old, new, 1)
text = replace_once(
    text,
    '        - --metrics-bind-address=:10264\n        env:\n',
    '        - --metrics-bind-address=:10264\n        - --health-bind-address=:10274\n        - --metrics-tls-cert-file=/var/run/space-compute-metrics/tls.crt\n        - --metrics-tls-private-key-file=/var/run/space-compute-metrics/tls.key\n        env:\n',
    'transport metrics args',
)
text = replace_once(
    text,
    '        ports: [{name: health, containerPort: 10264}]\n        readinessProbe: {httpGet: {path: /readyz, port: health}, initialDelaySeconds: 3}\n        livenessProbe: {httpGet: {path: /livez, port: health}, initialDelaySeconds: 10}\n',
    '        ports:\n        - {name: metrics, containerPort: 10264, protocol: TCP}\n        - {name: health, containerPort: 10274, protocol: TCP}\n        readinessProbe: {httpGet: {scheme: HTTP, path: /readyz, port: health}, initialDelaySeconds: 3}\n        livenessProbe: {httpGet: {scheme: HTTP, path: /livez, port: health}, initialDelaySeconds: 10}\n',
    'transport health ports',
)
volume_old = '          capabilities: {drop: [ALL]}\n'
volume_new = '''          capabilities: {drop: [ALL]}\n        volumeMounts:\n        - name: metrics-tls\n          mountPath: /var/run/space-compute-metrics\n          readOnly: true\n      volumes:\n      - name: metrics-tls\n        secret:\n          secretName: space-compute-controller-metrics-tls\n          optional: false\n'''
if text.count(volume_old) != 4:
    raise SystemExit(f'expected four controller security contexts, got {text.count(volume_old)}')
text = text.replace(volume_old, volume_new)
write(path, text)

write('docs/space-compute/manifests/metrics-monitoring-access.yaml', r'''apiVersion: v1
kind: Namespace
metadata:
  name: space-compute-monitoring
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: space-compute-monitoring
  namespace: space-compute-monitoring
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: system:space-compute-metrics-auth-delegator
rules:
- apiGroups: [authentication.k8s.io]
  resources: [tokenreviews]
  verbs: [create]
- apiGroups: [authorization.k8s.io]
  resources: [subjectaccessreviews]
  verbs: [create]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: system:space-compute-metrics-auth-delegator
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: system:space-compute-metrics-auth-delegator
subjects:
- {kind: ServiceAccount, name: space-compute-mission-planner, namespace: kube-system}
- {kind: ServiceAccount, name: space-compute-workload-dispatcher, namespace: kube-system}
- {kind: ServiceAccount, name: space-compute-node-projector, namespace: kube-system}
- {kind: ServiceAccount, name: space-compute-transport-agent, namespace: kube-system}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: system:space-compute-metrics-reader
rules:
- nonResourceURLs: [/metrics]
  verbs: [get]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: system:space-compute-metrics-reader
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: system:space-compute-metrics-reader
subjects:
- kind: ServiceAccount
  name: space-compute-monitoring
  namespace: space-compute-monitoring
---
apiVersion: v1
kind: Service
metadata: {name: space-compute-mission-planner-metrics, namespace: kube-system}
spec:
  selector: {app.kubernetes.io/name: space-compute-mission-planner}
  ports: [{name: https-metrics, port: 443, targetPort: metrics, protocol: TCP, appProtocol: https}]
---
apiVersion: v1
kind: Service
metadata: {name: space-compute-workload-dispatcher-metrics, namespace: kube-system}
spec:
  selector: {app.kubernetes.io/name: space-compute-workload-dispatcher}
  ports: [{name: https-metrics, port: 443, targetPort: metrics, protocol: TCP, appProtocol: https}]
---
apiVersion: v1
kind: Service
metadata: {name: space-compute-node-projector-metrics, namespace: kube-system}
spec:
  selector: {app.kubernetes.io/name: space-compute-node-projector}
  ports: [{name: https-metrics, port: 443, targetPort: metrics, protocol: TCP, appProtocol: https}]
---
apiVersion: v1
kind: Service
metadata: {name: space-compute-transport-agent-metrics, namespace: kube-system}
spec:
  selector: {app.kubernetes.io/name: space-compute-transport-agent}
  ports: [{name: https-metrics, port: 443, targetPort: metrics, protocol: TCP, appProtocol: https}]
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: space-compute-controller-ingress
  namespace: kube-system
spec:
  podSelector:
    matchExpressions:
    - {key: spacecompute.k3s.io/controller-role, operator: Exists}
  policyTypes: [Ingress]
  ingress:
  - from:
    - namespaceSelector:
        matchLabels: {kubernetes.io/metadata.name: space-compute-monitoring}
    ports:
    - {protocol: TCP, port: metrics}
''')

write('docs/space-compute/manifests/image-signature-policy.yaml', r'''# Requires Sigstore policy-controller. Opt the workload namespace into enforcement
# according to the installed policy-controller release before applying these policies.
apiVersion: policy.sigstore.dev/v1beta1
kind: ClusterImagePolicy
metadata:
  name: space-compute-image-signature
spec:
  mode: enforce
  images:
  - glob: "ghcr.io/miss-bug-c-programmer/kubespace_lxy/space-compute-**"
  authorities:
  - name: github-actions-keyless
    keyless:
      url: https://fulcio.sigstore.dev
      identities:
      - issuer: https://token.actions.githubusercontent.com
        subject: https://github.com/Miss-Bug-C-programmer/kubespace_lxy/.github/workflows/space-compute-supply-chain.yml@refs/heads/main
---
apiVersion: policy.sigstore.dev/v1beta1
kind: ClusterImagePolicy
metadata:
  name: space-compute-image-sbom
spec:
  mode: enforce
  images:
  - glob: "ghcr.io/miss-bug-c-programmer/kubespace_lxy/space-compute-**"
  authorities:
  - name: github-actions-keyless-spdx
    keyless:
      url: https://fulcio.sigstore.dev
      identities:
      - issuer: https://token.actions.githubusercontent.com
        subject: https://github.com/Miss-Bug-C-programmer/kubespace_lxy/.github/workflows/space-compute-supply-chain.yml@refs/heads/main
    attestations:
    - name: require-spdx-sbom
      predicateType: https://spdx.dev/Document
      policy:
        type: cue
        data: |
          predicateType: "https://spdx.dev/Document"
''')

write('docs/space-compute/PHASE11_SECURITY_AND_SUPPLY_CHAIN.md', r'''# Phase 11 metrics and supply-chain hardening

## Metrics and health

`space-compute-mission-planner` and its split controller roles expose health and
metrics on separate listeners. Health remains HTTP and is intended only for
kubelet probes. Metrics is disabled when `--metrics-bind-address` is empty; when
enabled it has no plaintext fallback and requires a valid TLS keypair before the
controller starts.

The HTTPS metrics handler delegates bearer-token authentication to Kubernetes
`TokenReview` and authorization to `SubjectAccessReview` for `GET /metrics`.
`metrics-monitoring-access.yaml` grants that non-resource URL only to
`system:serviceaccount:space-compute-monitoring:space-compute-monitoring` and
grants the controller ServiceAccounts only the TokenReview/SAR calls needed for
delegation. The NetworkPolicy allows pod-originated ingress only from the
`space-compute-monitoring` namespace to the named `metrics` port. Kubernetes
node-to-local-Pod traffic remains available for kubelet health probes.

Provision `kube-system/space-compute-controller-metrics-tls` as a TLS Secret.
The certificate should include every metrics Service name used by monitoring,
including the following cluster DNS names:

- `space-compute-mission-planner-metrics.kube-system.svc`
- `space-compute-workload-dispatcher-metrics.kube-system.svc`
- `space-compute-node-projector-metrics.kube-system.svc`
- `space-compute-transport-agent-metrics.kube-system.svc`

Do not store the private key in this repository.

## Supply chain

Space Compute runtime Dockerfiles pin both the Go 1.25.12 builder image and the
distroless runtime image by multi-architecture OCI digest. Dapper/local build
images pin their Go Alpine base digest as well. `golangci-lint` is pinned to
v2.12.2 and the repository config uses the v2 schema. `govulncheck` is pinned to
v1.6.0 and CI fails on reachable findings; there is no finding exclusion list.

`space-compute-supply-chain.yml` scans git history for secrets with pinned
Gitleaks, runs the pinned linter and vulnerability scanner, builds the six
repository Space Compute images with BuildKit provenance and SBOM attestations,
generates an SPDX JSON SBOM with Syft, signs each pushed digest keylessly with
Cosign, attaches the SPDX document as a signed attestation and verifies both the
signature and the SBOM attestation.

`image-signature-policy.yaml` provides enforcing Sigstore policy-controller
policies for the repository image namespace. One policy requires the exact
GitHub Actions workflow keyless identity; the second independently requires an
SPDX attestation from the same identity. Install policy-controller and opt the
intended workload namespace into its admission webhook before applying the
policies.
''')

# Pin builder/runtime base image digests for all Space Compute runtime images.
for dockerfile in sorted(ROOT.glob('Dockerfile.space-compute-*')):
    text = dockerfile.read_text()
    lines = text.splitlines()
    for i, line in enumerate(lines):
        if line.startswith('FROM golang:') and line.endswith(' AS build'):
            lines[i] = f'FROM {BOOKWORM} AS build'
        elif line == 'FROM gcr.io/distroless/static-debian12:nonroot':
            lines[i] = f'FROM {DISTROLESS}'
    dockerfile.write_text('\n'.join(lines) + '\n')

# Pin Go and golangci-lint in developer/build containers.
for path in ['Dockerfile.dapper', 'Dockerfile.local']:
    text = read(path)
    if 'ARG GOLANG=golang:1.24.11-alpine3.22' not in text:
        raise SystemExit(f'{path}: old Go image marker missing')
    text = text.replace('ARG GOLANG=golang:1.24.11-alpine3.22', f'ARG GOLANG={ALPINE}', 1)
    if path == 'Dockerfile.dapper':
        old = '''# Install golangci-lint for amd64\n# RUN if [ "$(go env GOARCH)" = "amd64" ]; then \\\n# RUN  curl -sL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s v1.55.2;  \\\n    # fi\nCOPY golangci-lint ./golangci-lint\n'''
        new = '''# Install the pinned Go-1.25-compatible linter for the container architecture.\nRUN GOBIN=/usr/local/bin go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2\n'''
        text = replace_once(text, old, new, 'Dockerfile.dapper linter')
    else:
        old = '''RUN if [ "$(go env GOARCH)" = "amd64" ]; then \\\n    curl -sL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s v1.51.2;  \\\n    fi\n'''
        new = '''RUN GOBIN=/usr/local/bin go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2\n'''
        text = replace_once(text, old, new, 'Dockerfile.local linter')
    write(path, text)

# Migrate golangci-lint configuration to v2 without disabling the Space Compute tree.
write('.golangci.json', r'''{
  "version": "2",
  "linters": {
    "default": "none",
    "enable": ["govet", "revive", "misspell"],
    "exclusions": {
      "generated": "lax",
      "paths": ["^build/", "^manifests/", "^package/", "^scripts/", "^vendor/"],
      "rules": [
        {"linters": ["revive"], "text": "should have comment"},
        {"linters": ["revive"], "text": "exported"}
      ]
    }
  },
  "formatters": {
    "enable": ["gofmt", "goimports"],
    "exclusions": {
      "generated": "lax",
      "paths": ["^build/", "^manifests/", "^package/", "^scripts/", "^vendor/"]
    }
  },
  "run": {"timeout": "5m"}
}
''')

# Upgrade vulnerable dependency floors. go get/tidy in the validation workflow writes go.sum.
path = 'go.mod'
text = read(path)
text = replace_once(text, 'google.golang.org/grpc => google.golang.org/grpc v1.79.3', 'google.golang.org/grpc => google.golang.org/grpc v1.82.1', 'grpc replace')
write(path, text)

write('.github/workflows/govulncheck.yml', r'''name: govulncheck

on:
  push:
    branches: [main]
    paths:
      - 'go.mod'
      - 'go.sum'
      - 'pkg/scheduler/plugins/gpustability/**'
      - 'contrib/space-compute/pkg/**'
      - 'cmd/space-compute-*/**'
      - '.github/workflows/govulncheck.yml'
  pull_request:
    paths:
      - 'go.mod'
      - 'go.sum'
      - 'pkg/scheduler/plugins/gpustability/**'
      - 'contrib/space-compute/pkg/**'
      - 'cmd/space-compute-*/**'
  schedule:
    - cron: '0 6 * * 1'
  workflow_dispatch:

permissions:
  contents: read

jobs:
  govulncheck:
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@11d5960a326750d5838078e36cf38b85af677262
      - uses: actions/setup-go@40f1582b2485089dde7abd97c1529aa768e1baff
        with:
          go-version: '1.25.12'
          cache: true
      - name: Install pinned govulncheck
        run: go install golang.org/x/vuln/cmd/govulncheck@v1.6.0
      - name: Fail on reachable vulnerabilities
        run: govulncheck ./pkg/scheduler/plugins/gpustability ./contrib/space-compute/pkg/... ./cmd/space-compute-scheduler ./cmd/space-compute-mission-planner ./cmd/space-compute-domain-agent
''')

write('.github/workflows/space-compute-supply-chain.yml', r'''name: space-compute-supply-chain

on:
  push:
    branches: [main]
    paths:
      - 'go.mod'
      - 'go.sum'
      - '.golangci.json'
      - 'Dockerfile.space-compute-*'
      - 'pkg/scheduler/plugins/gpustability/**'
      - 'contrib/space-compute/pkg/**'
      - 'cmd/space-compute-*/**'
      - '.github/workflows/space-compute-supply-chain.yml'
  workflow_dispatch:

permissions:
  contents: read
  packages: write
  id-token: write

jobs:
  verify:
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@11d5960a326750d5838078e36cf38b85af677262
        with: {fetch-depth: 0}
      - uses: actions/setup-go@40f1582b2485089dde7abd97c1529aa768e1baff
        with: {go-version: '1.25.12', cache: true}
      - name: Install pinned analyzers
        run: |
          set -euo pipefail
          go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2
          go install golang.org/x/vuln/cmd/govulncheck@v1.6.0
          go install github.com/zricethezav/gitleaks/v8@v8.30.1
      - name: Lint Space Compute
        run: golangci-lint run ./pkg/scheduler/plugins/gpustability/... ./contrib/space-compute/pkg/... ./cmd/space-compute-scheduler/... ./cmd/space-compute-mission-planner/... ./cmd/space-compute-domain-agent/...
      - name: Reachable vulnerability gate
        run: govulncheck ./pkg/scheduler/plugins/gpustability ./contrib/space-compute/pkg/... ./cmd/space-compute-scheduler ./cmd/space-compute-mission-planner ./cmd/space-compute-domain-agent
      - name: Secret and credential history scan
        run: gitleaks git --redact --no-banner --verbose .

  build-sign:
    needs: verify
    runs-on: ubuntu-24.04
    strategy:
      fail-fast: false
      matrix:
        include:
          - {component: mission-planner, dockerfile: Dockerfile.space-compute-mission-planner}
          - {component: scheduler, dockerfile: Dockerfile.space-compute-scheduler}
          - {component: mission-webhook, dockerfile: Dockerfile.space-compute-mission-webhook}
          - {component: reporter-webhook, dockerfile: Dockerfile.space-compute-reporter-webhook}
          - {component: conversion-webhook, dockerfile: Dockerfile.space-compute-conversion-webhook}
          - {component: storage-migrator, dockerfile: Dockerfile.space-compute-storage-migrator}
    steps:
      - uses: actions/checkout@11d5960a326750d5838078e36cf38b85af677262
      - uses: actions/setup-go@40f1582b2485089dde7abd97c1529aa768e1baff
        with: {go-version: '1.25.12', cache: true}
      - name: Install pinned signing and SBOM tools
        run: |
          set -euo pipefail
          go install github.com/sigstore/cosign/v2/cmd/cosign@v2.6.3
          go install github.com/anchore/syft/cmd/syft@v1.44.0
          cosign version
          syft version
      - name: Authenticate GHCR
        env:
          GHCR_TOKEN: ${{ github.token }}
        run: echo "$GHCR_TOKEN" | docker login ghcr.io -u "$GITHUB_ACTOR" --password-stdin
      - name: Build with provenance and sign immutable digest
        env:
          COMPONENT: ${{ matrix.component }}
          DOCKERFILE: ${{ matrix.dockerfile }}
        run: |
          set -euo pipefail
          repo="${GITHUB_REPOSITORY,,}"
          image="ghcr.io/${repo}/space-compute-${COMPONENT}"
          metadata="/tmp/${COMPONENT}-metadata.json"
          sbom="/tmp/${COMPONENT}.spdx.json"
          docker buildx build --platform linux/amd64 --push --provenance=mode=max --sbom=true --metadata-file "$metadata" -f "$DOCKERFILE" -t "${image}:${GITHUB_SHA}" .
          digest="$(jq -r '."containerimage.digest"' "$metadata")"
          test -n "$digest" && test "$digest" != null
          ref="${image}@${digest}"
          cosign sign --yes "$ref"
          syft "$ref" -o spdx-json > "$sbom"
          test -s "$sbom"
          cosign attest --yes --type https://spdx.dev/Document --predicate "$sbom" "$ref"
          identity="https://github.com/${GITHUB_REPOSITORY}/.github/workflows/space-compute-supply-chain.yml@refs/heads/main"
          issuer="https://token.actions.githubusercontent.com"
          cosign verify --certificate-identity="$identity" --certificate-oidc-issuer="$issuer" "$ref" >/dev/null
          cosign verify-attestation --type https://spdx.dev/Document --certificate-identity="$identity" --certificate-oidc-issuer="$issuer" "$ref" >/dev/null
          docker buildx imagetools inspect "${image}:${GITHUB_SHA}"
''')

write('scripts/space-compute-phase11', r'''#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mode="${1:-all}"
case "$mode" in
  scheduler)
    go test ./pkg/scheduler/plugins/gpustability -count=1
    go test -race ./pkg/scheduler/plugins/gpustability -count=1
    ;;
  api-admission)
    go test ./contrib/space-compute/pkg/apis/v1alpha1 ./contrib/space-compute/pkg/apis/v1beta1 ./contrib/space-compute/pkg/admission -count=1
    go test -race ./contrib/space-compute/pkg/admission -count=1
    ;;
  planner)
    go test ./contrib/space-compute/pkg/planner ./contrib/space-compute/pkg/kube -count=1
    go test -race ./contrib/space-compute/pkg/planner ./contrib/space-compute/pkg/kube -count=1
    ;;
  transport-execution)
    go test ./contrib/space-compute/pkg/transport ./contrib/space-compute/pkg/execution -count=1
    go test -race ./contrib/space-compute/pkg/transport ./contrib/space-compute/pkg/execution -count=1
    ;;
  controller)
    go test ./contrib/space-compute/pkg/workload ./contrib/space-compute/pkg/kube ./cmd/space-compute-mission-planner ./cmd/space-compute-domain-agent -count=1
    go test -race ./contrib/space-compute/pkg/workload ./contrib/space-compute/pkg/kube ./cmd/space-compute-mission-planner ./cmd/space-compute-domain-agent -count=1
    ;;
  all)
    for group in scheduler api-admission planner transport-execution controller; do "$0" "$group"; done
    ;;
  *)
    echo "usage: $0 {scheduler|api-admission|planner|transport-execution|controller|all}" >&2
    exit 2
    ;;
esac
''')

# Main tests must parse and assert the new operational manifests.
path = 'cmd/space-compute-mission-planner/main_test.go'
text = read(path)
text = replace_once(
    text,
    '"phase4-crds.yaml", "phase9-canonical-crds.yaml", "conversion-webhook.yaml", "storage-version-migrator.yaml", "phase4-admission.yaml", "mission-planner.yaml", "reporter-admission-webhook.yaml", "mission-admission-webhook.yaml", "controller-quotas.yaml"',
    '"phase4-crds.yaml", "phase9-canonical-crds.yaml", "conversion-webhook.yaml", "storage-version-migrator.yaml", "phase4-admission.yaml", "mission-planner.yaml", "metrics-monitoring-access.yaml", "image-signature-policy.yaml", "reporter-admission-webhook.yaml", "mission-admission-webhook.yaml", "controller-quotas.yaml"',
    'main test manifest list',
)
anchor = '''\t\tif name == "reporter-admission-webhook.yaml" {\n'''
insert = '''\t\tif name == "metrics-monitoring-access.yaml" {\n\t\t\tfor _, required := range []string{"tokenreviews", "subjectaccessreviews", "nonResourceURLs: [/metrics]", "space-compute-monitoring", "kind: NetworkPolicy", "port: metrics", "kubernetes.io/metadata.name: space-compute-monitoring"} {\n\t\t\t\tif !strings.Contains(text, required) {\n\t\t\t\t\tt.Fatalf("metrics access manifest missing %q", required)\n\t\t\t\t}\n\t\t\t}\n\t\t}\n\t\tif name == "image-signature-policy.yaml" {\n\t\t\tfor _, required := range []string{"kind: ClusterImagePolicy", "mode: enforce", "https://token.actions.githubusercontent.com", "space-compute-supply-chain.yml@refs/heads/main", "predicateType: https://spdx.dev/Document"} {\n\t\t\t\tif !strings.Contains(text, required) {\n\t\t\t\t\tt.Fatalf("image signature policy missing %q", required)\n\t\t\t\t}\n\t\t\t}\n\t\t}\n'''
text = replace_once(text, anchor, insert + anchor, 'manifest assertion insertion')
# Tighten the existing planner manifest regression with Phase 11 requirements.
text = replace_once(
    text,
    '\t\t\t\t"--api-burst=40",\n',
    '\t\t\t\t"--api-burst=40",\n\t\t\t\t"--health-bind-address=:10271",\n\t\t\t\t"--metrics-tls-cert-file=/var/run/space-compute-metrics/tls.crt",\n\t\t\t\t"--metrics-tls-private-key-file=/var/run/space-compute-metrics/tls.key",\n\t\t\t\t"name: metrics",\n\t\t\t\t"name: health",\n',
    'planner manifest phase11 assertions',
)
write(path, text)
