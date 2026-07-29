from pathlib import Path


def edit(path, replacements):
    p = Path(path)
    text = p.read_text()
    for old, new in replacements:
        count = text.count(old)
        if count != 1:
            raise SystemExit(f'{path}: expected one occurrence of {old!r}, got {count}')
        text = text.replace(old, new, 1)
    p.write_text(text)

edit('cmd/space-compute-domain-agent/store.go', [
    ('func sameOrAdvance(old, new *unstructured.Unstructured) (bool, error) {\n\toldSeq, _, _ := unstructured.NestedInt64(old.Object, "spec", "provenance", "sequence")\n\tnewSeq, _, _ := unstructured.NestedInt64(new.Object, "spec", "provenance", "sequence")\n\toldDigest, _, _ := unstructured.NestedString(old.Object, "spec", "provenance", "digest")\n\tnewDigest, _, _ := unstructured.NestedString(new.Object, "spec", "provenance", "digest")',
     'func sameOrAdvance(old, next *unstructured.Unstructured) (bool, error) {\n\toldSeq, _, _ := unstructured.NestedInt64(old.Object, "spec", "provenance", "sequence")\n\tnewSeq, _, _ := unstructured.NestedInt64(next.Object, "spec", "provenance", "sequence")\n\toldDigest, _, _ := unstructured.NestedString(old.Object, "spec", "provenance", "digest")\n\tnewDigest, _, _ := unstructured.NestedString(next.Object, "spec", "provenance", "digest")'),
    ('return reflect.DeepEqual(old.Object["spec"], new.Object["spec"]), nil', 'return reflect.DeepEqual(old.Object["spec"], next.Object["spec"]), nil'),
])

edit('cmd/space-compute-mission-planner/bounded_queue.go', [
    ('package main\n', '// Package main implements the space-compute mission planner controllers.\npackage main\n'),
])
edit('contrib/space-compute/pkg/admission/handler.go', [
    ('package admission\n', '// Package admission provides fail-closed Space Compute admission validation.\npackage admission\n'),
])
edit('contrib/space-compute/pkg/migration/migrator.go', [
    ('package migration\n', '// Package migration provides storage-version migration for Space Compute APIs.\npackage migration\n'),
])

edit('contrib/space-compute/pkg/apis/v1alpha1/canonical_test.go', [
    ('\tcopy := base.DeepCopy()\n\tcopy.Spec.Windows[0].BandwidthBitsPerSec++\n\tdigestB, err := ReporterDigest(copy)',
     '\tchanged := base.DeepCopy()\n\tchanged.Spec.Windows[0].BandwidthBitsPerSec++\n\tdigestB, err := ReporterDigest(changed)'),
])

p = Path('contrib/space-compute/pkg/apis/v1alpha1/validation_test.go')
text = p.read_text()
if text.count('f.Fuzz(func(t *testing.T, raw string) {') != 3:
    raise SystemExit('validation fuzz callback count changed')
text = text.replace('f.Fuzz(func(t *testing.T, raw string) {', 'f.Fuzz(func(_ *testing.T, raw string) {')
p.write_text(text)

edit('contrib/space-compute/pkg/kube/repository.go', [
    ('\t\tif desired.ActivePod != nil {\n\t\t\tcopy := *desired.ActivePod\n\t\t\tout.ActivePod = &copy\n\t\t}',
     '\t\tif desired.ActivePod != nil {\n\t\t\tactivePod := *desired.ActivePod\n\t\t\tout.ActivePod = &activePod\n\t\t}'),
    ('\t\tif desired.LastObservation != nil {\n\t\t\tcopy := *desired.LastObservation\n\t\t\tout.LastObservation = &copy\n\t\t}',
     '\t\tif desired.LastObservation != nil {\n\t\t\tlastObservation := *desired.LastObservation\n\t\t\tout.LastObservation = &lastObservation\n\t\t}'),
    ('func (s *WorkloadStore) Event(ctx context.Context, namespace, name, eventType, reason, message string) {',
     'func (s *WorkloadStore) Event(_ context.Context, namespace, name, eventType, reason, message string) {'),
])

edit('contrib/space-compute/pkg/kube/repository_test.go', [
    ('func(action k8stesting.Action) (bool, runtime.Object, error) {\n\t\treturn true, nil, fmt.Errorf("unexpected resource summary API list")',
     'func(_ k8stesting.Action) (bool, runtime.Object, error) {\n\t\treturn true, nil, fmt.Errorf("unexpected resource summary API list")'),
    ('func(action k8stesting.Action) (bool, runtime.Object, error) {\n\t\treturn true, nil, fmt.Errorf("unexpected link API list")',
     'func(_ k8stesting.Action) (bool, runtime.Object, error) {\n\t\treturn true, nil, fmt.Errorf("unexpected link API list")'),
])

edit('contrib/space-compute/pkg/planner/planner_test.go', [
    ('repository.links[0].Spec.Windows[0].BandwidthBitsPerSec += 1', 'repository.links[0].Spec.Windows[0].BandwidthBitsPerSec++'),
])

edit('contrib/space-compute/pkg/transport/agent.go', [
    ('func (a *Agent) acceptAck(ctx context.Context, e *Envelope, ack *TransferAck) error {',
     'func (a *Agent) acceptAck(ctx context.Context, _ *Envelope, ack *TransferAck) error {'),
    ('func (a *Agent) ReportExecution(ctx context.Context, namespace string, report spaceexecution.Report) error {',
     'func (a *Agent) ReportExecution(ctx context.Context, _ string, report spaceexecution.Report) error {'),
    ('func OpenFileAssignmentStore(dir string, max int) (*FileAssignmentStore, error) {\n\tif max < 1 || max > 10000 {',
     'func OpenFileAssignmentStore(dir string, maxEntries int) (*FileAssignmentStore, error) {\n\tif maxEntries < 1 || maxEntries > 10000 {'),
    ('return &FileAssignmentStore{dir: dir, max: max}, nil', 'return &FileAssignmentStore{dir: dir, max: maxEntries}, nil'),
])

edit('contrib/space-compute/pkg/transport/hardening.go', [
    ('\tvar max int64\n\tfor _, lease := range leases {\n\t\tif lease != nil && lease.Spec.Fence.MissionUID == missionUID && lease.Spec.Fence.LeaseEpoch > max {\n\t\t\tmax = lease.Spec.Fence.LeaseEpoch\n\t\t}\n\t}\n\treturn max',
     '\tvar latest int64\n\tfor _, lease := range leases {\n\t\tif lease != nil && lease.Spec.Fence.MissionUID == missionUID && lease.Spec.Fence.LeaseEpoch > latest {\n\t\t\tlatest = lease.Spec.Fence.LeaseEpoch\n\t\t}\n\t}\n\treturn latest'),
    ('\tcopy := intent.DeepCopy()\n\tcopy.ResourceVersion = ""\n\tcopy.UID = types.UID("")\n\tcopy.Generation = 0\n\tcopy.CreationTimestamp = metav1.Time{}\n\tcopy.DeletionTimestamp = nil\n\tcopy.DeletionGracePeriodSeconds = nil\n\tcopy.ManagedFields = nil\n\tcopy.OwnerReferences = nil\n\tcopy.Finalizers = nil\n\traw, err := json.Marshal(copy)',
     '\tintentCopy := intent.DeepCopy()\n\tintentCopy.ResourceVersion = ""\n\tintentCopy.UID = types.UID("")\n\tintentCopy.Generation = 0\n\tintentCopy.CreationTimestamp = metav1.Time{}\n\tintentCopy.DeletionTimestamp = nil\n\tintentCopy.DeletionGracePeriodSeconds = nil\n\tintentCopy.ManagedFields = nil\n\tintentCopy.OwnerReferences = nil\n\tintentCopy.Finalizers = nil\n\traw, err := json.Marshal(intentCopy)'),
    ('id := transferIntentEnvelopeID(copy.Name, destination)\n\te := NewEnvelope(id, TransferIntentKind, a.Local, destination, copy.Spec.MissionUID, copy.Spec.PlanID, copy.Spec.Attempt, 1, a.now(), copy.Spec.ExpiresAt.Time, raw)',
     'id := transferIntentEnvelopeID(intentCopy.Name, destination)\n\te := NewEnvelope(id, TransferIntentKind, a.Local, destination, intentCopy.Spec.MissionUID, intentCopy.Spec.PlanID, intentCopy.Spec.Attempt, 1, a.now(), intentCopy.Spec.ExpiresAt.Time, raw)'),
])

edit('contrib/space-compute/pkg/workload/controller.go', [
    ('func BuildAttemptPod(mission *spacev1.SpaceMission, placement *spacev1.SpacePlacementIntent, template corev1.PodTemplateSpec) (*corev1.Pod, error) {',
     'func BuildAttemptPod(_ *spacev1.SpaceMission, placement *spacev1.SpacePlacementIntent, template corev1.PodTemplateSpec) (*corev1.Pod, error) {'),
])

edit('pkg/scheduler/plugins/gpustability/phase0_test.go', [
    ('func(w http.ResponseWriter, r *http.Request) {\n\t\tw.Header().Set("Content-Type", "text/plain; version=0.0.4")',
     'func(w http.ResponseWriter, _ *http.Request) {\n\t\tw.Header().Set("Content-Type", "text/plain; version=0.0.4")'),
    ('func(w http.ResponseWriter, r *http.Request) {\n\t\t<-r.Context().Done()',
     'func(_ http.ResponseWriter, r *http.Request) {\n\t\t<-r.Context().Done()'),
])

# Keep the real Phase 4 K3s e2e surface intact: run the planner with HTTPS metrics,
# using a temporary self-signed server certificate rather than disabling metrics.
p = Path('tests/integration/spacecomputescheduler/phase4_external_int_test.go')
text = p.read_text()
imports = '\t"bytes"\n\t"context"\n'
secure_imports = '\t"bytes"\n\t"context"\n\t"crypto/ecdsa"\n\t"crypto/elliptic"\n\t"crypto/rand"\n\t"crypto/x509"\n\t"encoding/pem"\n'
if text.count(imports) != 1:
    raise SystemExit('phase4 imports marker missing')
text = text.replace(imports, secure_imports, 1)
text = text.replace('\t"net/http"\n', '\t"math/big"\n\t"net"\n\t"net/http"\n', 1)
old = 'process.cmd = exec.Command(binary, "--kubeconfig="+kubeconfig, "--leader-elect=false", "--workers=2", "--metrics-bind-address=", "--health-bind-address=127.0.0.1:13361")'
new = 'certFile, keyFile := writePlannerMetricsTLS(t)\n\tprocess.cmd = exec.Command(binary, "--kubeconfig="+kubeconfig, "--leader-elect=false", "--workers=2", "--metrics-bind-address=127.0.0.1:13361", "--health-bind-address=127.0.0.1:13362", "--metrics-tls-cert-file="+certFile, "--metrics-tls-private-key-file="+keyFile)'
if text.count(old) != 1:
    raise SystemExit('phase4 secure planner command marker missing')
text = text.replace(old, new, 1)
marker = '\nfunc (p *plannerProcess) stop(t *testing.T) {'
helper = r'''

func writePlannerMetricsTLS(t *testing.T) (string, string) {
	t.Helper()
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore: now.Add(-time.Minute),
		NotAfter: now.Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames: []string{"localhost"},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}
	certificateDER, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	privateDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certFile := filepath.Join(dir, "metrics.crt")
	keyFile := filepath.Join(dir, "metrics.key")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificateDER}), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateDER}), 0600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile
}
'''
if text.count(marker) != 1:
    raise SystemExit('phase4 planner stop marker missing')
text = text.replace(marker, helper + marker, 1)
p.write_text(text)
