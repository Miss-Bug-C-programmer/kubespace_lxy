package admission

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic/fake"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
)

func TestKubernetesTrustSourceUsesDerivedBindingAndSingleSecret(t *testing.T) {
	principal := "system:serviceaccount:reporters:leo-a"
	domain := spacev1.DomainReference{Name: "leo-a", ClusterID: "leo-cluster", OrbitClass: spacev1.OrbitLEO}
	ref := spacev1.SecretReference{Namespace: "kube-system", Name: "space-compute-reporter-public-keys", Key: "leo-a"}
	binding := &spacev1.SpaceDomainReporterBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: spacev1.CanonicalAPIVersion, Kind: "SpaceDomainReporterBinding"},
		ObjectMeta: metav1.ObjectMeta{Name: spacev1.ReporterBindingName(principal)},
		Spec:       spacev1.SpaceDomainReporterBindingSpec{ReporterPrincipal: principal, Domain: domain, AllowedKinds: []string{"SpaceDomainResourceSummary"}, PublicKeyRef: ref},
	}
	raw, err := runtime.DefaultUnstructuredConverter.ToUnstructured(binding)
	if err != nil {
		t.Fatal(err)
	}
	publicKey, _, _ := ed25519.GenerateKey(rand.Reader)
	scheme := runtime.NewScheme()
	dynamicClient := fake.NewSimpleDynamicClient(scheme, &unstructured.Unstructured{Object: raw})
	coreClient := kubernetesfake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: ref.Name, Namespace: ref.Namespace},
		Data:       map[string][]byte{ref.Key: publicKey},
	})
	source, err := NewKubernetesTrustSource(dynamicClient, coreClient, ref.Namespace, ref.Name)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := source.Binding(context.Background(), principal)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Spec.ReporterPrincipal != principal || resolved.Spec.Domain != domain {
		t.Fatalf("resolved binding=%+v", resolved.Spec)
	}
	key, err := source.PublicKey(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if !key.Equal(publicKey) {
		t.Fatal("resolved public key mismatch")
	}
	outside := ref
	outside.Name = "other-secret"
	if _, err := source.PublicKey(context.Background(), outside); err == nil || !strings.Contains(err.Error(), "outside configured trust Secret") {
		t.Fatalf("outside Secret error=%v", err)
	}
}
