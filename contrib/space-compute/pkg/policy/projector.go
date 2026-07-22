package policy

import (
	"encoding/json"
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	spacev1 "github.com/k3s-io/k3s/contrib/space-compute/pkg/apis/v1alpha1"
)

const MaxProjectedLinks = 64

// ProjectNode builds the bounded local informer projection owned by the
// resource controller. Scheduler callbacks parse this watched Node annotation;
// they never list CRDs or contact a remote domain.
func ProjectNode(node *corev1.Node, summary *spacev1.SpaceDomainResourceSummary, links []*spacev1.SpaceLinkSnapshot, clock spacev1.Clock) (*corev1.Node, error) {
	if node == nil {
		return nil, fmt.Errorf("node is required")
	}
	if err := spacev1.ValidateResourceSummary(summary, clock); err != nil {
		return nil, err
	}
	projection := NodeProjection{TypeMeta: metav1.TypeMeta{APIVersion: spacev1.SchemeGroupVersion.String(), Kind: "NodeLinkProjection"}, Domain: summary.Spec.Domain, ObservedAt: summary.Spec.ObservedAt, ValidUntil: summary.Spec.ValidUntil, ResourceDigest: summary.Spec.ExporterSnapshotDigest, ResilienceMilli: summary.Spec.ResilienceMilli}
	sorted := append([]*spacev1.SpaceLinkSnapshot(nil), links...)
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i] == nil {
			return false
		}
		if sorted[j] == nil {
			return true
		}
		return sorted[i].Name < sorted[j].Name
	})
	for _, link := range sorted {
		if link == nil || (link.Spec.Source != summary.Spec.Domain && link.Spec.Destination != summary.Spec.Domain) {
			continue
		}
		if err := spacev1.ValidateLinkSnapshot(link, nil, clock); err != nil {
			return nil, fmt.Errorf("link %s: %w", link.Name, err)
		}
		if len(projection.Links) >= MaxProjectedLinks {
			return nil, fmt.Errorf("domain %s exceeds maximum %d projected links", summary.Spec.Domain.Name, MaxProjectedLinks)
		}
		projection.Links = append(projection.Links, ProjectedLink{Name: link.Name, Spec: link.Spec})
		if link.Spec.ObservedAt.Before(&projection.ObservedAt) {
			projection.ObservedAt = link.Spec.ObservedAt
		}
		if link.Spec.ValidUntil.Before(&projection.ValidUntil) {
			projection.ValidUntil = link.Spec.ValidUntil
		}
	}
	raw, err := json.Marshal(projection)
	if err != nil {
		return nil, err
	}
	if len(raw) > maxProjectionBytes {
		return nil, fmt.Errorf("node projection exceeds %d bytes", maxProjectionBytes)
	}
	result := node.DeepCopy()
	if result.Labels == nil {
		result.Labels = map[string]string{}
	}
	if result.Annotations == nil {
		result.Annotations = map[string]string{}
	}
	result.Labels[spacev1.LabelDomain] = summary.Spec.Domain.Name
	result.Labels[spacev1.LabelOrbitClass] = string(summary.Spec.Domain.OrbitClass)
	result.Annotations[spacev1.AnnotationLinkProjection] = string(raw)
	return result, nil
}
