package admission

import (
	"encoding/json"
	"reflect"

	admissionv1 "k8s.io/api/admission/v1"
)

const StorageMigratorPrincipal = "system:serviceaccount:kube-system:space-compute-storage-migrator"

// IsStorageMigrationNoop permits only the dedicated storage migrator to rewrite
// an object through another served API version without changing its semantic
// spec/status. This is intentionally narrower than an admission bypass: any
// application-level mutation still follows the normal authorization, signature,
// provenance and validation paths.
func IsStorageMigrationNoop(request *admissionv1.AdmissionRequest) bool {
	if request == nil || request.Operation != admissionv1.Update || request.UserInfo.Username != StorageMigratorPrincipal {
		return false
	}
	var current, previous map[string]interface{}
	if json.Unmarshal(request.Object.Raw, &current) != nil || json.Unmarshal(request.OldObject.Raw, &previous) != nil {
		return false
	}
	if !sameObjectIdentity(current, previous) {
		return false
	}
	return reflect.DeepEqual(current["spec"], previous["spec"]) && reflect.DeepEqual(current["status"], previous["status"])
}

func sameObjectIdentity(current, previous map[string]interface{}) bool {
	cm, _ := current["metadata"].(map[string]interface{})
	pm, _ := previous["metadata"].(map[string]interface{})
	for _, key := range []string{"name", "namespace", "uid", "labels", "annotations", "finalizers", "ownerReferences"} {
		if !reflect.DeepEqual(cm[key], pm[key]) {
			return false
		}
	}
	return current["kind"] == previous["kind"]
}
