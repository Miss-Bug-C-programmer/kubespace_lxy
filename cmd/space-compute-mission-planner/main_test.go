package main

import (
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"k8s.io/apimachinery/pkg/util/yaml"
)

func TestHealthEndpointsDistinguishLiveAndLeaderReady(t *testing.T) {
	var ready atomic.Bool
	server := healthServer(":0", &ready)
	request := httptest.NewRequest("GET", "/livez", nil)
	response := httptest.NewRecorder()
	server.Handler.ServeHTTP(response, request)
	if response.Code != 200 {
		t.Fatalf("livez=%d", response.Code)
	}
	request = httptest.NewRequest("GET", "/readyz", nil)
	response = httptest.NewRecorder()
	server.Handler.ServeHTTP(response, request)
	if response.Code != 503 {
		t.Fatalf("standby readyz=%d", response.Code)
	}
	ready.Store(true)
	response = httptest.NewRecorder()
	server.Handler.ServeHTTP(response, request)
	if response.Code != 200 {
		t.Fatalf("leader readyz=%d", response.Code)
	}
}

func TestControllerRolesAreMutuallyExplicit(t *testing.T) {
	for _, role := range []controllerRole{rolePlanner, roleDispatcher, roleProjector, roleTransport} {
		if !validControllerRole(role) {
			t.Fatalf("expected valid role %q", role)
		}
	}
	for _, role := range []controllerRole{"", "all", "combined"} {
		if validControllerRole(role) {
			t.Fatalf("unexpected valid role %q", role)
		}
	}
}

func TestPhase4AndPhase6ManifestsHaveAdmissionIsolationAndLeastPrivilege(t *testing.T) {
	root := filepath.Join("..", "..", "docs", "space-compute", "manifests")
	for _, name := range []string{"phase4-crds.yaml", "phase9-canonical-crds.yaml", "conversion-webhook.yaml", "storage-version-migrator.yaml", "phase4-admission.yaml", "mission-planner.yaml", "reporter-admission-webhook.yaml", "mission-admission-webhook.yaml", "controller-quotas.yaml"} {
		raw, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		decoder := yaml.NewYAMLOrJSONDecoder(strings.NewReader(string(raw)), 4096)
		count := 0
		for {
			var object map[string]interface{}
			err := decoder.Decode(&object)
			if err != nil {
				if strings.Contains(err.Error(), "EOF") {
					break
				}
				t.Fatalf("decode %s document %d: %v", name, count, err)
			}
			if len(object) == 0 {
				continue
			}
			if name == "phase4-crds.yaml" || name == "phase9-canonical-crds.yaml" {
				assertStructuralSchemaShape(t, object, name)
			}
			count++
		}
		if count == 0 {
			t.Fatalf("%s contains no objects", name)
		}
		text := string(raw)
		if name == "phase4-crds.yaml" {
			for _, kind := range []string{"SpaceLinkSnapshot", "SpaceDomainResourceSummary", "SpaceDomainReporterBinding", "SpaceTransferReceipt", "SpaceResultReceipt", "SpaceMission", "SpacePlacementIntent"} {
				if !strings.Contains(text, "kind: "+kind) {
					t.Fatalf("%s missing %s", name, kind)
				}
			}
			for _, required := range []string{
				"count: {type: integer, format: int64, minimum: 0, maximum: 1000000}",
				"computeMilli: {type: integer, format: int64, minimum: 0, maximum: 1000000000}",
				"queueDelaySeconds: {type: integer, format: int64, minimum: 0, maximum: 2592000}",
				"maximumSnapshotAgeSeconds: {type: integer, format: int64, minimum: 1, maximum: 604800}",
				"items: &missionDataLocation",
				"self.durationUncertaintySeconds <= self.maximumDurationSeconds - self.expectedDurationSeconds",
				"planningInputDigest: {type: string, pattern: '^[a-f0-9]{64}$'}",
				"cacheResourceVersions: {type: object, maxProperties: 2",
			} {
				if !strings.Contains(text, required) {
					t.Fatalf("phase7 CRD bound/location schema missing %q", required)
				}
			}
		}
		if name == "phase9-canonical-crds.yaml" {
			for _, required := range []string{
				"name: v1alpha1", "name: v1beta1", "strategy: Webhook", "path: /convert",
				"kind: PhysicalDeviceInventory", "workingMemoryBytes:", "workingStorageBytes:",
				"minimumBandwidthBitsPerSecond:", "maximumRTTMicroseconds:", "maximumLossPartsPerMillion:",
				"selectedCapabilitySetName:", "selectedPhysicalDeviceConstraints:", "transferState:",
				"remoteAcknowledgementSequence:", "draAllocationID:", "vendorAllocationID:",
			} {
				if !strings.Contains(text, required) {
					t.Fatalf("phase9 canonical CRD missing %q", required)
				}
			}
			if strings.Count(text, "storage: true") != 11 || strings.Count(text, "storage: false") != 11 {
				t.Fatalf("phase9 storage version counts are not 11 beta/11 alpha")
			}
		}
		if name == "conversion-webhook.yaml" {
			for _, required := range []string{"space-compute-conversion-webhook", "--bind-address=:9445"} {
				if !strings.Contains(text, required) {
					t.Fatalf("conversion webhook manifest missing %q", required)
				}
			}
		}
		if name == "storage-version-migrator.yaml" {
			for _, required := range []string{"space-compute-storage-migrator", "suspend: true", "--target-version=v1beta1", "customresourcedefinitions/status"} {
				if !strings.Contains(text, required) {
					t.Fatalf("storage migrator manifest missing %q", required)
				}
			}
		}
		if name == "phase4-admission.yaml" {
			for _, required := range []string{
				"failurePolicy: Fail",
				"request.userInfo.username",
				"object.spec.provenance.sequence == oldObject.spec.provenance.sequence + 1",
				"system:serviceaccount:kube-system:space-compute-mission-planner",
				"resources: [spacemissions]",
				"resources: [spacelinksnapshots]",
				"resources: [spacedomainresourcesummaries]",
				"resources: [spaceplacementintents]",
				"object.spec.durationUncertaintySeconds <= object.spec.maximumDurationSeconds - object.spec.expectedDurationSeconds",
			} {
				if !strings.Contains(text, required) {
					t.Fatalf("admission policy missing %q", required)
				}
			}
		}
		if name == "mission-planner.yaml" {
			for _, required := range []string{
				"name: space-compute-workload-dispatcher",
				"name: space-compute-node-projector",
				"name: space-compute-transport-agent",
				"name: system:space-compute-workload-dispatcher-namespace",
				"name: system:space-compute-node-projector",
				"name: system:space-compute-transport-agent",
				"--controller-role=planner",
				"--controller-role=workload-dispatcher",
				"--controller-role=node-projector",
				"--controller-role=transport-agent",
				"verbs: [get, list, watch, patch]",
				"--max-pending-unique-keys=10000",
				"--api-qps=20",
				"--api-burst=40",
			} {
				if !strings.Contains(text, required) {
					t.Fatalf("split planner manifest missing %q", required)
				}
			}
			plannerStart := strings.Index(text, "name: system:space-compute-mission-planner")
			plannerEnd := strings.Index(text, "name: space-compute-mission-planner\nroleRef:")
			if plannerStart < 0 || plannerEnd <= plannerStart {
				t.Fatal("planner role boundaries not found")
			}
			plannerRole := text[plannerStart:plannerEnd]
			if strings.Contains(plannerRole, "resources: [pods]") || strings.Contains(plannerRole, "resources: [nodes]") || strings.Contains(plannerRole, "spacetransferintents") {
				t.Fatal("planner role retained Pod/Node/transport privileges")
			}
			transportStart := strings.Index(text, "name: system:space-compute-transport-agent")
			transportEnd := strings.Index(text[transportStart:], "kind: ClusterRoleBinding")
			if transportStart < 0 || transportEnd < 0 {
				t.Fatal("transport role boundaries not found")
			}
			transportRole := text[transportStart : transportStart+transportEnd]
			for _, forbidden := range []string{"resources: [pods]", "resources: [nodes]", "resources: [secrets]"} {
				if strings.Contains(transportRole, forbidden) {
					t.Fatalf("transport role retained forbidden privilege %q", forbidden)
				}
			}
		}
		if name == "reporter-admission-webhook.yaml" {
			for _, required := range []string{"kind: ValidatingWebhookConfiguration", "failurePolicy: Fail", "space-compute-reporter-public-keys", "resourceNames: [space-compute-reporter-public-keys]", "resources: [spacedomainreporterbindings]", "resources: [spacelinksnapshots, spacedomainresourcesummaries, physicaldeviceinventories]", "--max-link-snapshots=10000", "--max-resource-summaries=10000", "--max-physical-device-inventories=10000", "--reporter-qps=20", "--reporter-burst=40", "spacetransferreceipts", "spaceresultreceipts", "physicaldeviceinventories", "apiVersions: [v1alpha1, v1beta1]"} {
				if !strings.Contains(text, required) {
					t.Fatalf("reporter webhook manifest missing %q", required)
				}
			}
		}
		if name == "controller-quotas.yaml" {
			for _, required := range []string{"kind: ResourceQuota", "count/spacemissions.spacecompute.k3s.io", "count/spaceplacementintents.spacecompute.k3s.io", "maxPendingUniqueKeys", "maxLinkSnapshots", "reporterQPS"} {
				if !strings.Contains(text, required) {
					t.Fatalf("controller quota manifest missing %q", required)
				}
			}
		}
		if name == "mission-admission-webhook.yaml" {
			for _, required := range []string{
				"resources: [subjectaccessreviews]",
				"kind: ValidatingWebhookConfiguration",
				"failurePolicy: Fail",
				"resources: [spacemissions]",
				"resources: [pods]",
				"allowedServiceAccounts",
				"allowedImageRegistries",
				"attemptPodCreators",
				"matchConditions:",
				"controlled-attempt-or-approved-dispatcher",
			} {
				if !strings.Contains(text, required) {
					t.Fatalf("mission webhook manifest missing %q", required)
				}
			}
		}
	}
}

// Kubernetes rejects CRD schema nodes that combine named properties with an
// explicit additionalProperties value. Keep this local guard because a YAML
// decoder alone cannot detect that API-server structural-schema constraint.
func assertStructuralSchemaShape(t *testing.T, value interface{}, path string) {
	t.Helper()
	switch current := value.(type) {
	case map[string]interface{}:
		if _, hasProperties := current["properties"]; hasProperties {
			if _, hasAdditional := current["additionalProperties"]; hasAdditional {
				t.Fatalf("%s combines properties and additionalProperties", path)
			}
		}
		for key, child := range current {
			assertStructuralSchemaShape(t, child, path+"."+key)
		}
	case []interface{}:
		for index, child := range current {
			assertStructuralSchemaShape(t, child, path+"["+strconv.Itoa(index)+"]")
		}
	}
}
