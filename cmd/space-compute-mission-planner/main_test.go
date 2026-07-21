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

func TestPhase4ManifestsHaveCRDsAdmissionIsolationAndLeastPrivilege(t *testing.T) {
	root := filepath.Join("..", "..", "docs", "space-compute", "manifests")
	for _, name := range []string{"phase4-crds.yaml", "phase4-admission.yaml", "mission-planner.yaml"} {
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
			if name == "phase4-crds.yaml" {
				assertStructuralSchemaShape(t, object, name)
			}
			count++
		}
		if count == 0 {
			t.Fatalf("%s contains no objects", name)
		}
		text := string(raw)
		if name == "phase4-crds.yaml" {
			for _, kind := range []string{"SpaceLinkSnapshot", "SpaceDomainResourceSummary", "SpaceMission", "SpacePlacementIntent"} {
				if !strings.Contains(text, "kind: "+kind) {
					t.Fatalf("%s missing %s", name, kind)
				}
			}
		}
		if name == "phase4-admission.yaml" && (!strings.Contains(text, "failurePolicy: Fail") || !strings.Contains(text, "request.userInfo.username")) {
			t.Fatal("admission policy does not fail closed on reporter identity")
		}
		if name == "mission-planner.yaml" {
			if strings.Contains(text, "resources: [secrets]") || !strings.Contains(text, "resourceNames: [space-compute-mission-planner]") || !strings.Contains(text, "replicas: 2") {
				t.Fatal("planner RBAC/deployment isolation regression")
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
