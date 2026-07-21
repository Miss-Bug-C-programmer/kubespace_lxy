package main

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	schedulerconfig "k8s.io/kubernetes/pkg/scheduler/apis/config"
	schedulerconfigscheme "k8s.io/kubernetes/pkg/scheduler/apis/config/scheme"
	"sigs.k8s.io/yaml"
)

func TestCommandIdentityAndUpstreamSchedulerSurface(t *testing.T) {
	stop := make(chan struct{})
	command := newSchedulerCommand(stop)
	if command.Use != componentName {
		t.Fatalf("command use = %q, want %q", command.Use, componentName)
	}
	for _, flag := range []string{"config", "leader-elect", "secure-port", "authentication-kubeconfig", "authorization-kubeconfig"} {
		if command.Flags().Lookup(flag) == nil {
			t.Fatalf("upstream kube-scheduler flag %q is not exposed", flag)
		}
	}
	var output bytes.Buffer
	command.SetOut(&output)
	command.SetErr(&output)
	command.SetArgs([]string{"--help"})
	if err := command.Execute(); err != nil {
		t.Fatalf("help: %v", err)
	}
	text := output.String()
	if !strings.Contains(text, componentName) || !strings.Contains(text, "--config") {
		t.Fatalf("help does not identify component/configuration:\n%s", text)
	}
}

func TestDeploymentManifestHasIsolatedIdentityAndRequiredResources(t *testing.T) {
	path := "../../docs/gpu-scheduler/manifests/space-compute-scheduler.yaml"
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	decoder := utilyaml.NewYAMLOrJSONDecoder(file, 4096)
	found := map[string]bool{}
	for {
		object := &unstructured.Unstructured{}
		if err := decoder.Decode(object); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("decode manifest: %v", err)
		}
		if object.GetKind() == "" {
			continue
		}
		found[object.GetKind()+"/"+object.GetName()] = true
		if object.GetKind() == "Deployment" {
			serviceAccount, _, _ := unstructured.NestedString(object.Object, "spec", "template", "spec", "serviceAccountName")
			if serviceAccount != componentName {
				t.Fatalf("Deployment serviceAccountName = %q", serviceAccount)
			}
		}
		if object.GetKind() == "ConfigMap" {
			config, _, _ := unstructured.NestedString(object.Object, "data", "scheduler.yaml")
			if strings.Contains(config, "schedulerName: default-scheduler") || !strings.Contains(config, "resourceName: "+componentName) {
				t.Fatalf("ConfigMap does not isolate scheduler profile/lease:\n%s", config)
			}
		}
	}
	for _, required := range []string{
		"ServiceAccount/space-compute-scheduler", "ClusterRole/system:space-compute-scheduler",
		"ClusterRoleBinding/space-compute-scheduler", "Role/space-compute-scheduler-leader-election",
		"RoleBinding/space-compute-scheduler-leader-election", "ConfigMap/space-compute-scheduler-config",
		"Deployment/space-compute-scheduler", "Service/space-compute-scheduler",
	} {
		if !found[required] {
			t.Fatalf("manifest lacks %s; found=%v", required, found)
		}
	}
}

func TestProductionConfigurationContainsOnlyIsolatedProfile(t *testing.T) {
	path := "../../docs/gpu-scheduler/kube-scheduler-gpu-stability.yaml"
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	jsonData, err := yaml.YAMLToJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	decoded, _, err := schedulerconfigscheme.Codecs.UniversalDecoder().Decode(jsonData, nil, nil)
	if err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	configuration, ok := decoded.(*schedulerconfig.KubeSchedulerConfiguration)
	if !ok {
		t.Fatalf("decoded type = %T", decoded)
	}
	if len(configuration.Profiles) != 1 || configuration.Profiles[0].SchedulerName != componentName {
		t.Fatalf("profiles = %+v, want only %q", configuration.Profiles, componentName)
	}
	if configuration.LeaderElection.ResourceName != componentName || configuration.LeaderElection.ResourceNamespace != "kube-system" {
		t.Fatalf("leader election = %+v", configuration.LeaderElection)
	}
	if strings.Contains(string(raw), "schedulerName: default-scheduler") {
		t.Fatal("standalone configuration couples the default-scheduler profile")
	}
}
