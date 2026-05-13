package server

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestGetNodeSKU(t *testing.T) {
	tests := []struct {
		name   string
		labels map[string]string
		want   string
	}{
		{
			name: "uses crusoe labels ahead of stable instance type",
			labels: map[string]string{
				corev1.LabelInstanceTypeStable: "l40s-48gb.10x",
				"crusoe.ai/instance.class":     "l40s-48gb",
				"crusoe.ai/nodepool.name":      "prod-use1a-toaster-gpu-l40s-10x",
			},
			want: "l40s-48gb.10x",
		},
		{
			name: "uses gpu product instead of generic rke2 instance type",
			labels: map[string]string{
				corev1.LabelInstanceTypeStable: "rke2",
				"nvidia.com/gpu.product":       "NVIDIA-RTX-PRO-6000-Blackwell-Server-Edition",
				"nvidia.com/gpu.count":         "8",
			},
			want: "8x NVIDIA RTX PRO 6000 Blackwell Server Edition",
		},
		{
			name: "normalizes crusoe cpu nodepool",
			labels: map[string]string{
				"crusoe.ai/instance.class": "c1a",
				"crusoe.ai/nodepool.name":  "prod-use1a-toaster-cpu-c1a-16x",
			},
			want: "c1a.16x",
		},
		{
			name: "normalizes crusoe gpu nodepool",
			labels: map[string]string{
				"crusoe.ai/instance.class": "l40s-48gb",
				"crusoe.ai/nodepool.name":  "prod-use1a-toaster-gpu-l40s-10x",
			},
			want: "l40s-48gb.10x",
		},
		{
			name: "falls back to instance class",
			labels: map[string]string{
				"crusoe.ai/instance.class": "c1a",
			},
			want: "c1a",
		},
		{
			name:   "uses unknown without labels",
			labels: map[string]string{},
			want:   "unknown",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			node := corev1.Node{}
			node.Labels = test.labels
			if got := getNodeSKU(node); got != test.want {
				t.Fatalf("getNodeSKU() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestAddResourceListIncludesFirstClassResources(t *testing.T) {
	summary := resourceSummary{}
	addResourceList(&summary, corev1.ResourceList{
		corev1.ResourceCPU:                    resource.MustParse("2500m"),
		corev1.ResourceMemory:                 resource.MustParse("8Gi"),
		corev1.ResourceEphemeralStorage:       resource.MustParse("100Gi"),
		corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("4"),
	})

	if summary.CPUCores != 2.5 {
		t.Fatalf("CPUCores = %v, want 2.5", summary.CPUCores)
	}
	if summary.MemoryBytes != 8*1024*1024*1024 {
		t.Fatalf("MemoryBytes = %v, want 8Gi", summary.MemoryBytes)
	}
	if summary.EphemeralStorageBytes != 100*1024*1024*1024 {
		t.Fatalf("EphemeralStorageBytes = %v, want 100Gi", summary.EphemeralStorageBytes)
	}
	if summary.GPUs != 4 {
		t.Fatalf("GPUs = %v, want 4", summary.GPUs)
	}
}

func TestSummarizeAssignedPodRequests(t *testing.T) {
	pods := []corev1.Pod{
		{
			Spec: corev1.PodSpec{
				NodeName: "node-a",
				Containers: []corev1.Container{
					{
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:                    resource.MustParse("1500m"),
								corev1.ResourceMemory:                 resource.MustParse("4Gi"),
								corev1.ResourceEphemeralStorage:       resource.MustParse("20Gi"),
								corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("1"),
							},
						},
					},
					{
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("500m"),
								corev1.ResourceMemory: resource.MustParse("2Gi"),
							},
						},
					},
				},
				InitContainers: []corev1.Container{
					{
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("4"),
								corev1.ResourceMemory: resource.MustParse("1Gi"),
							},
						},
					},
				},
				Overhead: corev1.ResourceList{
					corev1.ResourceCPU: resource.MustParse("250m"),
				},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		},
		{
			Spec: corev1.PodSpec{
				NodeName: "node-a",
				Containers: []corev1.Container{
					{
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU: resource.MustParse("10"),
							},
						},
					},
				},
			},
			Status: corev1.PodStatus{Phase: corev1.PodSucceeded},
		},
		{
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU: resource.MustParse("10"),
							},
						},
					},
				},
			},
			Status: corev1.PodStatus{Phase: corev1.PodPending},
		},
	}

	requestsByNode := summarizeAssignedPodRequests(pods)
	requested := requestsByNode["node-a"]
	if requested.CPUCores != 4.25 {
		t.Fatalf("CPUCores = %v, want 4.25", requested.CPUCores)
	}
	if requested.MemoryBytes != 6*1024*1024*1024 {
		t.Fatalf("MemoryBytes = %v, want 6Gi", requested.MemoryBytes)
	}
	if requested.EphemeralStorageBytes != 20*1024*1024*1024 {
		t.Fatalf("EphemeralStorageBytes = %v, want 20Gi", requested.EphemeralStorageBytes)
	}
	if requested.GPUs != 1 {
		t.Fatalf("GPUs = %v, want 1", requested.GPUs)
	}
	if _, ok := requestsByNode[""]; ok {
		t.Fatal("expected unassigned pending pod to be ignored")
	}
}

func TestSummarizeActiveExecutionsRequiresExecutionLabels(t *testing.T) {
	pods := []corev1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "flytesnacks-development",
				Labels: map[string]string{
					"project":      "flytesnacks",
					"domain":       "development",
					"execution-id": "execution-a",
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:                    resource.MustParse("1"),
								corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("1"),
							},
						},
					},
				},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "flytesnacks-development",
				Labels: map[string]string{
					"app": "flytepropeller",
				},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		},
	}

	executions := summarizeActiveExecutions(pods, map[string]bool{"flytesnacks-development": true})
	if len(executions) != 1 {
		t.Fatalf("len(executions) = %v, want 1", len(executions))
	}
	if executions[0].Name != "execution-a" {
		t.Fatalf("execution name = %q, want execution-a", executions[0].Name)
	}
	if executions[0].Requested.GPUs != 1 {
		t.Fatalf("requested GPUs = %v, want 1", executions[0].Requested.GPUs)
	}
}

func TestGetNodeSKUFallsBackToAllocatableShape(t *testing.T) {
	node := corev1.Node{}
	node.Labels = map[string]string{
		corev1.LabelInstanceTypeStable: "rke2",
	}
	node.Status.Allocatable = corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("15500m"),
		corev1.ResourceMemory: resource.MustParse("32Gi"),
	}

	if got := getNodeSKU(node); got != "15.5 CPU / 32 GiB RAM" {
		t.Fatalf("getNodeSKU() = %q, want allocatable shape", got)
	}
}

func TestCrusoeClusterHelpers(t *testing.T) {
	node := corev1.Node{}
	node.Name = "np-123.us-east1-a.compute.internal"
	node.Labels = map[string]string{
		"crusoe.ai/instance.id": "instance-id",
	}

	if !isCrusoeOnDemandNode(node) {
		t.Fatal("expected Crusoe node to be detected as on-demand")
	}
	if got := getNodeRegion(node); got != "us-east1-a" {
		t.Fatalf("getNodeRegion() = %q, want us-east1-a", got)
	}
	if !isCrusoeCapacitySKU("l40s-48gb.10x") {
		t.Fatal("expected l40s-48gb.10x to be a Crusoe capacity SKU")
	}
	if isCrusoeCapacitySKU("8x NVIDIA RTX PRO 6000 Blackwell Server Edition") {
		t.Fatal("did not expect display-only GPU product name to be a Crusoe capacity SKU")
	}
}

func TestResolveCrusoeCredentialsFromSecret(t *testing.T) {
	client := fake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      crusoeSecretName,
			Namespace: crusoeSecretNamespace,
		},
		Data: map[string][]byte{
			"CRUSOE_ACCESS_KEY": []byte("access"),
			"CRUSOE_SECRET_KEY": []byte("c2VjcmV0"),
		},
	})

	credentials, err := resolveCrusoeCredentials(context.Background(), client)
	if err != nil {
		t.Fatalf("resolveCrusoeCredentials() returned error: %v", err)
	}
	if credentials.AccessKey != "access" || credentials.SecretKey != "c2VjcmV0" {
		t.Fatalf("resolveCrusoeCredentials() = %+v, want secret credentials", credentials)
	}
}

func TestGenerateCrusoeSignature(t *testing.T) {
	signature, err := generateCrusoeSignature("c2VjcmV0", "/v1alpha5/capacities", "", "GET", "2025-01-01T00:00:00Z")
	if err != nil {
		t.Fatalf("generateCrusoeSignature() returned error: %v", err)
	}
	if signature == "" {
		t.Fatal("expected non-empty signature")
	}
	for _, char := range signature {
		if !strings.ContainsRune("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_", char) {
			t.Fatalf("signature contains non-base64url character %q", char)
		}
	}
}

func TestSummarizeNodeCachedImages(t *testing.T) {
	node := corev1.Node{}
	node.Name = "node-a"
	node.Status.Allocatable = corev1.ResourceList{
		corev1.ResourceCPU:                    resource.MustParse("2"),
		corev1.ResourceMemory:                 resource.MustParse("4Gi"),
		corev1.ResourceEphemeralStorage:       resource.MustParse("10Gi"),
		corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("1"),
	}
	node.Status.Images = []corev1.ContainerImage{
		{Names: []string{"repo/small:latest"}, SizeBytes: 10},
		{Names: []string{"repo/large:latest", "repo/large@sha256:abc"}, SizeBytes: 30},
		{Names: []string{"repo/medium:latest"}, SizeBytes: 20},
	}

	summary := summarizeNodeCachedImages(node, "c1a.16x", resourceSummary{
		CPUCores:              0.5,
		MemoryBytes:           1 * 1024 * 1024 * 1024,
		EphemeralStorageBytes: 2 * 1024 * 1024 * 1024,
		GPUs:                  1,
	})

	if summary.Node != "node-a" {
		t.Fatalf("Node = %q, want node-a", summary.Node)
	}
	if summary.SKU != "c1a.16x" {
		t.Fatalf("SKU = %q, want c1a.16x", summary.SKU)
	}
	if summary.ImageCount != 3 {
		t.Fatalf("ImageCount = %v, want 3", summary.ImageCount)
	}
	if summary.ImageBytes != 60 {
		t.Fatalf("ImageBytes = %v, want 60", summary.ImageBytes)
	}
	if summary.FreeCapacity.CPUCores != 1.5 {
		t.Fatalf("FreeCapacity.CPUCores = %v, want 1.5", summary.FreeCapacity.CPUCores)
	}
	if summary.FreeCapacity.MemoryBytes != 3*1024*1024*1024 {
		t.Fatalf("FreeCapacity.MemoryBytes = %v, want 3Gi", summary.FreeCapacity.MemoryBytes)
	}
	if summary.FreeCapacity.EphemeralStorageBytes != 8*1024*1024*1024 {
		t.Fatalf("FreeCapacity.EphemeralStorageBytes = %v, want 8Gi", summary.FreeCapacity.EphemeralStorageBytes)
	}
	if summary.FreeCapacity.GPUs != 0 {
		t.Fatalf("FreeCapacity.GPUs = %v, want 0", summary.FreeCapacity.GPUs)
	}
	if summary.TopImages[0].Names[0] != "repo/large:latest" {
		t.Fatalf("largest image = %q, want repo/large:latest", summary.TopImages[0].Names[0])
	}
}
