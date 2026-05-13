package server

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
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

	summary := summarizeNodeCachedImages(node, "c1a.16x")

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
	if summary.Allocatable.CPUCores != 2 {
		t.Fatalf("Allocatable.CPUCores = %v, want 2", summary.Allocatable.CPUCores)
	}
	if summary.Allocatable.MemoryBytes != 4*1024*1024*1024 {
		t.Fatalf("Allocatable.MemoryBytes = %v, want 4Gi", summary.Allocatable.MemoryBytes)
	}
	if summary.Allocatable.EphemeralStorageBytes != 10*1024*1024*1024 {
		t.Fatalf("Allocatable.EphemeralStorageBytes = %v, want 10Gi", summary.Allocatable.EphemeralStorageBytes)
	}
	if summary.Allocatable.GPUs != 1 {
		t.Fatalf("Allocatable.GPUs = %v, want 1", summary.Allocatable.GPUs)
	}
	if summary.TopImages[0].Names[0] != "repo/large:latest" {
		t.Fatalf("largest image = %q, want repo/large:latest", summary.TopImages[0].Names[0])
	}
}
