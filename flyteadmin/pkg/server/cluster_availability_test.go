package server

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestGetNodeSKU(t *testing.T) {
	tests := []struct {
		name   string
		labels map[string]string
		want   string
	}{
		{
			name: "uses stable instance type",
			labels: map[string]string{
				corev1.LabelInstanceTypeStable: "l40s-48gb.10x",
				"crusoe.ai/instance.class":     "l40s-48gb",
				"crusoe.ai/nodepool.name":      "prod-use1a-toaster-gpu-l40s-10x",
			},
			want: "l40s-48gb.10x",
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
