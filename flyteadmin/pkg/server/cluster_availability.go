package server

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/flyteorg/flyte/flyteadmin/auth"
	authInterfaces "github.com/flyteorg/flyte/flyteadmin/auth/interfaces"
	"github.com/flyteorg/flyte/flyteadmin/pkg/flytek8s"
	runtime2 "github.com/flyteorg/flyte/flyteadmin/pkg/runtime"
	runtimeInterfaces "github.com/flyteorg/flyte/flyteadmin/pkg/runtime/interfaces"
	"github.com/flyteorg/flyte/flytestdlib/logger"
)

const (
	clusterAvailabilityPath = "/api/v1/odyssey/clusters/availability"
	crusoeCapacityURL       = "https://api.crusoecloud.com/v1alpha5/capacities"
	crusoeCapacityPath      = "/v1alpha5/capacities"
	crusoeSecretNamespace   = "crusoe-system"
	crusoeSecretName        = "crusoe-secrets"
)

type clusterAvailabilityResponse struct {
	CheckedAt string                `json:"checked_at"`
	Clusters  []clusterAvailability `json:"clusters"`
}

type clusterAvailability struct {
	ID               string                   `json:"id"`
	Endpoint         string                   `json:"endpoint"`
	Enabled          bool                     `json:"enabled"`
	CapacityType     string                   `json:"capacity_type"`
	Region           string                   `json:"region,omitempty"`
	Reachable        bool                     `json:"reachable"`
	Error            string                   `json:"error,omitempty"`
	Warnings         []string                 `json:"warnings"`
	Nodes            nodeSummary              `json:"nodes"`
	Allocatable      resourceSummary          `json:"allocatable"`
	FlytePodPhases   podPhaseSummary          `json:"flyte_pod_phases"`
	ActiveExecutions []activeExecutionSummary `json:"active_executions"`
	CheckedAt        string                   `json:"checked_at"`
	CheckDurationMs  int64                    `json:"check_duration_ms"`
}

type activeExecutionSummary struct {
	Project     string          `json:"project"`
	Domain      string          `json:"domain"`
	Name        string          `json:"name"`
	RunningPods int             `json:"running_pods"`
	PendingPods int             `json:"pending_pods"`
	Requested   resourceSummary `json:"requested"`
}

type nodeSummary struct {
	Total        int                      `json:"total"`
	Ready        int                      `json:"ready"`
	Schedulable  int                      `json:"schedulable"`
	Groups       []nodeSkuSummary         `json:"groups"`
	CachedImages []nodeCachedImageSummary `json:"cached_images"`
}

type nodeSkuSummary struct {
	SKU              string                   `json:"sku"`
	Total            int                      `json:"total"`
	Ready            int                      `json:"ready"`
	Schedulable      int                      `json:"schedulable"`
	Allocatable      resourceSummary          `json:"allocatable"`
	OnDemandCapacity *onDemandCapacitySummary `json:"on_demand_capacity,omitempty"`
}

type nodeCachedImageSummary struct {
	Node        string               `json:"node"`
	SKU         string               `json:"sku"`
	Allocatable resourceSummary      `json:"allocatable"`
	ImageCount  int                  `json:"image_count"`
	ImageBytes  int64                `json:"image_bytes"`
	TopImages   []cachedImageSummary `json:"top_images"`
}

type cachedImageSummary struct {
	Names     []string `json:"names"`
	SizeBytes int64    `json:"size_bytes"`
}

type onDemandCapacitySummary struct {
	Region    string `json:"region"`
	Available int64  `json:"available"`
	CheckedAt string `json:"checked_at"`
}

type resourceSummary struct {
	CPUCores              float64 `json:"cpu_cores"`
	MemoryBytes           int64   `json:"memory_bytes"`
	GPUs                  int64   `json:"gpus"`
	EphemeralStorageBytes int64   `json:"ephemeral_storage_bytes"`
}

type podPhaseSummary struct {
	Pending   int `json:"pending"`
	Running   int `json:"running"`
	Succeeded int `json:"succeeded"`
	Failed    int `json:"failed"`
	Unknown   int `json:"unknown"`
}

func getClusterAvailabilityHandler(ctx context.Context, authCtx authInterfaces.AuthenticationContext, requireAuth bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		requestCtx := r.Context()
		if requireAuth {
			if _, err := auth.IdentityContextFromRequest(requestCtx, r, authCtx); err != nil {
				logger.Infof(requestCtx, "unauthenticated cluster availability request: %v", err)
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
		}

		configuration := runtime2.NewConfigurationProvider()
		clusterConfigs := configuration.ClusterConfiguration().GetClusterConfigs()

		checkedAt := time.Now().UTC()
		clusters := make([]clusterAvailability, len(clusterConfigs))
		var wg sync.WaitGroup
		for i := range clusterConfigs {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				clusterCtx, cancel := context.WithTimeout(requestCtx, 15*time.Second)
				defer cancel()
				clusters[idx] = checkClusterAvailability(clusterCtx, clusterConfigs[idx], checkedAt)
			}(i)
		}
		wg.Wait()

		sort.SliceStable(clusters, func(i, j int) bool {
			return clusters[i].ID < clusters[j].ID
		})

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(clusterAvailabilityResponse{
			CheckedAt: checkedAt.Format(time.RFC3339),
			Clusters:  clusters,
		}); err != nil {
			logger.Errorf(ctx, "failed to encode cluster availability response: %v", err)
		}
	}
}

func checkClusterAvailability(ctx context.Context, cluster runtimeInterfaces.ClusterConfig, checkedAt time.Time) (result clusterAvailability) {
	start := time.Now()
	result = clusterAvailability{
		ID:               cluster.Name,
		Endpoint:         cluster.Endpoint,
		Enabled:          cluster.Enabled,
		Warnings:         []string{},
		ActiveExecutions: []activeExecutionSummary{},
		CheckedAt:        checkedAt.Format(time.RFC3339),
		CapacityType:     "unknown",
	}
	defer func() {
		result.CheckDurationMs = time.Since(start).Milliseconds()
	}()

	restConfig, err := flytek8s.GetRestClientConfig("", "", &cluster)
	if err != nil {
		result.Error = fmt.Sprintf("failed to build Kubernetes client config: %v", err)
		return result
	}

	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		result.Error = fmt.Sprintf("failed to create Kubernetes client: %v", err)
		return result
	}

	nodes, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		result.Error = fmt.Sprintf("failed to list nodes: %v", err)
		return result
	}
	result.Reachable = true
	result.Nodes.Total = len(nodes.Items)

	nodeGroups := map[string]*nodeSkuSummary{}
	nodeCachedImages := make([]nodeCachedImageSummary, 0, len(nodes.Items))
	crusoeOnDemand := false
	crusoeRegion := ""
	for _, node := range nodes.Items {
		if isCrusoeOnDemandNode(node) {
			crusoeOnDemand = true
			if crusoeRegion == "" {
				crusoeRegion = getNodeRegion(node)
			}
		}
		sku := getNodeSKU(node)
		group, ok := nodeGroups[sku]
		if !ok {
			group = &nodeSkuSummary{SKU: sku}
			nodeGroups[sku] = group
		}

		group.Total++
		ready := isNodeReady(node)
		if ready {
			result.Nodes.Ready++
			group.Ready++
		}
		if ready && !node.Spec.Unschedulable {
			result.Nodes.Schedulable++
			group.Schedulable++
			addNodeAllocatable(&result.Allocatable, node)
			addNodeAllocatable(&group.Allocatable, node)
		}
		nodeCachedImages = append(nodeCachedImages, summarizeNodeCachedImages(node, sku))
	}
	if crusoeOnDemand {
		result.CapacityType = "on-demand"
		result.Region = crusoeRegion
		addOnDemandCapacity(ctx, clientset, result.Region, nodeGroups, checkedAt, &result.Warnings)
	} else {
		result.CapacityType = "reservation"
	}
	result.Nodes.Groups = sortNodeGroups(nodeGroups)
	result.Nodes.CachedImages = sortNodeCachedImages(nodeCachedImages)

	flyteNamespaces, err := listFlyteNamespaces(ctx, clientset)
	if err != nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("failed to list Flyte namespaces: %v", err))
		return result
	}

	pods, err := clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("failed to list pods: %v", err))
		return result
	}

	for _, pod := range pods.Items {
		if flyteNamespaces[pod.Namespace] {
			addPodPhase(&result.FlytePodPhases, pod.Status.Phase)
		}
	}
	result.ActiveExecutions = summarizeActiveExecutions(pods.Items, flyteNamespaces)

	return result
}

func addOnDemandCapacity(
	ctx context.Context,
	clientset kubernetes.Interface,
	region string,
	nodeGroups map[string]*nodeSkuSummary,
	checkedAt time.Time,
	warnings *[]string,
) {
	if region == "" {
		*warnings = append(*warnings, "failed to query Crusoe capacity: region is unknown")
		return
	}

	skus := make([]string, 0, len(nodeGroups))
	for sku := range nodeGroups {
		if isCrusoeCapacitySKU(sku) {
			skus = append(skus, sku)
		}
	}
	if len(skus) == 0 {
		return
	}
	sort.Strings(skus)

	capacities, err := queryCrusoeCapacity(ctx, clientset, region, skus)
	if err != nil {
		*warnings = append(*warnings, fmt.Sprintf("failed to query Crusoe capacity: %v", err))
		return
	}

	for _, sku := range skus {
		group := nodeGroups[sku]
		group.OnDemandCapacity = &onDemandCapacitySummary{
			Region:    region,
			Available: capacities[sku],
			CheckedAt: checkedAt.Format(time.RFC3339),
		}
	}
}

func isCrusoeOnDemandNode(node corev1.Node) bool {
	for _, label := range []string{
		"crusoe.ai/instance.id",
		"crusoe.ai/instance.class",
		"crusoe.ai/nodepool.id",
		"crusoe.ai/project.id",
	} {
		if node.Labels[label] != "" {
			return true
		}
	}
	return false
}

func getNodeRegion(node corev1.Node) string {
	for _, label := range []string{
		corev1.LabelTopologyRegion,
		"failure-domain.beta.kubernetes.io/region",
		"fs.csi.crusoe.ai/location",
		"ssd.csi.crusoe.ai/location",
	} {
		if value := node.Labels[label]; value != "" {
			return value
		}
	}

	hostname := node.Name
	const suffix = ".compute.internal"
	if strings.HasSuffix(hostname, suffix) {
		hostname = strings.TrimSuffix(hostname, suffix)
		parts := strings.Split(hostname, ".")
		if len(parts) > 1 {
			return parts[len(parts)-1]
		}
	}
	return ""
}

func isCrusoeCapacitySKU(sku string) bool {
	if sku == "" || strings.Contains(sku, " ") || strings.Contains(sku, "/") {
		return false
	}
	return strings.Contains(sku, ".")
}

type crusoeCredentials struct {
	AccessKey string
	SecretKey string
}

type crusoeCapacityResponse struct {
	Items []crusoeCapacityItem `json:"items"`
}

type crusoeCapacityItem struct {
	Location string `json:"location"`
	Type     string `json:"type"`
	Quantity int64  `json:"quantity"`
}

func queryCrusoeCapacity(
	ctx context.Context,
	clientset kubernetes.Interface,
	region string,
	skus []string,
) (map[string]int64, error) {
	credentials, err := resolveCrusoeCredentials(ctx, clientset)
	if err != nil {
		return nil, err
	}

	query := url.Values{}
	query.Add("location", region)
	for _, sku := range skus {
		query.Add("product_name", sku)
	}
	encodedQuery := query.Encode()
	timestamp := time.Now().UTC().Truncate(time.Second).Format(time.RFC3339)

	signature, err := generateCrusoeSignature(credentials.SecretKey, crusoeCapacityPath, encodedQuery, http.MethodGet, timestamp)
	if err != nil {
		return nil, err
	}

	requestURL := crusoeCapacityURL
	if encodedQuery != "" {
		requestURL += "?" + encodedQuery
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer 1.0:%s:%s", credentials.AccessKey, signature))
	req.Header.Set("X-Crusoe-Timestamp", timestamp)

	client := http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Crusoe capacity API returned HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var parsed crusoeCapacityResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, err
	}

	result := make(map[string]int64, len(skus))
	for _, sku := range skus {
		result[sku] = 0
	}
	for _, item := range parsed.Items {
		if item.Location == region {
			result[item.Type] += item.Quantity
		}
	}
	return result, nil
}

func resolveCrusoeCredentials(ctx context.Context, clientset kubernetes.Interface) (crusoeCredentials, error) {
	credentials := crusoeCredentials{
		AccessKey: firstNonEmpty(os.Getenv("CRUSOE_ACCESS_KEY_ID"), os.Getenv("CRUSOE_ACCESS_KEY")),
		SecretKey: os.Getenv("CRUSOE_SECRET_KEY"),
	}
	if credentials.AccessKey != "" && credentials.SecretKey != "" {
		return credentials, nil
	}

	secret, err := clientset.CoreV1().Secrets(crusoeSecretNamespace).Get(ctx, crusoeSecretName, metav1.GetOptions{})
	if err != nil {
		return crusoeCredentials{}, fmt.Errorf("failed to read %s/%s: %v", crusoeSecretNamespace, crusoeSecretName, err)
	}
	credentials.AccessKey = firstNonEmpty(
		string(secret.Data["CRUSOE_ACCESS_KEY_ID"]),
		string(secret.Data["CRUSOE_ACCESS_KEY"]),
		credentials.AccessKey,
	)
	credentials.SecretKey = firstNonEmpty(
		string(secret.Data["CRUSOE_SECRET_KEY"]),
		credentials.SecretKey,
	)
	if credentials.AccessKey == "" || credentials.SecretKey == "" {
		return crusoeCredentials{}, fmt.Errorf("%s/%s is missing Crusoe API credentials", crusoeSecretNamespace, crusoeSecretName)
	}
	return credentials, nil
}

func generateCrusoeSignature(secretKey, path, query, verb, timestamp string) (string, error) {
	decodedSecret, err := decodeBase64URLNoPadding(secretKey)
	if err != nil {
		return "", err
	}

	payload := fmt.Sprintf("%s\n%s\n%s\n%s\n", path, query, verb, timestamp)
	mac := hmac.New(sha256.New, decodedSecret)
	if _, err := mac.Write([]byte(payload)); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

func decodeBase64URLNoPadding(value string) ([]byte, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err == nil {
		return decoded, nil
	}
	padding := strings.Repeat("=", (4-len(value)%4)%4)
	return base64.URLEncoding.DecodeString(value + padding)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func getNodeSKU(node corev1.Node) string {
	if value := getNodeSKUFromCrusoeLabels(node.Labels); value != "" {
		return value
	}
	if value := getNodeSKUFromAcceleratorLabels(node); value != "" {
		return value
	}
	if value := node.Labels["odyssey.systems/gpu-type"]; value != "" {
		return value
	}
	for _, label := range []string{
		corev1.LabelInstanceTypeStable,
		corev1.LabelInstanceType,
	} {
		if value := node.Labels[label]; value != "" {
			if !isGenericInstanceType(value) {
				return value
			}
		}
	}
	for _, label := range []string{
		"crusoe.ai/instance.class",
		"crusoe.ai/nodepool.name",
	} {
		if value := node.Labels[label]; value != "" {
			return value
		}
	}
	if value := getNodeSKUFromAllocatable(node); value != "" {
		return value
	}
	return "unknown"
}

func isGenericInstanceType(value string) bool {
	switch strings.ToLower(value) {
	case "rke2", "unknown":
		return true
	default:
		return false
	}
}

func getNodeSKUFromAcceleratorLabels(node corev1.Node) string {
	product := node.Labels["nvidia.com/gpu.product"]
	if product == "" {
		return ""
	}

	product = strings.TrimPrefix(product, "NVIDIA-")
	product = strings.ReplaceAll(product, "-", " ")

	gpus := getNodeGPUCount(node)
	if gpus <= 0 {
		return product
	}
	return fmt.Sprintf("%dx NVIDIA %s", gpus, product)
}

func getNodeGPUCount(node corev1.Node) int64 {
	for _, resourceName := range []corev1.ResourceName{
		corev1.ResourceName("nvidia.com/gpu"),
		corev1.ResourceName("amd.com/gpu"),
		corev1.ResourceName("google.com/gpu"),
	} {
		if gpu, ok := node.Status.Allocatable[resourceName]; ok {
			return gpu.Value()
		}
	}

	if value := node.Labels["nvidia.com/gpu.count"]; value != "" {
		if count, err := strconv.ParseInt(value, 10, 64); err == nil {
			return count
		}
	}
	return 0
}

func getNodeSKUFromAllocatable(node corev1.Node) string {
	summary := resourceSummary{}
	addNodeAllocatable(&summary, node)

	parts := make([]string, 0, 3)
	if summary.GPUs > 0 {
		parts = append(parts, fmt.Sprintf("%d GPU", summary.GPUs))
	}
	if summary.CPUCores > 0 {
		parts = append(parts, fmt.Sprintf("%s CPU", formatCompactFloat(summary.CPUCores)))
	}
	if summary.MemoryBytes > 0 {
		parts = append(parts, fmt.Sprintf("%s RAM", formatCompactBytes(summary.MemoryBytes)))
	}

	return strings.Join(parts, " / ")
}

func getNodeSKUFromCrusoeLabels(labels map[string]string) string {
	instanceClass := labels["crusoe.ai/instance.class"]
	nodepool := labels["crusoe.ai/nodepool.name"]
	if instanceClass == "" || nodepool == "" {
		return ""
	}

	parts := strings.Split(nodepool, "-")
	if len(parts) == 0 {
		return ""
	}

	size := parts[len(parts)-1]
	if !strings.HasSuffix(size, "x") {
		return ""
	}

	for _, char := range strings.TrimSuffix(size, "x") {
		if char < '0' || char > '9' {
			return ""
		}
	}

	return fmt.Sprintf("%s.%s", instanceClass, size)
}

func sortNodeGroups(groups map[string]*nodeSkuSummary) []nodeSkuSummary {
	result := make([]nodeSkuSummary, 0, len(groups))
	for _, group := range groups {
		result = append(result, *group)
	}
	sort.SliceStable(result, func(i, j int) bool {
		if result[i].Allocatable.GPUs != result[j].Allocatable.GPUs {
			return result[i].Allocatable.GPUs > result[j].Allocatable.GPUs
		}
		if result[i].Total != result[j].Total {
			return result[i].Total > result[j].Total
		}
		return result[i].SKU < result[j].SKU
	})
	return result
}

func summarizeNodeCachedImages(node corev1.Node, sku string) nodeCachedImageSummary {
	images := make([]corev1.ContainerImage, len(node.Status.Images))
	copy(images, node.Status.Images)
	sort.SliceStable(images, func(i, j int) bool {
		if images[i].SizeBytes != images[j].SizeBytes {
			return images[i].SizeBytes > images[j].SizeBytes
		}
		return firstImageName(images[i].Names) < firstImageName(images[j].Names)
	})

	allocatable := resourceSummary{}
	addNodeAllocatable(&allocatable, node)

	result := nodeCachedImageSummary{
		Node:        node.Name,
		SKU:         sku,
		Allocatable: allocatable,
		ImageCount:  len(images),
	}
	for _, image := range images {
		result.ImageBytes += image.SizeBytes
	}

	const maxTopImages = 5
	for i, image := range images {
		if i >= maxTopImages {
			break
		}
		result.TopImages = append(result.TopImages, cachedImageSummary{
			Names:     image.Names,
			SizeBytes: image.SizeBytes,
		})
	}

	return result
}

func formatCompactBytes(bytes int64) string {
	gib := float64(bytes) / 1024 / 1024 / 1024
	if gib >= 1024 {
		return fmt.Sprintf("%s TiB", formatCompactFloat(gib/1024))
	}
	return fmt.Sprintf("%s GiB", formatCompactFloat(gib))
}

func formatCompactFloat(value float64) string {
	return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.1f", value), "0"), ".")
}

func sortNodeCachedImages(images []nodeCachedImageSummary) []nodeCachedImageSummary {
	sort.SliceStable(images, func(i, j int) bool {
		if images[i].ImageBytes != images[j].ImageBytes {
			return images[i].ImageBytes > images[j].ImageBytes
		}
		return images[i].Node < images[j].Node
	})
	return images
}

func firstImageName(names []string) string {
	if len(names) == 0 {
		return ""
	}
	return names[0]
}

func isNodeReady(node corev1.Node) bool {
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func addNodeAllocatable(summary *resourceSummary, node corev1.Node) {
	if cpu, ok := node.Status.Allocatable[corev1.ResourceCPU]; ok {
		summary.CPUCores += float64(cpu.MilliValue()) / 1000
	}
	if memory, ok := node.Status.Allocatable[corev1.ResourceMemory]; ok {
		summary.MemoryBytes += memory.Value()
	}
	if ephemeralStorage, ok := node.Status.Allocatable[corev1.ResourceEphemeralStorage]; ok {
		summary.EphemeralStorageBytes += ephemeralStorage.Value()
	}
	if gpu, ok := node.Status.Allocatable[corev1.ResourceName("nvidia.com/gpu")]; ok {
		summary.GPUs += gpu.Value()
	}
}

func listFlyteNamespaces(ctx context.Context, clientset kubernetes.Interface) (map[string]bool, error) {
	namespaces, err := clientset.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	result := map[string]bool{
		"flyte": true,
	}
	for _, namespace := range namespaces.Items {
		if namespace.Labels["odyssey.systems/flyte-domain"] != "" {
			result[namespace.Name] = true
		}
	}
	return result, nil
}

func addPodPhase(summary *podPhaseSummary, phase corev1.PodPhase) {
	switch phase {
	case corev1.PodPending:
		summary.Pending++
	case corev1.PodRunning:
		summary.Running++
	case corev1.PodSucceeded:
		summary.Succeeded++
	case corev1.PodFailed:
		summary.Failed++
	default:
		summary.Unknown++
	}
}

func summarizeActiveExecutions(pods []corev1.Pod, flyteNamespaces map[string]bool) []activeExecutionSummary {
	executions := map[string]*activeExecutionSummary{}
	for _, pod := range pods {
		if !flyteNamespaces[pod.Namespace] || (pod.Status.Phase != corev1.PodRunning && pod.Status.Phase != corev1.PodPending) {
			continue
		}

		project := pod.Labels["project"]
		domain := pod.Labels["domain"]
		name := pod.Labels["execution-id"]
		if project == "" || domain == "" || name == "" {
			continue
		}

		key := project + "/" + domain + "/" + name
		execution, ok := executions[key]
		if !ok {
			execution = &activeExecutionSummary{
				Project: project,
				Domain:  domain,
				Name:    name,
			}
			executions[key] = execution
		}

		switch pod.Status.Phase {
		case corev1.PodRunning:
			execution.RunningPods++
		case corev1.PodPending:
			execution.PendingPods++
		}
		addPodRequests(&execution.Requested, pod)
	}

	result := make([]activeExecutionSummary, 0, len(executions))
	for _, execution := range executions {
		result = append(result, *execution)
	}
	sort.SliceStable(result, func(i, j int) bool {
		left := result[i].Requested.GPUs
		right := result[j].Requested.GPUs
		if left != right {
			return left > right
		}
		if result[i].Requested.MemoryBytes != result[j].Requested.MemoryBytes {
			return result[i].Requested.MemoryBytes > result[j].Requested.MemoryBytes
		}
		if result[i].Requested.CPUCores != result[j].Requested.CPUCores {
			return result[i].Requested.CPUCores > result[j].Requested.CPUCores
		}
		return result[i].Name < result[j].Name
	})
	if len(result) > 5 {
		return result[:5]
	}
	return result
}

func addPodRequests(summary *resourceSummary, pod corev1.Pod) {
	for _, container := range pod.Spec.Containers {
		addResourceList(summary, container.Resources.Requests)
	}
	for _, container := range pod.Spec.InitContainers {
		addResourceList(summary, container.Resources.Requests)
	}
}

func addResourceList(summary *resourceSummary, resources corev1.ResourceList) {
	if cpu, ok := resources[corev1.ResourceCPU]; ok {
		summary.CPUCores += float64(cpu.MilliValue()) / 1000
	}
	if memory, ok := resources[corev1.ResourceMemory]; ok {
		summary.MemoryBytes += memory.Value()
	}
	if ephemeralStorage, ok := resources[corev1.ResourceEphemeralStorage]; ok {
		summary.EphemeralStorageBytes += ephemeralStorage.Value()
	}
	if gpu, ok := resources[corev1.ResourceName("nvidia.com/gpu")]; ok {
		summary.GPUs += gpu.Value()
	}
	if gpu, ok := resources[corev1.ResourceName("amd.com/gpu")]; ok {
		summary.GPUs += gpu.Value()
	}
	if gpu, ok := resources[corev1.ResourceName("google.com/gpu")]; ok {
		summary.GPUs += gpu.Value()
	}
}
