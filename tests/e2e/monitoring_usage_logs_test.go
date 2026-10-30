package e2e_test

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"

	common "github.com/opendatahub-io/odh-platform-utilities/api/common"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
	ctrlcfg "sigs.k8s.io/controller-runtime/pkg/client/config"

	"github.com/opendatahub-io/odh-observability/internal/controller/conditions"
	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"
	jq "github.com/opendatahub-io/odh-observability/tests/e2e/matchers/jq"

	. "github.com/onsi/gomega" //nolint:revive // dot import is idiomatic for gomega matchers
)

// ========================================================================
// Group 11: Usage Logs Collection
// ========================================================================

func (tc *MonitoringTestCtx) runUsageLogsCollectionTests(t *testing.T) {
	t.Helper()

	t.Run("Group 11: Usage Logs Collection", func(t *testing.T) {
		tc = tc.WithT(t)
		const lifecycleSecretName = "test-loki-lifecycle-secret"
		t.Cleanup(tc.cleanupSeaweedFS)
		t.Cleanup(func() {
			for _, secretName := range []string{usageLogsLokiSecretName, logsLokiSecretName, lifecycleSecretName} {
				tc.DeleteResource(
					WithMinimalObject(gvk.Secret, types.NamespacedName{Name: secretName, Namespace: tc.MonitoringNamespace}),
					WithIgnoreNotFound(true),
					WithWaitForDeletion(true),
				)
			}
		})
		t.Cleanup(func() { tc.cleanupGroup(t, "") })

		// Test 1: Validate not deployed without config (modifies state, run first)
		t.Run("Test Usage Logs Collector not deployed without usage logs config", tc.ValidateUsageLogsCollectorNotDeployedWithoutConfig)

		// Set up the resources the validation tests share once. The two LokiStacks
		// have independent storage, so the fixture serves one bucket per stack.
		tc.startSeaweedFS(t, lokiS3Bucket, logsS3Bucket)
		if !t.Run("Setup UsageLogs resources", func(t *testing.T) {
			tc = tc.WithT(t)
			tc.setupUsageLogsWithStorage(t, "s3", usageLogsLokiSecretName)

			// Wait for everything to be ready
			tc.EnsureResourceExists(
				WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
				WithCondition(And(
					jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, common.ConditionTypeReady, metav1.ConditionTrue),
					jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionUsageLogsCollectorAvailable, metav1.ConditionTrue),
					jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionUsageLogsLokiStackAvailable, metav1.ConditionTrue),
				)),
				WithCustomErrorMsg("Shared setup: Monitoring should be ready with UsageLogs"),
			)

			tc.EnsureDeploymentReady(
				WithMinimalObject(gvk.Deployment, types.NamespacedName{
					Name:      UsageLogsCollectorName + "-collector",
					Namespace: tc.MonitoringNamespace,
				}),
			)
		}) {
			return
		}

		// Shared-resource checks run before stateful certificate rotation and lifecycle tests.
		// LokiStack tests run first since OTEL depends on LokiStack.
		t.Run("Validate usage logs LokiStack deployment", tc.ValidateUsageLogsLokiStackDeployment)
		t.Run("Validate usage logs LokiStack configuration", tc.ValidateUsageLogsLokiStackConfiguration)
		t.Run("Validate Usage Logs Collector deployment", tc.ValidateUsageLogsCollectorDeployment)
		t.Run("Validate Usage Logs Collector configuration", tc.ValidateUsageLogsCollectorConfiguration)
		t.Run("Validate Usage Logs Collector RBAC", tc.ValidateUsageLogsCollectorRBACConfiguration)
		t.Run("Validate Usage Logs Collector network policies", tc.ValidateUsageLogsCollectorNetworkPolicies)

		// Log forwarding owns a second, independent LokiStack. It is configured on
		// top of the usage logs setup so the two are exercised side by side.
		if !t.Run("Setup log forwarding resources", tc.setupLogsLokiStack) {
			return
		}
		t.Run("Validate log forwarding LokiStack deployment", tc.ValidateLogsLokiStackDeployment)
		t.Run("Validate log forwarding LokiStack configuration", tc.ValidateLogsLokiStackConfiguration)
		t.Run("Validate optional ClusterLogForwarder and Vector contract", tc.ValidateClusterLogForwarderContract)
		t.Run("Validate log forwarding LokiStack removal", tc.ValidateLogsLokiStackRemoved)
		t.Run("Test Usage Logs Collector serving certificate rotation", tc.ValidateUsageLogsCollectorServingCertificateRotation)

		// Lifecycle removes the collector and LokiStack, so it runs last.
		t.Run("Test Usage Logs lifecycle (LokiStack + Collector)", tc.ValidateUsageLogsLifecycle)
	})
}

// ValidateUsageLogsCollectorNotDeployedWithoutConfig tests that the logs collector is not deployed when logs are not configured.
func (tc *MonitoringTestCtx) ValidateUsageLogsCollectorNotDeployedWithoutConfig(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)
	t.Cleanup(tc.resetMonitoringConfigToManaged)

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		withNoUsageLogs(),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`.spec.usageLogs == null`),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, common.ConditionTypeReady, metav1.ConditionTrue),
		)),
		WithCustomErrorMsg("Monitoring resource should be created without logs configuration"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(jq.Match(
			`[.status.conditions[] | select(.type=="%s" and .status=="False")] | length==1`,
			conditions.ConditionUsageLogsCollectorAvailable,
		)),
		WithCustomErrorMsg("UsageLogsCollectorAvailable condition should be False when logs are not configured"),
	)

	tc.EnsureResourceGone(
		WithMinimalObject(gvk.OpenTelemetryCollector, types.NamespacedName{
			Name:      UsageLogsCollectorName,
			Namespace: tc.MonitoringNamespace,
		}),
	)

	tc.EnsureResourceGone(
		WithMinimalObject(gvk.LokiStack, types.NamespacedName{
			Name:      UsageLogsLokiStackName,
			Namespace: tc.MonitoringNamespace,
		}),
	)

	for _, name := range []string{
		UsageLogsCollectorNetworkPolicyName, UsageLogsLokiStackNetworkPolicyName,
	} {
		tc.EnsureResourceGone(
			WithMinimalObject(gvk.NetworkPolicy, types.NamespacedName{
				Name:      name,
				Namespace: tc.MonitoringNamespace,
			}),
		)
	}
}

// ValidateUsageLogsCollectorDeployment tests that the logs collector is deployed and ready when logs are configured.
func (tc *MonitoringTestCtx) ValidateUsageLogsCollectorDeployment(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`.spec.usageLogs.storage.type == "s3"`),
			jq.Match(`.spec.usageLogs.storage.credentialMode == "static"`),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, common.ConditionTypeReady, metav1.ConditionTrue),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionUsageLogsCollectorAvailable, metav1.ConditionTrue),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionUsageLogsLokiStackAvailable, metav1.ConditionTrue),
		)),
		WithCustomErrorMsg("Monitoring resource should be ready with logs configuration and UsageLogsCollector and LokiStack available"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.OpenTelemetryCollector, types.NamespacedName{
			Name:      UsageLogsCollectorName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(
			jq.Match(`.spec.mode == "deployment"`),
			jq.Match(`.spec.replicas == 2`),
			tc.monitoringOwnerReferencesCondition(),
		)),
		WithCustomErrorMsg("Logs OpenTelemetryCollector should be created in deployment mode with 2 replicas"),
	)

	tc.EnsureDeploymentReady(
		WithMinimalObject(gvk.Deployment, types.NamespacedName{
			Name:      UsageLogsCollectorName + "-collector",
			Namespace: tc.MonitoringNamespace,
		}),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Service, types.NamespacedName{
			Name:      UsageLogsCollectorName + "-collector",
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(jq.Match(`[.spec.ports[] | select(.port == 4317 or .port == 4318)] | length == 2`)),
		WithCustomErrorMsg("Usage Logs OTLP Service should expose gRPC and HTTP"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Service, types.NamespacedName{
			Name:      UsageLogsCollectorName + "-collector-headless",
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(jq.Match(`.metadata.annotations."service.beta.openshift.io/serving-cert-secret-name" == "%s-collector-headless-tls"`, UsageLogsCollectorName)),
		WithCustomErrorMsg("Usage Logs OTLP headless Service should own an OpenShift serving certificate"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Secret, types.NamespacedName{
			Name:      UsageLogsCollectorName + "-collector-headless-tls",
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(
			jq.Match(`.type == "kubernetes.io/tls"`),
			jq.Match(`.data."tls.crt" != null`),
			jq.Match(`.data."tls.key" != null`),
		)),
		WithCustomErrorMsg("service-ca should issue the Usage Logs OTLP headless Service certificate and key"),
	)
	tc.assertServiceCACertificateOriginMatchesService(
		t,
		UsageLogsCollectorName+"-collector-headless",
		UsageLogsCollectorName+"-collector-headless-tls",
	)
}

// ValidateUsageLogsCollectorServingCertificateRotation deletes the service-ca
// serving Secret and verifies the running collector starts serving its replacement.
func (tc *MonitoringTestCtx) ValidateUsageLogsCollectorServingCertificateRotation(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)
	collectorDeploymentName := UsageLogsCollectorName + "-collector"
	secretName := UsageLogsCollectorName + "-collector-headless-tls"
	serviceDNS := fmt.Sprintf("%s.%s.svc", UsageLogsCollectorName+"-collector-headless", tc.MonitoringNamespace)

	tc.EnsureDeploymentReady(WithMinimalObject(gvk.Deployment, types.NamespacedName{
		Name: collectorDeploymentName, Namespace: tc.MonitoringNamespace,
	}))

	config, err := ctrlcfg.GetConfig()
	if err != nil {
		t.Fatalf("load Kubernetes config: %v", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatalf("create Kubernetes client: %v", err)
	}

	deployment, err := clientset.AppsV1().Deployments(tc.MonitoringNamespace).Get(
		tc.Context(), collectorDeploymentName, metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("get Usage Logs collector Deployment: %v", err)
	}
	selector, err := metav1.LabelSelectorAsSelector(deployment.Spec.Selector)
	if err != nil {
		t.Fatalf("build Usage Logs collector pod selector: %v", err)
	}

	var collectorPod *corev1.Pod
	tc.g.Eventually(func() error {
		pods, err := clientset.CoreV1().Pods(tc.MonitoringNamespace).List(
			tc.Context(), metav1.ListOptions{LabelSelector: selector.String()},
		)
		if err != nil {
			return fmt.Errorf("list Usage Logs collector pods: %w", err)
		}
		for i := range pods.Items {
			pod := &pods.Items[i]
			if pod.DeletionTimestamp != nil || !usageLogsCollectorPodReady(pod) {
				continue
			}
			collectorPod = pod.DeepCopy()
			return nil
		}
		return errors.New("no ready Usage Logs collector pod found")
	}, 2*time.Minute, 5*time.Second).Should(Succeed(), "a ready Usage Logs collector pod should exist")

	portForwardAddress, stopPortForward, err := startUsageLogsCollectorPortForward(
		t, config, clientset, tc.MonitoringNamespace, collectorPod.Name,
	)
	if err != nil {
		t.Fatalf("port-forward to Usage Logs collector pod: %v", err)
	}
	defer stopPortForward()

	caConfigMap, err := clientset.CoreV1().ConfigMaps(tc.MonitoringNamespace).Get(
		tc.Context(), "openshift-service-ca.crt", metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("get injected service CA bundle: %v", err)
	}
	serviceCAPool := x509.NewCertPool()
	if !serviceCAPool.AppendCertsFromPEM([]byte(caConfigMap.Data["service-ca.crt"])) {
		t.Fatal("injected service CA bundle contains no certificates")
	}

	secret, err := clientset.CoreV1().Secrets(tc.MonitoringNamespace).Get(
		tc.Context(), secretName, metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("get service-ca serving Secret: %v", err)
	}
	originalFingerprint, err := usageLogsCertificateFingerprint(secret.Data[corev1.TLSCertKey])
	if err != nil {
		t.Fatalf("parse current service-ca certificate: %v", err)
	}

	tc.g.Eventually(func() error {
		fingerprint, err := usageLogsCollectorPresentedCertificateFingerprint(
			tc.Context(), portForwardAddress, serviceDNS, serviceCAPool,
		)
		if err != nil {
			return err
		}
		if fingerprint != originalFingerprint {
			return fmt.Errorf("collector serves certificate %x, Secret contains %x", fingerprint, originalFingerprint)
		}
		return nil
	}, 2*time.Minute, 5*time.Second).Should(Succeed(), "the collector should initially serve the certificate from its Secret")

	if err := clientset.CoreV1().Secrets(tc.MonitoringNamespace).Delete(
		tc.Context(), secretName, metav1.DeleteOptions{},
	); err != nil {
		t.Fatalf("delete service-ca serving Secret to trigger rotation: %v", err)
	}
	t.Cleanup(func() {
		tc.g.Eventually(func() error {
			_, err := clientset.CoreV1().Secrets(tc.MonitoringNamespace).Get(
				tc.Context(), secretName, metav1.GetOptions{},
			)
			return err
		}, 2*time.Minute, 5*time.Second).Should(Succeed(), "service-ca should restore the serving Secret after the rotation test")
	})

	var rotatedFingerprint [sha256.Size]byte
	tc.g.Eventually(func() error {
		rotatedSecret, err := clientset.CoreV1().Secrets(tc.MonitoringNamespace).Get(
			tc.Context(), secretName, metav1.GetOptions{},
		)
		if err != nil {
			return fmt.Errorf("wait for service-ca to recreate serving Secret: %w", err)
		}
		fingerprint, err := usageLogsCertificateFingerprint(rotatedSecret.Data[corev1.TLSCertKey])
		if err != nil {
			return fmt.Errorf("parse replacement service-ca certificate: %w", err)
		}
		if fingerprint == originalFingerprint {
			return errors.New("service-ca has not replaced the original serving certificate yet")
		}
		rotatedFingerprint = fingerprint
		return nil
	}, 2*time.Minute, 5*time.Second).Should(Succeed(), "service-ca should issue a new serving certificate after its Secret is deleted")

	tc.g.Eventually(func() error {
		currentPod, err := clientset.CoreV1().Pods(tc.MonitoringNamespace).Get(
			tc.Context(), collectorPod.Name, metav1.GetOptions{},
		)
		if err != nil {
			return StopErr(err, "Usage Logs collector pod disappeared during certificate rotation")
		}
		if currentPod.UID != collectorPod.UID {
			return StopErr(errors.New("pod UID changed"), "Usage Logs collector pod restarted during certificate rotation")
		}
		if usageLogsCollectorContainerRestartCount(currentPod) != usageLogsCollectorContainerRestartCount(collectorPod) {
			return StopErr(errors.New("container restart count changed"), "Usage Logs collector restarted during certificate rotation")
		}

		fingerprint, err := usageLogsCollectorPresentedCertificateFingerprint(
			tc.Context(), portForwardAddress, serviceDNS, serviceCAPool,
		)
		if err != nil {
			return err
		}
		if fingerprint != rotatedFingerprint {
			return fmt.Errorf("collector still serves certificate %x; waiting for rotated certificate %x", fingerprint, rotatedFingerprint)
		}
		return nil
	}, 8*time.Minute, 10*time.Second).Should(Succeed(), "the running collector should serve the rotated certificate without restarting")
}

func usageLogsCollectorPodReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func usageLogsCollectorContainerRestartCount(pod *corev1.Pod) int32 {
	var restarts int32
	for _, container := range pod.Status.ContainerStatuses {
		restarts += container.RestartCount
	}
	return restarts
}

func startUsageLogsCollectorPortForward(
	t *testing.T,
	config *rest.Config,
	clientset *kubernetes.Clientset,
	namespace, podName string,
) (string, func(), error) {
	t.Helper()

	requestURL := clientset.CoreV1().RESTClient().Post().
		Resource("pods").Name(podName).Namespace(namespace).SubResource("portforward").URL()
	transport, upgrader, err := spdy.RoundTripperFor(config)
	if err != nil {
		return "", nil, fmt.Errorf("create SPDY transport: %w", err)
	}
	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, http.MethodPost, requestURL)
	stopChannel := make(chan struct{})
	readyChannel := make(chan struct{})
	forwarder, err := portforward.NewOnAddresses(
		dialer, []string{"127.0.0.1"}, []string{"0:4318"}, stopChannel, readyChannel, io.Discard, io.Discard,
	)
	if err != nil {
		return "", nil, fmt.Errorf("create port-forwarder: %w", err)
	}
	forwarderErrors := make(chan error, 1)
	go func() {
		forwarderErrors <- forwarder.ForwardPorts()
	}()

	select {
	case <-readyChannel:
	case err := <-forwarderErrors:
		return "", nil, fmt.Errorf("start port-forward: %w", err)
	case <-time.After(30 * time.Second):
		close(stopChannel)
		<-forwarderErrors
		return "", nil, errors.New("timed out waiting for port-forward readiness")
	}

	ports, err := forwarder.GetPorts()
	if err != nil {
		close(stopChannel)
		<-forwarderErrors
		return "", nil, fmt.Errorf("get forwarded port: %w", err)
	}
	if len(ports) != 1 {
		close(stopChannel)
		<-forwarderErrors
		return "", nil, fmt.Errorf("expected one forwarded port, got %d", len(ports))
	}
	stop := func() {
		close(stopChannel)
		select {
		case err := <-forwarderErrors:
			if err != nil {
				t.Logf("port-forward stopped with error: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Log("timed out waiting for port-forward to stop")
		}
	}
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(int(ports[0].Local))), stop, nil
}

func usageLogsCertificateFingerprint(certificatePEM []byte) ([sha256.Size]byte, error) {
	var zero [sha256.Size]byte
	block, _ := pem.Decode(certificatePEM)
	if block == nil || block.Type != "CERTIFICATE" {
		return zero, errors.New("PEM data does not contain a certificate")
	}
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return zero, fmt.Errorf("parse certificate: %w", err)
	}
	return sha256.Sum256(certificate.Raw), nil
}

func usageLogsCollectorPresentedCertificateFingerprint(
	ctx context.Context,
	address, serverName string,
	rootCAs *x509.CertPool,
) ([sha256.Size]byte, error) {
	var zero [sha256.Size]byte
	dialer := tls.Dialer{
		NetDialer: &net.Dialer{Timeout: 10 * time.Second},
		Config: &tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    rootCAs,
			ServerName: serverName,
		},
	}
	connection, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return zero, fmt.Errorf("connect to collector TLS receiver: %w", err)
	}
	defer connection.Close()

	tlsConnection, ok := connection.(*tls.Conn)
	if !ok {
		return zero, fmt.Errorf("expected TLS connection, got %T", connection)
	}
	peerCertificates := tlsConnection.ConnectionState().PeerCertificates
	if len(peerCertificates) == 0 {
		return zero, errors.New("collector sent no TLS certificate")
	}
	return sha256.Sum256(peerCertificates[0].Raw), nil
}

// ValidateUsageLogsCollectorConfiguration validates the logs collector configuration details.
func (tc *MonitoringTestCtx) ValidateUsageLogsCollectorConfiguration(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.OpenTelemetryCollector, types.NamespacedName{
			Name:      UsageLogsCollectorName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(
			// The operator-managed NetworkPolicy is disabled; ingress is governed by
			// the NetworkPolicies this operator renders (see ValidateUsageLogsCollectorNetworkPolicies).
			jq.Match(`.spec.networkPolicy.enabled == false`),
			jq.Match(`.metadata.annotations."service.beta.openshift.io/serving-cert-secret-name" == null`),
			jq.Match(`[.spec.volumeMounts[] | select(.name == "otlp-tls-certs" and .mountPath == "/etc/otel-collector/otlp-tls" and .readOnly == true)] | length > 0`),
			jq.Match(`.spec.volumes[] | select(.name == "otlp-tls-certs") | .secret.secretName == "%s-collector-headless-tls"`, UsageLogsCollectorName),

			// Verify receivers
			jq.Match(`.spec.config.receivers.otlp.protocols.grpc.endpoint == "0.0.0.0:4317"`),
			jq.Match(`.spec.config.receivers.otlp.protocols.grpc.tls.cert_file == "/etc/otel-collector/otlp-tls/tls.crt"`),
			jq.Match(`.spec.config.receivers.otlp.protocols.grpc.tls.key_file == "/etc/otel-collector/otlp-tls/tls.key"`),
			jq.Match(`.spec.config.receivers.otlp.protocols.http.endpoint == "0.0.0.0:4318"`),
			jq.Match(`.spec.config.receivers.otlp.protocols.http.tls.cert_file == "/etc/otel-collector/otlp-tls/tls.crt"`),
			jq.Match(`.spec.config.receivers.otlp.protocols.http.tls.key_file == "/etc/otel-collector/otlp-tls/tls.key"`),

			// Verify processors
			jq.Match(`.spec.config.processors.k8sattributes != null`),
			jq.Match(`.spec.config.processors.k8sattributes.auth_type == "serviceAccount"`),
			jq.Match(`.spec.config.processors."groupbyattrs/maas" != null`),
			jq.Match(`.spec.config.processors.batch != null`),

			// log_type is pinned to the Loki application tenant. It must NOT be copied
			// from log_name: MaaS relies on log_name keeping its envoy access-logger value.
			jq.Match(`
				[.spec.config.processors.resource.attributes[] | select(.key == "log_type")] ==
				[{"action": "upsert", "key": "log_type", "value": "application"}]
			`),
			// The AI tenant namespace arrives on service.namespace and drives LokiStack
			// RBAC via kubernetes_namespace_name. insert (not upsert) so a namespace
			// already set by the sender wins.
			jq.Match(`
				[.spec.config.processors.resource.attributes[] | select(.key == "kubernetes_namespace_name")] ==
				[{"action": "insert", "key": "kubernetes_namespace_name", "from_attribute": "service.namespace"}]
			`),
			// Cleanup must preserve log_name for MaaS consumers.
			jq.Match(`
				[.spec.config.processors."transform/cleanup".log_statements[].statements[] | select(test("log_name"))] | length == 0
			`),
			jq.Match(`
				[.spec.config.processors."transform/cleanup".log_statements[].statements[] | select(test("service.namespace"))] | length == 1
			`),

			// Verify exporter endpoint (auto-generated from the usage logs LokiStack,
			// never from the log forwarding one).
			jq.Match(
				`.spec.config.exporters."otlphttp/loki".endpoint | test("https://%s-gateway-http\\..+\\.svc\\.cluster\\.local:8080/api/logs/v1/application/otlp")`,
				UsageLogsLokiStackName,
			),
			jq.Match(`
				(.spec.config.exporters."otlphttp/loki".tls.ca_file == "/var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt") and
				(.spec.config.exporters."otlphttp/loki".auth.authenticator == "bearertokenauth")
			`),
			jq.Match(`.spec.config.exporters."otlphttp/loki".headers."X-Scope-OrgID" == "application"`),

			// Verify pipeline
			jq.Match(`.spec.config.service.pipelines.logs.receivers | contains(["otlp"])`),
			jq.Match(`.spec.config.service.pipelines.logs.processors | contains(["resource", "k8sattributes", "groupbyattrs/maas", "batch"])`),
			jq.Match(`.spec.config.service.pipelines.logs.exporters | contains(["otlphttp/loki"])`),
		)),
		WithCustomErrorMsg("Logs collector should have correct OTLP receivers, processors, and Loki exporter configuration"),
	)
}

// ValidateUsageLogsCollectorRBACConfiguration tests that the logs collector has correct RBAC permissions.
func (tc *MonitoringTestCtx) ValidateUsageLogsCollectorRBACConfiguration(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.ServiceAccount, types.NamespacedName{
			Name:      UsageLogsCollectorServiceAccount,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCustomErrorMsg("ServiceAccount for logs collector should exist"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.ClusterRole, types.NamespacedName{
			Name: UsageLogsCollectorName + "-processor",
		}),
		WithCondition(And(
			// k8sattributes processor requires pod/namespace metadata access
			jq.Match(`.rules[] | select(.apiGroups[] == "") | .resources | contains(["pods", "namespaces"])`),
			jq.Match(`.rules[] | select(.apiGroups[] == "") | .verbs | contains(["get", "watch", "list"])`),
			jq.Match(`.rules[] | select(.apiGroups[] == "apps") | .resources | contains(["replicasets"])`),
			jq.Match(`.rules[] | select(.apiGroups[] == "apps") | .verbs | contains(["get", "watch", "list"])`),
			// Loki application tenant write access for the otlphttp/loki exporter
			jq.Match(`
				.rules[] | select(.apiGroups[] == "loki.grafana.com") |
				(.resources | contains(["application"])) and
				(.resourceNames | contains(["logs"])) and
				(.verbs | contains(["create"]))
			`),
		)),
		WithCustomErrorMsg("ClusterRole should grant logs collector permissions for k8sattributes processor and Loki writes"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.ClusterRoleBinding, types.NamespacedName{
			Name: UsageLogsCollectorName + "-processor",
		}),
		WithCondition(And(
			jq.Match(`.roleRef.name == "%s"`, UsageLogsCollectorName+"-processor"),
			jq.Match(`.subjects[0].name == "%s"`, UsageLogsCollectorServiceAccount),
			jq.Match(`.subjects[0].namespace == "%s"`, tc.MonitoringNamespace),
		)),
		WithCustomErrorMsg("ClusterRoleBinding should bind logs collector ClusterRole to ServiceAccount"),
	)
}

// ValidateUsageLogsCollectorNetworkPolicies tests the NetworkPolicies that gate traffic into the
// collector and into the LokiStack gateway. These stand in for RBAC on the write path: write
// authorization cannot be delegated to Loki, so reachability is restricted instead.
func (tc *MonitoringTestCtx) ValidateUsageLogsCollectorNetworkPolicies(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.NetworkPolicy, types.NamespacedName{
			Name:      UsageLogsCollectorNetworkPolicyName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(
			jq.Match(`.spec.podSelector.matchLabels."app.kubernetes.io/name" == "%s"`, UsageLogsCollectorName+"-collector"),
			jq.Match(`.spec.policyTypes == ["Ingress"]`),
			// OTLP ingest is reachable only from the ingress policy group (the MaaS gateway).
			jq.Match(`
				[.spec.ingress[] | select(.from != null)] | length == 1
			`),
			jq.Match(`
				[.spec.ingress[] | select(.from != null) |
					.from[0].namespaceSelector.matchLabels."network.openshift.io/policy-group" == "ingress"] == [true]
			`),
			jq.Match(`
				[.spec.ingress[] | select(.from != null) | .ports[] | select(.protocol == "TCP") | .port] == [4317, 4318]
			`),
			// Internal telemetry scraping stays open cluster-wide (no from selector).
			jq.Match(`
				[.spec.ingress[] | select(.from == null) | .ports[] | select(.protocol == "TCP") | .port] == [8888]
			`),
		)),
		WithCustomErrorMsg("Usage logs collector NetworkPolicy should allow OTLP only from the ingress policy group and expose the metrics port"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.NetworkPolicy, types.NamespacedName{
			Name:      UsageLogsLokiStackNetworkPolicyName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(
			// Guards the gateway pods that receive the connection.
			jq.Match(`.spec.podSelector.matchLabels."app.kubernetes.io/component" == "lokistack-gateway"`),
			jq.Match(`.spec.podSelector.matchLabels."app.kubernetes.io/instance" == "%s"`, UsageLogsLokiStackName),
			jq.Match(`.spec.policyTypes == ["Ingress"]`),
			jq.Match(`
				[.spec.ingress[].from[] | .podSelector.matchLabels."app.kubernetes.io/name"] == ["%s"]
			`, UsageLogsCollectorName+"-collector"),
			jq.Match(`
				[.spec.ingress[].ports[] | select(.protocol == "TCP") | .port] == [8080]
			`),
		)),
		WithCustomErrorMsg("LokiStack gateway NetworkPolicy should only admit the usage logs collector on 8080"),
	)
}

// ValidateUsageLogsLifecycle tests the complete lifecycle of usage logs (LokiStack + collector) deployment and cleanup.
func (tc *MonitoringTestCtx) ValidateUsageLogsLifecycle(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)
	t.Cleanup(tc.resetMonitoringConfigToManaged)

	secretName := "test-loki-lifecycle-secret"

	// Step 1: Enable usage logs
	tc.setupUsageLogsWithStorage(t, "s3", secretName)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.LokiStack, types.NamespacedName{
			Name:      UsageLogsLokiStackName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCustomErrorMsg("LokiStack should be deployed when usage logs are enabled"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.OpenTelemetryCollector, types.NamespacedName{
			Name:      UsageLogsCollectorName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(jq.Match(`.spec.config.exporters."otlphttp/loki" != null`)),
		WithCustomErrorMsg("Logs collector should be deployed when usage logs are enabled"),
	)

	for _, name := range []string{
		UsageLogsCollectorNetworkPolicyName, UsageLogsLokiStackNetworkPolicyName,
	} {
		tc.EnsureResourceExists(
			WithMinimalObject(gvk.NetworkPolicy, types.NamespacedName{
				Name:      name,
				Namespace: tc.MonitoringNamespace,
			}),
			WithCustomErrorMsg("NetworkPolicy %s should be deployed when usage logs are enabled", name),
		)
	}

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionUsageLogsLokiStackAvailable, metav1.ConditionTrue),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionUsageLogsCollectorAvailable, metav1.ConditionTrue),
		)),
		WithCustomErrorMsg("LokiStackAvailable and UsageLogsCollectorAvailable conditions should be True when usage logs are enabled"),
	)

	// Step 2: Disable usage logs
	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		withNoUsageLogs(),
	)

	tc.EnsureResourceGone(
		WithMinimalObject(gvk.LokiStack, types.NamespacedName{
			Name:      UsageLogsLokiStackName,
			Namespace: tc.MonitoringNamespace,
		}),
	)

	tc.EnsureResourceGone(
		WithMinimalObject(gvk.OpenTelemetryCollector, types.NamespacedName{
			Name:      UsageLogsCollectorName,
			Namespace: tc.MonitoringNamespace,
		}),
	)

	for _, name := range []string{
		UsageLogsCollectorNetworkPolicyName, UsageLogsLokiStackNetworkPolicyName,
	} {
		tc.EnsureResourceGone(
			WithMinimalObject(gvk.NetworkPolicy, types.NamespacedName{
				Name:      name,
				Namespace: tc.MonitoringNamespace,
			}),
		)
	}

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionUsageLogsLokiStackAvailable, metav1.ConditionFalse),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionUsageLogsCollectorAvailable, metav1.ConditionFalse),
		)),
		WithCustomErrorMsg("LokiStackAvailable and UsageLogsCollectorAvailable conditions should be False when usage logs are disabled"),
	)

	// Step 3: Re-enable usage logs
	tc.setupUsageLogsWithStorage(t, "s3", secretName)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.LokiStack, types.NamespacedName{
			Name:      UsageLogsLokiStackName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCustomErrorMsg("LokiStack should be recreated when usage logs are re-enabled"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.OpenTelemetryCollector, types.NamespacedName{
			Name:      UsageLogsCollectorName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(jq.Match(`.spec.config.exporters."otlphttp/loki" != null`)),
		WithCustomErrorMsg("Logs collector should be recreated when usage logs are re-enabled"),
	)

	for _, name := range []string{
		UsageLogsCollectorNetworkPolicyName, UsageLogsLokiStackNetworkPolicyName,
	} {
		tc.EnsureResourceExists(
			WithMinimalObject(gvk.NetworkPolicy, types.NamespacedName{
				Name:      name,
				Namespace: tc.MonitoringNamespace,
			}),
			WithCustomErrorMsg("NetworkPolicy %s should be recreated when usage logs are re-enabled", name),
		)
	}

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionUsageLogsLokiStackAvailable, metav1.ConditionTrue),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionUsageLogsCollectorAvailable, metav1.ConditionTrue),
		)),
		WithCustomErrorMsg("LokiStackAvailable and UsageLogsCollectorAvailable conditions should be True when usage logs are re-enabled"),
	)
}

// ValidateUsageLogsLokiStackDeployment tests that LokiStack is deployed with correct configuration.
func (tc *MonitoringTestCtx) ValidateUsageLogsLokiStackDeployment(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	// Verify LokiStack CR is created
	tc.EnsureResourceExists(
		WithMinimalObject(gvk.LokiStack, types.NamespacedName{
			Name:      UsageLogsLokiStackName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(
			tc.monitoringOwnerReferencesCondition(),
			jq.Match(`.spec.size == "1x.demo"`),
			jq.Match(`.spec.storage.secret.type == "s3"`),
			jq.Match(`.spec.storage.secret.credentialMode == "static"`),
			jq.Match(`.spec.storageClassName == "%s"`, tc.DefaultStorageClass),
			jq.Match(`.spec.tenants.mode == "openshift-logging"`),
		)),
		WithCustomErrorMsg("LokiStack should be created with correct storage configuration"),
	)

	// Verify Monitoring condition
	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, conditions.ConditionUsageLogsLokiStackAvailable, metav1.ConditionTrue),
		),
		WithCustomErrorMsg("LokiStackAvailable condition should be True when LokiStack is deployed"),
	)
}

// ValidateUsageLogsLokiStackConfiguration tests LokiStack with OTLP stream labels configuration.
func (tc *MonitoringTestCtx) ValidateUsageLogsLokiStackConfiguration(t *testing.T) {
	t.Helper()
	tc = tc.WithT(t)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.LokiStack, types.NamespacedName{
			Name:      UsageLogsLokiStackName,
			Namespace: tc.MonitoringNamespace,
		}),
		WithCondition(And(
			jq.Match(`.spec.limits.tenants.application.otlp.streamLabels.resourceAttributes | length == 8`),
			jq.Match(`[.spec.limits.tenants.application.otlp.streamLabels.resourceAttributes[] | select(.name == "kubernetes_namespace_name")] | length == 1`),
			jq.Match(`[.spec.limits.tenants.application.otlp.streamLabels.resourceAttributes[] | select(.name == "model")] | length == 1`),
			jq.Match(`[.spec.limits.tenants.application.otlp.streamLabels.resourceAttributes[] | select(.name == "subscription")] | length == 1`),
			jq.Match(`[.spec.limits.tenants.application.otlp.streamLabels.resourceAttributes[] | select(.name == "response_type")] | length == 1`),
			jq.Match(`[.spec.limits.tenants.application.otlp.streamLabels.resourceAttributes[] | select(.name == "gateway_namespace_name")] | length == 1`),
			jq.Match(`[.spec.limits.tenants.application.otlp.streamLabels.resourceAttributes[] | select(.name == "gateway_deployment_name")] | length == 1`),
			jq.Match(`[.spec.limits.tenants.application.otlp.streamLabels.resourceAttributes[] | select(.name == "upstream_namespace_name")] | length == 1`),
			jq.Match(`[.spec.limits.tenants.application.otlp.streamLabels.resourceAttributes[] | select(.name == "upstream_deployment_name")] | length == 1`),
		)),
		WithCustomErrorMsg("LokiStack should have correct OTLP stream labels"),
	)
}
