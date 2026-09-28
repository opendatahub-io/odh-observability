package e2e_test

import (
	"fmt"
	"testing"
	"time"

	gTypes "github.com/onsi/gomega/types"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"
	jq "github.com/opendatahub-io/odh-observability/tests/e2e/matchers/jq"
)

const (
	seaweedFSPodName     = "seaweedfs"
	seaweedFSServiceName = "seaweedfs"
	seaweedFSBucketPod   = "seaweedfs-bucket-creator"
	seaweedFSImage       = "chrislusf/seaweedfs@sha256:08d516132314207d10c8e37cbffc1f32b147d870169688734cc61c6231625b62"
	seaweedFSAccessKey   = "seaweedfs-test-key"
	seaweedFSSecretKey   = "seaweedfs-test-secret"
	tempoS3Bucket        = "tempo-traces"
	lokiS3Bucket         = "loki-logs"

	fakeGCSPodName     = "fake-gcs-server"
	fakeGCSServiceName = "fake-gcs-server"
	fakeGCSBucketPod   = "fake-gcs-bucket-creator"
	fakeGCSImage       = "fsouza/fake-gcs-server@sha256:797ce226d62f947c009dc40246b30cfb456b8473d8241407f9d6f2c04e4d69ef"
	fakeGCSClientImage = "curlimages/curl@sha256:58adaa4e8dca9c988bae2aba4ab3434a0bb2da16bbe3f92dec39ec7785166777"
	fakeGCSBucket      = "tempo-traces"

	seaweedFSS3Port     int32 = 8333
	seaweedFSMasterPort int32 = 9333
	fakeGCSPort         int32 = 4443
)

func fixturePodSecurityContext() *corev1.PodSecurityContext {
	return &corev1.PodSecurityContext{
		RunAsNonRoot: new(true),
		SeccompProfile: &corev1.SeccompProfile{
			Type: corev1.SeccompProfileTypeRuntimeDefault,
		},
	}
}

func fixtureContainerSecurityContext() *corev1.SecurityContext {
	return &corev1.SecurityContext{
		AllowPrivilegeEscalation: new(false),
		Capabilities: &corev1.Capabilities{
			Drop: []corev1.Capability{"ALL"},
		},
	}
}

func (tc *MonitoringTestCtx) createFixtureResource(t *testing.T, kind schema.GroupVersionKind, obj client.Object) {
	t.Helper()
	content, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	require.NoError(t, err)
	tc.EventuallyResourceCreated(
		WithMinimalObject(kind, types.NamespacedName{Name: obj.GetName(), Namespace: obj.GetNamespace()}),
		WithObjectContent(content),
	)
}

func (tc *MonitoringTestCtx) waitForFixturePod(name string, condition gTypes.GomegaMatcher) {
	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Pod, types.NamespacedName{Name: name, Namespace: tc.MonitoringNamespace}),
		WithCondition(condition),
		WithEventuallyTimeout(10*time.Minute),
		WithCustomErrorMsg("storage fixture pod %s did not become ready", name),
	)
}

func (tc *MonitoringTestCtx) deleteFixtureResources(names ...string) {
	for _, name := range names {
		tc.DeleteResource(
			WithMinimalObject(gvk.Pod, types.NamespacedName{Name: name, Namespace: tc.MonitoringNamespace}),
			WithIgnoreNotFound(true),
			WithWaitForDeletion(true),
		)
	}
}

func (tc *MonitoringTestCtx) cleanupSeaweedFS() {
	tc.deleteFixtureResources(seaweedFSBucketPod, seaweedFSPodName)
	tc.DeleteResource(
		WithMinimalObject(gvk.Service, types.NamespacedName{Name: seaweedFSServiceName, Namespace: tc.MonitoringNamespace}),
		WithIgnoreNotFound(true),
		WithWaitForDeletion(true),
	)
}

func (tc *MonitoringTestCtx) startSeaweedFS(t *testing.T, bucket string) {
	t.Helper()
	require.Regexp(t, `^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`, bucket)
	tc.cleanupSeaweedFS()

	tc.createFixtureResource(t, gvk.Pod, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      seaweedFSPodName,
			Namespace: tc.MonitoringNamespace,
			Labels:    map[string]string{"app": seaweedFSPodName},
		},
		Spec: corev1.PodSpec{
			SecurityContext: fixturePodSecurityContext(),
			Containers: []corev1.Container{{
				Name:    seaweedFSPodName,
				Image:   seaweedFSImage,
				Command: []string{"weed"},
				Args:    []string{"server", "-s3"},
				Ports: []corev1.ContainerPort{
					{Name: "s3", ContainerPort: seaweedFSS3Port},
					{Name: "master", ContainerPort: seaweedFSMasterPort},
				},
				ReadinessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(seaweedFSS3Port)},
					},
					InitialDelaySeconds: 10,
					PeriodSeconds:       5,
				},
				VolumeMounts:    []corev1.VolumeMount{{Name: "data", MountPath: "/data"}},
				SecurityContext: fixtureContainerSecurityContext(),
			}},
			Volumes: []corev1.Volume{{
				Name:         "data",
				VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
			}},
		},
	})

	tc.createFixtureResource(t, gvk.Service, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: seaweedFSServiceName, Namespace: tc.MonitoringNamespace},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": seaweedFSPodName},
			Ports: []corev1.ServicePort{
				{Name: "s3", Port: seaweedFSS3Port, TargetPort: intstr.FromInt32(seaweedFSS3Port)},
				{Name: "master", Port: seaweedFSMasterPort, TargetPort: intstr.FromInt32(seaweedFSMasterPort)},
			},
		},
	})

	tc.waitForFixturePod(seaweedFSPodName, jq.Match(`.status.phase == "Running" and any(.status.conditions[]; .type == "Ready" and .status == "True")`))
	endpoint := fmt.Sprintf("http://%s.%s.svc.cluster.local:%d/%s", seaweedFSServiceName, tc.MonitoringNamespace, seaweedFSS3Port, bucket)
	script := fmt.Sprintf(`i=0; until curl -fsS -X PUT %q; do i=$((i+1)); [ "$i" -lt 60 ] || exit 1; sleep 2; done`, endpoint)
	tc.createBucketPod(t, seaweedFSBucketPod, seaweedFSImage, script)
}

func (tc *MonitoringTestCtx) cleanupFakeGCS() {
	tc.deleteFixtureResources(fakeGCSBucketPod, fakeGCSPodName)
	tc.DeleteResource(
		WithMinimalObject(gvk.Service, types.NamespacedName{Name: fakeGCSServiceName, Namespace: tc.MonitoringNamespace}),
		WithIgnoreNotFound(true),
		WithWaitForDeletion(true),
	)
}

func (tc *MonitoringTestCtx) startFakeGCS(t *testing.T) {
	t.Helper()
	tc.cleanupFakeGCS()

	tc.createFixtureResource(t, gvk.Pod, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fakeGCSPodName,
			Namespace: tc.MonitoringNamespace,
			Labels:    map[string]string{"app": fakeGCSPodName},
		},
		Spec: corev1.PodSpec{
			SecurityContext: fixturePodSecurityContext(),
			Containers: []corev1.Container{{
				Name:    fakeGCSPodName,
				Image:   fakeGCSImage,
				Command: []string{"/bin/fake-gcs-server"},
				Args:    []string{"-filesystem-root", "/data", "-scheme", "http"},
				Ports:   []corev1.ContainerPort{{Name: "http", ContainerPort: fakeGCSPort}},
				ReadinessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						HTTPGet: &corev1.HTTPGetAction{Path: "/_internal/healthcheck", Port: intstr.FromInt32(fakeGCSPort)},
					},
					InitialDelaySeconds: 5,
					PeriodSeconds:       5,
				},
				VolumeMounts:    []corev1.VolumeMount{{Name: "data", MountPath: "/data"}},
				SecurityContext: fixtureContainerSecurityContext(),
			}},
			Volumes: []corev1.Volume{{
				Name:         "data",
				VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
			}},
		},
	})

	tc.createFixtureResource(t, gvk.Service, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: fakeGCSServiceName, Namespace: tc.MonitoringNamespace},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": fakeGCSPodName},
			Ports:    []corev1.ServicePort{{Name: "http", Port: fakeGCSPort, TargetPort: intstr.FromInt32(fakeGCSPort)}},
		},
	})

	tc.waitForFixturePod(fakeGCSPodName, jq.Match(`.status.phase == "Running" and any(.status.conditions[]; .type == "Ready" and .status == "True")`))
	baseURL := fmt.Sprintf("http://%s.%s.svc.cluster.local:%d", fakeGCSServiceName, tc.MonitoringNamespace, fakeGCSPort)
	script := fmt.Sprintf(
		`i=0; until curl -fsS -X POST -H 'Content-Type: application/json' -d '{"name":"%s"}' %q; do i=$((i+1)); [ "$i" -lt 60 ] || exit 1; sleep 2; done`,
		fakeGCSBucket,
		baseURL+"/storage/v1/b?project=fake-test-project",
	)
	tc.createBucketPod(t, fakeGCSBucketPod, fakeGCSClientImage, script)
}

func (tc *MonitoringTestCtx) createBucketPod(t *testing.T, name, image, script string) {
	t.Helper()
	tc.createFixtureResource(t, gvk.Pod, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: tc.MonitoringNamespace},
		Spec: corev1.PodSpec{
			RestartPolicy:   corev1.RestartPolicyNever,
			SecurityContext: fixturePodSecurityContext(),
			Containers: []corev1.Container{{
				Name:            "create-bucket",
				Image:           image,
				Command:         []string{"/bin/sh", "-c"},
				Args:            []string{script},
				SecurityContext: fixtureContainerSecurityContext(),
			}},
		},
	})
	tc.waitForFixturePod(name, jq.Match(`.status.phase == "Succeeded"`))
}
