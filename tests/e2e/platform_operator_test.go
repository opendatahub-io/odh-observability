package e2e_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestDetectPlatformOperatorPrefersRHOAI(t *testing.T) {
	t.Parallel()

	c := fake.NewClientBuilder().WithObjects(
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      platformOperatorRHOAIDeployment,
				Namespace: platformOperatorRHOAINamespace,
			},
		},
	).Build()

	op, err := detectPlatformOperator(context.Background(), c)
	require.NoError(t, err)
	require.Equal(t, platformOperatorRHOAINamespace, op.Namespace)
	require.Equal(t, platformOperatorRHOAIDeployment, op.Deployment)
	require.Equal(t, platformOperatorRHOAIWebhookService, op.WebhookService)
}

func TestDetectPlatformOperatorFallsBackToODH(t *testing.T) {
	t.Parallel()

	c := fake.NewClientBuilder().WithObjects(
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      platformOperatorODHDeployment,
				Namespace: platformOperatorODHNamespace,
			},
		},
	).Build()

	op, err := detectPlatformOperator(context.Background(), c)
	require.NoError(t, err)
	require.Equal(t, platformOperatorODHNamespace, op.Namespace)
	require.Equal(t, platformOperatorODHDeployment, op.Deployment)
}
