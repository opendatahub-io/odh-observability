package e2e_test

import "testing"

func TestFormatFailureAttribution(t *testing.T) {
	tests := []struct {
		name string
		in   failureAttribution
		want string
	}{
		{
			name: "multiple diagnostics",
			in: failureAttribution{
				component: "Thanos",
				rollout:   "resourceVersion=123 generation=4",
				assertion: "Deployment thanos-querier ready replicas",
				diagnostics: []string{
					"oc -n ns logs deployment/thanos-querier",
					"oc -n ns describe deployment thanos-querier",
				},
			},
			want: "FAILURE ATTRIBUTION | Component=Thanos | Rollout=resourceVersion=123 generation=4 | " +
				"Assertion=Deployment thanos-querier ready replicas | " +
				"Diagnostics: oc -n ns logs deployment/thanos-querier ; oc -n ns describe deployment thanos-querier",
		},
		{
			name: "single diagnostic",
			in: failureAttribution{
				component:   "Perses",
				rollout:     "resourceVersion=9 generation=1",
				assertion:   "Pod perses-0 Running/Ready",
				diagnostics: []string{"oc -n ns logs perses-0"},
			},
			want: "FAILURE ATTRIBUTION | Component=Perses | Rollout=resourceVersion=9 generation=1 | Assertion=Pod perses-0 Running/Ready | Diagnostics: oc -n ns logs perses-0",
		},
		{
			name: "absent rollout and no diagnostics",
			in: failureAttribution{
				component: "Perses",
				rollout:   "<absent>",
				assertion: "Pod perses-0 Running/Ready",
			},
			want: "FAILURE ATTRIBUTION | Component=Perses | Rollout=<absent> | Assertion=Pod perses-0 Running/Ready | Diagnostics: ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := formatFailureAttribution(tt.in); got != tt.want {
				t.Errorf("formatFailureAttribution()\n got: %q\nwant: %q", got, tt.want)
			}
		})
	}
}
