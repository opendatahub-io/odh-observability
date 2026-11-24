# AI Grid metrics and usage logs (RHAI-13673)

`TestAIGridObservability` sends controlled inference traffic through two spoke
MaaS gateways and checks organization-attributed metrics in ACM Thanos and usage
logs in hub Loki through OATS. The tenant label is `organization_id`, with the
value configured as the test subscription's `spec.organization.id`.

- [Expected environment and permissions](#expected-test-environment)
- [Opt in and run locally or in a container](#opt-in-and-run)
- [Resources created and removed](#resources-and-lifecycle)
- [Troubleshooting MaaS endpoint certificates](#troubleshooting)
- [Failure diagnostics and manual cleanup](#cleanup-after-failure-or-interruption)

## Expected test environment

Provision the three clusters and their platform installation before running the
suite. The suite creates its own Grafana and MaaS traffic fixtures; it does not
install operators, deploy models, or configure fleet telemetry forwarding.

| Example kubecontext | Example cluster name | Required role |
|---|---|---|
| `hub` | `atarocch-hub` | ACM hub with central Thanos and Loki query endpoints |
| `spoke1` | `atarocch-spoke1` | First independent MaaS gateway and telemetry source |
| `spoke2` | `atarocch-spoke2` | Second independent MaaS gateway and telemetry source |

These names are examples, not required names. All three contexts must resolve to
different OpenShift cluster IDs and have Ready nodes. The suite selects contexts
explicitly and does not change the current kubecontext. A context is a connection
alias: `clusterName` is the spoke's ACM ManagedCluster name, and `clusterID` is its
OpenShift `ClusterVersion.spec.clusterID`. The hub's ACM membership may be named
`local-cluster` even when its kubecontext is `hub`.

Both spokes require:

- Available ACM ManagedCluster membership with matching cluster identity claims.
- A converged `odh-observability` Deployment in `controllerNamespace`, running
  exactly `expectedControllerImage`, and Ready Monitoring `default-monitoring`.
- The `rhai-metrics` ScrapeConfig delivered to
  `open-cluster-management-agent-addon`. PrometheusAgent
  `user-workload-metrics-collector` must be Available, select resources, and
  successfully collect the RHOAI collector metrics. Central series must retain
  `cluster`, `clusterID` and `ai_grid_collection_path="rhai-collector"`; matching
  `up` series must all be `1`.
- Working MaaS, Kuadrant/Limitador and AI Grid routing, with an existing ready
  MaaS model reference and a model reachable through that spoke's own gateway.
  Two model backends behind one gateway do not establish two telemetry sources.
- An HTTPS OpenAI-compatible chat-completions endpoint accepting the configured
  `model` alias. A non-streaming request must return choices and positive prompt
  and completion usage, with total usage equal to their sum. Each request asks
  for at most 16 completion tokens. The URL is used without adding a query string.
- MaaS organization capture enabled for metrics and logs. Usage-log forwarding
  must reach hub Loki with `cluster_name`, `cluster_id`, `organization_id`,
  `subscription`, `model`, `response_type`, and token fields `tokens_total`,
  `tokens_prompt`, `tokens_completion`. The token fields are structured metadata;
  the log body is an HTTP status and path. Successful test logs must contain
  `200 <path-component-of-inferenceURL>`, as checked by the generated case.
- Controllers able to enforce and remove policies for temporary subscriptions.
  The suite waits for the generated TokenRateLimitPolicy to include the new
  subscription, observe its current generation, and be Accepted and Enforced.
  Subscription Ready alone can precede metering enforcement.

The hub requires queryable ACM Thanos and a ready Loki tenant. Provide their
external **query base URLs**, not ingestion or internal raw service URLs:

| Configuration | Example |
|---|---|
| `metrics.url` | `https://rbac-query-proxy.apps.hub.example.test` |
| `logs.url` | `https://usage-logs.apps.hub.example.test/api/logs/v1/infrastructure` |
| `spokes[].maas.apiURL` | `https://maas.apps.spoke.example.test/maas-api` |
| `spokes[].inferenceURL` | `https://maas.apps.spoke.example.test/v1/chat/completions` |

An OTLP ingestion URL is rejected for `logs.url`. Both central stores must be
reachable from the hub Grafana Pod. The runner must reach the three Kubernetes
APIs, each MaaS API/inference endpoint, and the hub's admitted Grafana HTTPS Route.
The hub must allow the configured Grafana image to be pulled in a new namespace
and run on port 3000 under its restricted SCC. Grafana uses ephemeral storage.

### Credentials, TLS and permissions

| Connection / identity | Required access |
|---|---|
| Hub kubecontext | List Nodes; get ClusterVersion and ManagedClusters; create a Namespace, Pod, Service, Secret and Route; get Namespace/Route and delete the test Namespace |
| Each spoke kubecontext | List Nodes; get ClusterVersion, Monitoring, controller Deployment, ScrapeConfig, PrometheusAgent and TokenRateLimitPolicy; create/get/delete MaaSAuthPolicy and MaaSSubscription in `maas.namespace` |
| MaaS management identity | The explicit spoke kubecontext's authentication must be recognized by MaaS as `maas.username`, with permission to mint, search and revoke that subscription's keys |
| Central query credentials | Reader tokens authorized to query both spokes' metrics in ACM Thanos and usage logs in the configured Loki tenant |

Reader credentials are supplied as single-line token files, not values embedded
in YAML or command arguments. Keep them outside the repository with mode `0600`.
They must remain valid throughout setup, both spoke cases and cleanup. The suite
does not provision hub readers or extract kubeconfig tokens. MaaS management uses
the spoke kubecontext's authentication internally. Inference keys are generated
by the suite and remain in memory; do not supply `subscription` or `apiKeyFile`.

TLS verification is enabled on HTTPS connections made by the runner and Grafana:

- `grafana.routeCAFile`: runner/GCX trust for the temporary Grafana Route.
- `metrics.caFile` / `logs.caFile`: trust configured in the Grafana datasources.
- Each spoke's `caFile`: trust for its MaaS management and inference endpoints,
  independently of the Kubernetes API CA.

Omit a CA file when the endpoint is trusted by the system CA store. Supply the
appropriate PEM CA bundle for private issuers. A certificate must also match the
hostname: adding its CA does not fix a Service-name certificate presented by a
public hostname. Prepare a correctly certified endpoint or an externally managed
trusted proxy before testing. Such a proxy is an environment fixture, with its
own lifecycle; the suite does not create or remove it. See the
[temporary HTTPS proxy workaround](#maas-certificate-does-not-match-its-public-hostname)
for the setup used during three-cluster validation.

### Check identities and installation

These are read-only examples; repeat the spoke-specific checks for `spoke2` and
substitute configured namespaces/names where applicable:

```sh
oc config get-contexts
oc --context=hub get nodes
oc --context=hub get managedclusters
oc --context=spoke1 get nodes
oc --context=spoke1 whoami
oc --context=spoke1 get clusterversion version -o jsonpath='{.spec.clusterID}{"\n"}'
oc --context=spoke1 get monitoring.services.platform.opendatahub.io default-monitoring
oc --context=spoke1 -n redhat-ods-applications get deployment odh-observability
oc --context=spoke1 -n open-cluster-management-agent-addon get scrapeconfig.monitoring.rhobs rhai-metrics
oc --context=spoke1 -n open-cluster-management-agent-addon get prometheusagent.monitoring.rhobs user-workload-metrics-collector
oc --context=spoke1 -n models-as-a-service get maasmodelrefs.maas.opendatahub.io
```

### Deploy the intended controller code first

To test local controller changes, build and push the checkout's controller image
to a registry reachable by both spokes. Configure the parent platform operator's
`RELATED_IMAGE_ODH_OBSERVABILITY_IMAGE` through its owning GitOps/OLM configuration
and wait for both Deployments to converge. Set `expectedControllerImage` to the
complete `repository@sha256:<64-hex-digit-digest>` reference. To validate an
already installed release, supply its intended digest instead. The preflight
compares that exact image and its rollout; it does not deploy the controller.

The controller image contains the embedded templates. Changes to chart/CRDs
embedded in the parent platform operator also require a corresponding parent
operator build. Building the E2E image compiles tests, not the controller under test.

## Opt in and run

The live suite is opt-in: `-ai-grid-enabled` defaults to `false`. Set it to
`true`, or set `E2E_TEST_AI_GRID_ENABLED=true`, to enable the suite. When disabled,
`TestAIGridObservability` skips before reading its configuration or connecting to
Kubernetes, even if a configuration path is supplied. When enabled, a nonempty
`-ai-grid-config` or `E2E_TEST_AI_GRID_CONFIG` is required; omission fails
configuration validation before cluster access.

The helper package and its regressions live in `tests/e2e/aigrid`.
`make test`, `make unit-test` and `make test-verbose` explicitly include that
package while excluding the live E2E packages. Its regressions use local HTTP
servers and fake Kubernetes clients, so normal unit CI needs no clusters. Run
them independently from the repository root with:

```sh
go test -race ./tests/e2e/aigrid
```

Normal unit CI does not run the live three-cluster suite.

Copy [config.example.yaml](config.example.yaml) to a private directory, replace
every example value, and provide the reader token/CA files it references. Relative
file paths resolve from the YAML file's directory. Configuration identities are:

| Field | Meaning |
|---|---|
| `organizationID` | Expected organization, written to the new subscription and required in both central stores |
| `maas.namespace` | Existing tenant namespace where the test creates its MaaS policy/subscription |
| `maas.modelRef`, `maas.modelNamespace` | Name/namespace of the existing MaaSModelRef, not necessarily the inference service or request alias |
| `maas.username` | MaaS identity associated with that spoke's kubecontext; owner and sole user in the test policy |
| `model` | Alias sent in the inference request |
| `modelLabel` | Expected telemetry model value; defaults to `model`, and can account for a different served-model name |

For a local run from the repository root, use Bash and an available Go toolchain
matching `go.mod`. `oc` is useful for prechecks and recovery, but the suite uses
client-go to talk to Kubernetes. First prepare the input directory:

```sh
umask 077
GRID_INPUT_DIR=/absolute/path/to/private/ai-grid
mkdir -p "$GRID_INPUT_DIR"
cp tests/e2e/oats/ai-grid/config.example.yaml "$GRID_INPUT_DIR/grid.yaml"
chmod 600 "$GRID_INPUT_DIR/grid.yaml"
# Edit grid.yaml and supply its referenced reader credentials and CA files.
```

Then use the dedicated Make target, which sets `-ai-grid-enabled=true`, and
retain the Go output, including cleanup resource names. The configuration path
can be supplied through the environment as shown, or through `E2E_TEST_FLAGS`:

```sh
export E2E_TEST_AI_GRID_CONFIG="$GRID_INPUT_DIR/grid.yaml"
export ARTIFACTS=/absolute/path/to/ai-grid-artifacts
mkdir -p "$ARTIFACTS"
GRID_RUN_LOG="$ARTIFACTS/ai-grid-$(date -u +%Y%m%dT%H%M%SZ).log"
set -o pipefail
make e2e-test-ai-grid 2>&1 | tee "$GRID_RUN_LOG"
```

The equivalent direct command is:

```sh
go test ./tests/e2e/ -v -count=1 -timeout=90m \
  -run '^TestAIGridObservability$' -ai-grid-enabled=true \
  -ai-grid-config="$GRID_INPUT_DIR/grid.yaml"
```

Use this target or the anchored test selector to select only AI Grid. The generic
E2E targets also select other suites, which have their own installation behavior.
For direct or package-wide runs, `E2E_TEST_AI_GRID_ENABLED=true` is the environment
equivalent of `-ai-grid-enabled=true`. Disable the suite with
`-ai-grid-enabled=false` or `E2E_TEST_AI_GRID_ENABLED=false`, or leave the enable
flag/environment variable unset. The configuration path may remain set while
the suite is disabled. The dedicated `e2e-test-ai-grid` target always enables it;
omit that target to exclude AI Grid from a run.

`timeout` defaults to `5m`, with `pollInterval: 5s`; both must be positive,
`pollInterval <= timeout`, and `timeout <= 30m`. Readiness and OATS assertions use
bounded polling. Each OATS subprocess also has a `2 * timeout + 1m` deadline.
Keep the outer Go timeout large enough for setup, both spokes and cleanup.
Neither inference nor key-mint POSTs are retried. Up to one inference request is
sent per spoke; setup failure can prevent a spoke's request from being sent.

### Tooling and container execution

The suite reuses OATS 0.10.0 and GCX 1.2.0. Local execution installs OATS when
missing and may download GCX. Prepare tool caches for disconnected environments;
`OATS_GCX=/absolute/path/to/gcx` selects a preinstalled compatible GCX binary.

To run through the existing E2E container runner, first build the checkout's test
image with `make e2e-image-build E2E_IMG=<your-image>`. Prepare a kubeconfig usable
inside the container, plus the input/credential directory. For example:

```sh
GRID_E2E_IMAGE=quay.io/your-org/odh-observability-e2e:your-tag
GRID_KUBECONFIG=/absolute/path/to/container-ready-kubeconfig
podman run --rm \
  --userns=keep-id --user "$(id -u):$(id -g)" \
  -v "$GRID_KUBECONFIG:/tmp/kubeconfig:ro,z" \
  -v "$GRID_INPUT_DIR:/inputs:ro,z" \
  -v "$ARTIFACTS:/artifacts:Z" \
  -e KUBECONFIG=/tmp/kubeconfig \
  -e E2E_TEST_AI_GRID_ENABLED=true \
  -e E2E_TEST_AI_GRID_CONFIG=/inputs/grid.yaml \
  -e ARTIFACTS=/artifacts -e E2E_TEST_TIMEOUT=90m \
  "$GRID_E2E_IMAGE" '-test.run=^TestAIGridObservability$'
```

Input paths must resolve inside the container; relative credential paths make
this easier. Referenced kubeconfig certificate/key files and credential plugins
must also be present at their configured paths. The container must have the same
endpoint connectivity as the local runner. The existing
`make e2e-test-container*` targets select `TestMonitoring`; the command above
selects AI Grid through the image's `/e2e/e2e-run` entrypoint.

## Resources and lifecycle

After preflight, the suite provisions Grafana and its two named datasources:
**ACM Thanos** (`acm-thanos`) and **Hub Loki** (`hub-loki`). Both must pass health
checks before inference begins. It then runs the spokes sequentially:

1. Confirm collection is UP in central Thanos.
2. Create a fresh `aigrid-<uuid>` MaaSAuthPolicy and MaaSSubscription, scoped to
   the configured user, organization and model. The subscription requests a
   token rate limit of 10,000 per minute and priority `-100`.
3. Wait for parent readiness and current downstream token-policy enforcement.
4. Mint a one-hour ephemeral key and verify it is bound to that subscription.
5. Require a zero pre-request counter baseline and send one inference request
   with a successful, consistent positive-usage response.
6. Use OATS to require an increased attributed counter and at least one attributed
   successful usage log with all three token fields positive.
7. Revoke the key and remove that spoke's subscription/policy before continuing.

OATS' ten-minute log lookback cannot match an earlier run because subscription
names are always fresh and cannot be supplied in configuration. A UUID may remain
in the prompt/header for diagnostics; no request-UUID match in the log is needed.
Exact organization, model, subscription and source identity checks remain in place.

| Owner / location | Created | Normal removal |
|---|---|---|
| Suite / hub | Namespace `ai-grid-oats-*`, labeled `app.kubernetes.io/managed-by=ai-grid-oats` | Namespace deletion at suite cleanup |
| Suite / that namespace | Pod `grafana`, Service `grafana`, Secret `grafana-admin`, Route `grafana`, and Grafana `emptyDir` data/tmp storage | Removed with the namespace |
| Suite / Grafana | Datasources with reader tokens in Grafana's secure datasource fields | Removed with Grafana's ephemeral storage |
| Suite / each existing `maas.namespace` | MaaSAuthPolicy and MaaSSubscription sharing a fresh `aigrid-<uuid>` name and label `e2e.opendatahub.io/run=<same-name>` | Subscription first, then auth policy; original UIDs/run labels checked |
| Suite / MaaS API | One ephemeral key per prepared spoke, named and bound to that subscription; plaintext held only in memory | Explicit revocation before deleting the subscription; one-hour expiry is a backstop |
| MaaS/Kuadrant controllers / spokes | Reconciled generated/shared auth and token-limit configuration; subscription finalization may create cleanup Jobs | Controllers remove the test subscription's contribution when its parent CRs are deleted |
| Runner / local temporary directory | Private GCX configuration containing the temporary Grafana password | Go temporary-directory cleanup on normal test completion |
| Runner / artifact directory | Generated OATS cases and redacted `oats.log`; container runner also writes its normal results | Retained for diagnosis |

Existing models, tenant namespaces, operators, Monitoring/DSCI specifications,
subscriptions and auth policies are preserved. Shared generated policies can be
reconciled by their controllers while fixtures exist; recovery must remove the
test's parent resources rather than deleting shared generated policies.

Kubeconfig, hub reader credentials/RBAC, optional HTTPS proxies, and the three
clusters are environment-owned. The suite does not remove them. Generated
metrics/logs remain in the central stores under their normal retention policies.

## Troubleshooting

### MaaS certificate does not match its public hostname

The MaaS management or inference request can fail with an `x509` hostname error,
or `curl: (60) SSL: no alternative certificate subject name matches target
hostname`. In the validation environment, the Gateway's public HTTPS listener
presented a service-serving certificate whose DNS names did not include the
public `maas.apps...` hostname. Adding the certificate's CA to `spokes[].caFile`
does not resolve a hostname mismatch.

Check the configured endpoint without sending inference traffic. Repeat for
`spoke2`, adjusting Gateway and namespace names to match the installation:

```sh
GRID_SPOKE_CONTEXT=spoke1
GRID_MAAS_HOST=$(oc --context="$GRID_SPOKE_CONTEXT" -n openshift-ingress \
  get gateway maas-default-gateway \
  -o jsonpath='{.spec.listeners[?(@.protocol=="HTTP")].hostname}')
curl --silent --show-error --output /dev/null "https://$GRID_MAAS_HOST/maas-api/"
```

The permanent fix is to configure the Gateway's public HTTPS listener with a
certificate valid for that hostname through the installation's owning
GitOps/operator configuration.

For temporary testing, the successful hub/two-spoke validation used a separate
reverse proxy on **each spoke**, exposed by an edge-terminated OpenShift Route:

```text
runner -> HTTPS Route (router certificate) -> proxy -> HTTP MaaS Gateway Service
```

Prepare this outside the suite with permission to create a Namespace, ConfigMap,
Pod, Service and Route on each spoke:

1. Create a dedicated temporary namespace containing the proxy configuration,
   a proxy Pod and its Service. Use an approved image that runs under the
   restricted SCC. The validation proxy listened on port 8080 and needed no
   Kubernetes API credentials.
2. Forward requests to the existing Gateway Service's HTTP listener. In the
   validation environment this was
   `maas-default-gateway-maas-gateway-class.openshift-ingress.svc:80`.
   Set the upstream `Host` header to that spoke's original MaaS hostname so the
   existing Gateway routes and policies still apply. Preserve the request method,
   path, body and authorization header, and return the upstream status and body.
   Support `GET`, `POST` and `DELETE` for MaaS key management and inference;
   avoid logging authorization headers, keys or request/response bodies.
3. Expose the proxy Service through an edge-terminated HTTPS Route, using a
   certificate trusted by the runner. Wait for Pod readiness and Route admission,
   and verify the proxy's health endpoint over HTTPS with certificate verification.
4. Update **both** endpoint fields for that spoke in the private `grid.yaml`:

   ```yaml
   # Fragment of one spokes[] entry; retain its other required configuration.
   context: spoke1
   maas:
     apiURL: https://gateway-proxy-<temporary-namespace>.apps.<spoke-domain>/maas-api
     # Retain namespace, modelRef, modelNamespace and username.
   inferenceURL: https://gateway-proxy-<temporary-namespace>.apps.<spoke-domain>/v1/chat/completions
   # caFile: credentials/proxy-route-ca.pem # Only for a private Route issuer.
   ```

   Use the other spoke's own proxy Route in its entry. Keep cluster identities,
   organization, model and controller image settings unchanged, then use the
   documented [opt-in command](#opt-in-and-run).

The runner verifies the proxy Route's TLS certificate. The proxy-to-Gateway hop
uses the existing in-cluster HTTP listener. This exercises MaaS authentication,
metering, AI Grid routing and the central metrics/logs pipeline, while the original
public MaaS HTTPS certificate remains outside the run's coverage.

The proxy namespace and its resources are **environment-owned**: the suite does
not create or delete them. Keep them available until test cleanup, including key
revocation, has completed. If the run is interrupted, perform
[manual recovery](#cleanup-after-failure-or-interruption) through the proxy first.
Then delete each exact temporary namespace, wait for deletion, and restore the
permanent endpoints in `grid.yaml`. Once deleted, a saved proxy URL cannot be
reused by another run. Any separately provisioned hub reader ServiceAccount,
RBAC and local token files also require their own cleanup. The validation run
removed both proxies and its temporary reader after verifying key revocation and
restoration of the original resource baseline.

## Cleanup after failure or interruption

### Automatic cleanup and diagnostics

An ordinary setup or assertion failure still runs registered `t.Cleanup` handlers,
including partial setup. Each spoke gets an independent three-minute cleanup
context; hub Grafana namespace deletion gets five minutes. Key revocation happens
before subscription and auth-policy deletion. An ambiguous mint response triggers
an exact-ownership metadata search, including ephemeral keys. Cleanup failures
are reported as test failures, even if the OATS assertions passed.

There is no keep-fixtures mode for this suite. Legacy `NO_CLEANUP` or
`cleanup-test-state` settings do not disable its cleanup. A killed process,
process-level timeout, lost runner, or unavailable cluster/controller can leave
resources behind. Preserve the Go output and check for cleanup errors before
assuming a failed run has finished removing resources.

The output identifies resources with lines like:

```text
Temporary hub Grafana namespace: ai-grid-oats-abcde (deleted on success or failure)
[spoke1] Fresh MaaS policy/subscription/key: aigrid-<uuid> (revoked/deleted on success or failure)
AI Grid cases and redacted OATS output: /path/to/artifacts/ai-grid-<suffix>
```

The spoke fixtures have independent UUIDs. Partial setup may have created only
one resource or no key, and may not have reached the second spoke. Retained cases
and `oats.log` are under `ARTIFACTS`, otherwise `E2E_ARTIFACTS`, otherwise
`./e2e-artifacts`, in a unique `ai-grid-*` directory. A failure before case
generation can leave only the Go output. Known credentials are redacted from
OATS output; HTTP errors omit response bodies. Collect relevant Pod/Route events
while resources still exist if diagnosing Grafana startup.

### Manual recovery: identify the stopped run

Stop the affected runner before manual deletion. Use its saved Go output to set
the exact names below; substitute the configured contexts and namespace. Repeat
the spoke steps separately for every fixture printed by that run.

```sh
GRID_HUB_CONTEXT=hub
GRID_GRAFANA_NAMESPACE=ai-grid-oats-REPLACE_FROM_LOG
GRID_SPOKE_CONTEXT=spoke1
GRID_MAAS_NAMESPACE=models-as-a-service
GRID_FIXTURE=aigrid-REPLACE_WITH_FULL_UUID_FROM_LOG

oc --context="$GRID_HUB_CONTEXT" get namespace "$GRID_GRAFANA_NAMESPACE" --show-labels
oc --context="$GRID_SPOKE_CONTEXT" -n "$GRID_MAAS_NAMESPACE" \
  get maassubscriptions.maas.opendatahub.io,maasauthpolicies.maas.opendatahub.io \
  -l "e2e.opendatahub.io/run=$GRID_FIXTURE" \
  -o custom-columns=KIND:.kind,NAME:.metadata.name,UID:.metadata.uid,LABELS:.metadata.labels,DELETING:.metadata.deletionTimestamp
```

Confirm the exact namespace, names and ownership labels match the stopped run.
If the log is unavailable or namespace creation failed before its name was
printed, inventory candidates and compare their creation times with the run:

```sh
oc --context="$GRID_HUB_CONTEXT" get namespaces \
  -l app.kubernetes.io/managed-by=ai-grid-oats --show-labels
oc --context="$GRID_SPOKE_CONTEXT" -n "$GRID_MAAS_NAMESPACE" \
  get maassubscriptions.maas.opendatahub.io,maasauthpolicies.maas.opendatahub.io \
  -l e2e.opendatahub.io/run --show-labels
```

Inventory selectors can include other runs. Delete only individually confirmed
resources from the stopped run, not every `aigrid-*` resource or labeled namespace.

### Manual recovery: revoke keys, then delete parent resources

Use the configured MaaS API as the original management identity or an authorized
administrator. Inference keys cannot be used for key management. No lost plaintext
key value is needed: identify keys by metadata and revoke their IDs. For example,
send `POST <maas.apiURL>/v1/api-keys/search` with the full fixture name:

```json
{
  "filters": {
    "nameContains": "aigrid-REPLACE_WITH_FULL_UUID_FROM_LOG",
    "includeEphemeral": true,
    "status": ["active"]
  },
  "pagination": {"limit": 100, "offset": 0}
}
```

Only entries whose `name` **and** `subscription` exactly equal `GRID_FIXTURE` and
whose `ephemeral` is `true` belong to this fixture. Handle `has_more` pagination
before concluding none remain. Revoke every matching ID with
`DELETE <maas.apiURL>/v1/api-keys/<id>` using an authenticated HTTP client.
A `404` can mean the key is already absent; confirm with the correct management
identity. Repeating the search with `status: ["active"]` must return no matching
active keys.
Using `nameContains` plus exact response checks also works after the subscription
has gone, when a subscription-reference search may no longer resolve it.

Then delete only the verified parent CRs, allowing their normal finalizers to run:

```sh
oc --context="$GRID_SPOKE_CONTEXT" -n "$GRID_MAAS_NAMESPACE" \
  delete maassubscriptions.maas.opendatahub.io "$GRID_FIXTURE" \
  --ignore-not-found --wait=true --timeout=3m
oc --context="$GRID_SPOKE_CONTEXT" -n "$GRID_MAAS_NAMESPACE" \
  delete maasauthpolicies.maas.opendatahub.io "$GRID_FIXTURE" \
  --ignore-not-found --wait=true --timeout=3m
```

MaaS subscription finalization also provides a controller-managed key cleanup
path, subject to the environment's retention configuration. If direct revocation
is unavailable, normal subscription deletion can initiate that path; verify key
state afterward. One-hour expiry limits key lifetime but does not remove policies
or namespaces. Do not remove finalizers or change key-retention annotations to
bypass an incomplete cleanup.

Once both spokes are handled, remove the verified hub Grafana namespace:

```sh
oc --context="$GRID_HUB_CONTEXT" delete namespace "$GRID_GRAFANA_NAMESPACE" \
  --ignore-not-found --wait=true --timeout=5m
```

### Verify completion and handle stuck deletion

Repeat these checks for each spoke's exact fixture. Empty output confirms the
Kubernetes resources are absent; also confirm the MaaS search has no matching
active keys:

```sh
oc --context="$GRID_SPOKE_CONTEXT" -n "$GRID_MAAS_NAMESPACE" \
  get maassubscriptions.maas.opendatahub.io "$GRID_FIXTURE" --ignore-not-found
oc --context="$GRID_SPOKE_CONTEXT" -n "$GRID_MAAS_NAMESPACE" \
  get maasauthpolicies.maas.opendatahub.io "$GRID_FIXTURE" --ignore-not-found
oc --context="$GRID_HUB_CONTEXT" get namespace "$GRID_GRAFANA_NAMESPACE" --ignore-not-found
```

If deletion times out, inspect the resource's `deletionTimestamp`, finalizers,
events, MaaS cleanup Jobs and controller health. For a terminating hub namespace,
inspect its status conditions and remaining contents. Restore cluster, MaaS API,
MaaS/Kuadrant controller or worker availability as indicated, then retry normal
cleanup. The shared TokenRateLimitPolicy/other generated policies must be
reconciled by their controllers, not manually removed to clear test references.

After a hard kill, remove only the stopped run's leftover Go temporary directory
containing `gcx.yaml`; it holds that Grafana's password. Retain redacted artifacts
as needed. Caller-provided token/CA/kubeconfig files and any external preparation
fixtures remain the caller's responsibility. A new test run creates new names;
rerunning the suite does not clean up an earlier interrupted run.
