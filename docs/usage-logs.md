# Operator-managed MaaS usage logs

Configure `spec.usageLogs` on the `Monitoring` resource to collect structured MaaS
inference usage logs. When the platform operator manages `Monitoring`, configure
`DSCInitialization.spec.monitoring.usageLogs` instead. The platform operator passes
that configuration to this module.

The observability operator manages the usage-log collector and optional local
LokiStack. MaaS manages the gateway access logger, identity capture, and read
proxy. This guide covers the operator-managed pipeline used by AI Grid.

| Resource | Managed by | Purpose |
|----------|------------|---------|
| Gateway usage-log EnvoyFilter | MaaS controller | Emits structured inference access logs over OTLP |
| `OpenTelemetryCollector/data-science-usage-logs` | ODH observability operator | Enriches and exports usage logs |
| `data-science-usage-logs-collector-headless` Service | OpenTelemetry Operator | Receives OTLP with an OpenShift service-CA certificate |
| `LokiStack/data-science-usage-logs-lokistack` | ODH observability operator | Configures local usage-log storage and indexed attributes when `usageLogs.storage` is set |
| MaaS API Loki read proxy | MaaS controller | Restricts queries to the caller's organization and, for user views, their username |

`spec.logs` is a separate capability: it configures application-log forwarding
through ClusterLogForwarder and `data-science-lokistack`. See
[deployment modes](observability-modes.md#enabling-built-in-logs-loki) for that
pipeline. Cluster Logging Operator is needed for ClusterLogForwarder; MaaS usage
logs use the OpenTelemetry Operator.

## Configure the managed collector and local storage

Install the observability module and OpenTelemetry Operator. For local storage,
also install Loki Operator, provide an S3-compatible bucket, and create the
storage credential Secret in the monitoring namespace. Use the Secret format in
[the Loki storage procedure](observability-modes.md#enabling-built-in-logs-loki).
If application-log forwarding is enabled too, give its LokiStack a different
bucket from the usage-log store.

The following is a fragment to merge into an existing `DSCInitialization`:

```yaml
spec:
  monitoring:
    managementState: Managed
    namespace: redhat-ods-monitoring
    usageLogs:
      storage:
        type: s3
        secretName: rhoai-usage-logs-s3
        credentialMode: static
        storageClassName: gp3-csi
```

Use the monitoring namespace already configured on the cluster and an available
StorageClass. The DSCI schema and platform operator must support `usageLogs`;
check with `oc explain dscinitialization.spec.monitoring.usageLogs` before applying
the fragment. Preserve existing metrics, traces, and other monitoring settings.

For a standalone module installation, put the same `managementState`, `namespace`,
and `usageLogs` fields directly under `spec` on
`Monitoring/default-monitoring`. For platform-managed installations, update DSCI
so the parent operator retains the desired configuration.

The operator generates the collector and LokiStack from its
[collector template](../internal/controller/resources/usage-logs-opentelemetry-collector.tmpl.yaml)
and [usage-log LokiStack template](../internal/controller/resources/usage-logs-loki-stack.tmpl.yaml).
There is no `usageLogs.streamLabels` API field. Changes to generated resource
defaults belong in those templates; direct edits to generated resources can be
overwritten by reconciliation.

## Enable MaaS identity capture

For this pipeline, leave MaaS `Config/default.spec.usageLogging` false or omitted.
Setting it to true selects the legacy MaaS-owned collector. MaaS sends logs to
the operator-managed headless Service over TLS when the setting is false or
omitted. See the
[MaaS usage-log setup](https://github.com/opendatahub-io/models-as-a-service/blob/main/docs/content/observability/setup.md#logs-based-usage-dashboards)
for gateway and dashboard prerequisites.

Merge this fragment into the relevant `MaasTenantConfig` when both organization
and user views are required:

```yaml
spec:
  telemetry:
    enabled: true
    logs:
      captureOrganization: true
      captureUser: true
```

Organization capture defaults to true and requires organization attribution on
the subscription; `MaaSSubscription.spec.organization.id` supplies that association
when set. User capture defaults to false and must be enabled for user-scoped
queries. Log capture settings are independent of `telemetry.metrics` settings.
MaaS's read proxy matches `user_id` against the caller's trusted username.

## Identity attributes and Loki indexing

The collector's `groupbyattrs/maas` processor includes `organization_id` and
`user_id` alongside `model`, `subscription`, and `response_type`. It moves present
log attributes to OTLP resource attributes before exporting. Records missing
either or both IDs continue through the pipeline; grouping does not supply a
missing identity.

The generated local LokiStack indexes both IDs under
`spec.limits.tenants.application.otlp.streamLabels.resourceAttributes`. It also
retains the existing application attributes and the global `service.name`
attribute, which becomes Loki's `service_name` label.

Both steps are required by the
[MaaS API Loki read proxy](https://github.com/opendatahub-io/models-as-a-service/pull/1773).
Organization views inject an `organization_id` matcher into the LogQL stream
selector. Queries using `only_user=true` also inject a `user_id` matcher. A field
stored only as structured metadata cannot satisfy these stream-selector matchers.

For example, a user-scoped query requires newly ingested logs that match:

```logql
{service_name="models-as-a-service", organization_id="<organization-id>", user_id="<username>"}
```

Records without the required identity remain ingested but are excluded from that
identity-scoped query. Adding indexed attributes applies to newly ingested logs;
existing records retain their original indexing. After upgrading the operator,
wait for the generated resources to reflect the new configuration before sending
verification traffic. Indexing `user_id` increases the number of Loki streams as
users grow; account for this when sizing the store.

## Forward to an AI Grid hub

`usageLogs.exporters` adds remote destinations. With `storage` present, the
collector writes locally and remotely. With `storage` omitted, it forwards only;
no local LokiStack or object-storage credentials are needed. Loki Operator is required
only where a local LokiStack is deployed.

For example, merge the following into DSCI's monitoring configuration:

```yaml
spec:
  monitoring:
    managementState: Managed
    usageLogs:
      credentialSecret: hub-usage-logs-auth
      attributes:
        cluster_name: spoke-a
        cluster_id: "<spoke-cluster-id>"
      exporters:
        otlphttp/hub:
          endpoint: https://loki.hub.example.com/api/logs/v1/infrastructure/otlp
```

Create `hub-usage-logs-auth` in the monitoring namespace with the receiving hub's
bearer credential under the `token` key. The operator mounts it and attaches
the remote bearer-token authenticator to the exporter. Use an HTTPS endpoint
whose certificate the collector trusts. The exporter appends `/v1/logs` to the
configured OTLP base endpoint.

The `attributes` map stamps resource attributes on the remote pipeline only. Use
it for source-cluster identity; organization and user identity come from each
MaaS log record. The grouping processor promotes those IDs for both local and
remote delivery.

The hub's LokiStack must also index `organization_id` and `user_id` in the
receiving tenant. Configure that through the hub's owning chart or operator.
This repository's usage-log LokiStack template configures only the local
`application` tenant; it does not configure a remote hub's `infrastructure`
tenant. Retain the hub's existing `service.name` and cluster identity indexes.

## Configure reads and verify new traffic

MaaS owns the read-proxy configuration. Its controller reads `LOKI_READ_ENDPOINT`
and an optional `LOKI_READ_TOKEN` from the `usage-logs-loki` Secret in the MaaS
application namespace. Configure the token required by the receiving Loki
gateway, its read permissions, and TLS trust through the MaaS deployment.
The proxy stays disabled when the endpoint is absent.

For the locally managed store in `redhat-ods-monitoring`, the read base URL is:

```text
https://data-science-usage-logs-lokistack-gateway-http.redhat-ods-monitoring.svc.cluster.local:8080/api/logs/v1/application
```

`Monitoring.status.usageLogsEndpoint` is an OTLP ingestion endpoint, which ends
in `/otlp`. Configure the read proxy with the read API base URL above, or the
equivalent read URL and tenant for the hub. `status.logsEndpoint` belongs to the
separate application-log forwarding store.

1. Check the Monitoring conditions. For local storage, expect
   `UsageLogsCollectorAvailable` and `UsageLogsLokiStackAvailable` to be true.
   In forwarding-only mode, check `UsageLogsCollectorAvailable` and
   `status.usageLogsForwarding` instead.
2. Inspect the configured grouping keys and, when using local storage, the
   indexed attributes. Substitute your monitoring namespace:

   ```bash
   oc get opentelemetrycollector data-science-usage-logs -n redhat-ods-monitoring -o json \
     | jq '.spec.config.processors["groupbyattrs/maas"].keys'
   oc get lokistack data-science-usage-logs-lokistack -n redhat-ods-monitoring -o json \
     | jq '.spec.limits.tenants.application.otlp.streamLabels.resourceAttributes | map(.name)'
   ```

3. Send fresh successful inference traffic through the MaaS gateway using a
   subscription with the expected organization association. Enable user capture
   when verifying user views. Generate this traffic after the indexing change.
4. Query through the MaaS API as an authorized organization administrator, then
   verify the user view with `only_user=true`. Confirm the organization and user
   matchers find the new records in the intended local or hub store. The proxy
   enforces caller identity even if a query supplies different identity matchers.

If an identity-scoped query returns no records, check source capture and
subscription attribution, collector resource attributes, the destination's
indexed labels, the read endpoint and tenant, and the query time range. An
accepted log without the required identity or index will not match the proxy's
stream selector.
