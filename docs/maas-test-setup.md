# Test-only MaaS backend setup

This repository's MaaS usage-log coverage needs a reachable PostgreSQL database
for MaaS API state. The repository does not install or manage that database in
production. For short-lived validation on an OpenShift AI test cluster, the
optional [`docs/examples/maas-ephemeral-postgresql.yaml`](examples/maas-ephemeral-postgresql.yaml)
provides a single-replica PostgreSQL deployment, PVC, Service, and a restricted
database NetworkPolicy. The two required Secrets are generated separately so
the password is not duplicated in a manifest.

## Apply the test backend

The AI Gateway operator must have created `redhat-ai-gateway-infra` first. If
MaaS is already enabled, apply the manifest and restart the API after the
Secrets exist so it reloads the connection URL. Generate a private test
password and create both Secrets before applying the fixture:

```bash
export MAAS_DB_PASSWORD="$(openssl rand -hex 24)"
oc -n redhat-ai-gateway-infra create secret generic maas-postgres-credentials \
  --from-literal=POSTGRESQL_PASSWORD="${MAAS_DB_PASSWORD}" \
  --dry-run=client -o yaml | oc apply -f -
oc -n redhat-ai-gateway-infra create secret generic maas-db-config \
  --from-literal=DB_CONNECTION_URL="postgresql://maasadmin:${MAAS_DB_PASSWORD}@maas-postgres.redhat-ai-gateway-infra.svc.cluster.local:5432/maasdb?sslmode=disable" \
  --dry-run=client -o yaml | oc apply -f -
oc apply -f docs/examples/maas-ephemeral-postgresql.yaml
oc -n redhat-ai-gateway-infra wait --for=condition=Available \
  deployment/maas-postgres --timeout=2m
oc -n redhat-ai-gateway-infra rollout restart deployment/maas-api
oc -n redhat-ai-gateway-infra rollout status deployment/maas-api --timeout=3m
```

The manifest uses `gp3-csi` and a 5 GiB PVC. Change `storageClassName` and the
requested size when validating on a cluster with different storage classes.
The generated `maas-db-config` Secret is the Kubernetes Secret reference MaaS
uses for `DB_CONNECTION_URL`; the PostgreSQL Deployment references the same
generated credential through `secretKeyRef`. Never commit or reuse the
generated password. Keep `sslmode=disable` only for this short-lived,
in-cluster test fixture.

## Why `app=postgres` is intentional

The MaaS controller generates `maas-api-egress-restrict`. Its default port
5432 rule selects PostgreSQL pods using the `app=postgres` label (and, on some
versions, the corresponding label in the legacy application namespace). The
temporary Deployment therefore sets `app: postgres` on both its Deployment and
pod template. Without that label, PostgreSQL can be healthy and the connection
URL can be correct while `maas-api` still cannot reach the database and
remains unready.

This fixture also uses a stable in-cluster DNS name. Do not copy its
credentials, image digest, or storage design into a production environment.
Use a supported PostgreSQL service with TLS, durable backups, secret
management, and an appropriate network-policy rule in production.

## Network policy

The manifest adds `maas-postgres-allow-maas-api`, selecting the PostgreSQL pod
with its existing `app=postgres` label and allowing TCP 5432 only from pods
with the labels used by the MaaS API deployment:
`app.kubernetes.io/name=maas-api`, `app.kubernetes.io/component=api`, and
`app.kubernetes.io/part-of=models-as-a-service`. The API and database are in
the same infrastructure namespace, so the pod selector is namespace-local.
NetworkPolicies are additive; this fixture policy adds the required database
ingress rule without replacing other ingress policies in that namespace.

## Cleanup

Remove the fixture after validation. This deletes the PVC and the test data:

```bash
oc delete -f docs/examples/maas-ephemeral-postgresql.yaml
oc -n redhat-ai-gateway-infra delete secret maas-db-config maas-postgres-credentials --ignore-not-found
```

If MaaS remains enabled, remove or replace `maas-db-config` only after the
MaaS API has been disabled or pointed at a production database.

## Validate the usage dashboards

The test backend only provides MaaS API state. Usage logs still require the
Monitoring resource to configure `spec.usageLogs.storage` with a separate S3
secret and bucket. The observability operator creates the dedicated
`LokiStack/usage`; MaaS then creates its collector, tenancy proxy, Perses
datasources, and dashboards.

Check the complete dependency path in order:

```bash
oc -n redhat-ods-monitoring get lokistack usage
oc -n redhat-ods-monitoring get svc usage-gateway-http
oc -n redhat-ods-monitoring get cm usage-gateway-ca-bundle
oc -n redhat-ods-monitoring get persesdatasource usage-logs-all usage-logs-multi-tenancy
oc -n redhat-ods-monitoring get persesdashboard dashboard-4-maas-usage-logs-admin dashboard-5-maas-usage-logs
```

Both datasources should report `Available=True`. The admin datasource should
target `usage-gateway-http` and reference `usage-gateway-ca-bundle` with
`service-ca.crt`; the tenant datasource should target the MaaS tenancy proxy.
If the gateway or CA resources are missing, fix the `LokiStack/usage` setup
first. Do not disable TLS verification as a workaround.
