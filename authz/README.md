# Data access policies of the examples

Access to the Iceberg tables is written as `DataAccessPolicy` resources (`data.okdp.io/v1alpha1`), in the
namespace that owns the data. OPA reads them and decides for Trino and Polaris. Nothing is allowed
without a policy.

- The CRD and its roles come from the `okdp-authz` chart, OPA and the OKDP Rego from the `okdp-opa`
  chart (repository opa-opal). The Trino and Polaris packages install `okdp-opa` themselves.
- Members of a project read the policies of the project (`okdp-project-view`); its admins write them.

## A policy

| Field | Meaning |
|---|---|
| `subjects.groups` | groups of the identity provider (users) |
| `subjects.clients` | OIDC client ids (`azp`): workloads, from the `status.clientId` of their `OIDCClient` |
| `resource.catalog` | the Polaris instance of the namespace (its release name) |
| `resource.warehouse` | the Polaris catalog (`bronze`, `silver`, `gold`) |
| `resource.namespace`, `resource.table` | Iceberg namespace and table, `"*"` for any |
| `privileges` | `select`, `insert`, `update`, `delete`, `create`, `drop` |
| `rowFilter` | conditions joined with AND, Iceberg predicates (`eq`, `in`, `lt`, `is-null`...) |
| `columns` | masks of the Iceberg read restrictions (`mask-alphanum`, `show-last-4`...) |

Several policies on a table add up. A policy with a `rowFilter` or masked `columns` grants nothing
through Polaris, which hands out files: only Trino applies it, so Superset and SQL users see filtered
and masked rows, Spark does not read the table.

## The policies of the examples

[`policies.yaml`](policies.yaml) is written for a project `demo` with a Polaris instance `demo-polaris`:

| Policy | Who | What |
|---|---|---|
| `etl-bronze`, `etl-silver`, `etl-gold` | the workload client of the project (DAGs, Spark jobs) | builds the three layers |
| `hub-silver`, `hub-gold` | the catalog client of JupyterHub | notebooks read silver and gold |
| `engineers-bronze`, `engineers-silver` | `data_engineer` | read bronze, build silver |
| `analysts-gold` | `business_analyst` | read gold, French rows only, `email` and `phone` masked |

Fill in the client ids, then apply:

```sh
ETL=$(kubectl get oidcclient -n demo svc-demo-etl -o jsonpath='{.status.clientId}')
HUB=$(kubectl get oidcclient -n demo demo-jupyterhub-catalog -o jsonpath='{.status.clientId}')
sed "s/ETL_CLIENT_ID/$ETL/; s/HUB_CLIENT_ID/$HUB/" policies.yaml | kubectl apply -f -
```

The DAGs take the workload client from the Airflow package (`workloadClientSecretRef`); a DAG that
writes to a catalog without a policy is refused by Polaris.
