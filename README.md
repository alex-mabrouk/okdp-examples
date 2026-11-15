[![ci](https://github.com/okdp/okdp-examples/actions/workflows/ci.yml/badge.svg)](https://github.com/okdp/okdp-examples/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/okdp/okdp-examples)](https://github.com/okdp/okdp-examples/releases/latest)
[![License Apache2](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)
<a href="https://okdp.io">
<img src="https://okdp.io/logos/okdp-notext.svg" height="20px" style="margin: 0 2px;" />
</a>

# Overview

A collection of hands-on examples, helper utilities, Jupyter notebooks, Airflow DAGs, and data workflows showcasing how to work with the [OKDP Platform](https://okdp.io/).
This repository is meant to help you explore OKDP capabilities around compute, object storage, data catalog, SQL engines, Spark, workflow orchestration, and analytics.

The project follows a [**Bronze → Silver → Gold** Medallion](https://www.databricks.com/blog/what-is-medallion-architecture) architecture:

- **Bronze** stores raw Parquet files in S3-compatible object storage and supports exploration, profiling, and source understanding.
- **Silver** publishes a trusted, conformed Iceberg table in the `silver` Polaris catalog.
- **Gold** publishes curated business-facing Iceberg tables in the `gold` Polaris catalog.

Over time, these examples will be extended with features, such as:

- Shared metadata with stronger schema enforcement and evolution.
- Snapshot-based table management (time travel, retention, cleanup).
- Incremental processing and analytics-ready datasets, etc.
- Automated ingestion, transformations, and dataset publishing through Apache Airflow.

The diagram below illustrates how the OKDP services interact:

<p align="center">
  <img src="docs/assets/architecture.drawio.svg" alt="OKDP examples: medallion architecture across Superset, Jupyter, Airflow, Trino, Polaris and the S3 object storage" width="820"/>
</p>

#### Data flow:

1. Raw Parquet datasets are stored in the S3 object storage as the Bronze layer.
2. Bronze data is exposed to Trino through Hive Metastore external tables.
3. Jupyter notebooks use PySpark to read Bronze data and produce trusted Silver Iceberg tables.
4. Silver and Gold tables are registered in Apache Polaris through the Iceberg REST catalog.
5. Polaris uses OAuth2 and STS-based credential vending to allow temporary S3 access for Iceberg writes.
6. Gold tables are built from Silver tables and expose curated business-facing datasets.
7. Superset connects to Trino over HTTPS and queries the published datasets for analytics and dashboards.

#### Security and access model:

- Keycloak provides OIDC / OAuth2 identity.
- Jupyter accesses Polaris through OAuth2.
- Polaris manages catalog permissions and returns temporary credentials for object storage access.
- The storage service provides S3-compatible object storage with IAM and STS.
- Bronze can use static S3 credentials for raw data access.
- Silver and Gold should use temporary credentials through Polaris / STS where possible.
- Superset accesses datasets through Trino rather than directly accessing object storage.

# Notebooks

The notebooks analyze datasets stored as Parquet on S3-compatible storage.
The same underlying dataset is queried using Trino and Spark.

An [index.ipynb](./notebooks/index.ipynb) notebook is also provided as an entry point.

## Trino notebooks

The following notebooks query data using Trino:

- Querying data using Trino (Python/SQLAlchemy).
- Querying data using Trino (SQL engine).

These notebooks use Trino external tables defined over Parquet data stored in object storage and registered via a metadata service.

## PySpark notebook

A PySpark notebook is included to showcase Spark-native exploratory data analysis on the same dataset.

# Superset

Use Apache Superset (SQL Lab) to query Trino and build visualizations/dashboards on top of the same datasets.

# Airflow

The [airflow/](./airflow/) directory contains example DAGs orchestrated by Apache Airflow on the OKDP platform. They demonstrate how to:

- Submit Spark jobs to **Spark Operator** via `SparkApplication` custom resources from a DAG.
- Build daily ETL pipelines reading from and writing to S3-compatible object storage.
- Use Airflow `gitSync` to pull DAGs directly from this repository at runtime.

See [`airflow/README.md`](./airflow/README.md) for the full list of DAGs and quick-start instructions.

# Running the examples:

Using [okdp-ui](https://github.com/OKDP/okdp-sandbox), deploy the following components:

- Storage: S3-compatible object storage ([SeaweedFS](https://github.com/seaweedfs/seaweedfs) in the sandbox)
- Data Catalog: [Hive Metastore](https://hive.apache.org/), [Apache Polaris](https://polaris.apache.org/)
- Interactive Query: [Trino](https://trino.io/)
- Notebooks: [Jupyter](https://jupyter.org/)
- DataViz: [Apache Superset](https://superset.apache.org/)
- Workflow orchestration: [Apache Airflow](https://airflow.apache.org/)
- Applications: [okdp-examples](https://okdp.io)

# About the datasets

At deployment time, the Helm chart:
1. Downloads public datasets.
2. Uploads them into object storage.
3. Creates the corresponding Trino external tables.

> ℹ️ NOTE
>
> The datasets are not bundled in this repository and are not baked into container images.

# Authentication and Authorization Model

This example uses **Keycloak** as the central identity provider. Applications authenticate users through **OIDC**, and Keycloak realm roles are exposed as OIDC `groups` claims.

The model is based on centralized authentication with role-based authorization across platform services.

## Central Identity Provider

Keycloak is used as the central identity provider. The platform defines OIDC clients for:

* Superset
* JupyterHub
* Trino
* Spark History Server
* Polaris
* Airflow
* Service accounts used by platform components

Keycloak realm roles are mapped into the OIDC `groups` claim.
As a result, platform applications consume Keycloak roles as user groups.

## OIDC Groups

| Group                   | Purpose                                              |
| ----------------------- | ---------------------------------------------------- |
| `platform_admin`        | Full platform administration                         |
| `data_engineer`         | Data engineering and lakehouse write access          |
| `data_scientist`        | Analytical access to curated data                    |
| `business_analyst`      | BI and read-only access                              |
| `data_steward`          | Governance, stewardship, and metadata administration |
| `auditor`               | Audit and read-only access                           |

## OIDC Users

| User    | Groups             |
| ------- | ------------------ |
| `bob`   | `data_engineer`    |
| `mark`  | `data_scientist`   |
| `nina`  | `business_analyst` |
| `grace` | `data_steward`     |
| `eve`   | `auditor`          |
| `alice` | `platform_admin`   |
| `adm`   | `platform_admin`   |


<details>
<summary><strong>Application-level authorization</strong></summary>

Each platform service consumes the OIDC `groups` claim and maps it to application-specific permissions.

### Superset

| Group              | Superset Roles     |
| ------------------ | ------------------ |
| `platform_admin`   | `Admin`            |
| `data_engineer`    | `Alpha`, `sql_lab` |
| `data_scientist`   | `Alpha`, `sql_lab` |
| `business_analyst` | `Gamma`            |
| `data_steward`     | `Gamma`, `sql_lab` |
| `auditor`          | `Gamma`            |

Superset is configured to impersonate users when connecting to Trino, allowing downstream authorization to be based on the end-user identity.

### JupyterHub

| Permission   | Groups                                                                                  |
| ------------ | --------------------------------------------------------------------------------------- |
| Admin access | `platform_admin`                                                                        |
| Login access | `platform_admin`, `data_engineer`, `data_scientist`, `data_steward`, `business_analyst` |

### Spark History Server

| Permission    | Groups                                                                                  |
| ------------- | --------------------------------------------------------------------------------------- |
| Admin         | `platform_admin`                                                                        |
| History admin | `platform_admin`, `auditor`                                                             |
| Modify        | `platform_admin`, `data_engineer`                                                       |
| View          | `platform_admin`, `data_engineer`, `data_scientist`, `data_steward`, `business_analyst` |

### Airflow

| Group              | Airflow Role |
| ------------------ | ------------ |
| `platform_admin`   | `Admin`      |
| `data_engineer`    | `Op`         |
| `data_scientist`   | `User`       |
| `business_analyst` | `Viewer`     |
| `data_steward`     | `Viewer`     |
| `auditor`          | `Viewer`     |

</details>

<details>
<summary><strong>Polaris authorization</strong></summary>

Polaris is used as the main authorization layer for Iceberg catalogs such as `silver` and `gold`.

### Catalog roles

| Polaris Catalog Role  | Purpose                                                       |
| --------------------- | ------------------------------------------------------------- |
| `catalog_reader`      | Read catalog metadata and table data                          |
| `catalog_contributor` | Create and write namespaces, tables, and views                |
| `data_administrator`  | Manage catalog, namespace, table, view metadata, and policies |

### Principal role mapping

| Principal Role     | Polaris Catalog Roles                       | Access Level                     |
| ------------------ | ------------------------------------------- | -------------------------------- |
| `business_analyst` | `catalog_reader`                            | Read-only                        |
| `data_scientist`   | `catalog_reader`                            | Read-only                        |
| `auditor`          | `catalog_reader`                            | Read-only                        |
| `data_steward`     | `catalog_reader`, `data_administrator`      | Read + governance administration |
| `data_engineer`    | `catalog_contributor`, `data_administrator` | Read/write + administration      |
| `platform_admin`   | `catalog_contributor`, `data_administrator` | Full data/catalog administration |
| `service_admin`    | Service administration                      | Polaris admin-plane access       |
| `catalog_admin`    | Catalog administration                      | Polaris admin-plane access       |

</details>

<details>
<summary><strong>Service accounts</strong></summary>

Service accounts are used for platform-to-platform communication.

| Service Account             | Purpose                         |
| --------------------------- | ------------------------------- |
| `svc-trino-examples-writer` | AI assistant queries to Trino   |

The services that reach Polaris (Trino, JupyterHub, Spark jobs) use OIDC clients registered
for them; Polaris authorizes them by client id through OPA.

</details>

<details>
<summary><strong>Storage authorization</strong></summary>

Object storage access is managed through service identities rather than individual end-user identities.

| Identity        | Access                   |
| --------------- | ------------------------ |
| `hiveMetastore` | Read, write, list        |
| `trino`         | Read, write, list, admin |
| `jupyterHub`    | Read, write, list        |
| `sparkHistory`  | Read, write, list        |
| `polaris`       | Read, write, list, admin |
| `airflow`       | Read, write, list, admin |
| `examples`      | Admin, read, write, list |
| `seaweedfs`     | Admin                    |

Storage access is controlled at the service level, while user-level data access is enforced mainly through Trino, Polaris, and application-level role mappings.

</details>

# Known issues
1. [Polaris - Spark Iceberg REST Catalog refresh token](https://github.com/apache/iceberg/issues/12363) (closed as not planned)
    > Long-running jobs may need more metadata calls to Polaris during execution, not just one initial call.
    >
    > Closed upstream as not planned, so the limitation still applies.
2. [Polaris - OAuth 2 grant type "refresh_token" not implemented](https://github.com/apache/iceberg/issues/12196) (closed as not planned)
3. [Trino - Issue with Vended Credential Renewal with Iceberg REST Catalog](https://github.com/trinodb/trino/issues/25827) (fixed)
   > Reported upstream: with `iceberg.rest-catalog.vended-credentials-enabled=true`, long-running queries may fail once the STS token expires because Trino appears not to refresh vended credentials from the Iceberg REST catalog `/credentials` endpoint.
   >
   > Fixed by [PR #28998](https://github.com/trinodb/trino/pull/28998), merged on 2026-04-27. An earlier proposal, [PR #28792](https://github.com/trinodb/trino/pull/28792), was closed without being merged.
4. [Trino - Extra credential support for user token passthrough](https://github.com/trinodb/trino/issues/27197)
    > Requests support for passing per-user OAuth tokens/credentials to the Iceberg REST catalog
5. [Trino - Include oauth user in the request to the iceberg REST catalog](https://github.com/trinodb/trino/issues/26320)
   > [Starburst supports OAuth 2.0 token pass-through for the Iceberg REST catalog](https://docs.starburst.io/latest/object-storage/metastores.html#oauth-2-0-token-pass-through), which forwards the delegated OAuth token from the coordinator to the catalog:
   >
   > ```properties
   > http-server.authentication.type=DELEGATED-OAUTH2
   > iceberg.rest-catalog.security=OAUTH2_PASSTHROUGH
   > ```
6. [STS assume role fails with credentials (from Lakekeeper) due to incomplete STS implementation](https://github.com/seaweedfs/seaweedfs/discussions/8312)
   > The discussion initially points to a possible SeaweedFS STS compatibility issue, but the later reproducer narrows the failure to Lakekeeper's scoped session policy: multipart writes fail when the policy omits the required multipart S3 permissions.
   >
   > It demonstrates that multipart upload can fail if the scoped session policy does not include multipart actions such as:
   > - `s3:CreateMultipartUpload`
   > - `s3:UploadPart`
   > - `s3:CompleteMultipartUpload`
   > - `s3:AbortMultipartUpload`
   >
   > Fixed by [PR #8445](https://github.com/seaweedfs/seaweedfs/pull/8445), merged upstream.


---

## Contributing & License

Contributions follow the [OKDP contribution guide](https://github.com/OKDP/.github/blob/main/CONTRIBUTING.md). Released under the [Apache License 2.0](LICENSE).

---

**Built 🚀 for the OKDP Community**
<a href="https://okdp.io">
  <img src="https://okdp.io/logos/okdp-notext.svg" height="20px" style="margin: 0 2px;" />
</a>
