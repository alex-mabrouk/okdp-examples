# Iceberg / Polaris reference pipeline

`iceberg_polaris_pipeline` is the minimal reference for publishing a **managed
Iceberg table** rather than files. The job authenticates as an OAuth2 service
account and builds `spark.sql.catalog.<catalog>.credential` from a mounted Secret,
so the client secret never appears in the `SparkApplication`.

Defaults target the OKDP sandbox; override `POLARIS_CATALOG_URI`,
`POLARIS_OAUTH2_SERVER_URI`, `POLARIS_SCOPE`, `POLARIS_REALM`,
`POLARIS_CREDENTIALS_SECRET`, `SPARK_TRUSTSTORE_SECRET` and
`ICEBERG_CATALOG`/`ICEBERG_NAMESPACE`/`ICEBERG_TABLE`.

> Iceberg sends `scope=catalog` by default, which Keycloak rejects. The sandbox
> needs `scope=profile`.
