# OKDP data access policies in Rego

OPA policies that read the `DataAccessPolicy` resources (`data.okdp.io/v1alpha1`) and decide for
Trino and Polaris. kube-mgmt replicates the resources of every namespace under
`data.kubernetes.dataaccesspolicies`.

| File | Package | Queried by |
|---|---|---|
| `lib.rego` | `okdp.authz` | the adapters: subjects, wildcards, deny by default |
| `trino.rego` | `okdp.trino` | Trino: `allow`, `rowFilters`, `columnMasks` |
| `polaris.rego` | `okdp.polaris` | Polaris: `allow` |
| `idp_keycloak.rego` | `okdp.idp` | `trino.rego`: groups of a user, from the Keycloak admin API |

Trino 484 does not pass the groups of the token to OPA, so `idp_keycloak.rego` reads them from
Keycloak and keeps them 60 seconds. Polaris receives the groups and the client id (`azp`) of the
token in `actor.roles`.

A policy with a `rowFilter` or masked `columns` grants nothing through Polaris: Polaris hands out
the files, it cannot filter or mask them. Only Trino applies these policies.

Run the tests:

```
docker run --rm -v $PWD:/p:ro openpolicyagent/opa:1.21.3 test /p -v
```

`testdata/` holds inputs captured in the decision logs of Trino and Polaris.
