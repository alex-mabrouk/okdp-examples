package okdp.polaris_test

import data.okdp.polaris
import rego.v1

env := {
	"OKDP_CATALOG_NAMESPACE": "authz",
	"OKDP_CATALOG_NAME": "authz-polaris",
	"OKDP_ADMIN_GROUPS": "[\"platform_admin\"]",
	"OKDP_ENGINE_CLIENTS": "[\"cd17d3e3-f484-472d-a262-d2920b2d3bb9\"]",
}

etl := "2e97ae93-a55f-41a9-908e-622628f66e73"

hub := "43db057e-c761-40f8-aa8e-ada778b92910"

trino_client := "cd17d3e3-f484-472d-a262-d2920b2d3bb9"

allowed(i) if {
	polaris.allow with input as i
		with data.kubernetes.dataaccesspolicies as data.testdata.policies
		with polaris.env as env
}

# Real inputs of the lot 1 decision logs, on catalog "lake", moved to the given warehouse.
on(name, warehouse, roles) := json.unmarshal(replace(json.marshal(i), "\"name\":\"lake\"", sprintf("\"name\":%q", [warehouse]))) if {
	i := json.patch(data.testdata[name], [{"op": "replace", "path": "/actor/roles", "value": roles}])
}

test_etl_writes_silver if {
	allowed(on("etl_load_table_write", "silver", [etl]))
	allowed(on("etl_create_table_staged", "silver", [etl]))
	allowed(on("etl_update_table", "silver", [etl]))
	allowed(on("etl_load_namespace", "silver", [etl]))
}

test_etl_denied_bronze if {
	not allowed(on("etl_load_table_write", "bronze", [etl]))
	not allowed(on("etl_load_namespace", "bronze", [etl]))
}

test_hub_reads_gold_only if {
	not allowed(on("etl_load_table_write", "gold", [hub]))
	i := json.patch(on("etl_load_table_write", "gold", [hub]), [{"op": "replace", "path": "/action", "value": "LOAD_TABLE_WITH_READ_DELEGATION"}])
	allowed(i)
	not allowed(on("etl_load_table_write", "bronze", [hub]))
}

test_hub_namespace_scope if {
	i := on("trino_list_namespaces", "gold", [hub])
	allowed(i)
	not allowed(on("trino_list_namespaces", "bronze", [hub]))
}

test_engine_client_allowed_on_tables if {
	allowed(on("etl_load_table_write", "bronze", [trino_client]))
	not allowed(on("server_list_catalogs", "lake", [trino_client]))
}

test_restricted_read_grants_nothing if {
	# business_analyst reads gold with a row filter and a mask: only Trino can apply them.
	i := json.patch(on("etl_load_table_write", "gold", ["business_analyst"]), [{"op": "replace", "path": "/action", "value": "LOAD_TABLE_WITH_READ_DELEGATION"}])
	not allowed(i)
}

test_admin_manages_catalogs if {
	allowed(on("server_list_catalogs", "lake", ["platform_admin", "d7fa4db5-ddde-4f48-860d-07ebba464797"]))
	not allowed(on("server_list_catalogs", "lake", ["business_analyst"]))
}

test_admin_has_no_data_by_default if not allowed(on("etl_load_table_write", "silver", ["platform_admin"]))

test_groups_in_roles if allowed(on("etl_load_table_write", "silver", ["data_engineer", "d7fa4db5-ddde-4f48-860d-07ebba464797"]))

test_empty_input_denied if not allowed({})

test_other_instance_policies_ignored if {
	# other-catalog grants data_scientist everything on other-polaris only.
	not allowed(on("etl_load_namespace", "gold", ["data_scientist"]))
}

test_without_catalog_identity_denied if {
	not polaris.allow with input as on("etl_load_table_write", "silver", [etl])
		with data.kubernetes.dataaccesspolicies as data.testdata.policies
		with polaris.env as {}
}
