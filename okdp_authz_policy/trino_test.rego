package okdp.trino_test

import data.okdp.trino
import rego.v1

policies := data.testdata.policies

idp_groups := {"nina": {"business_analyst"}, "bob": {"data_engineer"}}

mock_groups(user) := object.get(idp_groups, user, set())

# Real inputs of the lot 1 decision logs, catalog "lake" served from the warehouse given here.
on(name, warehouse) := json.patch(data.testdata[name], [{
	"op": "add",
	"path": "/context/properties",
	"value": {"okdp.catalog.lake": concat("/", ["authz", "authz-polaris", warehouse])},
}])

as_user(i, user) := json.patch(i, [{"op": "replace", "path": "/context/identity/user", "value": user}])

allowed(i) if {
	trino.allow with input as i
		with data.kubernetes.dataaccesspolicies as policies
		with data.okdp.idp.groups as mock_groups
}

row_filters(i) := f if {
	f := trino.rowFilters with input as i
		with data.kubernetes.dataaccesspolicies as policies
		with data.okdp.idp.groups as mock_groups
}

masks(i) := m if {
	m := trino.columnMasks with input as i
		with data.kubernetes.dataaccesspolicies as policies
		with data.okdp.idp.groups as mock_groups
}

test_execute_query if allowed(data.testdata.nina_execute_query)

test_nina_reads_gold if {
	allowed(on("nina_access_catalog", "gold"))
	allowed(on("nina_filter_catalogs", "gold"))
	allowed(on("nina_filter_tables", "gold"))
	allowed(on("nina_filter_columns", "gold"))
	allowed(on("nina_select", "gold"))
	allowed(on("nina_select_information_schema", "gold"))
	allowed(on("nina_filter_schemas", "gold"))
}

test_nina_denied_silver if {
	not allowed(on("nina_access_catalog", "silver"))
	not allowed(on("nina_filter_tables", "silver"))
	not allowed(on("nina_select", "silver"))
	not allowed(on("nina_select_information_schema", "silver"))
}

test_nina_cannot_write_gold if not allowed(as_user(on("bob_insert", "gold"), "nina"))

test_catalog_without_alias_denied if not allowed(data.testdata.nina_select)

test_bad_alias_denied if {
	i := json.patch(data.testdata.nina_select, [{"op": "add", "path": "/context/properties", "value": {"okdp.catalog.lake": "authz/gold"}}])
	not allowed(i)
}

test_policies_of_another_namespace_ignored if {
	# fraud grants insert on authz-polaris to business_analyst, but authz owns the catalog.
	not allowed(as_user(on("bob_insert", "silver"), "nina"))
}

test_bob_writes_silver if {
	allowed(on("bob_show_schemas", "silver"))
	allowed(on("bob_insert", "silver"))
	allowed(on("bob_create_table", "silver"))
}

test_bob_cannot_drop_silver if {
	i := json.patch(on("bob_insert", "silver"), [{"op": "replace", "path": "/action/operation", "value": "DropTable"}])
	not allowed(i)
}

test_unknown_user_denied if not allowed(as_user(on("nina_select", "gold"), "mallory"))

test_workload_by_client_id if {
	allowed(as_user(on("bob_insert", "silver"), "2e97ae93-a55f-41a9-908e-622628f66e73"))
	not allowed(as_user(on("bob_insert", "gold"), "2e97ae93-a55f-41a9-908e-622628f66e73"))
}

test_unknown_operation_denied if {
	i := json.patch(on("nina_select", "gold"), [{"op": "replace", "path": "/action/operation", "value": "SetSystemSessionProperty"}])
	not allowed(i)
}

test_own_queries_only if {
	i := json.patch(data.testdata.nina_execute_query, [
		{"op": "replace", "path": "/action/operation", "value": "KillQueryOwnedBy"},
		{"op": "add", "path": "/action/resource", "value": {"user": {"user": "bob"}}},
	])
	not allowed(i)
}

row_filter_input(warehouse, user) := as_user(
	json.patch(on("nina_select", warehouse), [
		{"op": "replace", "path": "/action/operation", "value": "GetRowFilters"},
		{"op": "remove", "path": "/action/resource/table/columns"},
	]),
	user,
)

test_row_filter_of_nina if {
	row_filters(row_filter_input("gold", "nina")) == [{"expression": "(\"country\" = 'FR')"}]
}

test_no_row_filter_without_one_in_the_policy if row_filters(row_filter_input("silver", "bob")) == []

test_no_row_filter_without_access if row_filters(row_filter_input("silver", "nina")) == []

test_row_filters_of_two_policies_or if {
	extra := {"metadata": {"namespace": "authz", "name": "analysts-gold-de"}, "spec": {
		"subjects": {"groups": ["business_analyst"]},
		"resource": {"catalog": "authz-polaris", "warehouse": "gold"},
		"privileges": ["select"],
		"rowFilter": [{"column": "country", "op": "in", "values": ["DE", "AT"]}, {"column": "amount", "op": "gt", "value": 100}],
	}}
	p := json.patch(policies, [{"op": "add", "path": "/authz/analysts-gold-de", "value": extra}])
	f := trino.rowFilters with input as row_filter_input("gold", "nina")
		with data.kubernetes.dataaccesspolicies as p
		with data.okdp.idp.groups as mock_groups
	f == [{"expression": "(\"country\" = 'FR') OR (\"country\" IN ('DE', 'AT') AND \"amount\" > 100)"}]
}

test_policy_without_filter_lifts_it if {
	extra := {"metadata": {"namespace": "authz", "name": "analysts-gold-all"}, "spec": {
		"subjects": {"groups": ["business_analyst"]},
		"resource": {"catalog": "authz-polaris", "warehouse": "gold"},
		"privileges": ["select"],
	}}
	p := json.patch(policies, [{"op": "add", "path": "/authz/analysts-gold-all", "value": extra}])
	f := trino.rowFilters with input as row_filter_input("gold", "nina")
		with data.kubernetes.dataaccesspolicies as p
		with data.okdp.idp.groups as mock_groups
	f == []
}

mask_input(warehouse, columns) := json.patch(data.testdata.nina_select, [
	{"op": "add", "path": "/context/properties", "value": {"okdp.catalog.lake": concat("/", ["authz", "authz-polaris", warehouse])}},
	{"op": "replace", "path": "/action/operation", "value": "GetColumnMask"},
	{"op": "remove", "path": "/action/resource"},
	{"op": "add", "path": "/action/filterResources", "value": [{"column": {
		"catalogName": "lake", "schemaName": "sales", "tableName": "customers",
		"columnName": c[0], "columnType": c[1],
	}} |
		some c in columns
	]},
])

test_mask_of_nina if {
	masks(mask_input("gold", [["id", "bigint"], ["email", "varchar"]])) == [{"index": 1, "viewExpression": {"expression": "CAST(regexp_replace(regexp_replace(\"email\", '[^0-9(),.@-]', 'x'), '[0-9]', 'n') AS varchar)"}}]
}

test_no_mask_without_access if masks(mask_input("silver", [["email", "varchar"]])) == []

mask_of(action, type) := e if {
	p := json.patch(policies, [{"op": "replace", "path": "/authz/analysts-gold/spec/columns", "value": [{"name": "c", "action": action}]}])
	m := trino.columnMasks with input as mask_input("gold", [["c", type]])
		with data.kubernetes.dataaccesspolicies as p
		with data.okdp.idp.groups as mock_groups
	e := m[0].viewExpression.expression
}

test_mask_actions if {
	mask_of("show-last-4", "varchar(20)") == "CAST(CASE WHEN length(\"c\") <= 4 THEN \"c\" ELSE concat(regexp_replace(regexp_replace(substr(\"c\", 1, length(\"c\") - 4), '[^0-9(),.@-]', 'x'), '[0-9]', 'n'), substr(\"c\", length(\"c\") - 3)) END AS varchar(20))"
	mask_of("replace-with-null", "bigint") == "CAST(NULL AS bigint)"
	mask_of("mask-to-fixed-value", "date") == "CAST(DATE '1970-01-01' AS date)"
	mask_of("truncate-to-month", "timestamp(6) with time zone") == "CAST(date_trunc('month', \"c\" AT TIME ZONE 'UTC') AS timestamp(6) with time zone)"
	mask_of("sha-256-global", "varchar") == "CAST(lower(to_hex(sha256(to_utf8(\"c\")))) AS varchar)"
	mask_of("sha-256-global", "bigint") == "from_big_endian_64(reverse(substr(sha256(reverse(to_big_endian_64(\"c\"))), 1, 8)))"
}

test_mask_fails_closed if {
	mask_of("sha-256-query-local", "varchar") == "CAST(fail('masking action sha-256-query-local is not supported on varchar') AS varchar)"
	mask_of("mask-alphanum", "bigint") == "CAST(fail('masking action mask-alphanum is not supported on bigint') AS bigint)"
}

schema_input(name, warehouse, operation, schema) := json.patch(on(name, warehouse), [
	{"op": "replace", "path": "/action/operation", "value": operation},
	{"op": "replace", "path": "/action/resource", "value": {"schema": {"catalogName": "lake", "schemaName": schema}}},
])

test_schemas if {
	allowed(schema_input("nina_filter_schemas", "gold", "FilterSchemas", "sales"))
	allowed(schema_input("nina_filter_schemas", "gold", "ShowTables", "sales"))
	not allowed(schema_input("nina_filter_schemas", "silver", "FilterSchemas", "sales"))
	allowed(as_user(schema_input("nina_filter_schemas", "silver", "CreateSchema", "staging"), "bob"))
	not allowed(as_user(schema_input("nina_filter_schemas", "silver", "DropSchema", "staging"), "bob"))
	not allowed(schema_input("nina_filter_schemas", "gold", "CreateSchema", "staging"))
}

test_rename_table if {
	etl := "2e97ae93-a55f-41a9-908e-622628f66e73"
	i := json.patch(as_user(on("bob_insert", "silver"), etl), [
		{"op": "replace", "path": "/action/operation", "value": "RenameTable"},
		{"op": "add", "path": "/action/targetResource", "value": {"table": {"catalogName": "lake", "schemaName": "sales", "tableName": "t2"}}},
	])
	allowed(i)
	not allowed(as_user(i, "bob"))
}

test_conditions if {
	extra := {"metadata": {"namespace": "authz", "name": "analysts-gold"}, "spec": {
		"subjects": {"groups": ["business_analyst"]},
		"resource": {"catalog": "authz-polaris", "warehouse": "gold"},
		"privileges": ["select"],
		"rowFilter": [
			{"column": "a", "op": "not-eq", "value": "it's"},
			{"column": "b", "op": "not-in", "values": [1, 2]},
			{"column": "c", "op": "starts-with", "value": "FR"},
			{"column": "d", "op": "not-starts-with", "value": "X"},
			{"column": "e", "op": "is-null"},
			{"column": "f\"g", "op": "not-null"},
			{"column": "h", "op": "lt-eq", "value": 3},
		],
	}}
	p := json.patch(policies, [{"op": "replace", "path": "/authz/analysts-gold", "value": extra}])
	f := trino.rowFilters with input as row_filter_input("gold", "nina")
		with data.kubernetes.dataaccesspolicies as p
		with data.okdp.idp.groups as mock_groups
	f == [{"expression": "((\"a\" IS NULL OR \"a\" <> 'it''s') AND (\"b\" IS NULL OR \"b\" NOT IN (1, 2)) AND starts_with(\"c\", 'FR') AND (\"d\" IS NULL OR NOT starts_with(\"d\", 'X')) AND \"e\" IS NULL AND \"f\"\"g\" IS NOT NULL AND \"h\" <= 3)"}]
}

test_fixed_values if {
	mask_of("mask-to-fixed-value", "varchar(3)") == "CAST('XXXXXXXX' AS varchar(3))"
	mask_of("mask-to-fixed-value", "boolean") == "CAST(false AS boolean)"
	mask_of("mask-to-fixed-value", "decimal(10,2)") == "CAST(0 AS decimal(10,2))"
	mask_of("mask-to-fixed-value", "timestamp(6)") == "CAST(TIMESTAMP '1970-01-01 00:00:00' AS timestamp(6))"
	mask_of("mask-to-fixed-value", "timestamp(6) with time zone") == "CAST(TIMESTAMP '1970-01-01 00:00:00 UTC' AS timestamp(6) with time zone)"
	mask_of("mask-to-fixed-value", "uuid") == "CAST('00000000-0000-0000-0000-000000000000' AS uuid)"
	mask_of("mask-to-fixed-value", "array(varchar)") == "CAST(ARRAY[] AS array(varchar))"
	mask_of("mask-to-fixed-value", "row(a integer)") == "CAST(fail('masking action mask-to-fixed-value is not supported on row(a integer)') AS row(a integer))"
	mask_of("show-first-4", "varchar") == "CAST(CASE WHEN length(\"c\") <= 4 THEN \"c\" ELSE concat(substr(\"c\", 1, 4), regexp_replace(regexp_replace(substr(\"c\", 5), '[^0-9(),.@-]', 'x'), '[0-9]', 'n')) END AS varchar)"
	mask_of("truncate-to-year", "date") == "CAST(date_trunc('year', \"c\") AS date)"
	mask_of("sha-256-global", "integer") == "from_big_endian_32(reverse(substr(sha256(reverse(to_big_endian_32(\"c\"))), 1, 4)))"
}

test_mask_needs_every_policy if {
	extra := {"metadata": {"namespace": "authz", "name": "analysts-gold-all"}, "spec": {
		"subjects": {"groups": ["business_analyst"]},
		"resource": {"catalog": "authz-polaris", "warehouse": "gold"},
		"privileges": ["select"],
	}}
	p := json.patch(policies, [{"op": "add", "path": "/authz/analysts-gold-all", "value": extra}])
	m := trino.columnMasks with input as mask_input("gold", [["email", "varchar"]])
		with data.kubernetes.dataaccesspolicies as p
		with data.okdp.idp.groups as mock_groups
	m == []
}

test_no_row_filter_on_information_schema if {
	i := json.patch(row_filter_input("gold", "nina"), [{"op": "replace", "path": "/action/resource/table/schemaName", "value": "information_schema"}])
	row_filters(i) == []
}
