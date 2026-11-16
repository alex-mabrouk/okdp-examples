# Trino adapter. Trino queries allow (opa.policy.uri), rowFilters (opa.policy.row-filters-uri)
# and columnMasks (opa.policy.batch-column-masking-uri).
#
# Each Trino catalog served from a catalog instance is declared in the context file of Trino
# (opa.context-file), which Trino sends with every request:
#   okdp.catalog.<trino catalog>=<namespace>/<instance>/<warehouse>
# A catalog without this entry is denied.
package okdp.trino

import data.okdp.authz
import data.okdp.idp
import rego.v1

default allow := false

identity := input.context.identity

operation := input.action.operation

resource := object.get(input.action, "resource", {})

# Interactive users come with their name: their groups are read from the identity provider.
# Workloads sign in with a JWT whose principal is their client id.
groups := {g | some g in object.get(identity, "groups", [])} | idp.groups(identity.user)

clients := {identity.user}

alias(catalog) := {"owner": {"namespace": parts[0], "catalog": parts[1]}, "warehouse": parts[2]} if {
	value := input.context.properties[concat(".", ["okdp.catalog", catalog])]
	parts := split(value, "/")
	count(parts) == 3
}

target(catalog, schema, table) := {
	"owner": a.owner,
	"warehouse": a.warehouse,
	"namespace_path": schema,
	"table": table,
} if a := alias(catalog)

catalog_visible(catalog) if {
	a := alias(catalog)
	authz.warehouse_visible(a.owner, a, groups, clients)
}

schema_visible(catalog, "information_schema") if catalog_visible(catalog)

schema_visible(catalog, schema) if {
	t := target(catalog, schema, "*")
	authz.namespace_listed(t.owner, t, groups, clients)
}

granted(catalog, schema, table, privileges) if {
	t := target(catalog, schema, table)
	count(authz.on_table(t.owner, t, privileges, groups, clients)) > 0
}

granted_on_schema(catalog, schema, privileges) if {
	t := target(catalog, schema, "*")
	count(authz.on_namespace(t.owner, t, privileges, groups, clients)) > 0
}

any_privilege := {"select", "insert", "update", "delete", "create", "drop"}

table_privilege := {
	"SelectFromColumns": "select",
	"ShowColumns": "select",
	"FilterColumns": "select",
	"ShowCreateTable": "select",
	"CreateViewWithSelectFromColumns": "select",
	"InsertIntoTable": "insert",
	"UpdateTableColumns": "update",
	"DeleteFromTable": "delete",
	"TruncateTable": "delete",
	"CreateTable": "create",
	"CreateView": "create",
	"CreateMaterializedView": "create",
	"RefreshMaterializedView": "insert",
	"AddColumn": "create",
	"AlterColumn": "create",
	"DropColumn": "create",
	"RenameColumn": "create",
	"SetTableProperties": "create",
	"SetTableComment": "create",
	"SetViewComment": "create",
	"SetColumnComment": "create",
	"SetMaterializedViewProperties": "create",
	"DropTable": "drop",
	"DropView": "drop",
	"DropMaterializedView": "drop",
}

schema_privilege := {
	"CreateSchema": "create",
	"DropSchema": "drop",
}

# Any authenticated user may run a query; what it reads is checked below.
allow if {
	operation == "ExecuteQuery"
	identity.user != ""
}

# Own queries only.
allow if {
	operation in {"ViewQueryOwnedBy", "KillQueryOwnedBy", "FilterViewQueryOwnedBy"}
	resource.user.user == identity.user
}

allow if {
	operation in {"AccessCatalog", "FilterCatalogs", "ShowSchemas"}
	catalog_visible(resource.catalog.name)
}

allow if {
	operation in {"FilterSchemas", "ShowTables"}
	schema_visible(resource.schema.catalogName, resource.schema.schemaName)
}

allow if {
	operation == "FilterTables"
	resource.table.schemaName != "information_schema"
	granted(resource.table.catalogName, resource.table.schemaName, resource.table.tableName, any_privilege)
}

allow if {
	privilege := table_privilege[operation]
	resource.table.schemaName != "information_schema"
	granted(resource.table.catalogName, resource.table.schemaName, resource.table.tableName, {privilege})
}

# Metadata queries: Trino filters the rows of information_schema with FilterTables and
# FilterColumns.
allow if {
	operation in {"SelectFromColumns", "ShowColumns", "FilterColumns", "FilterTables"}
	resource.table.schemaName == "information_schema"
	catalog_visible(resource.table.catalogName)
}

allow if {
	privilege := schema_privilege[operation]
	granted_on_schema(resource.schema.catalogName, resource.schema.schemaName, {privilege})
}

# A rename drops the old name and creates the new one.
allow if {
	operation in {"RenameTable", "RenameView", "RenameMaterializedView"}
	granted(resource.table.catalogName, resource.table.schemaName, resource.table.tableName, {"drop"})
	target_table := input.action.targetResource.table
	granted_on_schema(target_table.catalogName, target_table.schemaName, {"create"})
}

# Policies that let the user read the table.
readers(table) := authz.on_table(t.owner, t, {"select"}, groups, clients) if {
	table.schemaName != "information_schema"
	t := target(table.catalogName, table.schemaName, table.tableName)
}

# One filter per table. A row is visible when one of the policies lets it through; a policy
# without a filter shows every row.
rowFilters := [{"expression": expression}] if {
	operation == "GetRowFilters"
	policies := readers(resource.table)
	count(policies) > 0
	every p in policies {
		count(object.get(p.spec, "rowFilter", [])) > 0
	}
	expression := concat(" OR ", [sprintf("(%s)", [conjunction(p.spec.rowFilter)]) | some p in sort_policies(policies)])
} else := []

conjunction(conditions) := concat(" AND ", [condition_sql(c) | some c in conditions])

# A column is masked when every policy that lets the user read the table masks it. When the
# policies disagree on the action, the first policy by namespace and name decides.
columnMasks := [{"index": i, "viewExpression": {"expression": expression}} |
	operation == "GetColumnMask"
	some i, item in input.action.filterResources
	column := item.column
	policies := sort_policies(readers(column))
	count(policies) > 0
	actions := [a | some p in policies; some c in object.get(p.spec, "columns", []); c.name == column.columnName; a := c.action]
	count(actions) == count(policies)
	expression := mask(actions[0], column.columnName, lower(column.columnType))
]

sort_policies(policies) := [by_key[k] | some k in sort(object.keys(by_key))] if {
	by_key := {concat("/", [p.metadata.namespace, p.metadata.name]): p | some p in policies}
}

quote_identifier(name) := concat("", ["\"", replace(name, "\"", "\"\""), "\""])

literal(value) := concat("", ["'", replace(value, "'", "''"), "'"]) if is_string(value)

literal(value) := format_int(value, 10) if is_number(value)

literal_list(values) := concat(", ", [literal(v) | some v in values])

comparison := {"eq": "=", "lt": "<", "lt-eq": "<=", "gt": ">", "gt-eq": ">="}

# Iceberg predicates never return NULL: not-eq, not-in and not-starts-with keep NULL values.
condition_sql(c) := sprintf("%s %s %s", [quote_identifier(c.column), comparison[c.op], literal(c.value)])

condition_sql(c) := sprintf("(%s IS NULL OR %s <> %s)", [col, col, literal(c.value)]) if {
	c.op == "not-eq"
	col := quote_identifier(c.column)
}

condition_sql(c) := sprintf("%s IN (%s)", [quote_identifier(c.column), literal_list(c.values)]) if c.op == "in"

condition_sql(c) := sprintf("(%s IS NULL OR %s NOT IN (%s))", [col, col, literal_list(c.values)]) if {
	c.op == "not-in"
	col := quote_identifier(c.column)
}

condition_sql(c) := sprintf("starts_with(%s, %s)", [quote_identifier(c.column), literal(c.value)]) if c.op == "starts-with"

condition_sql(c) := sprintf("(%s IS NULL OR NOT starts_with(%s, %s))", [col, col, literal(c.value)]) if {
	c.op == "not-starts-with"
	col := quote_identifier(c.column)
}

condition_sql(c) := sprintf("%s IS NULL", [quote_identifier(c.column)]) if c.op == "is-null"

condition_sql(c) := sprintf("%s IS NOT NULL", [quote_identifier(c.column)]) if c.op == "not-null"

# Masks of the Iceberg read restrictions, as Trino expressions of the column type. An action
# that does not apply to the type, or that Trino cannot compute, fails the query.
string_type(t) if startswith(t, "varchar")

string_type(t) if startswith(t, "char")

time_type(t) if t == "date"

time_type(t) if startswith(t, "timestamp")

alphanum(e) := sprintf("regexp_replace(regexp_replace(%s, '[^0-9(),.@-]', 'x'), '[0-9]', 'n')", [e])

cast(e, t) := sprintf("CAST(%s AS %s)", [e, t])

fail_query(action, t) := cast(sprintf("fail('masking action %s is not supported on %s')", [action, t]), t)

mask(action, name, t) := e if {
	e := mask_sql(action, name, t)
} else := fail_query(action, t)

mask_sql("mask-alphanum", name, t) := cast(alphanum(quote_identifier(name)), t) if string_type(t)

mask_sql("show-first-4", name, t) := cast(sprintf("CASE WHEN length(%s) <= 4 THEN %s ELSE concat(substr(%s, 1, 4), %s) END", [c, c, c, alphanum(sprintf("substr(%s, 5)", [c]))]), t) if {
	string_type(t)
	c := quote_identifier(name)
}

mask_sql("show-last-4", name, t) := cast(sprintf("CASE WHEN length(%s) <= 4 THEN %s ELSE concat(%s, substr(%s, length(%s) - 3)) END", [c, c, alphanum(sprintf("substr(%s, 1, length(%s) - 4)", [c, c])), c, c]), t) if {
	string_type(t)
	c := quote_identifier(name)
}

mask_sql("replace-with-null", _, t) := cast("NULL", t)

mask_sql("mask-to-fixed-value", _, t) := cast(v, t) if v := fixed_value(t)

mask_sql(action, name, t) := cast(sprintf("date_trunc('%s', %s)", [unit, column_utc(name, t)]), t) if {
	unit := {"truncate-to-year": "year", "truncate-to-month": "month"}[action]
	time_type(t)
}

mask_sql("sha-256-global", name, t) := cast(sprintf("lower(to_hex(sha256(to_utf8(%s))))", [quote_identifier(name)]), t) if string_type(t)

mask_sql("sha-256-global", name, "integer") := sprintf("from_big_endian_32(reverse(substr(sha256(reverse(to_big_endian_32(%s))), 1, 4)))", [quote_identifier(name)])

mask_sql("sha-256-global", name, "bigint") := sprintf("from_big_endian_64(reverse(substr(sha256(reverse(to_big_endian_64(%s))), 1, 8)))", [quote_identifier(name)])

mask_sql("sha-256-global", name, "varbinary") := sprintf("sha256(%s)", [quote_identifier(name)])

# timestamptz truncates in UTC.
column_utc(name, t) := sprintf("%s AT TIME ZONE 'UTC'", [quote_identifier(name)]) if contains(t, "with time zone")

column_utc(name, t) := quote_identifier(name) if not contains(t, "with time zone")

fixed_value(t) := "'XXXXXXXX'" if string_type(t)

fixed_value("boolean") := "false"

fixed_value(t) := "0" if t in {"tinyint", "smallint", "integer", "bigint", "real", "double"}

fixed_value(t) := "0" if startswith(t, "decimal")

fixed_value("date") := "DATE '1970-01-01'"

fixed_value(t) := "TIME '00:00:00'" if startswith(t, "time(")

fixed_value(t) := "TIMESTAMP '1970-01-01 00:00:00 UTC'" if {
	startswith(t, "timestamp")
	contains(t, "with time zone")
}

fixed_value(t) := "TIMESTAMP '1970-01-01 00:00:00'" if {
	startswith(t, "timestamp")
	not contains(t, "with time zone")
}

fixed_value("uuid") := "'00000000-0000-0000-0000-000000000000'"

fixed_value("varbinary") := "X''"

fixed_value(t) := "ARRAY[]" if startswith(t, "array")

fixed_value(t) := "MAP()" if startswith(t, "map")
