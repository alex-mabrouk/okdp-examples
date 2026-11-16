# Polaris adapter (polaris.authorization.opa.policy-uri = .../v1/data/okdp/polaris).
# Polaris sends the subject of the token as actor.principal and its groups and client id (azp)
# as actor.roles. Settings from the environment of OPA:
#   OKDP_CATALOG_NAMESPACE  namespace of the Polaris instance
#   OKDP_CATALOG_NAME       name of the Polaris instance
#   OKDP_ADMIN_GROUPS       JSON list: groups allowed to manage the instance (catalogs, roles)
#   OKDP_ENGINE_CLIENTS     JSON list: clients of engines that enforce the policies themselves
#                           (Trino), allowed every operation on the tables
package okdp.polaris

import data.okdp.authz
import rego.v1

default allow := false

env := opa.runtime().env

owner := {"namespace": env.OKDP_CATALOG_NAMESPACE, "catalog": env.OKDP_CATALOG_NAME}

admin_groups := json.unmarshal(object.get(env, "OKDP_ADMIN_GROUPS", "[]"))

engine_clients := json.unmarshal(object.get(env, "OKDP_ENGINE_CLIENTS", "[]"))

roles := {r | some r in object.get(input.actor, "roles", [])}

action := input.action

targets := array.concat(object.get(input.resource, "targets", []), object.get(input.resource, "secondaries", []))

# Operations on the tables, by privilege. Polaris tries write delegation before read
# delegation when it vends credentials.
read_actions := {
	"LIST_NAMESPACES", "LIST_NAMESPACE", "LOAD_NAMESPACE_METADATA", "NAMESPACE_EXISTS",
	"LIST_TABLES", "LIST_TABLE", "LIST_VIEWS", "LIST_VIEW", "GET_CATALOG_CONFIG",
	"LOAD_TABLE", "LOAD_TABLE_WITH_READ_DELEGATION", "TABLE_EXISTS", "LOAD_VIEW", "VIEW_EXISTS",
	"REPORT_READ_METRICS",
}

write_actions := {
	"LOAD_TABLE_WITH_WRITE_DELEGATION", "UPDATE_TABLE", "COMMIT_TRANSACTION", "ADD_TABLE_SNAPSHOT",
	"SET_TABLE_SNAPSHOT_REF", "REMOVE_TABLE_SNAPSHOT_REF", "REMOVE_TABLE_SNAPSHOTS",
	"SET_TABLE_STATISTICS", "REMOVE_TABLE_STATISTICS", "REPORT_WRITE_METRICS",
}

create_actions := {
	"CREATE_NAMESPACE", "UPDATE_NAMESPACE_PROPERTIES",
	"CREATE_TABLE_DIRECT", "CREATE_TABLE_DIRECT_WITH_WRITE_DELEGATION", "CREATE_TABLE_STAGED",
	"CREATE_TABLE_STAGED_WITH_WRITE_DELEGATION", "UPDATE_TABLE_FOR_STAGED_CREATE",
	"REGISTER_TABLE", "REGISTER_TABLE_WITH_READ_DELEGATION", "REGISTER_TABLE_WITH_WRITE_DELEGATION",
	"REGISTER_TABLE_OVERWRITE", "REGISTER_TABLE_OVERWRITE_WITH_READ_DELEGATION",
	"REGISTER_TABLE_OVERWRITE_WITH_WRITE_DELEGATION", "RENAME_TABLE", "RENAME_VIEW",
	"CREATE_VIEW", "REGISTER_VIEW", "REPLACE_VIEW",
	"ASSIGN_TABLE_UUID", "UPGRADE_TABLE_FORMAT_VERSION", "ADD_TABLE_SCHEMA", "SET_TABLE_CURRENT_SCHEMA",
	"ADD_TABLE_PARTITION_SPEC", "ADD_TABLE_SORT_ORDER", "SET_TABLE_DEFAULT_SORT_ORDER",
	"SET_TABLE_LOCATION", "SET_TABLE_PROPERTIES", "REMOVE_TABLE_PROPERTIES",
}

drop_actions := {"DROP_NAMESPACE", "DROP_TABLE_WITHOUT_PURGE", "DROP_TABLE_WITH_PURGE", "DROP_VIEW"}

table_actions := ((read_actions | write_actions) | create_actions) | drop_actions

privileges_of(a) := {"select"} if a in read_actions

privileges_of(a) := authz.write_privileges if a in write_actions

privileges_of(a) := {"create"} if a in create_actions

privileges_of(a) := {"drop"} if a in drop_actions

# Everything else (catalogs, principals, roles, grants, policies) is the management of the instance.
allow if {
	not action in table_actions
	some group in admin_groups
	group in roles
}

allow if {
	action in table_actions
	some client in engine_clients
	client in roles
}

allow if {
	action in table_actions
	count(targets) > 0
	every t in targets {
		target_allowed(target(t))
	}
}

# {"level", "warehouse", "namespace_path", "table"} of a Polaris entity.
target(entity) := object.union({"warehouse": warehouse(entity)}, entity_leaf(entity, namespaces)) if {
	namespaces := [p.name | some p in entity.parents; p.type == "NAMESPACE"]
}

warehouse(entity) := entity.name if entity.type == "CATALOG"

warehouse(entity) := [p.name | some p in entity.parents; p.type == "CATALOG"][0] if entity.type != "CATALOG"

entity_leaf(entity, namespaces) := {"level": "table", "namespace_path": concat(".", namespaces), "table": entity.name} if {
	entity.type in {"TABLE_LIKE", "ICEBERG_TABLE", "ICEBERG_VIEW"}
}

entity_leaf(entity, namespaces) := {"level": "namespace", "namespace_path": concat(".", array.concat(namespaces, [entity.name])), "table": "*"} if {
	entity.type == "NAMESPACE"
}

entity_leaf(entity, _) := {"level": "catalog", "namespace_path": "*", "table": "*"} if entity.type == "CATALOG"

# A read through Polaris returns the raw files: a policy with a row filter or masked columns
# grants nothing here, Polaris cannot apply it.
grants(t, privileges) := [p |
	some p in authz.on_table(owner, t, privileges, roles, roles)
	not restricted_read(p, privileges)
]

restricted_read(p, privileges) if {
	privileges == {"select"}
	authz.restricted(p)
}

# Listing and metadata on a catalog or a namespace: any policy below it.
target_allowed(t) if {
	t.level == "catalog"
	action in read_actions
	authz.warehouse_visible(owner, t, roles, roles)
}

target_allowed(t) if {
	t.level == "namespace"
	action in read_actions
	authz.namespace_listed(owner, t, roles, roles)
}

# Creating or dropping in a catalog or a namespace: a policy on all of it.
target_allowed(t) if {
	t.level in {"catalog", "namespace"}
	not action in read_actions
	count(authz.on_namespace(owner, t, privileges_of(action), roles, roles)) > 0
}

target_allowed(t) if {
	t.level == "table"
	count(grants(t, privileges_of(action))) > 0
}
