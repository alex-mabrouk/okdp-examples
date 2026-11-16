# Common model of the DataAccessPolicy (data.okdp.io/v1alpha1) shared by the engine adapters.
# kube-mgmt replicates the policies of every namespace under data.kubernetes.dataaccesspolicies.
package okdp.authz

import rego.v1

# A catalog instance is {"namespace", "catalog"}: the namespace that owns it and the instance
# name. A target adds "warehouse" and, when the operation is below them, "namespace_path"
# (Iceberg namespace, levels joined by dots) and "table".

write_privileges := {"insert", "update", "delete"}

policies(owner) := [p |
	some p in data.kubernetes.dataaccesspolicies[owner.namespace]
	name_matches(p.spec.resource.catalog, owner.catalog)
]

subject_matches(spec, groups, _) if {
	some g in object.get(spec.subjects, "groups", [])
	g in groups
}

subject_matches(spec, _, clients) if {
	some c in object.get(spec.subjects, "clients", [])
	c in clients
}

name_matches("*", _)

name_matches(pattern, value) if pattern == value

# For discovery: a policy on namespace a.b makes namespace a visible.
namespace_visible("*", _)

namespace_visible(pattern, path) if pattern == path

namespace_visible(pattern, path) if startswith(pattern, concat("", [path, "."]))

# Policies of the subject that cover the warehouse of the target.
on_warehouse(owner, target, groups, clients) := [p |
	some p in policies(owner)
	subject_matches(p.spec, groups, clients)
	name_matches(p.spec.resource.warehouse, target.warehouse)
]

# Policies that cover a table of the target with one of the privileges.
on_table(owner, target, privileges, groups, clients) := [p |
	some p in on_warehouse(owner, target, groups, clients)
	name_matches(object.get(p.spec.resource, "namespace", "*"), target.namespace_path)
	name_matches(object.get(p.spec.resource, "table", "*"), target.table)
	some privilege in p.spec.privileges
	privilege in privileges
]

# Policies that cover a whole namespace (table "*") with one of the privileges: creating or
# dropping in a namespace, where the table may not be known yet.
on_namespace(owner, target, privileges, groups, clients) := [p |
	some p in on_warehouse(owner, target, groups, clients)
	name_matches(object.get(p.spec.resource, "namespace", "*"), target.namespace_path)
	object.get(p.spec.resource, "table", "*") == "*"
	some privilege in p.spec.privileges
	privilege in privileges
]

# The subject sees the warehouse in a listing.
warehouse_visible(owner, target, groups, clients) if count(on_warehouse(owner, target, groups, clients)) > 0

# The subject sees the namespace in a listing.
namespace_listed(owner, target, groups, clients) if {
	some p in on_warehouse(owner, target, groups, clients)
	namespace_visible(object.get(p.spec.resource, "namespace", "*"), target.namespace_path)
}

# A read with a row filter or masked columns.
restricted(p) if count(object.get(p.spec, "rowFilter", [])) > 0

restricted(p) if count(object.get(p.spec, "columns", [])) > 0
