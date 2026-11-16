# Groups of a user read from the Keycloak admin API, for the engines that do not get them in
# the token (Trino). Settings from the environment of OPA:
#   OKDP_IDP_URL            https://<host>/realms/<realm> (the issuer)
#   OKDP_IDP_CLIENT_ID      client with the view-users role of the realm
#   OKDP_IDP_CLIENT_SECRET
#   OKDP_IDP_CA_FILE        CA of the issuer (optional)
#   OKDP_IDP_CACHE_SECONDS  how long a membership is kept (default 60)
package okdp.idp

import rego.v1

env := opa.runtime().env

issuer := trim_right(env.OKDP_IDP_URL, "/")

# https://host/realms/r -> https://host/admin/realms/r
admin_url := replace(issuer, "/realms/", "/admin/realms/")

cache_seconds := to_number(object.get(env, "OKDP_IDP_CACHE_SECONDS", "60"))

tls := {"tls_ca_cert_file": env.OKDP_IDP_CA_FILE} if {
	env.OKDP_IDP_CA_FILE != ""
} else := {}

token := t if {
	response := http.send(object.union(tls, {
		"method": "POST",
		"url": concat("", [issuer, "/protocol/openid-connect/token"]),
		"headers": {"Content-Type": "application/x-www-form-urlencoded"},
		"raw_body": urlquery.encode_object({
			"grant_type": "client_credentials",
			"client_id": env.OKDP_IDP_CLIENT_ID,
			"client_secret": env.OKDP_IDP_CLIENT_SECRET,
		}),
		"force_cache": true,
		"force_cache_duration_seconds": 240,
		"raise_error": false,
	}))
	response.status_code == 200
	t := response.body.access_token
}

get(path) := body if {
	response := http.send(object.union(tls, {
		"method": "GET",
		"url": concat("", [admin_url, path]),
		"headers": {"Authorization": concat(" ", ["Bearer", token])},
		"force_cache": true,
		"force_cache_duration_seconds": cache_seconds,
		"raise_error": false,
	}))
	response.status_code == 200
	body := response.body
}

# Empty when the user is unknown or the identity provider does not answer.
groups(user) := {g.name |
	users := get(concat("", ["/users?exact=true&briefRepresentation=true&username=", urlquery.encode(user)]))
	count(users) == 1
	some g in get(concat("", ["/users/", users[0].id, "/groups?briefRepresentation=true"]))
}
