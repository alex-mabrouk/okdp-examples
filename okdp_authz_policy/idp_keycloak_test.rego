package okdp.idp_test

import data.okdp.idp
import rego.v1

env := {
	"OKDP_IDP_URL": "https://keycloak.example/realms/master",
	"OKDP_IDP_CLIENT_ID": "opa",
	"OKDP_IDP_CLIENT_SECRET": "s",
}

keycloak(request) := {"status_code": 200, "body": {"access_token": "t"}} if {
	request.url == "https://keycloak.example/realms/master/protocol/openid-connect/token"
}

keycloak(request) := {"status_code": 200, "body": [{"id": "u-nina", "username": "nina"}]} if {
	request.url == "https://keycloak.example/admin/realms/master/users?exact=true&briefRepresentation=true&username=nina"
	request.headers.Authorization == "Bearer t"
}

keycloak(request) := {"status_code": 200, "body": []} if {
	request.url == "https://keycloak.example/admin/realms/master/users?exact=true&briefRepresentation=true&username=mallory"
}

keycloak(request) := {"status_code": 200, "body": [{"name": "business_analyst", "path": "/business_analyst"}]} if {
	request.url == "https://keycloak.example/admin/realms/master/users/u-nina/groups?briefRepresentation=true"
}

down(_) := {"status_code": 503, "body": null}

test_groups_of_nina if {
	g := idp.groups("nina") with idp.env as env with http.send as keycloak
	g == {"business_analyst"}
}

test_unknown_user if {
	g := idp.groups("mallory") with idp.env as env with http.send as keycloak
	g == set()
}

test_identity_provider_down if {
	g := idp.groups("nina") with idp.env as env with http.send as down
	g == set()
}

test_not_configured if {
	g := idp.groups("nina") with idp.env as {} with http.send as keycloak
	g == set()
}
