#!/usr/bin/env python3
"""Reset the Polaris bench fixture between runs (016): drop every table in
the bench namespace (purge) and the namespace itself, so each counted run
loads the full dataset fresh (state doc included — it rides the namespace's
`_rdlt_state` marker table). Stdlib-only; fixture tooling.

Usage: polaris_reset.py <polaris-api> <client-id> <client-secret>
         <warehouse> <namespace>
"""
import json
import sys
import urllib.error
import urllib.parse
import urllib.request

polaris, client_id, client_secret, warehouse, namespace = sys.argv[1:6]


def api(method, path, token=None, form=None, ok=(200, 201, 202, 204, 404)):
    data = None
    headers = {}
    if form is not None:
        data = urllib.parse.urlencode(form).encode()
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(polaris + path, method=method, data=data, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        if e.code not in ok:
            sys.exit(f"{method} {path}: HTTP {e.code}: {e.read()[:300]}")
        return e.code, b""


def main():
    _, raw = api(
        "POST",
        "/api/catalog/v1/oauth/tokens",
        form={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "PRINCIPAL_ROLE:ALL",
        },
    )
    token = json.loads(raw)["access_token"]
    status, raw = api(
        "GET", f"/api/catalog/v1/{warehouse}/namespaces/{namespace}/tables", token=token
    )
    if status == 200:
        for ident in json.loads(raw).get("identifiers", []):
            name = ident["name"]
            api(
                "DELETE",
                f"/api/catalog/v1/{warehouse}/namespaces/{namespace}/tables/{name}"
                "?purgeRequested=true",
                token=token,
            )
    api("DELETE", f"/api/catalog/v1/{warehouse}/namespaces/{namespace}", token=token)


if __name__ == "__main__":
    main()
