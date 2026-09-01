"""
Run this FIRST, once, before patching the ETL.

Purpose: MyAdmin v3 introduced pagination, so GetDeviceContracts may now return a
WRAPPER OBJECT (result -> {data/records + pagination metadata}) instead of the plain
array v2 returned. Which shape you get, and which pagination style the method
accepts, is the one thing that can't be read off the docs reliably -- so print it.

Usage:  python geotab_v3_probe.py
Needs:  GEOTAB_USERNAME, GEOTAB_PASSWORD in the environment (same as the ETL).
"""
import json
import os
import requests

V3_URL = "https://myadminapi.geotab.com/v3/MyAdminApi.ashx"


def call(url, method, params, timeout=120):
    payload = {"id": -1, "method": method, "params": params}
    r = requests.post(url, data={"JSON-RPC": json.dumps(payload)}, timeout=timeout)
    r.raise_for_status()
    body = r.json()
    if body.get("error"):
        raise RuntimeError(json.dumps(body["error"], indent=2))
    return body.get("result")


def describe(label, result):
    print(f"\n--- {label}")
    print(f"    python type : {type(result).__name__}")
    if isinstance(result, dict):
        print(f"    top-level keys: {list(result.keys())}")
        for k, v in result.items():
            if isinstance(v, list):
                print(f"      '{k}' is a list of {len(v)} item(s)"
                      f"{' -> first item keys: ' + str(list(v[0].keys())[:12]) if v and isinstance(v[0], dict) else ''}")
            elif isinstance(v, dict):
                print(f"      '{k}' is a dict with keys {list(v.keys())}")
            else:
                print(f"      '{k}' = {v!r}")
    elif isinstance(result, list):
        print(f"    plain array of {len(result)} record(s)  <-- same shape as v2")
        if result and isinstance(result[0], dict):
            print(f"    first record keys: {list(result[0].keys())[:15]}")
            print(f"    first record 'id' = {result[0].get('id') or result[0].get('Id')!r}")
    else:
        print(f"    value: {result!r}")


creds = call(V3_URL, "Authenticate", {
    "username": os.getenv("GEOTAB_USERNAME"),
    "password": os.getenv("GEOTAB_PASSWORD"),
})
print("Authenticate on /v3/ OK.")
accounts = [a.get("accountId") for a in creds.get("accounts", []) if a.get("accountId")]
print(f"accounts returned by Authenticate: {accounts}")

acct = accounts[0] if accounts else "GOFL01"
base = {
    "apiKey": creds["userId"],
    "sessionId": creds["sessionId"],
    "forAccount": acct,
    "userCompanyId": -1,
    "devicePlanId": -1,
    "fromDate": "2000-01-01T00:00:00Z",
    "toDate": "2050-01-01T00:00:00Z",
}

# Attempt 1: no pagination args at all -- does v3 accept the bare call?
for label, extra in [
    ("bare call (no pagination args)", {}),
    ("legacy nextId pagination",       {"nextId": 0}),
    ("v3 offset pagination",           {"pagination": {"page": 1, "perPage": 100}}),
    ("v3 keyset pagination",           {"pagination": {"perPage": 100}}),
]:
    try:
        describe(label, call(V3_URL, "GetDeviceContracts", {**base, **extra}))
    except Exception as e:
        print(f"\n--- {label}\n    FAILED: {e}")

print("\nWhichever attempt above succeeded tells you what to wire into the ETL.")
