"""
PROBE 2 -- resolves the two things probe 1 exposed.

Probe 1 told us: v3 returns a plain array (good), BUT every call returned the
SAME 20 records, and the first record has only 12 keys -- activeDevicePlan,
activeRatePlans, latestDeviceDatabase, contractStartDate and contractEndDate are
ALL GONE. So v3 changed two things at once:

  A) page size is capped (20) and my pagination guesses were ignored
  B) nested/extra fields are no longer returned by default -- the docs say
     optionalParam takes a "Device Contract GraphQL Schema" that "determines
     what property will be returned"

This probe brute-forces both. Run it, paste the output.
Usage: python geotab_v3_probe2.py
"""
import json
import os
import requests

V3 = "https://myadminapi.geotab.com/v3/MyAdminApi.ashx"


def call(method, params, timeout=120):
    payload = {"id": -1, "method": method, "params": params}
    r = requests.post(V3, data={"JSON-RPC": json.dumps(payload)}, timeout=timeout)
    r.raise_for_status()
    body = r.json()
    if body.get("error"):
        raise RuntimeError(json.dumps(body["error"])[:400])
    return body.get("result")


def ids_of(res):
    if isinstance(res, dict):
        for k in ("data", "records", "results", "items", "deviceContracts"):
            if isinstance(res.get(k), list):
                res = res[k]
                break
    if not isinstance(res, list):
        return [], res
    return [r.get("id") for r in res if isinstance(r, dict)], res


creds = call("Authenticate", {"username": os.getenv("GEOTAB_USERNAME"),
                              "password": os.getenv("GEOTAB_PASSWORD")})
ACCT = os.getenv("PROBE_ACCOUNT", "GOFL02")
BASE = {"apiKey": creds["userId"], "sessionId": creds["sessionId"],
        "forAccount": ACCT, "userCompanyId": -1, "devicePlanId": -1,
        "fromDate": "2000-01-01T00:00:00Z", "toDate": "2050-01-01T00:00:00Z"}
print(f"Probing account {ACCT}\n")

# ============================================================ PART A: PAGINATION
print("=" * 66)
print("PART A -- can we get past the first 20 records, and how?")
print("=" * 66)

p1_ids, _ = ids_of(call("GetDeviceContracts", {**BASE, "nextId": 0}))
print(f"\npage 1 (nextId=0): {len(p1_ids)} records, ids {min(p1_ids)}..{max(p1_ids)}")

# The real nextId test probe 1 never ran: ADVANCE the cursor.
try:
    p2_ids, _ = ids_of(call("GetDeviceContracts", {**BASE, "nextId": max(p1_ids)}))
    overlap = set(p1_ids) & set(p2_ids)
    print(f"page 2 (nextId={max(p1_ids)}): {len(p2_ids)} records, "
          f"ids {min(p2_ids) if p2_ids else '-'}..{max(p2_ids) if p2_ids else '-'}")
    print(f"  -> overlap with page 1: {len(overlap)} "
          f"{'*** nextId WORKS ***' if not overlap and p2_ids else '<-- nextId IGNORED' if overlap else '(empty = end of data)'}")
except Exception as e:
    print(f"page 2 FAILED: {e}")

# Try to lift the 20-record cap: every plausible spelling, top-level and nested.
print("\nAttempts to raise the page size above 20:")
for label, extra in [
    ("perPage=100 (top level)",        {"perPage": 100}),
    ("pageSize=100",                   {"pageSize": 100}),
    ("resultsLimit=100",               {"resultsLimit": 100}),
    ("limit=100",                      {"limit": 100}),
    ("pagination.perPage=100",         {"pagination": {"perPage": 100}}),
    ("pagination.pageSize=100",        {"pagination": {"pageSize": 100}}),
    ("page=2 + perPage=20 (top level)", {"page": 2, "perPage": 20}),
    ("pagination.page=2",              {"pagination": {"page": 2, "perPage": 20}}),
]:
    try:
        got, _ = ids_of(call("GetDeviceContracts", {**BASE, **extra}))
        flag = ""
        if len(got) > 20:
            flag = "  *** CAP LIFTED ***"
        elif got and not (set(got) & set(p1_ids)):
            flag = "  *** DIFFERENT PAGE ***"
        print(f"  {label:34s} -> {len(got):4d} records{flag}")
    except Exception as e:
        print(f"  {label:34s} -> ERROR {str(e)[:90]}")

# The separately-documented paginated method.
print("\nGetDeviceContractsByPage (a distinct documented method):")
for label, extra in [("page=1,perPage=100", {"page": 1, "perPage": 100}),
                     ("pagination obj", {"pagination": {"page": 1, "perPage": 100}}),
                     ("bare", {})]:
    try:
        got, raw = ids_of(call("GetDeviceContractsByPage", {**BASE, **extra}))
        print(f"  {label:20s} -> {len(got)} records"
              f"{' (wrapper keys: ' + str(list(raw.keys())) + ')' if isinstance(raw, dict) else ''}")
    except Exception as e:
        print(f"  {label:20s} -> ERROR {str(e)[:110]}")

# ========================================================= PART B: MISSING FIELDS
print("\n" + "=" * 66)
print("PART B -- getting activeDevicePlan / activeRatePlans /")
print("          latestDeviceDatabase / contract dates back via optionalParam")
print("=" * 66)

WANT = ["activeDevicePlan", "activeRatePlans", "latestDeviceDatabase",
        "contractStartDate", "contractEndDate", "device"]

SELECTION = """
  id
  contractStartDate
  contractEndDate
  isTerminated
  isUnactivated
  productCode
  device { id serialNumber }
  activeDevicePlan { id name }
  activeRatePlans { ratePlan { ratePlanName ratePlanType { name } } }
  latestDeviceDatabase { databaseName }
"""
compact = " ".join(SELECTION.split())

for label, op in [
    ("braced selection",        "{ " + compact + " }"),
    ("unbraced selection",      compact),
    ("query-wrapped",           "query { deviceContracts { " + compact + " } }"),
    ("apiDeviceContract-wrapped", "{ apiDeviceContract { " + compact + " } }"),
    ("dict form",               {"fields": WANT}),
    ("csv field list",          ",".join(WANT)),
]:
    try:
        _, raw = ids_of(call("GetDeviceContracts", {**BASE, "nextId": 0, "optionalParam": op}))
        if isinstance(raw, list) and raw and isinstance(raw[0], dict):
            keys = list(raw[0].keys())
            present = [w for w in WANT if w in keys]
            print(f"\n  {label}: {len(keys)} keys on record 0")
            print(f"    wanted fields present: {present or 'NONE'}")
            if present:
                print(f"    *** THIS SYNTAX WORKS *** sample: "
                      f"{json.dumps({k: raw[0].get(k) for k in present}, default=str)[:400]}")
        else:
            print(f"\n  {label}: unexpected shape {type(raw).__name__}")
    except Exception as e:
        print(f"\n  {label}: ERROR {str(e)[:200]}")

print("\nDone. Paste everything above.")
