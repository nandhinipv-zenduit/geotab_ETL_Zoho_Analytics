"""Offline verification. Mocks MyAdmin v3 -- including a real-ish GraphQL
validator, so the "one bad field rejects the whole query" behaviour that broke
run #677 is reproduced. No network, no credentials.

    python test_main.py
"""
import logging, os, sys
os.environ.setdefault("GEOTAB_MIN_ROWS", "0")
os.environ.pop("GEOTAB_FIELD_SELECTION", None)
logging.disable(logging.INFO)

import pandas as pd
import main as m
_REAL_CALL_API, _REAL_POST = m.call_api, m._post

FAIL = []
def check(name, cond, extra=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {name}" + (f"   <- {extra}" if extra and not cond else ""))
    if not cond: FAIL.append(name)

# ---------------------------------------------------------------- fake v3 schema
SCALAR = "scalar"
SCHEMA = {
    "id": SCALAR, "productCode": SCALAR, "isTerminated": SCALAR,
    "isUnactivated": SCALAR, "activeTrackingDisabled": SCALAR,
    "firstDeviceActivationDate": SCALAR, "startDate": SCALAR, "endDate": SCALAR,
    "assignedPurchaseOrderNo": SCALAR,
    "contractStartDate": SCALAR, "contractEndDate": SCALAR,
    "device": {"id": SCALAR, "serialNumber": SCALAR},
    "activeDevicePlan": {"id": SCALAR, "name": SCALAR},
    "activeRatePlans": {"ratePlan": {"ratePlanName": SCALAR,
                                     "ratePlanType": {"name": SCALAR}}},
    "latestDeviceDatabase": {"databaseName": SCALAR},
    "account": {"id": SCALAR, "name": SCALAR},      # object -> bare `account` is invalid
    "warrantyStatus": {"name": SCALAR},             # object -> bare is invalid
}

class GqlError(Exception):
    pass

# What the fake schema calls the root field. Discovery has to find this name AND
# pass forAccount inside the query -- exactly the constraint run #678 revealed.
ROOT_NAME = "deviceContracts"


def validate_query(op):
    """Mirrors run #678: with optionalParam present the server reads filtering
    arguments from the QUERY, not the JSON-RPC params, so a bare field list
    fails with 'forAccount not provided!' no matter how well-formed it is."""
    if isinstance(op, dict):
        raise RuntimeError({"message": "Object of type "
            "'System.Collections.Generic.Dictionary`2[System.String,System.Object]'"})
    t = op.strip()
    if t.startswith("query"):
        t = t[len("query"):].strip()
    if not (t.startswith("{") and t.endswith("}")):
        raise GqlError("malformed query")
    entries = parse_selection(t[1:-1])
    if len(entries) == 1 and entries[0][0] == ROOT_NAME:
        _name, args, sub = entries[0]
        if not args or "forAccount" not in args:
            raise GqlError("forAccount not provided!")
        if sub is None:
            raise GqlError(f"Field '{ROOT_NAME}' of object type must have a subselection")
        return validate(sub, SCHEMA_FOR_RUN)
    raise GqlError("forAccount not provided!")

def parse_selection(body):
    """-> [(field_name, args_or_None, subselection_or_None)] for one level."""
    out, i, n = [], 0, len(body)
    while i < n:
        while i < n and body[i] in " \t\r\n": i += 1
        if i >= n: break
        j = i
        while j < n and (body[j].isalnum() or body[j] == "_"): j += 1
        if j == i: raise GqlError(f"unexpected character {body[i]!r}")
        name = body[i:j]
        k = j
        while k < n and body[k] in " \t\r\n": k += 1
        args = None
        if k < n and body[k] == "(":               # name(args)
            close = body.find(")", k)
            if close == -1: raise GqlError("unbalanced parens")
            args = body[k + 1:close]
            k = close + 1
            while k < n and body[k] in " \t\r\n": k += 1
            j = k
        if k < n and body[k] == "{":
            depth, e = 0, k
            while e < n:
                if body[e] == "{": depth += 1
                elif body[e] == "}":
                    depth -= 1
                    if depth == 0: break
                e += 1
            if depth: raise GqlError("unbalanced braces")
            out.append((name, args, body[k + 1:e]))
            i = e + 1
        else:
            out.append((name, args, None))
            i = j
    return out

def validate(body, schema):
    """Mirrors GraphQL's rules: objects REQUIRE a subselection, scalars forbid
    one, unknown fields are errors -- and any single violation rejects the whole
    query. That last part is exactly why guessing whole selections never
    converged."""
    names = []
    for name, _args, sub in parse_selection(body):
        if name not in schema:
            raise GqlError(f"Unknown field '{name}'")
        spec = schema[name]
        if spec is SCALAR and sub is not None:
            raise GqlError(f"Field '{name}' is a scalar and takes no subselection")
        if spec is not SCALAR and sub is None:
            raise GqlError(f"Field '{name}' of object type must have a subselection")
        if spec is not SCALAR:
            validate(sub, spec)
        names.append(name)
    return names

FULL_ROW = {
    "id": None, "account": {"id": 1, "name": "GoFleet"}, "productCode": "GP9LTEATT",
    "isTerminated": False, "isUnactivated": False, "activeTrackingDisabled": False,
    "firstDeviceActivationDate": "2024-02-01T00:00:00Z",
    "startDate": "2024-01-01T00:00:00Z", "endDate": "2050-01-01T00:00:00Z",
    "assignedPurchaseOrderNo": "PO-1", "warrantyStatus": {"name": "In warranty"},
    "contractStartDate": "2024-03-01T00:00:00Z",
    "contractEndDate": "2027-03-01T00:00:00Z",
    "device": {"id": 571024830, "serialNumber": "G9X4SS9TAFX"},
    "activeDevicePlan": {"id": 7, "name": "GO"},
    "activeRatePlans": [
        {"ratePlan": {"ratePlanName": "GO Plan", "ratePlanType": {"name": "Live"}}},
        {"ratePlan": {"ratePlanName": "ProPlus Mode Plan", "ratePlanType": {"name": "Live"}}}],
    "latestDeviceDatabase": {"databaseName": "foss_internal"},
}
BARE_KEYS = ("id", "account", "device", "startDate", "endDate", "productCode",
             "assignedPurchaseOrderNo", "warrantyStatus", "isTerminated",
             "isUnactivated", "activeTrackingDisabled", "firstDeviceActivationDate")

SCHEMA_FOR_RUN = SCHEMA
CAP = 20
TOTAL = 47

def row(i, keys):
    r = {}
    for k in keys:
        v = FULL_ROW[k]
        if k == "id": v = 1000 + i
        elif k == "device": v = {"id": 571024830 + i, "serialNumber": f"G9X4SS9TAFX{i}"}
        r[k] = v
    return r

def fake_api(working_pagination="top_page", schema=None, total=None):
    schema = SCHEMA if schema is None else schema
    total = TOTAL if total is None else total
    def _call(method, params, timeout=120):
        if method == "Authenticate":
            return {"userId": "u", "sessionId": "s", "accounts": [{"accountId": "GOFL02"}]}
        if method == "GetDeviceDatabaseNamesAsync":
            return [{"serialNo": s, "ownerDatabaseName": "imperialpark",
                     "sharedDatabaseName": ["shareA", "shareB"]}
                    for s in params["serialNumbers"]]
        if method not in ("GetDeviceContracts", "GetDeviceContractsByPage"):
            raise RuntimeError({"message": "Unknown method"})

        op = params.get("optionalParam")
        if op is None:
            keys = BARE_KEYS                       # v3 default: nested fields absent
        else:
            global SCHEMA_FOR_RUN
            SCHEMA_FOR_RUN = schema
            try:
                keys = validate_query(op)
            except GqlError as e:
                raise RuntimeError({"name": "JSONRPCError",
                    "message": "Unable to process Graphql query",
                    "errors": [{"message": "MyAdminException: Unable to process Graphql query"},
                               {"message": f"MyAdminException: {e}"}]})

        data = [row(i, keys) for i in range(1, total + 1)]
        if method == "GetDeviceContractsByPage":
            if working_pagination != "by_page": return data[:CAP]
            p = params.get("page", 1); return data[(p - 1) * CAP: p * CAP]
        if working_pagination == "nextId" and "nextId" in params:
            return [r for r in data if r["id"] > params["nextId"]][:CAP]
        if working_pagination == "top_page" and "page" in params:
            p = params["page"]; return data[(p - 1) * CAP: p * CAP]
        if working_pagination == "pagination_obj" and "pagination" in params:
            p = params["pagination"].get("page", 1); return data[(p - 1) * CAP: p * CAP]
        return data[:CAP]
    return _call

def reset():
    m._pagination = None
    m._field_param = None
    m._field_resolved = False
    m._missing_required = ()
    m.PIN_SELECTION = None

CREDS = {"userId": "u", "sessionId": "s"}

print("\n[1] pagination discovery -- each dialect in turn is the working one")
for strategy in m.PAGINATION_STRATEGIES:
    reset(); m.call_api = fake_api(strategy)
    recs = m.fetch_contracts("GOFL02", CREDS)
    check(f"'{strategy}' discovered, all {TOTAL} records pulled",
          len(recs) == TOTAL and m._pagination == strategy,
          f"got {len(recs)} via {m._pagination}")

print("\n[2] completeness guard -- no dialect works (the dangerous case)")
reset(); m.call_api = fake_api("none_of_them")
try:
    m.fetch_contracts("GOFL02", CREDS)
    check("aborts instead of returning a truncated 20-row pull", False, "no raise")
except RuntimeError as e:
    check("aborts instead of returning a truncated 20-row pull", "Cannot paginate" in str(e))

print("\n[3] small account -- fewer devices than the cap, no dialect advances")
for n, expect_ok in ((7, True), (19, True), (20, False), (47, False)):
    reset(); m.call_api = fake_api("none_of_them", total=n)
    try:
        recs = m.fetch_contracts("GOFL02", CREDS)
        check(f"{n} devices -> " + ("accepted as complete" if expect_ok else "should have ABORTED"),
              expect_ok and len(recs) == min(n, CAP), f"returned {len(recs)}")
    except RuntimeError:
        check(f"{n} devices -> " + ("aborted (false alarm)" if expect_ok else "aborted, as it must"),
              not expect_ok)

print("\n[4] the run #677 / #678 failures are reproduced by the mock")
reset(); m.call_api = fake_api()
B = {"apiKey": "u", "sessionId": "s", "forAccount": "GOFL02"}
try:
    m._try_selection(B, "{ id productCode }")
    check("bare field list -> 'forAccount not provided!'", False, "no raise")
except RuntimeError as e:
    check("bare field list -> 'forAccount not provided!'", "forAccount not provided" in str(e))
try:
    m._try_selection(B, '{ deviceContracts { id } }')
    check("root field without args -> 'forAccount not provided!'", False, "no raise")
except RuntimeError as e:
    check("root field without args -> 'forAccount not provided!'",
          "forAccount not provided" in str(e))
try:
    m._try_selection(B, '{ wrongRoot(forAccount: "GOFL02") { id } }')
    check("wrong root name rejected", False, "no raise")
except RuntimeError as e:
    check("wrong root name rejected", "Unable to process Graphql query" in str(e))
try:
    m._try_selection(B, {"fields": ["id"]})
    check("dict optionalParam rejected as a .NET Dictionary", False, "no raise")
except RuntimeError as e:
    check("dict optionalParam rejected as a .NET Dictionary", "Dictionary" in str(e))
recs = m._try_selection(B, '{ deviceContracts(forAccount: "GOFL02") { id account { id } } }')
check("root field WITH args is accepted", bool(recs) and "account" in recs[0])

print("\n[5] bottom-up field discovery converges despite those rejections")
reset(); m.call_api = fake_api()
sel = m.discover_field_selection({"apiKey": "u", "sessionId": "s", "forAccount": "GOFL02"})
check("a selection was built", bool(sel), repr(sel))
check("no required field missing", m._missing_required == (), str(m._missing_required))
for f in m.REQUIRED_NESTED:
    check(f"  '{f}' recovered", f in (sel or ""))
check("discovered selection uses the root field with args",
      "deviceContracts(forAccount" in (sel or ""), repr(sel))
check("object fields carry a subselection (account)", "account {" in (sel or ""),
      repr(sel))
check("scalar fields carry none (contractStartDate)",
      "contractStartDate {" not in (sel or ""))
recs = m._try_selection({"apiKey": "u", "sessionId": "s"}, sel)
check("the built selection actually replays cleanly", bool(recs) and "activeRatePlans" in recs[0])

print("\n[6] a schema genuinely missing a required field is reported, not hidden")
reset()
partial = {k: v for k, v in SCHEMA.items() if k != "latestDeviceDatabase"}
m.call_api = fake_api(schema=partial)
m.discover_field_selection({"apiKey": "u", "sessionId": "s", "forAccount": "GOFL02"})
check("latestDeviceDatabase reported missing",
      m._missing_required == ("latestDeviceDatabase",), str(m._missing_required))
check("other required fields still recovered", len(m._missing_required) == 1)

print("\n[7] pinned GEOTAB_FIELD_SELECTION short-circuits discovery")
reset(); m.call_api = fake_api()
m.PIN_SELECTION = sel
got = m.discover_field_selection({"apiKey": "u", "sessionId": "s", "forAccount": "GOFL02"})
check("pin used verbatim", got == sel)
check("pin validated as complete", m._missing_required == ())
reset(); m.call_api = fake_api(); m.PIN_SELECTION = "{ id bogusField }"
got = m.discover_field_selection({"apiKey": "u", "sessionId": "s", "forAccount": "GOFL02"})
check("a broken pin falls back to discovery", got and got != "{ id bogusField }", repr(got))

print("\n[8] end-to-end enrichment on discovered data")
reset(); m.call_api = fake_api()
df = pd.DataFrame(m.extract_records(m.fetch_contracts("GOFL02", CREDS), "GOFL02"))
df = m.reconcile_databases(df, CREDS)
df = m.add_billing_status(df); df = m.add_hardware_id(df); df = m.add_contract_dates(df)
check("row count preserved", len(df) == TOTAL, str(len(df)))
check("Active Billing Plan matches MyAdmin format",
      df["Active Billing Plan"].iloc[0] == "GO: Live, ProPlus Mode-Live",
      repr(df["Active Billing Plan"].iloc[0]))
check("Billing Status = Active", df["Billing Status"].iloc[0] == "Active")
check("Hardware ID from device.id", str(df["Hardware ID"].iloc[0]) == "571024831",
      str(df["Hardware ID"].iloc[0]))
check("Product code present", df["Product code"].iloc[0] == "GP9LTEATT")
check("Contract End Date parsed", str(df["Contract End Date"].iloc[0]).startswith("2027-03-01"),
      str(df["Contract End Date"].iloc[0]))
check("stale database corrected foss_internal -> imperialpark",
      df["latestDeviceDatabase_databaseName"].iloc[0] == "imperialpark",
      str(df["latestDeviceDatabase_databaseName"].iloc[0]))
check("shared-database LIST flattened to scalar",
      df["SharedDatabaseName"].iloc[0] == "shareA, shareB",
      repr(df["SharedDatabaseName"].iloc[0]))

print("\n[9] the pd.NA truth-test bug")
check("has_value(pd.NA) is False", m.has_value(pd.NA) is False)
check("has_value(None) is False", m.has_value(None) is False)
check("has_value('') is False", m.has_value("") is False)
check("has_value('db') is True", m.has_value("db") is True)
try:
    bool(pd.NA); check("baseline: bool(pd.NA) raises", False)
except TypeError:
    check("baseline: bool(pd.NA) raises (the bug was real)", True)

print("\n[10] Zoho import summary honesty")
try:
    m._summarize_import({"data": {"importSummary": {"successRowCount": 0}}}, 500)
    check("zero-row import raises", False, "no raise")
except RuntimeError as e:
    check("zero-row import raises instead of logging success", "accepted 0" in str(e))
try:
    m._summarize_import({"data": {}}, 500)
    check("missing importSummary raises", False, "no raise")
except RuntimeError:
    check("missing importSummary raises", True)
check("good import returns Zoho's count",
      m._summarize_import({"data": {"importSummary": {"successRowCount": 47}}}, 47) == 47)

print("\n[11] v3 -> v2 per-method fallback")
def flaky(url, method, params, timeout):
    if "/v3/" in url and method == "GetDeviceDatabaseNamesAsync":
        raise RuntimeError({"message": "Method is no longer available"})
    return [{"serialNo": "X", "ownerDatabaseName": "db", "url": url}]
m.call_api = _REAL_CALL_API
m._post = flaky; m._V2_FALLBACK_OK.clear()
r = m.call_api("GetDeviceDatabaseNamesAsync", {})
check("falls back to /v2/ for a method v3 dropped", "/v2/" in r[0]["url"])
check("fallback is remembered", "GetDeviceDatabaseNamesAsync" in m._V2_FALLBACK_OK)
m._post = _REAL_POST

print("\n" + "=" * 62)
print("ALL CHECKS PASSED" if not FAIL else f"{len(FAIL)} FAILED: " + ", ".join(FAIL))
sys.exit(1 if FAIL else 0)
