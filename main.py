"""
Geotab MyAdmin -> Zoho Analytics ETL.

MIGRATED TO MyAdmin v3. What changed and why:

  1. ENDPOINT. Geotab retired GetDeviceContracts on /v2/. The API version lives
     in the URL path, not the method name, so /v2/ -> /v3/. This was the outage:
     every account fetch errored, df came back empty, the empty-guard skipped the
     sync, and the job still exited 0.

  2. PAGINATION. v3 paginates and caps a page (observed: 20 records). Which
     parameter advances the page is not reliably documented, so instead of
     hardcoding a guess this script TRIES each dialect and keeps the one that
     demonstrably returns new records. If none of them advance past a full first
     page, the pull is INCOMPLETE and the script aborts rather than truncate-add
     a partial dataset over the live table.

  3. FIELD SELECTION. v3 stopped returning nested/extra properties by default --
     activeDevicePlan, activeRatePlans, latestDeviceDatabase, contractStartDate
     and contractEndDate are all absent from a bare response. The docs say
     optionalParam takes a "Device Contract GraphQL Schema" that determines what
     is returned, so the script tries several encodings of that selection and
     keeps whichever actually brings the fields back.

  4. FAIL LOUD. A per-account fetch error used to be logged at INFO and
     swallowed. Now any account failure, incomplete pull, or zero-row Zoho import
     raises -- so the GitHub Actions job goes red.

Both discoveries are logged with a "DISCOVERY:" prefix. Once the log tells you
which strategy won, you can pin it via env vars (GEOTAB_PAGINATION /
GEOTAB_FIELD_SELECTION) to skip the probing on every run.
"""

import json
import logging
import os
import re
import smtplib
import traceback
from datetime import datetime, timezone
from email.mime.text import MIMEText

import pandas as pd
import requests

# ===== LOGGING CONFIG =====
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    # GEOTAB_DEBUG=1 also logs every rejected field variant during discovery,
    # which is what you want when a required field can't be recovered.
    level=logging.DEBUG if os.getenv("GEOTAB_DEBUG") == "1" else logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(f"{LOG_DIR}/etl.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# ===== GEOTAB CONFIG =====
MYADMIN_V3 = "https://myadminapi.geotab.com/v3/MyAdminApi.ashx"
MYADMIN_V2 = "https://myadminapi.geotab.com/v2/MyAdminApi.ashx"
USERNAME = os.getenv("GEOTAB_USERNAME")
PASSWORD = os.getenv("GEOTAB_PASSWORD")

# Requested page size. v3's offset cap is documented as 100; the server may
# clamp this lower (20 observed) -- the code adapts to whatever it actually gets.
PAGE_SIZE = int(os.getenv("GEOTAB_PAGE_SIZE", "100"))
MAX_PAGES = int(os.getenv("GEOTAB_MAX_PAGES", "2000"))

# Safety floor. After one good run, set this to ~90% of the real device count in
# GitHub secrets/vars; it is the backstop against a silently short pull.
MIN_EXPECTED_ROWS = int(os.getenv("GEOTAB_MIN_ROWS", "0"))

# v3 does not return the plan/contract-date fields at all (confirmed against
# Geotab's published v3 response format), so refusing to sync without them would
# block forever. Default is now to sync and warn loudly. Set
# GEOTAB_REQUIRE_FIELDS=1 to go back to aborting instead.
REQUIRE_FIELDS = os.getenv("GEOTAB_REQUIRE_FIELDS", "0") == "1"

# Pin these once the DISCOVERY log lines tell you what works.
PIN_PAGINATION = os.getenv("GEOTAB_PAGINATION") or None

# ===== ZOHO ANALYTICS CONFIG (v2 OAuth) =====
ZOHO_ANALYTICS = {
    "client_id": os.environ.get("ZOHO_CLIENT_ID_UNI"),
    "client_secret": os.environ.get("ZOHO_CLIENT_SECRET_UNI"),
    "refresh_token": os.environ.get("ZOHO_REFRESH_TOKEN_UNI"),
    "accounts_url": "https://accounts.zoho.com",
    "api_domain": "https://analyticsapi.zoho.com/restapi/v2",
}
ZOHO_ORG_ID = "67409019"
ZOHO_WORKSPACE_ID = "953790000013364003"
ZOHO_VIEW_ID = "953790000054827102"   # "Geotab Devices" table

EXCEL_OUT = os.getenv("GEOTAB_EXCEL_OUT", "OP/geotab_op.xlsx")
CSV_OUT = os.getenv("GEOTAB_CSV_OUT", "gofleet_devices_full.csv")


# ===== GEOTAB API HELPER =====
_V2_FALLBACK_OK = set()      # methods proven to still work on /v2/
_V3_DEAD = set()             # methods proven retired on /v3/


def _post(url, method, params, timeout, pagination=None):
    """NOTE the placement of `pagination`: Geotab's migration doc puts it at the
    TOP LEVEL of the JSON-RPC envelope, as a SIBLING of "method" and "params" --
    NOT inside params. Every pagination attempt before this failed because it was
    nested inside params, where the server never looks:

        {"id": -1, "method": "GetDeviceContracts",
         "params": {...},
         "pagination": {"page": 1, "perPage": 50}}
    """
    payload = {"id": -1, "method": method, "params": params}
    if pagination is not None:
        payload["pagination"] = pagination
    resp = requests.post(url, data={"JSON-RPC": json.dumps(payload)}, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()
    if data.get("error"):
        raise RuntimeError(data["error"])
    return data


def call_api(method, params, timeout=120):
    """v3 first, with a per-method fallback to v2.

    Not every MyAdmin method is served on v3 yet (the docs warn that v3 methods
    which don't support pagination return errors), while GetDeviceContracts is
    now v3-only. So neither version alone is sufficient -- try v3, and if the
    method reports itself unavailable there, remember that and use v2 for it.
    A timeout is always passed: without one a stalled connection blocks forever,
    which is what used to hang the job on "Fetching devices for account ...".
    """
    return (call_api_full(method, params, timeout) or {}).get("result")


def call_api_full(method, params, timeout=120, pagination=None):
    """Returns the WHOLE response body, so the caller can read both "result" and
    the sibling "pagination" block ({page, perPage, total})."""
    if method in _V2_FALLBACK_OK:
        return _post(MYADMIN_V2, method, params, timeout, pagination)
    try:
        return _post(MYADMIN_V3, method, params, timeout, pagination)
    except RuntimeError as e:
        msg = json.dumps(e.args[0]) if e.args else str(e)
        # "That method is not yet available for that API version" means the method
        # EXISTS but is not served on v3 -- so v2 is exactly where to retry. The
        # first version of this matcher missed that phrasing, so GetDevicePlans,
        # GetDeviceContractsByPage and friends were reported dead when they were
        # only absent from v3.
        unavailable = ("no longer available" in msg or "not supported" in msg
                       or "does not exist" in msg or "Unknown method" in msg
                       or "not yet available for that API version" in msg
                       or "not available for that API version" in msg)
        if not unavailable:
            raise
        logger.warning(f"{method} unavailable on v3 ({msg[:160]}); retrying on v2")
        result = _post(MYADMIN_V2, method, params, timeout, pagination)
        _V2_FALLBACK_OK.add(method)
        return result


def authenticate():
    return call_api("Authenticate", {"username": USERNAME, "password": PASSWORD})


# ===== v3 FIELD SELECTION (optionalParam) DISCOVERY =====
# Run #677 settled the envelope question empirically:
#   - every STRING encoding failed with "Unable to process Graphql query", which
#     is a parse/validation failure -- so optionalParam IS accepted and IS parsed
#     as GraphQL; the selection content was what it rejected.
#   - the dict encoding failed with "Object of type
#     'System.Collections.Generic.Dictionary`2[System.String,System.Object]'",
#     so optionalParam must be a string, never an object.
#
# The likely culprit in the original selection: GraphQL requires a subselection
# on object-valued fields and forbids one on scalars, so a single bad field name
# (bare `account`, bare `warrantyStatus`) rejects the WHOLE query. Guessing entire
# selections can therefore never converge -- one wrong field hides all the right
# ones.
#
# So discovery is now bottom-up:
#   1. find the envelope that accepts the single field `id`
#   2. add candidate fields ONE AT A TIME (trying each field's plausible
#      subselection shapes), keeping a field only if the call succeeds AND the key
#      actually appears in the response
# The result is the maximal selection this schema will accept. It is logged as a
# ready-to-paste GEOTAB_FIELD_SELECTION=... so you can pin it and skip the ~25
# discovery calls on later runs.

# Fields with no alternative source. Without these, Active Billing Plan and the
# Contract dates load empty, so their absence blocks the sync.
REQUIRED_NESTED = ("activeDevicePlan", "activeRatePlans",
                   "contractStartDate", "contractEndDate")

# Requested too, but NOT blocking: latestDeviceDatabase is only Pass 1's guess at
# the database, and reconcile_databases() overwrites it wholesale from
# GetDeviceDatabaseNamesAsync -- which is the administrative source of truth and
# needs no optionalParam. Losing it costs nothing.
OPTIONAL_NESTED = ("latestDeviceDatabase",)

# Note on partial degradation: isTerminated and isUnactivated ARE in v3's bare
# response, so the TERMINATED and "Never billed" branches of Billing Status work
# even with no optionalParam at all. Only the "Active" plan text needs
# activeDevicePlan + activeRatePlans.

# Run #678 named the real constraint. The full error was:
#     MyAdminException: Unable to process Graphql query
#     MyAdminException: forAccount not provided!
# forAccount WAS in the JSON-RPC params. So once optionalParam is present the
# server stops reading the params for filtering and expects the arguments INSIDE
# the GraphQL query -- meaning the selection needs a ROOT FIELD WITH ARGUMENTS,
#     { deviceContracts(forAccount: "GOFL02", ...) { id ... } }
# not the bare field list we kept sending. Neither the root field's name nor the
# accepted argument set is documented anywhere public, so both are discovered:
# every root name x argument set x wrapper is tried until one accepts `id`.
GQL_ROOTS = (
    # Run #679: deviceContracts and getDeviceContracts both reached
    # "InvalidOperationException: Operation is not valid due to the current state
    # of the object" rather than "forAccount not provided!" -- so the argument
    # list WAS read and the failure moved past it. That strongly suggests the
    # arguments are right and only the root field name is wrong, so this list is
    # the axis to widen.
    "deviceContracts",
    "getDeviceContracts",
    "GetDeviceContracts",
    "deviceContract",
    "apiDeviceContract",
    "apiDeviceContracts",
    "apiDeviceContractList",
    "deviceContractList",
    "contracts",
    "devices",
    "data",
    "result",
    "items",
    "query",
)

# The InvalidOperationException may also mean the document needs to be a NAMED
# operation, so try that as a separate axis rather than assuming.
OPERATION_WRAPPERS = (
    ("plain", "{ %s }"),
    ("query", "query { %s }"),
    ("named", "query GetDeviceContracts { %s }"),
)

# Widest first: if the schema accepts all five it is the closest match to what
# the JSON-RPC params used to do.
ARG_KEY_SETS = (
    ("forAccount", "fromDate", "toDate", "userCompanyId", "devicePlanId"),
    ("forAccount", "fromDate", "toDate"),
    ("forAccount",),
)

# Bare-selection forms, kept as a fallback in case some root name needs no args.
BARE_ENVELOPES = (("braced", "{ %s }"), ("bare", "%s"), ("query", "query { %s }"))


def _gql_args(base, keys):
    """GraphQL argument list built from the params we would otherwise send.
    json.dumps gives correct GraphQL literals for both cases here: strings come
    out quoted, integers bare."""
    return ", ".join(f"{k}: {json.dumps(base[k])}" for k in keys if k in base)


def _envelope_candidates(base):
    """(label, template) pairs; each template has exactly one %s for the fields."""
    for root in GQL_ROOTS:
        for keys in ARG_KEY_SETS:
            args = _gql_args(base, keys)
            if not args:
                continue
            body = f"{root}({args}) {{ %s }}"
            for wrap_label, wrap in OPERATION_WRAPPERS:
                yield f"{root}({'+'.join(keys)}) [{wrap_label}]", wrap % body
    for label, fmt in BARE_ENVELOPES:
        yield f"bare:{label}", fmt


# (response key, variants to try in order). Scalars have one variant; object
# fields list their plausible subselections, widest first. Order matters only in
# that a field is dropped if none of its variants are accepted.
CANDIDATE_FIELDS = (
    ("contractStartDate", ("contractStartDate",)),
    ("contractEndDate", ("contractEndDate",)),
    ("productCode", ("productCode",)),
    ("isTerminated", ("isTerminated",)),
    ("isUnactivated", ("isUnactivated",)),
    ("activeTrackingDisabled", ("activeTrackingDisabled",)),
    ("firstDeviceActivationDate", ("firstDeviceActivationDate",)),
    ("startDate", ("startDate",)),
    ("endDate", ("endDate",)),
    ("assignedPurchaseOrderNo", ("assignedPurchaseOrderNo",)),
    ("device", ("device { id serialNumber }", "device { id }",
                "device { serialNumber }", "device")),
    ("activeDevicePlan", ("activeDevicePlan { id name }", "activeDevicePlan { name }",
                          "activeDevicePlan")),
    ("activeRatePlans", (
        "activeRatePlans { ratePlan { ratePlanName ratePlanType { name } } }",
        "activeRatePlans { ratePlan { ratePlanName } }",
        "activeRatePlans { ratePlanName ratePlanType { name } }",
        "activeRatePlans { ratePlanName }",
        "activeRatePlans")),
    ("latestDeviceDatabase", ("latestDeviceDatabase { databaseName }",
                              "latestDeviceDatabase { name }",
                              "latestDeviceDatabase")),
    ("account", ("account { id name }", "account { accountId }",
                 "account { id }", "account")),
    ("warrantyStatus", ("warrantyStatus { name }", "warrantyStatus")),
)

PIN_SELECTION = os.getenv("GEOTAB_FIELD_SELECTION") or None

_field_param = None        # resolved optionalParam string, or None
_field_resolved = False
_missing_required = ()     # required fields discovery could not recover


def _as_records(result):
    """v3 returned a plain array when probed, but tolerate a wrapper too."""
    if isinstance(result, list):
        return result
    if isinstance(result, dict):
        for k in ("data", "records", "results", "items", "deviceContracts"):
            if isinstance(result.get(k), list):
                return result[k]
    return []


def _try_selection_raw(base, optional_param):
    """One GetDeviceContracts call with a candidate optionalParam. Returns the
    RAW result so a shape that succeeded with an unfamiliar payload shape can be
    reported instead of discarded."""
    params = dict(base)
    params["optionalParam"] = optional_param
    return call_api("GetDeviceContracts", params)


def _try_selection(base, optional_param):
    return _as_records(_try_selection_raw(base, optional_param))


REF_ID_RE = re.compile(r"\s*\(Ref ID: [0-9a-fA-F-]+\)")


def _err_text(e):
    """Flatten a MyAdmin JSON-RPC error into its innermost messages, which is
    where the useful text lives ('forAccount not provided!' was nested two levels
    down inside 'errors').

    The Ref ID is stripped. It is unique per call, so leaving it in made every
    error text distinct and the dedup in the failure report collapsed nothing --
    which is why run #679 showed only the first 10 of 39 shapes and none of the
    later root names. Diagnostics that silently drop 3/4 of the evidence are
    worse than none."""
    payload = e.args[0] if e.args else str(e)
    if isinstance(payload, dict):
        parts = [str(payload.get("message", "")).strip()]
        for sub in payload.get("errors") or []:
            if isinstance(sub, dict):
                msg = str(sub.get("message", "")).strip()
                if msg and msg not in parts:
                    parts.append(msg)
        return REF_ID_RE.sub("", " | ".join(p for p in parts if p))[:600]
    return REF_ID_RE.sub("", str(payload))[:600]


# GraphQL servers usually answer introspection, and one successful introspection
# call would replace all of this guessing with the actual schema. Only run on
# failure, to keep the happy path cheap.
INTROSPECTION_PROBES = (
    ("root query fields", "{ __schema { queryType { name fields { name } } } }"),
    ("all type names", "{ __schema { types { name } } }"),
    ("ApiDeviceContract", '{ __type(name: "ApiDeviceContract") { name kind '
                          'fields { name type { name kind } } } }'),
    ("DeviceContract", '{ __type(name: "DeviceContract") { name kind '
                       'fields { name type { name kind } } } }'),
)


ALTERNATE_METHODS = (
    "GetDeviceContractsByPage",
    "GetPartnerDeviceContractsAsync",
    "GetDevicePlans",
    "GetRatePlans",
    "GetDeviceContractRatePlans",
    "GetDeviceContractTransactions",
)


def _probe_alternate_methods(base):
    """Maybe a sibling method still returns the nested fields WITHOUT optionalParam.
    If one does, we can source activeDevicePlan / activeRatePlans / the contract
    dates from there and skip the GraphQL problem entirely."""
    logger.error("--- checking whether a sibling method returns the nested fields "
                 "without optionalParam ---")
    wanted = set(REQUIRED_NESTED) | set(OPTIONAL_NESTED)
    for method in ALTERNATE_METHODS:
        try:
            recs = _as_records(call_api(method, dict(base)))
        except Exception as e:
            logger.error(f"  {method}: {_err_text(e)}")
            continue
        if not recs or not isinstance(recs[0], dict):
            logger.error(f"  {method}: returned {len(recs)} record(s), no dict payload")
            continue
        keys = sorted(recs[0])
        have = sorted(wanted & set(keys))
        logger.error(f"  {method}: {len(recs)} record(s); keys = {keys}")
        logger.error(f"      wanted fields present: {have or 'NONE'}")


def _run_introspection(base):
    logger.error("--- attempting GraphQL introspection (this would give us the "
                 "real schema; a failure here is informative too) ---")
    for label, query in INTROSPECTION_PROBES:
        try:
            raw = _try_selection_raw(base, query)
            logger.error(f"  introspection '{label}' SUCCEEDED -> "
                         f"{json.dumps(raw, default=str)[:1500]}")
        except Exception as e:
            logger.error(f"  introspection '{label}': {_err_text(e)}")


DISCOVER_FIELDS = os.getenv("GEOTAB_DISCOVER_FIELDS") == "1"


def discover_field_selection(base):
    """Build the maximal optionalParam selection this schema accepts.

    Now OPT-IN (GEOTAB_DISCOVER_FIELDS=1). Geotab's migration doc publishes the
    full v3 response format, and activeDevicePlan, activeRatePlans,
    latestDeviceDatabase, contractStartDate and contractEndDate are simply not in
    it -- their absence is the documented behaviour of v3, not a discovery
    failure. Probing 129 shapes on every run cost ~30s and found nothing, so it
    stays off until Geotab documents a working optionalParam value.
    """
    global _field_param, _field_resolved, _missing_required
    if _field_resolved:
        return _field_param
    _field_resolved = True

    if not DISCOVER_FIELDS and not PIN_SELECTION:
        _missing_required = REQUIRED_NESTED
        logger.warning(
            "optionalParam discovery skipped (set GEOTAB_DISCOVER_FIELDS=1 to retry). "
            "v3 does not return %s, so these columns load EMPTY: Active Billing Plan "
            "(for active devices), Contract Start Date, Contract End Date.",
            list(REQUIRED_NESTED))
        return None

    if PIN_SELECTION:
        try:
            recs = _try_selection(base, PIN_SELECTION)
            keys = set(recs[0]) if recs else set()
            _missing_required = tuple(f for f in REQUIRED_NESTED if f not in keys)
            if _missing_required:
                logger.warning(f"Pinned GEOTAB_FIELD_SELECTION is missing "
                               f"{_missing_required}; unset it to re-discover.")
            else:
                logger.info("Using pinned GEOTAB_FIELD_SELECTION.")
            _field_param = PIN_SELECTION
            return _field_param
        except Exception as e:
            logger.error(f"Pinned GEOTAB_FIELD_SELECTION rejected: {str(e)[:400]}. "
                         f"Falling back to discovery.")

    # --- step 1: which root field + argument set accepts a single scalar field?
    envelope = None
    tried = 0
    errors = {}          # distinct error text -> [shape labels]
    unrecognized = []    # shapes that SUCCEEDED but returned an unfamiliar payload

    for name, fmt in _envelope_candidates(base):
        tried += 1
        try:
            raw = _try_selection_raw(base, fmt % "id")
        except Exception as e:
            errors.setdefault(_err_text(e), []).append(name)
            continue
        recs = _as_records(raw)
        if recs and isinstance(recs[0], dict) and "id" in recs[0]:
            logger.info(f"DISCOVERY: optionalParam envelope '{name}' accepted")
            logger.info(f"           {fmt % 'id'}")
            envelope = fmt
            break
        # Accepted, but we did not find records where we expected them. Never
        # discard this quietly -- it may be the winning shape with a payload
        # wrapper we have not seen.
        unrecognized.append((name, json.dumps(raw, default=str)[:400]))
        logger.warning(f"  envelope '{name}' was ACCEPTED but the payload had no "
                       f"'id' record: {json.dumps(raw, default=str)[:400]}")

    if envelope is None:
        # Report everything, unconditionally. Requiring a debug flag to see why
        # discovery failed just costs another round trip.
        logger.error(f"DISCOVERY FAILED: none of {tried} optionalParam shapes returned "
                     f"an 'id'. {len(errors)} distinct error(s), most common first:")
        for msg, labels in sorted(errors.items(), key=lambda kv: -len(kv[1])):
            logger.error(f"  [{len(labels)} of {tried} shape(s)] {msg}")
            logger.error(f"      shapes: {', '.join(labels[:6])}"
                         f"{' ...' if len(labels) > 6 else ''}")
        for name, snippet in unrecognized[:5]:
            logger.error(f"  ACCEPTED but unrecognized payload -- {name}: {snippet}")
        _run_introspection(base)
        _probe_alternate_methods(base)
        _field_param = None
        _missing_required = REQUIRED_NESTED
        return None

    # --- step 2: add fields one at a time; keep only what survives
    accepted, rejected = ["id"], []
    for key, variants in CANDIDATE_FIELDS:
        for variant in variants:
            candidate = envelope % " ".join(accepted + [variant])
            try:
                recs = _try_selection(base, candidate)
            except Exception as e:
                logger.debug(f"    {key} as '{variant}': {str(e)[:200]}")
                continue
            if recs and isinstance(recs[0], dict) and key in recs[0]:
                accepted.append(variant)
                break
        else:
            rejected.append(key)

    _field_param = envelope % " ".join(accepted)
    _missing_required = tuple(f for f in REQUIRED_NESTED if f in rejected)
    missing_optional = tuple(f for f in OPTIONAL_NESTED if f in rejected)
    if missing_optional:
        logger.info(f"Non-blocking fields unavailable: {missing_optional} "
                    f"(reconcile_databases sources the database independently)")

    logger.info(f"DISCOVERY: accepted {len(accepted)} field(s); "
                f"dropped {rejected or 'none'}")
    logger.info(f"DISCOVERY: pin this to skip discovery next run ->\n"
                f"           GEOTAB_FIELD_SELECTION={_field_param}")
    if _missing_required:
        logger.error(f"Required field(s) {_missing_required} could not be recovered "
                     f"from the v3 schema. The dependent columns will be EMPTY.")
    return _field_param


# ===== v3 PAGINATION (documented, no longer guessed) =====
# From Geotab's "Partner Announcement - New GetDeviceContracts V3 Endpoint":
#   - pagination is a top-level object in the envelope: {"page": N, "perPage": M}
#   - page starts at 1; perPage max is 100 and is silently capped at 100
#   - omitting it defaults to page 1, perPage 20  <- this is why every earlier
#     call returned exactly 20 records
#   - the response carries a sibling "pagination": {page, perPage, total}
# That `total` is the important part: completeness is now an exact check against
# a number the server gives us, replacing the round-page-size heuristic that had
# to guess whether 20 records meant "small account" or "truncated".
PER_PAGE = min(int(os.getenv("GEOTAB_PER_PAGE", "100")), 100)


def fetch_contracts(account_id, creds):
    """Pull every device contract for an account. Raises unless the row count
    matches the server-reported total."""
    params = {
        "apiKey": creds["userId"],
        "sessionId": creds["sessionId"],
        "forAccount": account_id,
        "userCompanyId": -1,
        "devicePlanId": -1,
        # includeConnectInfo stays OFF: it attaches connection/GPS data for every
        # device, making a large account huge and slow, and has no effect on the
        # billing fields.
        "fromDate": datetime.now(timezone.utc).strftime("%Y-%m-%dT00:00:00Z"),
        "toDate": datetime.now(timezone.utc).strftime("%Y-%m-%dT23:59:59Z"),
    }
    field_param = discover_field_selection(params)
    if field_param is not None:
        params["optionalParam"] = field_param

    out, page, total = [], 1, None
    while page <= MAX_PAGES:
        body = call_api_full("GetDeviceContracts", params,
                             pagination={"page": page, "perPage": PER_PAGE}) or {}
        recs = _as_records(body.get("result"))
        pag = body.get("pagination") or {}
        if total is None:
            total = pag.get("total")
            logger.info(f"  {account_id}: server reports total={total}, "
                        f"perPage={pag.get('perPage', PER_PAGE)}")
        out.extend(r for r in recs if isinstance(r, dict))

        if not recs:
            break
        if total is not None and len(out) >= total:
            break
        if total is None and len(recs) < PER_PAGE:
            # No pagination metadata came back; fall back to "a short page is the
            # last page" and say so, rather than pretending we verified anything.
            logger.warning(f"  {account_id}: response carried no pagination.total; "
                           f"completeness cannot be verified exactly.")
            break
        page += 1
    else:
        raise RuntimeError(f"{account_id}: hit MAX_PAGES={MAX_PAGES} at "
                           f"{len(out)} record(s) of total={total}")

    if total is not None and len(out) != total:
        raise RuntimeError(
            f"{account_id}: fetched {len(out)} contract(s) but the server reported "
            f"total={total}. Refusing to continue -- syncing an incomplete pull "
            f"would truncate the Zoho table.")

    logger.info(f"  {account_id}: {len(out)} contract(s) over {page} page(s) "
                f"of {PER_PAGE}")
    return out


def get_device_databases_by_serials(serials, creds):
    """PASS 2: OWNER (primary) + SHARED database names for specific serials.

    Not from GetDeviceContracts: its latestDeviceDatabase is the last database
    the device was detected communicating in, so a device that never connected
    has none, and a device administratively reassigned but silent since keeps
    reporting the OLD one. GetDeviceDatabaseNamesAsync returns the administrative
    assignment regardless of communication history.
    """
    if not serials:
        return []
    return call_api("GetDeviceDatabaseNamesAsync", {
        "apiKey": creds["userId"],
        "sessionId": creds["sessionId"],
        "serialNumbers": list(serials),
    })


def flatten_dict(d, parent_key="", sep="_"):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            items.append((new_key, json.dumps(v)))
        else:
            items.append((new_key, v))
    return dict(items)


def extract_records(raw_list, account_id):
    records = []
    for dc in raw_list or []:
        if not isinstance(dc, dict):
            continue
        dc["input_account_id"] = account_id
        records.append(flatten_dict(dc))
    return records


# ===== DATABASE RECONCILIATION HELPERS =====
DB_COL_PREFIX = "LatestDeviceDatabase"


def _get_serial_column(df):
    lower_map = {c.lower(): c for c in df.columns}
    return next((lower_map[k] for k in
                 ("device_serialnumber", "serialnumber", "device_serialno", "serialno")
                 if k in lower_map), None)


def _get_db_columns(df):
    return [c for c in df.columns if c.lower().startswith(DB_COL_PREFIX.lower())]


def has_value(v):
    """Truth test that is safe on pd.NA.

    `if owner:` used to be written directly here, and _stringify_db_field can
    return pd.NA -- which raises "boolean value of NA is ambiguous" rather than
    evaluating false. That was a latent crash waiting on the first device with an
    empty owner-database list.
    """
    if v is None:
        return False
    try:
        if pd.isna(v):
            return False
    except (TypeError, ValueError):
        pass
    return str(v).strip() != ""


def _stringify_db_field(val):
    """Flatten a database-name field into a single scalar string.

    sharedDatabaseName can be a LIST (MyAdmin shows "Shared database(s)" plural).
    Assigning a list to df.loc[mask, col] makes pandas align it element-wise
    against the mask instead of broadcasting, raising "Must have equal len keys
    and value when setting with an iterable". Joining to a string guarantees a
    scalar broadcast.
    """
    if val is None:
        return val
    if isinstance(val, list):
        names = []
        for item in val:
            if isinstance(item, dict):
                names.append(item.get("databaseName") or item.get("name") or str(item))
            elif item is not None:
                names.append(str(item))
        return ", ".join(n for n in names if n) or pd.NA
    return val


def find_serials_missing_database(df):
    """Serials whose latest-device-database columns are all blank. Logging only."""
    db_cols = _get_db_columns(df)
    serial_col = _get_serial_column(df)
    if not serial_col:
        logger.warning("No serial-number column found; cannot backfill databases.")
        return [], None, db_cols
    if not db_cols:
        missing_mask = pd.Series(True, index=df.index)
    else:
        missing_mask = df[db_cols].apply(
            lambda col: col.isna() | (col.astype(str).str.strip() == ""), axis=0
        ).all(axis=1)
    return df.loc[missing_mask, serial_col].dropna().unique().tolist(), serial_col, db_cols


def reconcile_databases(df, creds):
    """Reconcile EVERY device's database against GetDeviceDatabaseNamesAsync.

    Every device, not just the blank ones: latestDeviceDatabase can be stale
    rather than blank. Device GAH24274SU82 is the worked example -- MyAdmin shows
    primary database "imperialpark" (set 2026-07-15) but its last communication
    predates that, so the contract still reports "foss_internal". A
    backfill-only-if-blank check lets that wrong value through.
    """
    serial_col = _get_serial_column(df)
    db_cols = _get_db_columns(df)
    if not serial_col:
        logger.warning("No serial-number column found; cannot reconcile databases.")
        return df

    missing_serials, _, _ = find_serials_missing_database(df)
    if missing_serials:
        logger.info(f"{len(missing_serials)} device(s) have no communication-based "
                    f"database (never connected)")

    serials = df[serial_col].dropna().unique().tolist()
    if not serials:
        logger.info("No serials found - nothing to reconcile.")
        return df

    logger.info(f"Reconciling database assignment for all {len(serials)} device(s) "
                f"against GetDeviceDatabaseNamesAsync...")

    BATCH = 100
    fetched, failed_batches = [], 0
    for i in range(0, len(serials), BATCH):
        batch = serials[i:i + BATCH]
        try:
            raw = get_device_databases_by_serials(batch, creds)
        except Exception as e:
            logger.error(f"Database lookup failed for batch {i // BATCH + 1}: {e}")
            failed_batches += 1
            raw = []
        fetched.extend(r for r in (raw or []) if isinstance(r, dict))

    if failed_batches:
        logger.error(f"{failed_batches} database-lookup batch(es) failed; "
                     f"those devices keep their Pass-1 value.")
    if not fetched:
        logger.warning("Database lookup returned nothing; leaving databases as-is.")
        return df

    for c in ("OwnerDatabaseName", "SharedDatabaseName"):
        if c not in df.columns:
            df[c] = pd.NA

    primary_col = next((c for c in db_cols if c.lower().endswith("databasename")), None)
    if primary_col is None:
        primary_col = f"{DB_COL_PREFIX}_databaseName"
        if primary_col not in df.columns:
            df[primary_col] = pd.NA

    filled = corrected_stale = 0
    for rec in fetched:
        serial = rec.get("serialNo") or rec.get("SerialNo")
        if not serial:
            continue
        owner = _stringify_db_field(rec.get("ownerDatabaseName") or rec.get("OwnerDatabaseName"))
        shared = _stringify_db_field(rec.get("sharedDatabaseName") or rec.get("SharedDatabaseName"))

        mask = df[serial_col] == serial
        if not mask.any():
            continue
        df.loc[mask, "OwnerDatabaseName"] = owner
        df.loc[mask, "SharedDatabaseName"] = shared

        if has_value(owner):
            current = df.loc[mask, primary_col]
            blank_mask = mask & (
                df[primary_col].isna() | (df[primary_col].astype(str).str.strip() == ""))
            stale_mask = mask & ~blank_mask & (df[primary_col].astype(str) != str(owner))
            df.loc[blank_mask | stale_mask, primary_col] = owner
            filled += int(blank_mask.sum())
            corrected_stale += int(stale_mask.sum())
            if stale_mask.any():
                logger.info(f"  Serial {serial}: corrected stale database "
                            f"'{current.iloc[0] if len(current) else ''}' -> '{owner}'")

    logger.info(f"Database reconciliation: filled {filled} blank row(s), corrected "
                f"{corrected_stale} stale row(s), from {len(fetched)} lookup record(s).")
    return df


# ===== BILLING STATUS / ACTIVE PLAN =====
#   MyAdmin "Billing status"     MyAdmin "Active billing plan"
#   TERMINATED                   TERMINATED
#   Never billed                 NEVER ACTIVATED
#   Active                       "{activeDevicePlan.name}: {rate plan type}"
#                                 e.g. "GO: Live" / "GO: Live, ProPlus Mode-Live"
# Validated against multi-plan real rows. The Suspended branch is still an
# unverified keyword guess -- check it against a real suspended device.
STATUS_TERMINATED = "TERMINATED"
STATUS_NEVER_BILLED = "Never billed"
PLAN_NEVER_ACTIVATED = "NEVER ACTIVATED"
STATUS_ACTIVE = "Active"
SUSPENDED_PLAN_KEYWORDS = ("suspend", "seasonal", "standby")


def _find_col(df, *candidates):
    lower_map = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in lower_map:
            return lower_map[cand.lower()]
    return None


def _is_true(val):
    return str(val).strip().lower() == "true"


def _parse_rate_plans(raw):
    """flatten_dict json.dumps'd the list; parse it back."""
    if raw is None or (isinstance(raw, float) and pd.isna(raw)) or raw == "":
        return []
    if isinstance(raw, list):
        return raw
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, list) else []
    except (TypeError, ValueError):
        return []


def _strip_plan_word(name):
    name = (name or "").strip()
    if name.lower().endswith(" plan"):
        name = name[: -len(" plan")]
    return name.strip()


def _format_active_plan(plan_name, rate_plans):
    if not rate_plans:
        return plan_name or pd.NA
    parts = []
    for i, entry in enumerate(rate_plans):
        rp = (entry or {}).get("ratePlan", {}) or {}
        rp_type = (rp.get("ratePlanType") or {}).get("name") or ""
        rp_name = _strip_plan_word(rp.get("ratePlanName"))
        if i == 0:
            label = plan_name or rp_name
            parts.append(f"{label}: {rp_type}" if rp_type else label)
        else:
            parts.append(f"{rp_name}-{rp_type}" if rp_type else rp_name)
    return ", ".join(p for p in parts if p)


def add_billing_status(df):
    plan_name_col = _find_col(df, "ActiveDevicePlan_Name")
    terminated_col = _find_col(df, "IsTerminated")
    unactivated_col = _find_col(df, "IsUnactivated")
    rate_plans_col = _find_col(df, "ActiveRatePlans")

    if not plan_name_col:
        logger.warning("No ActiveDevicePlan_Name column; 'Active Billing Plan' incomplete.")
    if not terminated_col or not unactivated_col:
        logger.warning("IsTerminated/IsUnactivated missing; 'Billing Status' incomplete.")

    def compute(row):
        if terminated_col and _is_true(row.get(terminated_col)):
            return STATUS_TERMINATED, STATUS_TERMINATED
        if unactivated_col and _is_true(row.get(unactivated_col)):
            return STATUS_NEVER_BILLED, PLAN_NEVER_ACTIVATED
        plan_name = row.get(plan_name_col) if plan_name_col else None
        rate_plans = _parse_rate_plans(row.get(rate_plans_col)) if rate_plans_col else []
        plan_display = _format_active_plan(plan_name, rate_plans)
        status = STATUS_ACTIVE
        if any(k in str(plan_name or "").lower() for k in SUSPENDED_PLAN_KEYWORDS):
            status = "Suspended"
        return status, plan_display

    results = df.apply(compute, axis=1, result_type="expand")
    df["Billing Status"] = results[0]
    df["Active Billing Plan"] = results[1]
    logger.info("Billing Status breakdown:\n%s", df["Billing Status"].value_counts().to_string())
    return df


# ===== HARDWARE ID / PRODUCT CODE =====
# Two separate columns in MyAdmin (real row: Hardware ID 571024830,
# Product code GP9LTEATT). Hardware ID = device.id; Product code = productCode.
HARDWARE_ID_CANDIDATES = ("Device_Id", "Device_id", "HardwareId", "HwId")
PRODUCT_CODE_CANDIDATES = ("ProductCode",)


def add_hardware_id(df):
    hw_col = _find_col(df, *HARDWARE_ID_CANDIDATES)
    if hw_col:
        df["Hardware ID"] = df[hw_col]
        logger.info(f"Hardware ID sourced from column '{hw_col}'.")
    else:
        df["Hardware ID"] = pd.NA
        logger.warning("No Hardware ID column found among %s.", HARDWARE_ID_CANDIDATES)

    pc_col = _find_col(df, *PRODUCT_CODE_CANDIDATES)
    if pc_col and pc_col != "Product code":
        df["Product code"] = df[pc_col]
    elif not pc_col:
        logger.warning("No Product code column found among %s.", PRODUCT_CODE_CANDIDATES)
    return df


# ===== CONTRACT DATES =====
# contractStartDate / contractEndDate are top-level scalars, NOT the generic
# startDate / endDate on the same object (a sample device had endDate
# 2050-01-01 -- effectively "no end" -- but a real 3-year contractEndDate).
CONTRACT_START_CANDIDATES = ("contractStartDate",)
CONTRACT_END_CANDIDATES = ("contractEndDate",)


def add_contract_dates(df):
    """Parsed to tz-naive datetimes: df.to_excel() raises on tz-aware values, and
    the sources are already UTC ('...Z'), so the tz is dropped only after
    normalizing to UTC -- values are not shifted."""
    for label, candidates in (("Contract Start Date", CONTRACT_START_CANDIDATES),
                              ("Contract End Date", CONTRACT_END_CANDIDATES)):
        col = _find_col(df, *candidates)
        if col:
            df[label] = pd.to_datetime(df[col], errors="coerce", utc=True).dt.tz_localize(None)
        else:
            df[label] = pd.NaT
            logger.warning("No %s column found among %s.", label, candidates)
    return df


# ===== ZOHO ANALYTICS v2 =====
def zoho_get_access_token():
    r = requests.post(
        f"{ZOHO_ANALYTICS['accounts_url']}/oauth/v2/token",
        data={
            "refresh_token": ZOHO_ANALYTICS["refresh_token"],
            "client_id": ZOHO_ANALYTICS["client_id"],
            "client_secret": ZOHO_ANALYTICS["client_secret"],
            "grant_type": "refresh_token",
        },
        timeout=30,
    )
    r.raise_for_status()
    body = r.json()
    if "access_token" not in body:
        raise RuntimeError(f"Token refresh failed: {body}")
    return body["access_token"]


ZOHO_MAX_BYTES_PER_IMPORT = 14 * 1024 * 1024   # Zoho caps at 20MB; leave headroom


def _zoho_import_chunk(csv_bytes, import_type, access_token):
    url = f"{ZOHO_ANALYTICS['api_domain']}/workspaces/{ZOHO_WORKSPACE_ID}/views/{ZOHO_VIEW_ID}/data"
    config = {
        "importType": import_type,     # truncateadd (first chunk) then append
        "fileType": "csv",
        "autoIdentify": "true",
        "onError": "setcolumnempty",
    }
    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}",
        "ZANALYTICS-ORGID": ZOHO_ORG_ID,
    }
    r = requests.post(url, headers=headers,
                      data={"CONFIG": json.dumps(config)},
                      files={"FILE": ("geotab_devices.csv", csv_bytes, "text/csv")},
                      timeout=300)
    logger.info(f"  [{import_type}] status: {r.status_code}")
    if r.status_code != 200:
        logger.error(f"Zoho response error: {r.text[:2000]}")
    r.raise_for_status()
    return r.json()


def _summarize_import(result, sent):
    """Report ZOHO's row count, not ours.

    The old log line counted rows *sent* and fell back to len(chunk) when the
    response carried no summary -- so it printed "Zoho sync complete: N rows
    loaded" even for an import Zoho rejected entirely. Never trust a success
    message that can't fail.
    """
    summary = ((result or {}).get("data", {}) or {}).get("importSummary", {}) or {}
    accepted = summary.get("successRowCount", summary.get("totalRowCount"))
    errors = summary.get("errorRowCount") or summary.get("totalErrorCount") or 0
    if accepted is None:
        logger.error("Zoho returned no importSummary. Full response: %s",
                     json.dumps(result)[:2000])
        raise RuntimeError("Zoho import returned no row summary - load unconfirmed.")
    accepted = int(accepted)
    logger.info(f"    Zoho accepted {accepted}/{sent} row(s), rejected {errors}")
    if accepted == 0 and sent > 0:
        raise RuntimeError(f"Zoho accepted 0 of {sent} rows -- check that the CSV "
                           f"column names match the table's (autoIdentify).")
    return accepted


def zoho_truncate_add(df, access_token):
    """Replace the table with df, chunked under Zoho's import cap.

    Chunk 1 truncates. That makes every later chunk load-bearing: if one fails
    the table is left partial, so any failure here must raise and go red.
    """
    header_bytes = len(df.iloc[0:0].to_csv(index=False).encode("utf-8"))
    full_bytes = len(df.to_csv(index=False).encode("utf-8"))
    avg_row = max(1, (full_bytes - header_bytes) // max(1, len(df)))
    rows_per_chunk = max(1, (ZOHO_MAX_BYTES_PER_IMPORT - header_bytes) // avg_row)

    total_rows = len(df)
    n_chunks = (total_rows + rows_per_chunk - 1) // rows_per_chunk
    logger.info(f"Uploading {total_rows} rows in {n_chunks} chunk(s) of ~{rows_per_chunk}...")

    sent = accepted_total = 0
    for i in range(0, total_rows, rows_per_chunk):
        chunk = df.iloc[i:i + rows_per_chunk]
        import_type = "truncateadd" if i == 0 else "append"
        n = i // rows_per_chunk + 1
        logger.info(f"  chunk {n}/{n_chunks}: sending {len(chunk)} rows")
        try:
            result = _zoho_import_chunk(
                chunk.to_csv(index=False).encode("utf-8"), import_type, access_token)
            accepted_total += _summarize_import(result, len(chunk))
        except Exception as e:
            if i == 0:
                raise RuntimeError(f"First chunk failed; table untouched. {e}") from e
            raise RuntimeError(
                f"Chunk {n}/{n_chunks} failed AFTER the truncate -- the Zoho table "
                f"now holds only {accepted_total} of {total_rows} rows and must be "
                f"reloaded. {e}") from e
        sent += len(chunk)

    logger.info(f"Zoho sync complete: Zoho accepted {accepted_total} of {sent} rows sent.")
    if accepted_total < total_rows:
        logger.warning(f"{total_rows - accepted_total} row(s) were not accepted.")
    return accepted_total


def save_backups(df):
    """Run BEFORE the Zoho sync. The old code did this after, using a hardcoded
    C:\\Users\\suppo\\...\\.venv\\...\\OP path that does not exist on the CI runner
    -- so a missing folder raised after a SUCCESSFUL sync and mailed a false
    'ETL Failed'."""
    out_dir = os.path.dirname(EXCEL_OUT)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    df.to_csv(CSV_OUT, index=False)
    try:
        df.to_excel(EXCEL_OUT, index=False)
        logger.info(f"Saved backups: {CSV_OUT} and {EXCEL_OUT}")
    except Exception as e:
        # openpyxl missing or similar: the CSV is the real backup, don't fail the run.
        logger.warning(f"Excel backup skipped ({e}); CSV written to {CSV_OUT}")


# ===== MAIN =====
def main():
    creds = authenticate()
    logger.info("Authenticated against MyAdmin v3.")

    accounts = [a.get("accountId") for a in creds.get("accounts", []) if a.get("accountId")]
    if not accounts:
        accounts = ["GOFL01", "GOFL02", "GOFL03"]   # zeros, not letter O
        logger.warning(f"Authenticate returned no accounts; using fallback {accounts}")
    logger.info(f"Accounts: {accounts}")

    all_records, failures = [], []
    for acc in accounts:
        logger.info(f"Fetching devices for account {acc}...")
        try:
            raw = fetch_contracts(acc, creds)
        except Exception as e:
            logger.error(f"Error fetching for {acc}: {e}")
            failures.append((acc, str(e)))
            continue
        recs = extract_records(raw, acc)
        logger.info(f"Got {len(recs)} records for {acc}")
        all_records.extend(recs)

    # Fail loud. This is the hole that hid the v2 deprecation for weeks: errors
    # were logged at INFO, main() returned normally, the job went green, and
    # send_failure_email never fired.
    if failures:
        raise RuntimeError(
            "Account fetch failed for "
            + ", ".join(f"{a} ({m[:200]})" for a, m in failures)
            + ". Refusing to sync a partial dataset over the live table.")

    df = pd.DataFrame(all_records)

    if df.empty:
        raise RuntimeError("No records fetched - aborting (table left untouched).")

    if len(df) < MIN_EXPECTED_ROWS:
        raise RuntimeError(
            f"Only {len(df)} rows fetched but GEOTAB_MIN_ROWS={MIN_EXPECTED_ROWS}. "
            f"Refusing to truncate-add a short pull. Raise the floor deliberately "
            f"if the fleet really shrank.")
    if MIN_EXPECTED_ROWS == 0:
        logger.warning("GEOTAB_MIN_ROWS is unset. After this run succeeds, set it "
                       f"to ~{int(len(df) * 0.9)} so a short pull can never "
                       f"silently replace the table.")

    if _missing_required and REQUIRE_FIELDS:
        raise RuntimeError(
            f"v3 did not return {list(_missing_required)}, so the dependent columns "
            f"(Billing Status, Active Billing Plan, Contract dates, database "
            f"assignment) would load EMPTY and overwrite good data in the table. "
            f"Aborting because GEOTAB_REQUIRE_FIELDS=1. Unset it to sync anyway.")

    df = reconcile_databases(df, creds)
    df = add_billing_status(df)
    df = add_hardware_id(df)
    df = add_contract_dates(df)

    save_backups(df)          # before the sync, so a backup bug can't fake a failure

    token = zoho_get_access_token()
    zoho_truncate_add(df, token)

    logger.info(f"Done. {len(df)} rows, {len(df.columns)} columns.")


def send_failure_email(error_msg: str):
    app_password = os.getenv("gmail_pass") or os.getenv("GMAIL_PASS")
    if not app_password:
        # The workflow never passed this secret, so every failure email silently
        # died here. Say so plainly instead of raising an opaque SMTP error.
        logger.error("No gmail_pass/GMAIL_PASS in the environment - cannot send the "
                     "failure email. Add it to the workflow's env, or rely on "
                     "GitHub's failed-workflow notification (the job now exits non-zero).")
        return
    sender = receiver = "nandhinipv@zenduit.com"
    msg = MIMEText(f"ETL FAILED\nTime: {datetime.now().isoformat()}\n\nError:\n{error_msg}")
    msg["Subject"] = "Geotab ETL Failed"
    msg["From"] = sender
    msg["To"] = receiver
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(sender, app_password)
        server.send_message(msg)
    logger.info("Failure email sent.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error("ETL FAILED")
        logger.error(str(e))
        logger.error(traceback.format_exc())
        try:
            send_failure_email(str(e))
        except Exception as mail_err:
            logger.error(f"Failed to send email: {mail_err}")
        raise
