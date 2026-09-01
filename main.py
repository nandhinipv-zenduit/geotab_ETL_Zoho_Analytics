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
GEOTAB_FIELD_SYNTAX) to skip the probing on every run.
"""

import json
import logging
import os
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
    level=logging.INFO,
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

# Escape hatch: allow the sync even if the enrichment fields can't be recovered.
ALLOW_MISSING_FIELDS = os.getenv("GEOTAB_ALLOW_MISSING_FIELDS", "0") == "1"

# Pin these once the DISCOVERY log lines tell you what works.
PIN_PAGINATION = os.getenv("GEOTAB_PAGINATION") or None
PIN_FIELD_SYNTAX = os.getenv("GEOTAB_FIELD_SYNTAX") or None

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


def _post(url, method, params, timeout):
    payload = {"id": -1, "method": method, "params": params}
    resp = requests.post(url, data={"JSON-RPC": json.dumps(payload)}, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()
    if data.get("error"):
        raise RuntimeError(data["error"])
    return data.get("result")


def call_api(method, params, timeout=120):
    """v3 first, with a per-method fallback to v2.

    Not every MyAdmin method is served on v3 yet (the docs warn that v3 methods
    which don't support pagination return errors), while GetDeviceContracts is
    now v3-only. So neither version alone is sufficient -- try v3, and if the
    method reports itself unavailable there, remember that and use v2 for it.
    A timeout is always passed: without one a stalled connection blocks forever,
    which is what used to hang the job on "Fetching devices for account ...".
    """
    if method in _V2_FALLBACK_OK:
        return _post(MYADMIN_V2, method, params, timeout)
    try:
        return _post(MYADMIN_V3, method, params, timeout)
    except RuntimeError as e:
        msg = json.dumps(e.args[0]) if e.args else str(e)
        unavailable = ("no longer available" in msg or "not supported" in msg
                       or "does not exist" in msg or "Unknown method" in msg)
        if not unavailable:
            raise
        logger.warning(f"{method} unavailable on v3 ({msg[:160]}); retrying on v2")
        result = _post(MYADMIN_V2, method, params, timeout)
        _V2_FALLBACK_OK.add(method)
        return result


def authenticate():
    return call_api("Authenticate", {"username": USERNAME, "password": PASSWORD})


# ===== v3 FIELD SELECTION (optionalParam) DISCOVERY =====
# Fields the enrichment steps below depend on. v3 omits these unless asked.
REQUIRED_NESTED = ("activeDevicePlan", "activeRatePlans",
                   "latestDeviceDatabase", "contractStartDate", "contractEndDate")

_SELECTION = """
  id
  account
  startDate
  endDate
  contractStartDate
  contractEndDate
  productCode
  assignedPurchaseOrderNo
  warrantyStatus
  isTerminated
  isUnactivated
  activeTrackingDisabled
  firstDeviceActivationDate
  device { id serialNumber }
  activeDevicePlan { id name }
  activeRatePlans { ratePlan { ratePlanName ratePlanType { name } } }
  latestDeviceDatabase { databaseName }
"""
_COMPACT = " ".join(_SELECTION.split())

FIELD_SYNTAXES = {
    "braced": "{ " + _COMPACT + " }",
    "unbraced": _COMPACT,
    "query": "query { deviceContracts { " + _COMPACT + " } }",
    "typed": "{ apiDeviceContract { " + _COMPACT + " } }",
    "fieldlist": {"fields": list(REQUIRED_NESTED)},
}

_field_syntax = None          # resolved name, or "none"
_field_param = None           # resolved optionalParam value, or None


def _as_records(result):
    """v3 returned a plain array in probing, but tolerate a wrapper too."""
    if isinstance(result, list):
        return result
    if isinstance(result, dict):
        for k in ("data", "records", "results", "items", "deviceContracts"):
            if isinstance(result.get(k), list):
                return result[k]
    return []


def discover_field_syntax(base):
    """Find the optionalParam encoding that brings the nested fields back."""
    global _field_syntax, _field_param
    if _field_syntax is not None:
        return _field_param

    order = list(FIELD_SYNTAXES)
    if PIN_FIELD_SYNTAX in FIELD_SYNTAXES:
        order = [PIN_FIELD_SYNTAX]

    for name in order:
        candidate = FIELD_SYNTAXES[name]
        try:
            recs = _as_records(call_api(
                "GetDeviceContracts", {**base, "optionalParam": candidate}))
        except Exception as e:
            logger.info(f"  field syntax '{name}': rejected ({str(e)[:120]})")
            continue
        if not recs:
            logger.info(f"  field syntax '{name}': accepted but returned 0 records")
            continue
        keys = set(recs[0])
        present = [f for f in REQUIRED_NESTED if f in keys]
        if present:
            logger.info(f"DISCOVERY: field syntax '{name}' works -- recovered "
                        f"{len(present)}/{len(REQUIRED_NESTED)} field(s): {present}")
            if len(present) < len(REQUIRED_NESTED):
                logger.warning("  still missing: "
                               f"{[f for f in REQUIRED_NESTED if f not in keys]}")
            _field_syntax, _field_param = name, candidate
            return _field_param
        logger.info(f"  field syntax '{name}': accepted, but none of the wanted "
                    f"fields came back (keys: {sorted(keys)})")

    _field_syntax, _field_param = "none", None
    logger.error("DISCOVERY FAILED: no optionalParam encoding recovered "
                 f"{REQUIRED_NESTED}. Billing Status / Active Billing Plan / "
                 "Contract dates / database reconciliation will all be EMPTY.")
    return None


# ===== v3 PAGINATION DISCOVERY =====
PAGINATION_STRATEGIES = ("nextId", "top_page", "pagination_obj", "by_page")


def _page_call(strategy, base, page, cursor_id, page_size):
    """Build and issue one page request for a given pagination dialect."""
    method = "GetDeviceContracts"
    if strategy == "nextId":
        extra = {"nextId": cursor_id}
    elif strategy == "top_page":
        extra = {"page": page, "perPage": page_size}
    elif strategy == "pagination_obj":
        extra = {"pagination": {"page": page, "perPage": page_size}}
    elif strategy == "by_page":
        method = "GetDeviceContractsByPage"
        extra = {"page": page, "perPage": page_size}
    else:
        raise ValueError(strategy)
    return _as_records(call_api(method, {**base, **extra}))


# Page lengths that indicate the server clamped us rather than ran out of data.
# The server may silently clamp below what we asked for (20 observed while
# requesting 100), so "shorter than requested" is NOT proof of a last page.
SUSPICIOUS_CAPS = (20, 25, 50, 100, 200, 250, 500, 1000)


def _looks_clamped(n):
    """Is a page of exactly n records suspiciously round?

    This is the one genuinely ambiguous case: a dialect that is being ignored
    returns page 1 forever, and an account that simply has n devices also
    returns the same n forever. We cannot tell them apart from the responses
    alone, so we lean on the page length: 20 or 100 means "clamped, more data
    behind" (abort), while 7 or 47 means "that is the whole account" (accept).
    Erring toward abort is deliberate -- a false alarm costs a red build, a
    false pass truncates the live Zoho table.
    """
    return n == page_size_requested() or n in SUSPICIOUS_CAPS


def page_size_requested():
    return PAGE_SIZE


def _walk(strategy, base, page_size):
    """Page through with one strategy.

    Returns (records, complete). complete is False when a full page came back but
    the next request produced no new records -- i.e. the dialect is being ignored
    and we are stuck on page 1 with more data behind it. That distinction is the
    whole point: a short pull that looks successful is worse than a loud failure,
    because truncateadd would push it over the live table.
    """
    out, seen = [], set()
    page, cursor_id, first_len = 1, 0, None

    while page <= MAX_PAGES:
        recs = _page_call(strategy, base, page, cursor_id, page_size)
        if first_len is None:
            first_len = len(recs)
            if first_len == 0:
                return [], True          # account genuinely has no contracts
        if not recs:
            return out, True             # ran off the end cleanly

        new = [r for r in recs if isinstance(r, dict) and r.get("id") not in seen]
        out.extend(new)
        seen.update(r.get("id") for r in new)

        if len(recs) < first_len:
            return out, True             # short page = last page
        if not new:
            # Not advancing. Either the dialect is ignored, or this account just
            # has this many devices and there was never a page 2.
            return out, not _looks_clamped(len(recs))

        ids = [r.get("id") for r in recs if r.get("id") is not None]
        cursor_id = max(ids) if ids else cursor_id
        page += 1

    logger.warning(f"Hit MAX_PAGES={MAX_PAGES} with strategy '{strategy}'")
    return out, False


_pagination = None


def fetch_contracts(account_id, creds):
    """Pull every device contract for an account. Raises if it can't prove the
    pull is complete."""
    global _pagination

    base = {
        "apiKey": creds["userId"],
        "sessionId": creds["sessionId"],
        "forAccount": account_id,
        "userCompanyId": -1,
        "devicePlanId": -1,
        # includeConnectInfo stays OFF: it attaches connection/GPS data for every
        # device, which makes a large account (GOFL02) huge and slow. It has no
        # effect on activeDevicePlan / isTerminated / isUnactivated.
        "fromDate": datetime.now(timezone.utc).strftime("%Y-%m-%dT00:00:00Z"),
        "toDate": datetime.now(timezone.utc).strftime("%Y-%m-%dT23:59:59Z"),
    }
    field_param = discover_field_syntax(base)
    if field_param is not None:
        base["optionalParam"] = field_param

    order = list(PAGINATION_STRATEGIES)
    if PIN_PAGINATION in PAGINATION_STRATEGIES:
        order = [PIN_PAGINATION]
    elif _pagination:
        order = [_pagination] + [s for s in order if s != _pagination]

    best, best_strategy = [], None
    for strategy in order:
        try:
            recs, complete = _walk(strategy, base, PAGE_SIZE)
        except Exception as e:
            logger.info(f"  pagination '{strategy}': failed ({str(e)[:140]})")
            continue
        if complete:
            if _pagination != strategy:
                logger.info(f"DISCOVERY: pagination strategy '{strategy}' works "
                            f"({len(recs)} records for {account_id})")
            _pagination = strategy
            return recs
        logger.info(f"  pagination '{strategy}': stalled at {len(recs)} record(s) "
                    f"-- dialect appears to be ignored")
        if len(recs) > len(best):
            best, best_strategy = recs, strategy

    raise RuntimeError(
        f"Cannot paginate {account_id} on MyAdmin v3: every dialect "
        f"{order} stalled (best was '{best_strategy}' with {len(best)} records, "
        f"page size appears capped). Refusing to continue -- syncing a partial "
        f"pull would truncate the Zoho table. Run the probe to find the correct "
        f"pagination parameter, then set GEOTAB_PAGINATION."
    )


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

    if _field_syntax == "none" and not ALLOW_MISSING_FIELDS:
        raise RuntimeError(
            "v3 did not return activeDevicePlan / activeRatePlans / "
            "latestDeviceDatabase / contract dates, so Billing Status, Active "
            "Billing Plan, Contract dates and the database columns would all load "
            "EMPTY and overwrite good data. Aborting. Set "
            "GEOTAB_ALLOW_MISSING_FIELDS=1 to sync anyway.")

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
