import requests
import pandas as pd
from datetime import datetime, timezone
import json
import smtplib
from email.mime.text import MIMEText
import os
import logging
import traceback

# ===== LOGGING CONFIG =====
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(f"{LOG_DIR}/etl.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ===== GEOTAB CONFIG =====
MYADMIN_URL = "https://myadminapi.geotab.com/v2/MyAdminApi.ashx"
USERNAME = os.getenv("GEOTAB_USERNAME")
PASSWORD = os.getenv("GEOTAB_PASSWORD")

# ===== ZOHO ANALYTICS CONFIG (v2 OAuth) =====
# US data center. If your account is on .eu / .in / .com.au, change accounts_url + api_domain.
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

# ===== GEOTAB API HELPER =====
def call_api(method, params, timeout=120):
    payload = {"method": method, "params": params}
    # Always pass a timeout: without one a stalled connection blocks forever
    # (this is what made the job hang on "Fetching devices for account ...").
    resp = requests.post(MYADMIN_URL, data={"JSON-RPC": json.dumps(payload)}, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()
    if "error" in data and data["error"]:
        raise Exception(data["error"])
    return data.get("result")

def authenticate():
    return call_api("Authenticate", {
        "username": USERNAME,
        "password": PASSWORD
    })

def get_device_contracts_for_account(account_id, creds):
    """PASS 1: pull every device contract for an account (full details).

    ApiDeviceContract already includes ActiveDevicePlan, IsTerminated and
    IsUnactivated by default (no extra flag or optionalParam schema needed),
    so this single call is also where "active billing plan" and "billing
    status" come from -- see add_billing_status() below. It also nests the
    ApiGeotabDevice under "Device", which is where Hardware ID comes from --
    see add_hardware_id() below.
    """
    params = {
        "apiKey": creds["userId"],
        "sessionId": creds["sessionId"],
        "forAccount": account_id,
        "userCompanyId": -1,
        "devicePlanId": -1,
        # NOTE: includeConnectInfo is intentionally OFF here. Turning it on makes
        # Geotab attach connection/GPS data for every device, which turns the
        # large-account pull (e.g. GOFL02) into a huge, slow response. This flag
        # only affects connection/GPS data -- it has no effect on ActiveDevicePlan,
        # IsTerminated, or IsUnactivated, so billing status is unaffected.
        "fromDate": datetime.now(timezone.utc).strftime("%Y-%m-%dT00:00:00Z"),
        "toDate": datetime.now(timezone.utc).strftime("%Y-%m-%dT23:59:59Z")
    }
    return call_api("GetDeviceContracts", params)

def get_device_databases_by_serials(serials, creds):
    """
    PASS 2: get the OWNER (primary) and SHARED database names for specific serials.

    Why not GetDeviceContracts here: its LatestDeviceDatabase field is the most
    recent database the device was *detected/communicated* in. Devices that have
    never connected (no first-connect date, no GPS/comm records) have no such
    record, so that field is blank -- and re-fetching the contract won't change
    that. The administrative "Primary database" shown in MyAdmin comes from
    GetDeviceDatabaseNamesAsync instead, which returns OwnerDatabaseName +
    SharedDatabaseName per serial regardless of whether the device has connected.
    """
    if not serials:
        return []
    params = {
        "apiKey": creds["userId"],
        "sessionId": creds["sessionId"],
        "serialNumbers": list(serials),
    }
    return call_api("GetDeviceDatabaseNamesAsync", params)

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
    for dc in raw_list:
        if not isinstance(dc, dict):
            continue
        dc["input_account_id"] = account_id
        records.append(flatten_dict(dc))
    return records

# ===== MISSING-DATABASE HELPERS =====
# The database a device belongs to comes back under ApiDeviceContract.LatestDeviceDatabase,
# which flatten_dict turns into columns prefixed "LatestDeviceDatabase_".
DB_COL_PREFIX = "LatestDeviceDatabase"

def _serial_of(record):
    """Serial number lives on the nested Device object -> Device_SerialNumber after flatten."""
    for key in ("Device_SerialNumber", "SerialNumber", "Device_serialNumber"):
        if record.get(key):
            return record[key]
    return None

def find_serials_missing_database(df):
    """Return serials whose latest-device-database columns are all blank.

    Column matching is case-insensitive because the JSON-RPC endpoint returns
    camelCase keys (e.g. latestDeviceDatabase_databaseName), not the PascalCase
    shown in the Geotab docs.
    """
    db_cols = [c for c in df.columns if c.lower().startswith(DB_COL_PREFIX.lower())]
    lower_map = {c.lower(): c for c in df.columns}
    serial_col = next((lower_map[k] for k in
                       ("device_serialnumber", "serialnumber", "device_serialno", "serialno")
                       if k in lower_map), None)
    if not serial_col:
        logger.warning("No serial-number column found; cannot backfill databases.")
        return [], None, db_cols
    if not db_cols:
        # No database columns at all -> every device is missing it.
        missing_mask = pd.Series(True, index=df.index)
    else:
        # A row is "missing" when every database column is null or empty string.
        missing_mask = df[db_cols].apply(
            lambda col: col.isna() | (col.astype(str).str.strip() == ""), axis=0
        ).all(axis=1)
    serials = df.loc[missing_mask, serial_col].dropna().unique().tolist()
    return serials, serial_col, db_cols

def backfill_missing_databases(df, creds):
    """
    PASS 2 orchestration: for serials whose database came back blank in Pass 1,
    look up the administrative owner/shared database via GetDeviceDatabaseNamesAsync
    and write it back in. Adds OwnerDatabaseName / SharedDatabaseName columns and
    also fills the main database column (LatestDeviceDatabase_DatabaseName) so the
    primary database is populated even for devices that have never communicated.
    """
    serials, serial_col, db_cols = find_serials_missing_database(df)
    if not serials:
        logger.info("No devices with a missing database - nothing to backfill.")
        return df
    logger.info(f"{len(serials)} device(s) missing a database; looking up owner/shared DBs: {serials}")

    # GetDeviceDatabaseNamesAsync takes an array of serials; batch to keep payloads sane.
    BATCH = 100
    fetched = []
    for i in range(0, len(serials), BATCH):
        batch = serials[i:i + BATCH]
        try:
            raw = get_device_databases_by_serials(batch, creds)
        except Exception as e:
            logger.error(f"Database lookup failed for batch {batch}: {e}")
            raw = []
        fetched.extend(r for r in (raw or []) if isinstance(r, dict))

    if not fetched:
        logger.warning("Database lookup returned nothing; leaving databases blank.")
        return df

    # Ensure the destination columns exist.
    for c in ("OwnerDatabaseName", "SharedDatabaseName"):
        if c not in df.columns:
            df[c] = pd.NA
    # Main "database" column from Pass 1 (whatever its actual casing is).
    primary_col = next((c for c in db_cols if c.lower().endswith("databasename")), None)

    filled = 0
    for rec in fetched:
        # Endpoint returns camelCase keys; fall back to PascalCase just in case.
        serial = rec.get("serialNo") or rec.get("SerialNo")
        if not serial:
            continue
        owner = rec.get("ownerDatabaseName") or rec.get("OwnerDatabaseName")
        shared = rec.get("sharedDatabaseName") or rec.get("SharedDatabaseName")
        mask = df[serial_col] == serial
        if not mask.any():
            continue
        df.loc[mask, "OwnerDatabaseName"] = owner
        df.loc[mask, "SharedDatabaseName"] = shared
        # Backfill the main database column where it's still blank, using the owner DB.
        if primary_col in df.columns and owner:
            blank = mask & (
                df[primary_col].isna() | (df[primary_col].astype(str).str.strip() == "")
            )
            df.loc[blank, primary_col] = owner
        filled += int(mask.sum())

    logger.info(f"Backfilled owner/shared database for {filled} row(s) "
                f"from {len(fetched)} lookup record(s).")
    return df

# ===== BILLING STATUS / ACTIVE PLAN HELPERS =====
# ApiDeviceContract does NOT expose single "BillingStatus" / "ActiveBillingPlan"
# fields that match MyAdmin's UI verbatim. Reverse-engineered from MyAdmin's own
# Device Management grid (confirmed against real sample rows):
#
#   MyAdmin "Billing status"     MyAdmin "Active billing plan"
#   ---------------------------  --------------------------------
#   TERMINATED                   TERMINATED
#   Never billed                 NEVER ACTIVATED
#   Active                       "{ActiveDevicePlan.Name}: {rate plan type}"
#                                 e.g. "GO: Live", or with add-ons:
#                                 "GO: Live, ProPlus Mode-Live"
#
# The "Active billing plan" text for an active device is built from two
# fields GetDeviceContracts already returns:
#   - ActiveDevicePlan.Name         -> e.g. "GO"
#   - ActiveRatePlans[].ratePlan    -> each has ratePlanName (e.g. "GO Plan",
#                                      "ProPlus Mode Plan") and
#                                      ratePlanType.name (e.g. "Live")
# The first rate plan is shown as "{ActiveDevicePlan.Name}: {type}"; any
# additional simultaneously-active rate plans (add-ons) are appended as
# "{rate plan name minus the word 'Plan'}-{type}", comma-separated. This was
# validated against multi-plan examples ("GO: Live, ProPlus Mode-Live" and
# "GO: Live, GO-Live") and matched exactly.
#
# CAVEAT: there's no confirmed sample for a *Suspended* device in what we've
# seen so far, so that branch below is still a best-effort guess based on
# plan-name keywords -- verify it against MyAdmin for any suspended/seasonal
# devices in your account and adjust SUSPENDED_PLAN_KEYWORDS if needed.
#
# Column matching is case-insensitive because the JSON-RPC endpoint returns
# camelCase keys (isTerminated, activeDevicePlan_name, activeRatePlans, ...),
# not the PascalCase used in the Geotab docs.

STATUS_TERMINATED = "TERMINATED"
STATUS_NEVER_BILLED = "Never billed"           # MyAdmin "Billing status" wording
PLAN_NEVER_ACTIVATED = "NEVER ACTIVATED"       # MyAdmin "Active billing plan" wording
STATUS_ACTIVE = "Active"
SUSPENDED_PLAN_KEYWORDS = ("suspend", "seasonal", "standby")  # unverified, see caveat above

def _find_col(df, *candidates):
    lower_map = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in lower_map:
            return lower_map[cand.lower()]
    return None

def _is_true(val):
    return str(val).strip().lower() == "true"

def _parse_rate_plans(raw):
    """activeRatePlans comes through flatten_dict as a JSON string (lists are
    json.dumps'd). Parse it back into a list of dicts."""
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
    """Reconstructs MyAdmin's 'Active billing plan' text for an active device."""
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
    """Adds 'Billing Status' and 'Active Billing Plan' columns matching
    MyAdmin's Device Management grid wording (see note above)."""
    plan_name_col = _find_col(df, "ActiveDevicePlan_Name")
    terminated_col = _find_col(df, "IsTerminated")
    unactivated_col = _find_col(df, "IsUnactivated")
    rate_plans_col = _find_col(df, "ActiveRatePlans")

    if not plan_name_col:
        logger.warning("No ActiveDevicePlan_Name column found; "
                        "'Active Billing Plan' will be incomplete.")
    if not terminated_col or not unactivated_col:
        logger.warning("IsTerminated/IsUnactivated column(s) missing; "
                        "'Billing Status' may be incomplete.")

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
            status = "Suspended"  # unverified wording, see caveat above
        return status, plan_display

    results = df.apply(compute, axis=1, result_type="expand")
    df["Billing Status"] = results[0]
    df["Active Billing Plan"] = results[1]
    logger.info("Billing Status breakdown:\n%s", df["Billing Status"].value_counts().to_string())
    return df

# ===== HARDWARE ID / PRODUCT CODE HELPERS =====
# MyAdmin's Device Management grid shows "Hardware ID" and "Product code" as
# two SEPARATE columns (confirmed against a real row: Hardware ID "571024830",
# Product code "GP9LTEATT" -- they are not the same value).
#
#   - "Product code"  -> ApiDeviceContract.ProductCode (a top-level field on
#                         the contract, e.g. the GO9-LTE-ATT hardware/SKU code).
#   - "Hardware ID"    -> ApiGeotabDevice.Id, i.e. the nested Device object's
#                         "Id" property. Geotab's docs describe this as simply
#                         "Database Id of the device," and a 9-digit numeric
#                         value like 571024830 matches that shape. This is
#                         distinct from Device.SerialNumber (the human-facing
#                         serial, e.g. "G9X4SS9TAFX5").
#
# Column matching is case-insensitive because the JSON-RPC endpoint returns
# camelCase keys (device_id / deviceId), not the PascalCase used in the docs.
HARDWARE_ID_CANDIDATES = ("Device_Id", "Device_id", "HardwareId", "HwId")
PRODUCT_CODE_CANDIDATES = ("ProductCode",)

def add_hardware_id(df):
    """Adds a clean 'Hardware ID' column (from Device.Id) and a clean
    'Product code' column (from ApiDeviceContract.ProductCode), matching
    MyAdmin's Device Management grid."""
    hw_col = _find_col(df, *HARDWARE_ID_CANDIDATES)
    if hw_col:
        df["Hardware ID"] = df[hw_col]
        logger.info(f"Hardware ID sourced from column '{hw_col}'.")
    else:
        df["Hardware ID"] = pd.NA
        logger.warning(
            "No Hardware ID column found among candidates %s. "
            "Available columns: %s. If Device.Id is present under a "
            "different key, add it to HARDWARE_ID_CANDIDATES.",
            HARDWARE_ID_CANDIDATES, list(df.columns)
        )

    pc_col = _find_col(df, *PRODUCT_CODE_CANDIDATES)
    if pc_col and pc_col != "Product code":
        df["Product code"] = df[pc_col]
    elif not pc_col:
        logger.warning("No Product code column found among candidates %s.",
                        PRODUCT_CODE_CANDIDATES)
    return df

# ===== ZOHO ANALYTICS v2 FUNCTIONS =====
def zoho_get_access_token():
    """Exchange the long-lived refresh token for a 1-hour access token."""
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
        raise Exception(f"Token refresh failed: {body}")
    return body["access_token"]

# Zoho caps a single import at 20MB. Stay well under it for multipart + encoding overhead.
ZOHO_MAX_BYTES_PER_IMPORT = 14 * 1024 * 1024

def _zoho_import_chunk(csv_bytes, import_type, access_token):
    url = f"{ZOHO_ANALYTICS['api_domain']}/workspaces/{ZOHO_WORKSPACE_ID}/views/{ZOHO_VIEW_ID}/data"
    config = {
        "importType": import_type,     # "truncateadd" (first chunk) or "append" (rest)
        "fileType": "csv",
        "autoIdentify": "true",        # match incoming columns to table columns by name
        "onError": "setcolumnempty",   # don't abort the whole load on a bad cell
    }
    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}",
        "ZANALYTICS-ORGID": ZOHO_ORG_ID,
    }
    files = {"FILE": ("geotab_devices.csv", csv_bytes, "text/csv")}
    data = {"CONFIG": json.dumps(config)}
    r = requests.post(url, headers=headers, data=data, files=files, timeout=300)
    logger.info(f"  [{import_type}] status: {r.status_code}")
    if r.status_code != 200:
        logger.error(f"Zoho response error: {r.text}")
    r.raise_for_status()
    return r.json()

def zoho_truncate_add(df, access_token):
    """
    Replace the entire table with df, chunked to stay under Zoho's 20MB import cap.
    Chunk 1 uses truncateadd (wipes the table); every later chunk uses append.
    """
    header_bytes = len(df.iloc[0:0].to_csv(index=False).encode("utf-8"))
    full_bytes = len(df.to_csv(index=False).encode("utf-8"))
    # Estimate rows per chunk from the average encoded row size.
    avg_row = max(1, (full_bytes - header_bytes) // max(1, len(df)))
    rows_per_chunk = max(1, (ZOHO_MAX_BYTES_PER_IMPORT - header_bytes) // avg_row)
    total_rows = len(df)
    n_chunks = (total_rows + rows_per_chunk - 1) // rows_per_chunk
    logger.info(f"Uploading {total_rows} rows in {n_chunks} chunk(s) of ~{rows_per_chunk} rows...")
    imported = 0
    for i in range(0, total_rows, rows_per_chunk):
        chunk = df.iloc[i:i + rows_per_chunk]
        csv_bytes = chunk.to_csv(index=False).encode("utf-8")  # header included per chunk
        import_type = "truncateadd" if i == 0 else "append"
        result = _zoho_import_chunk(csv_bytes, import_type, access_token)
        summary = result.get("data", {}).get("importSummary", {})
        added = summary.get("totalRowCount", summary.get("successRowCount", len(chunk)))
        imported += len(chunk)
        logger.info(f"  chunk {i // rows_per_chunk + 1}/{n_chunks}: sent {len(chunk)} rows "
              f"(Zoho reported {added}) | cumulative {imported}/{total_rows}")
    logger.info(f"Zoho sync complete: {imported} rows loaded into the table.")

# ===== MAIN =====
def main():
    creds = authenticate()
    accounts = [
        a.get("accountId")
        for a in creds.get("accounts", [])
        if a.get("accountId")
    ]
    if not accounts:
        # Fixed typos: these were GOFLO2 / GOFLO3 (letter O) -> GOFL02 / GOFL03 (zero).
        accounts = ["GOFL01", "GOFL02", "GOFL03"]

    all_records = []
    for acc in accounts:
        logger.info(f"Fetching devices for account {acc}...")
        try:
            raw = get_device_contracts_for_account(acc, creds)
        except Exception as e:
            logger.info(f"Error fetching for {acc}: {e}")
            raw = []
        recs = extract_records(raw, acc)
        logger.info(f"Got {len(recs)} records for {acc}")
        all_records.extend(recs)

    df = pd.DataFrame(all_records)

    # Guard: never wipe the Zoho table if the fetch came back empty
    if df.empty:
        logger.info("No records fetched - skipping Zoho sync to avoid wiping the table.")
        return

    # ===== PASS 2: fill in databases for serials the per-account pull missed =====
    df = backfill_missing_databases(df, creds)

    # ===== BILLING STATUS / ACTIVE BILLING PLAN =====
    df = add_billing_status(df)

    # ===== HARDWARE ID / PRODUCT CODE =====
    df = add_hardware_id(df)

    # ===== ZOHO SYNC =====
    token = zoho_get_access_token()
    zoho_truncate_add(df, token)

    # Local backups
    df.to_csv("gofleet_devices_full.csv", index=False)
    df.to_excel(
        r"C:\Users\suppo\PyCharmMiscProject\.venv\Billing_audit_engine\OP\geotab_op.xlsx",
        index=False
    )
    logger.info("Saved CSV & Excel successfully")
    logger.info(f"Columns: {len(df.columns)}")
    logger.info(f"\n{df.head()}")

def send_failure_email(error_msg: str):
    sender = "nandhinipv@zenduit.com"   # change if needed
    receiver = "nandhinipv@zenduit.com"
    app_password = os.getenv("gmail_pass")
    msg = MIMEText(f"""
ETL FAILED
Time: {datetime.now().isoformat()}
Error:
{error_msg}
""")
    msg["Subject"] = "Geotab ETL Failed"
    msg["From"] = sender
    msg["To"] = receiver
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(sender, app_password)
        server.send_message(msg)

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
