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
def call_api(method, params):
    payload = {"method": method, "params": params}
    resp = requests.post(MYADMIN_URL, data={"JSON-RPC": json.dumps(payload)})
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
    """PASS 1: pull every device contract for an account (full details)."""
    params = {
        "apiKey": creds["userId"],
        "sessionId": creds["sessionId"],
        "forAccount": account_id,
        "userCompanyId": -1,
        "devicePlanId": -1,
        "includeConnectInfo": True,
        "fromDate": datetime.now(timezone.utc).strftime("%Y-%m-%dT00:00:00Z"),
        "toDate": datetime.now(timezone.utc).strftime("%Y-%m-%dT23:59:59Z")
    }
    return call_api("GetDeviceContracts", params)

def get_device_contracts_by_serials(serials, creds,
                                    from_date="2015-01-01T00:00:00Z"):
    """
    PASS 2: look up specific devices directly by serial number.

    Uses the GetDeviceContracts `serialNos` filter so shared / not-yet-databased
    devices that don't surface in the per-account pull can still be fetched.
    Date range is opened wide (not today-only) so the contract is always caught.
    """
    if not serials:
        return []
    params = {
        "apiKey": creds["userId"],
        "sessionId": creds["sessionId"],
        "userCompanyId": -1,
        "devicePlanId": -1,
        "includeConnectInfo": True,
        "serialNos": list(serials),
        "fromDate": from_date,
        "toDate": datetime.now(timezone.utc).strftime("%Y-%m-%dT23:59:59Z"),
    }
    return call_api("GetDeviceContracts", params)

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
    """Return serials whose LatestDeviceDatabase_* columns are all blank."""
    db_cols = [c for c in df.columns if c.startswith(DB_COL_PREFIX)]
    serial_col = next((c for c in ("Device_SerialNumber", "SerialNumber", "Device_serialNumber")
                       if c in df.columns), None)
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
    """PASS 2 orchestration: re-fetch missing-database serials and copy the DB value back in."""
    serials, serial_col, db_cols = find_serials_missing_database(df)
    if not serials:
        logger.info("No devices with a missing database - nothing to backfill.")
        return df
    logger.info(f"{len(serials)} device(s) missing a database; re-fetching by serial: {serials}")

    # Fetch in batches to respect the API and keep payloads reasonable.
    BATCH = 100
    refetched = []
    for i in range(0, len(serials), BATCH):
        batch = serials[i:i + BATCH]
        try:
            raw = get_device_contracts_by_serials(batch, creds)
        except Exception as e:
            logger.error(f"Serial re-fetch failed for batch {batch}: {e}")
            raw = []
        refetched.extend(r for r in (raw or []) if isinstance(r, dict))

    if not refetched:
        logger.warning("Serial re-fetch returned nothing; leaving databases blank.")
        return df

    # Build serial -> {db column: value} from the re-fetched records.
    fill = {}
    for rec in refetched:
        flat = flatten_dict(rec)
        serial = _serial_of(flat)
        if not serial:
            continue
        db_values = {c: flat[c] for c in flat if c.startswith(DB_COL_PREFIX)}
        if db_values:
            fill[serial] = db_values

    if not fill:
        logger.warning("Re-fetched records had no database info; leaving databases blank.")
        return df

    # Make sure every db column we discovered exists in df, then copy values in.
    all_db_cols = set(db_cols) | {c for v in fill.values() for c in v}
    for c in all_db_cols:
        if c not in df.columns:
            df[c] = pd.NA

    filled = 0
    for serial, db_values in fill.items():
        mask = df[serial_col] == serial
        if mask.any():
            for c, val in db_values.items():
                df.loc[mask, c] = val
            filled += int(mask.sum())
    logger.info(f"Backfilled database info for {filled} row(s) across {len(fill)} serial(s).")
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
