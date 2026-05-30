import requests
import pandas as pd
from datetime import datetime, timezone
import json
import os

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
    params = {
        "apiKey": creds["userId"],
        "sessionId": creds["sessionId"],
        "forAccount": account_id,
        "userCompanyId": -1,
        "devicePlanId": -1,
        "fromDate": datetime.now(timezone.utc).strftime("%Y-%m-%dT00:00:00Z"),
        "toDate": datetime.now(timezone.utc).strftime("%Y-%m-%dT23:59:59Z")
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
    print(f"  [{import_type}] status: {r.status_code}")
    if r.status_code != 200:
        print("  response:", r.text)
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
    print(f"Uploading {total_rows} rows in {n_chunks} chunk(s) of ~{rows_per_chunk} rows...")

    imported = 0
    for i in range(0, total_rows, rows_per_chunk):
        chunk = df.iloc[i:i + rows_per_chunk]
        csv_bytes = chunk.to_csv(index=False).encode("utf-8")  # header included per chunk
        import_type = "truncateadd" if i == 0 else "append"

        result = _zoho_import_chunk(csv_bytes, import_type, access_token)
        summary = result.get("data", {}).get("importSummary", {})
        added = summary.get("totalRowCount", summary.get("successRowCount", len(chunk)))
        imported += len(chunk)
        print(f"  chunk {i // rows_per_chunk + 1}/{n_chunks}: sent {len(chunk)} rows "
              f"(Zoho reported {added}) | cumulative {imported}/{total_rows}")

    print(f"Zoho sync complete: {imported} rows loaded into the table.")


# ===== MAIN =====
def main():
    creds = authenticate()

    accounts = [
        a.get("accountId")
        for a in creds.get("accounts", [])
        if a.get("accountId")
    ]
    if not accounts:
        accounts = ["GOFL01","GOFLO2","GOFLO3"]

    all_records = []
    for acc in accounts:
        print(f"Fetching devices for account {acc}...")
        try:
            raw = get_device_contracts_for_account(acc, creds)
        except Exception as e:
            print(f"Error fetching for {acc}: {e}")
            raw = []
        recs = extract_records(raw, acc)
        print(f"Got {len(recs)} records for {acc}")
        all_records.extend(recs)

    df = pd.DataFrame(all_records)

    # Guard: never wipe the Zoho table if the fetch came back empty
    if df.empty:
        print("No records fetched - skipping Zoho sync to avoid wiping the table.")
        return

    # ===== ZOHO SYNC =====
    token = zoho_get_access_token()
    zoho_truncate_add(df, token)

    # Local backups
    df.to_csv("gofleet_devices_full.csv", index=False)
    df.to_excel(
        r"C:\Users\suppo\PyCharmMiscProject\.venv\Billing_audit_engine\OP\geotab_op.xlsx",
        index=False
    )

    print("Saved CSV & Excel successfully")
    print("Columns:", len(df.columns))
    print(df.head())


if __name__ == "__main__":
    main()
