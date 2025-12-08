# rainfall_fg.py
# Python 3.9+ for Huawei FunctionGraph – SAVE AS CSV

import os
import json
import csv
import io
import logging
from datetime import datetime
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup
from obs import ObsClient  # from Huawei OBS Python SDK

# ---------- CONFIG ----------

HEADERS = {"User-Agent": "Mozilla/5.0"}

# Existing state codes (your current handler)
STATE_MAP = {
    "SEL": "Selangor",
    "NSN": "Negeri Sembilan",
    "JHR": "Johor",
    "PHG": "Pahang",
    "PER": "Perlis",
    "KED": "Kedah",
    "PP": "Pulau Pinang",
    "PRK": "Perak",
    "WPKL": "Wilayah Persekutuan Kuala Lumpur",
    "MEL": "Melaka",
    "TRG": "Terengganu",
    "KEL": "Kelantan",
    "WPL": "Wilayah Persekutuan Labuan",
    "SBH": "Sabah",
    "SWK": "Sarawak",
}

# Old rainfall HTML page
BASE_URL = "https://publicinfobanjir.water.gov.my/rainfalldata/{}"

# New rainfall endpoint from Rainfall_JPS_bs4.ipynb
NEW_RAINFALL_URL = (
    "https://publicinfobanjir.water.gov.my/wp-content/themes/"
    "shapely/agency/searchresultrainfall.php"
)

# Map your state_code -> "state" query param for NEW_RAINFALL_URL
NEW_STATE_PARAM_MAP = {
    "PER": "PLS",  # Perlis
    "KED": "KDH",  # Kedah
    "PP": "PNG",  # Pulau Pinang
    "PRK": "PRK",  # Perak
    "SEL": "SEL",  # Selangor
    "WPKL": "WLH",  # WP Kuala Lumpur
    # Putrajaya (PTJ) not in current STATE_MAP
    "NSN": "NSN",  # Negeri Sembilan
    "MEL": "MLK",  # Melaka
    "JHR": "JHR",  # Johor
    "PHG": "PHG",  # Pahang
    "TRG": "TRG",  # Terengganu
    "KEL": "KEL",  # Kelantan
    "SWK": "SRK",  # Sarawak
    "SBH": "SAB",  # Sabah
    "WPL": "WLP",  # WP Labuan
}

# OBS settings – you can override via environment variables in FunctionGraph
OBS_ENDPOINT = os.getenv(
    "OBS_ENDPOINT",
    "https://obs.my-kualalumpur-1.alphaedge.tmone.com.my",
)
OBS_BUCKET = os.getenv("OBS_BUCKET", "nahrim-raw")
OBS_OBJECT_PREFIX = os.getenv("OBS_OBJECT_PREFIX", "rainfall/raw/")

OBS_AK = os.getenv("OBS_AK")  # Access Key
OBS_SK = os.getenv("OBS_SK")  # Secret Key


# ---------- HTML PARSING HELPERS (OLD URL) ----------


def pick_rainfall_table(soup, logger, state_code, state_name):
    tables = soup.find_all("table")
    if not tables:
        logger.warning(f"[RAIN] {state_code} {state_name}: no <table> elements found")
        return None

    for table in tables:
        txt = " ".join(table.stripped_strings)
        if "Bil." in txt and "ID Stesen" in txt:
            return table

    logger.info(f"[RAIN] {state_code} {state_name}: using last table as fallback")
    return tables[-1]


def parse_state_page_old(html_text, state_code, state_name, logger):
    """
    Original parser for: https://publicinfobanjir.water.gov.my/rainfalldata/{state_name}
    Structure: 2 header rows + data rows.
    """
    soup = BeautifulSoup(html_text, "html.parser")

    table = pick_rainfall_table(soup, logger, state_code, state_name)
    if table is None:
        return []

    all_rows = table.find_all("tr")
    if len(all_rows) < 3:
        logger.warning(
            f"[RAIN-OLD] Expected at least 3 rows (2 header + data) in table for {state_name}, "
            f"but found {len(all_rows)}"
        )
        return []

    header_rows = all_rows[0:2]
    data_rows = all_rows[2:]

    def row_texts(row):
        return [" ".join(cell.stripped_strings) for cell in row.find_all(["th", "td"])]

    top_texts = row_texts(header_rows[0])
    bottom_texts = row_texts(header_rows[1])

    # Same column pattern we used before
    base_cols = top_texts[
        0:5
    ]  # Bil, ID Stesen, Nama Stesen, Daerah, Kemaskini Terakhir
    date_cols = bottom_texts[:]  # all date columns
    tail_cols = top_texts[-2:]  # Taburan Hujan dari Tengah Malam, Jumlah 1 Jam

    columns = base_cols + date_cols + tail_cols
    ncols = len(columns)

    items = []
    count = 0

    for tr in data_rows:
        tds = row_texts(tr)

        if len(tds) != ncols:
            # skip malformed rows
            continue

        item = {
            "state_code": state_code,
            "state_name": state_name,
        }
        for col_name, value in zip(columns, tds):
            item[col_name] = value

        items.append(item)
        count += 1

    logger.info(f"[RAIN-OLD] {state_code} {state_name}: scraped {count} rows")
    return items


# ---------- HTML PARSER (NEW URL) ----------


def parse_state_page_new(html_text, state_code, state_name, logger):
    """
    Parser for NEW_RAINFALL_URL (searchresultrainfall.php)
    This follows Rainfall_JPS_bs4.ipynb logic.
    """
    soup = BeautifulSoup(html_text, "html.parser")
    table = soup.find("table", id="normaltable1")
    if table is None:
        logger.warning(
            f"[RAIN-NEW] {state_code} {state_name}: table id='normaltable1' not found"
        )
        return []

    # --- build column names ---
    ths = table.find("thead").find_all("th")
    th_texts = [th.get_text(" ", strip=True) for th in ths]

    # Expected order:
    # 0: No.
    # 1: Station ID
    # 2: Station
    # 3: District
    # 4: Last Updated
    # 5: Daily Rainfall (group label)  -> skip
    # 6: Rainfall from Midnight (...)
    # 7: Total 1 Hour (Now)
    # 8..: date columns (under "Daily Rainfall")
    base_cols = th_texts[0:5]  # No., Station ID, Station, District, Last Updated
    date_cols = th_texts[8:]  # the actual daily rainfall dates
    tail_cols = th_texts[6:8]  # Rainfall from Midnight, Total 1 Hour

    columns = base_cols + date_cols + tail_cols
    ncols = len(columns)

    # --- extract body rows ---
    tbody = table.find("tbody")
    if not tbody:
        logger.warning(f"[RAIN-NEW] {state_code} {state_name}: no <tbody> found")
        return []

    td_values = [td.get_text(" ", strip=True) for td in tbody.find_all("td")]
    if not td_values:
        logger.warning(f"[RAIN-NEW] {state_code} {state_name}: no <td> values found")
        return []

    # group every ncols cells into 1 row
    rows = [td_values[i : i + ncols] for i in range(0, len(td_values), ncols)]

    items = []
    for row in rows:
        if len(row) != ncols:
            continue

        item = {
            "state_code": state_code,
            "state_name": state_name,
        }
        for col_name, value in zip(columns, row):
            item[col_name] = value

        items.append(item)

    logger.info(f"[RAIN-NEW] {state_code} {state_name}: scraped {len(items)} rows")
    return items


# ---------- CSV HELPER ----------


def items_to_csv(all_items):
    """
    Convert list of dicts to CSV string (UTF-8).
    """
    if not all_items:
        return ""

    fieldnames = []
    for item in all_items:
        for key in item.keys():
            if key not in fieldnames:
                fieldnames.append(key)

    # Put state_code and state_name first if present
    for key in ["state_code", "state_name"]:
        if key in fieldnames:
            fieldnames.insert(0, fieldnames.pop(fieldnames.index(key)))

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(all_items)
    return buf.getvalue()


# ---------- OBS UPLOAD ----------


def upload_to_obs(content_str, logger):
    """
    Upload the given CSV string to OBS as an object.
    """
    if not (OBS_AK and OBS_SK):
        raise RuntimeError("OBS_AK and OBS_SK environment variables must be set")

    obs_client = ObsClient(
        access_key_id=OBS_AK,
        secret_access_key=OBS_SK,
        server=OBS_ENDPOINT,
    )

    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    object_key = f"{OBS_OBJECT_PREFIX}rainfall_trend_{timestamp}.csv"

    logger.info(f"[OBS] Uploading CSV to bucket={OBS_BUCKET}, key={object_key}")

    # use positional arguments – bucket, key, content
    resp = obs_client.putContent(OBS_BUCKET, object_key, content_str)

    if resp.status >= 300:
        raise RuntimeError(
            f"OBS upload failed: status={resp.status}, "
            f"errorCode={getattr(resp, 'errorCode', '')}, "
            f"errorMessage={getattr(resp, 'errorMessage', '')}"
        )

    logger.info("[OBS] Upload successful")
    return object_key


# ---------- FUNCTIONGRAPH ENTRY POINT ----------


def handler(event, context):
    """
    Main entry point for Huawei FunctionGraph.
    - Scrapes rainfall data for all states.
    - Uploads CSV to OBS bucket `nahrim-raw`.
    Strategy per state:
      1. Try OLD URL /rainfalldata/{state_name} with old parser.
      2. If that fails (HTTP error or 0 rows), call NEW_RAINFALL_URL with new parser.
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    all_items = []
    MAX_RETRIES = 3
    REQUEST_TIMEOUT = 5

    for state_code, state_name in STATE_MAP.items():
        # -------------------- 1) TRY OLD URL --------------------
        url_old = BASE_URL.format(quote(state_name, safe=""))
        logger.info(f"[RAIN] Fetching {state_code} from OLD URL: {url_old}")

        resp_old = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp_old = requests.get(
                    url_old, headers=HEADERS, timeout=REQUEST_TIMEOUT
                )
                break
            except Exception as e:
                logger.error(
                    f"[RAIN-OLD] Attempt {attempt} failed for {state_name}: {e}"
                )
                if attempt == MAX_RETRIES:
                    logger.error(
                        f"[RAIN-OLD] Giving up on {state_name} after {MAX_RETRIES} attempts"
                    )

        state_items = []

        if resp_old is not None and resp_old.status_code == 200:
            state_items = parse_state_page_old(
                resp_old.text, state_code, state_name, logger
            )
        else:
            if resp_old is not None:
                logger.error(
                    f"[RAIN-OLD] HTTP {resp_old.status_code} for {state_name} ({url_old})"
                )

        # -------------------- 2) FALLBACK TO NEW URL IF NEEDED --------------------
        if not state_items:
            new_state_param = NEW_STATE_PARAM_MAP.get(state_code)
            if not new_state_param:
                logger.warning(
                    f"[RAIN-NEW] No mapping to new state param for {state_code}; skipping fallback"
                )
            else:
                params = {
                    "state": new_state_param,
                    "district": "ALL",
                    "station": "ALL",
                    "loginStatus": "0",
                    "language": "1",
                }
                logger.info(
                    f"[RAIN] Fetching {state_code} from NEW URL: {NEW_RAINFALL_URL} "
                    f"params={params}"
                )
                try:
                    resp_new = requests.get(
                        NEW_RAINFALL_URL,
                        params=params,
                        headers=HEADERS,
                        timeout=60,
                    )
                    if resp_new.status_code == 200:
                        state_items = parse_state_page_new(
                            resp_new.text, state_code, state_name, logger
                        )
                    else:
                        logger.error(
                            f"[RAIN-NEW] HTTP {resp_new.status_code} for {state_name}"
                        )
                except Exception as e:
                    logger.error(f"[RAIN-NEW] Request failed for {state_name}: {e}")

        all_items.extend(state_items)

    # Convert to CSV string
    csv_body = items_to_csv(all_items)

    # Upload to OBS
    object_key = upload_to_obs(csv_body, logger)

    result = {
        "rows_scraped": len(all_items),
        "bucket": OBS_BUCKET,
        "object_key": object_key,
    }

    return {
        "statusCode": 200,
        "body": json.dumps(result),
    }
