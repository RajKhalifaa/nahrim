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

BASE_URL = "https://publicinfobanjir.water.gov.my/rainfalldata/{}"

# OBS settings – you can override via environment variables in FunctionGraph
OBS_ENDPOINT = os.getenv(
    "OBS_ENDPOINT",
    "https://obs.my-kualalumpur-1.alphaedge.tmone.com.my",
)
OBS_BUCKET = os.getenv("OBS_BUCKET", "nahrim-raw")
OBS_OBJECT_PREFIX = os.getenv("OBS_OBJECT_PREFIX", "rainfall/raw/")

OBS_AK = os.getenv("OBS_AK")  # Access Key
OBS_SK = os.getenv("OBS_SK")  # Secret Key


# ---------- HTML PARSING HELPERS ----------


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


def parse_state_page(html_text, state_code, state_name, logger):
    soup = BeautifulSoup(html_text, "html.parser")

    table = pick_rainfall_table(soup, logger, state_code, state_name)
    if table is None:
        return []

    all_rows = table.find_all("tr")
    if len(all_rows) < 3:
        logger.warning(
            f"Expected at least 3 rows (2 header + data) in table for {state_name}, "
            f"but found {len(all_rows)}"
        )
        return []

    header_rows = all_rows[0:2]
    data_rows = all_rows[2:]

    def row_texts(row):
        return [" ".join(cell.stripped_strings) for cell in row.find_all(["th", "td"])]

    top_texts = row_texts(header_rows[0])
    bottom_texts = row_texts(header_rows[1])

    base_cols = top_texts[
        0:5
    ]  # Bil, ID Stesen, Nama Stesen, Daerah, Kemaskini Terakhir
    date_cols = bottom_texts[:]  # all dates
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

    logger.info(f"[RAIN] {state_code} {state_name}: scraped {count} rows")
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

    resp = obs_client.putContent(OBS_BUCKET, object_str=content_str)
    # if your SDK version does not accept named arg, use: obs_client.putContent(OBS_BUCKET, object_key, content_str)
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
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    all_items = []
    MAX_RETRIES = 3
    REQUEST_TIMEOUT = 5

    for state_code, state_name in STATE_MAP.items():
        url = BASE_URL.format(quote(state_name, safe=""))
        logger.info(f"[RAIN] Fetching {state_code} from {url}")

        resp = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
                break
            except Exception as e:
                logger.error(f"[RAIN] Attempt {attempt} failed for {state_name}: {e}")
                if attempt == MAX_RETRIES:
                    logger.error(
                        f"[RAIN] Giving up on {state_name} after {MAX_RETRIES} attempts"
                    )

        if resp is None:
            continue

        if resp.status_code != 200:
            logger.error(f"[RAIN] HTTP {resp.status_code} for {state_name} ({url})")
            continue

        state_items = parse_state_page(resp.text, state_code, state_name, logger)
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
