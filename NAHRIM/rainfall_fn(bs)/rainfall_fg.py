# rainfall_fg.py
# Python 3.9+ for Huawei FunctionGraph

import os
import json
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
    """
    From all <table>, pick the one whose text contains 'Bil.' and 'ID Stesen'.
    Otherwise fall back to the last table.
    """
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
    """
    Parse a single state's rainfall page and return a list of row dictionaries.
    Logic closely follows your original Scrapy spider.
    """
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

    # First header row (Bil, ID Stesen, Nama Stesen, Daerah, Kemaskini Terakhir, ..., tail columns)
    top_ths = header_rows[0].find_all(["th", "td"])
    top_texts = [" ".join(th.stripped_strings) for th in top_ths]

    # Second header row (usually date columns)
    bottom_ths = header_rows[1].find_all(["th", "td"])
    bottom_texts = [" ".join(th.stripped_strings) for th in bottom_ths]

    base_cols = top_texts[0:5]   # Bil, ID Stesen, Nama Stesen, Daerah, Kemaskini Terakhir
    date_cols = bottom_texts[:]  # all dates
    tail_cols = top_texts[-2:]   # Taburan Hujan dari Tengah Malam, Jumlah 1 Jam

    columns = base_cols + date_cols + tail_cols
    ncols = len(columns)

    items = []
    count = 0

    for tr in data_rows:
        cells = tr.find_all(["td", "th"])
        tds = [" ".join(td.stripped_strings) for td in cells]

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


# ---------- OBS UPLOAD ----------

def upload_to_obs(content_str, logger):
    """
    Upload the given string (JSON) to OBS as an object.
    """
    if not (OBS_AK and OBS_SK):
        raise RuntimeError("OBS_AK and OBS_SK environment variables must be set")

    obs_client = ObsClient(
        access_key_id=OBS_AK,
        secret_access_key=OBS_SK,
        server=OBS_ENDPOINT,
    )

    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    object_key = f"{OBS_OBJECT_PREFIX}rainfall_trend_{timestamp}.json"

    logger.info(f"[OBS] Uploading to bucket={OBS_BUCKET}, key={object_key}")

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
    - Uploads JSON to OBS bucket `nahrim-raw`.
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
                break  # success
            except Exception as e:
                logger.error(f"[RAIN] Attempt {attempt} failed for {state_name}: {e}")
                if attempt == MAX_RETRIES:
                    logger.error(f"[RAIN] Giving up on {state_name} after {MAX_RETRIES} attempts")
                else:
                    continue

        if resp is None:
            # all retries failed
            continue

        if resp.status_code != 200:
            logger.error(f"[RAIN] HTTP {resp.status_code} for {state_name} ({url})")
            continue

        state_items = parse_state_page(resp.text, state_code, state_name, logger)
        all_items.extend(state_items)

    # Convert to JSON string (UTF-8, keep unicode characters)
    json_body = json.dumps(all_items, ensure_ascii=False)

    # Upload to OBS
    object_key = upload_to_obs(json_body, logger)

    # Return a small summary (useful in Test Event result)
    result = {
        "rows_scraped": len(all_items),
        "bucket": OBS_BUCKET,
        "object_key": object_key,
    }

    return {
        "statusCode": 200,
        "body": json.dumps(result),
    }
