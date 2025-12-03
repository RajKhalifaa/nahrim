# rainfall_fg.py
# FunctionGraph + Scrapy spider + OBS upload

import os
import json
import logging
from datetime import datetime
from urllib.parse import quote

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.log import configure_logging

from obs import ObsClient  # Huawei OBS SDK


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

# OBS settings – override via FG environment variables if needed
OBS_ENDPOINT = os.getenv(
    "OBS_ENDPOINT",
    "https://obs.my-kualalumpur-1.alphaedge.tmone.com.my",
)
OBS_BUCKET = os.getenv("OBS_BUCKET", "nahrim-raw")
OBS_OBJECT_PREFIX = os.getenv("OBS_OBJECT_PREFIX", "rainfall/raw/")

OBS_AK = os.getenv("OBS_AK")
OBS_SK = os.getenv("OBS_SK")

# Scrapy timeouts / retries
REQUEST_TIMEOUT = 5        # seconds per request
MAX_RETRIES = 3

# Store scraped items here (shared between spider and handler)
SCRAPED_ITEMS = []


# ---------- Scrapy Spider ----------

class RainfallSpider(scrapy.Spider):
    name = "rainfall_spider"

    custom_settings = {
        # reduce Scrapy's own logging (FunctionGraph log is enough)
        "LOG_ENABLED": False,
        "DOWNLOAD_TIMEOUT": REQUEST_TIMEOUT,
        "RETRY_TIMES": MAX_RETRIES,
        "DEFAULT_REQUEST_HEADERS": HEADERS,
        # be polite with concurrency – we don't need a lot here
        "CONCURRENT_REQUESTS": 4,
    }

    def start_requests(self):
        """
        Generate one request per state. We pass state_code/state_name via meta.
        """
        for state_code, state_name in STATE_MAP.items():
            url = BASE_URL.format(quote(state_name, safe=""))
            yield scrapy.Request(
                url,
                callback=self.parse_state,
                meta={"state_code": state_code, "state_name": state_name},
                dont_filter=True,
            )

    def parse_state(self, response):
        """
        Parse a state's table using XPath selectors (Scrapy-style).
        """

        state_code = response.meta["state_code"]
        state_name = response.meta["state_name"]

        logger = logging.getLogger()
        logger.info(f"[RAIN-SPIDER] Parsing {state_code} {state_name} ({response.url})")

        # pick table that looks like rainfall table
        tables = response.xpath("//table")
        if not tables:
            logger.warning(f"[RAIN-SPIDER] {state_name}: no <table> found")
            return

        table = None
        for t in tables:
            txt = " ".join(t.xpath(".//text()").getall()).strip()
            if "Bil." in txt and "ID Stesen" in txt:
                table = t
                break
        if table is None:
            table = tables[-1]  # fallback

        rows = table.xpath(".//tr")
        if len(rows) < 3:
            logger.warning(
                f"[RAIN-SPIDER] {state_name}: expected at least 3 rows, found {len(rows)}"
            )
            return

        header_top = rows[0]
        header_bottom = rows[1]
        data_rows = rows[2:]

        def row_texts(row_sel):
            cells = row_sel.xpath("./th | ./td")
            texts = []
            for c in cells:
                txt = " ".join(c.xpath(".//text()").getall()).strip()
                texts.append(txt)
            return texts

        top_texts = row_texts(header_top)
        bottom_texts = row_texts(header_bottom)

        base_cols = top_texts[0:5]
        date_cols = bottom_texts[:]
        tail_cols = top_texts[-2:]

        columns = base_cols + date_cols + tail_cols
        ncols = len(columns)

        count = 0
        for row in data_rows:
            cells = row_texts(row)

            if len(cells) != ncols:
                # skip malformed rows
                continue

            item = {
                "state_code": state_code,
                "state_name": state_name,
            }
            for col_name, value in zip(columns, cells):
                item[col_name] = value

            SCRAPED_ITEMS.append(item)
            count += 1

            # also yield in case you later add pipelines
            yield item

        logger.info(f"[RAIN-SPIDER] {state_code} {state_name}: {count} rows")


# ---------- OBS upload helper ----------

def upload_to_obs(content_str, logger):
    if not (OBS_AK and OBS_SK):
        raise RuntimeError("OBS_AK and OBS_SK must be set as environment variables")

    obs_client = ObsClient(
        access_key_id=OBS_AK,
        secret_access_key=OBS_SK,
        server=OBS_ENDPOINT,
    )

    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    object_key = f"{OBS_OBJECT_PREFIX}rainfall_trend_{timestamp}.json"

    logger.info(f"[OBS] Uploading to {OBS_BUCKET}/{object_key}")

    resp = obs_client.putContent(OBS_BUCKET, object_key, content_str)
    if resp.status >= 300:
        raise RuntimeError(
            f"OBS upload failed: status={resp.status}, "
            f"errorCode={getattr(resp, 'errorCode', '')}, "
            f"errorMessage={getattr(resp, 'errorMessage', '')}"
        )

    logger.info("[OBS] Upload successful")
    return object_key


# ---------- FunctionGraph entry ----------

REACTOR_STARTED = False  # naive guard against Twisted restart issues


def run_spider(logger):
    """
    Run the Scrapy spider and return the collected items.

    NOTE: Twisted reactor can only start once per process. This simple
    implementation is mainly for testing; if you see 'ReactorNotRestartable'
    on subsequent runs in the same container, you will need a subprocess
    or the lighter requests+parsel approach.
    """
    global REACTOR_STARTED

    if REACTOR_STARTED:
        # In case container is reused, avoid trying to restart reactor.
        logger.warning(
            "[RAIN-SPIDER] Reactor already started in this container. "
            "Skipping spider run to avoid ReactorNotRestartable."
        )
        return []

    REACTOR_STARTED = True

    # Use Scrapy's logging config to avoid duplicate logs
    configure_logging()

    process = CrawlerProcess()
    process.crawl(RainfallSpider)
    process.start()  # blocks until crawl finishes

    # Return a copy of the list so we don't accidentally mutate global
    return list(SCRAPED_ITEMS)


def handler(event, context):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    logger.info("[RAIN-SPIDER] Handler start")

    items = run_spider(logger)

    # Convert to JSON string (UTF-8, keep unicode)
    json_body = json.dumps(items, ensure_ascii=False)

    # Upload to OBS
    object_key = upload_to_obs(json_body, logger)

    result = {
        "rows_scraped": len(items),
        "bucket": OBS_BUCKET,
        "object_key": object_key,
    }

    logger.info(f"[RAIN-SPIDER] Done, rows_scraped={len(items)}")

    return {
        "statusCode": 200,
        "body": json.dumps(result),
    }
