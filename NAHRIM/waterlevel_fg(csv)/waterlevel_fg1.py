# -*- coding: utf-8 -*-
"""
Automated Water Level Pipeline
Scrape → Save CSV to OBS → Trigger CDM Migration Job
"""

import os
import json
import csv
import io
import requests
import logging
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup
from obs import ObsClient


# -----------------------------
# Environment Variables
# -----------------------------
OBS_AK = os.getenv("OBS_AK")
OBS_SK = os.getenv("OBS_SK")
OBS_BUCKET = os.getenv("OBS_BUCKET")
OBS_ENDPOINT = os.getenv("OBS_ENDPOINT")
OBS_FOLDER = os.getenv("OBS_FOLDER")
CDM_ENDPOINT = os.getenv("CDM_ENDPOINT")
CDM_CLUSTER_ID = os.getenv("CDM_CLUSTER_ID")
CDM_JOB_NAME = os.getenv("CDM_JOB_NAME")

WATERLEVEL_FILENAME = "waterlevel_jps"


# -----------------------------
# CDM TRIGGER FUNCTION
# -----------------------------
def start_cdm_job(job_name, logger, context):
    """
    Call DataArts Migration (CDM) 'Start Job' API to run a job.
    Uses token from FunctionGraph context and env vars for IDs.
    """
    project_id = os.getenv("PROJECT_ID")
    cluster_id = os.getenv("CDM_CLUSTER_ID")
    cdm_endpoint = os.getenv(
        "CDM_ENDPOINT",
        "https://cdm.my-kualalumpur-1.alphaedge.tmone.com.my",
    )

    if not (project_id and cluster_id):
        raise RuntimeError("PROJECT_ID and CDM_CLUSTER_ID must be set as env vars")

    # Get token issued for this function execution
    token = context.getToken()

    url = (
        f"{cdm_endpoint}/v1.1/{project_id}"
        f"/clusters/{cluster_id}/cdm/job/{job_name}/start"
    )

    headers = {
        "Content-Type": "application/json;charset=utf-8",
        "X-Auth-Token": token,
    }

    # For now we don't use variables – empty object is fine
    body = {"variables": {}}

    logger.info(f"[CDM] Starting job '{job_name}' via {url}")

    try:
        resp = requests.put(url, headers=headers, data=json.dumps(body), timeout=30)
    except Exception as e:
        logger.error(f"[CDM] Request to start job failed: {e}")
        raise

    logger.info(f"[CDM] Response status={resp.status_code}, body={resp.text}")

    if resp.status_code != 200:
        raise RuntimeError(
            f"Failed to start CDM job {job_name}: "
            f"status={resp.status_code}, body={resp.text}"
        )

    # Parse JSON response (job submission info)
    try:
        return resp.json()
    except Exception:
        return {"raw_body": resp.text}


DEFAULT_STATE_CODES = [
    "PLS",  # Perlis
    "KDH",  # Kedah
    "PNG",  # Pulau Pinang
    "PRK",  # Perak
    "SEL",  # Selangor
    "WLH",  # Wilayah Persekutuan Kuala Lumpur
    "PTJ",  # Wilayah Persekutuan Putrajaya
    "NSN",  # Negeri Sembilan
    "MLK",  # Melaka
    "JHR",  # Johor
    "PHG",  # Pahang
    "TRG",  # Terengganu
    "KEL",  # Kelantan
    "SRK",  # Sarawak
    "SAB",  # Sabah
    "WLP",  # Wilayah Persekutuan Labuan
]

STATE_CODE_MAP = {
    "PLS": "Perlis",
    "KDH": "Kedah",
    "PNG": "Pulau Pinang",
    "PRK": "Perak",
    "SEL": "Selangor",
    "WLH": "Wilayah Persekutuan Kuala Lumpur",
    "PTJ": "Wilayah Persekutuan Putrajaya",
    "NSN": "Negeri Sembilan",
    "MLK": "Melaka",
    "JHR": "Johor",
    "PHG": "Pahang",
    "TRG": "Terengganu",
    "KEL": "Kelantan",
    "SRK": "Sarawak",
    "SAB": "Sabah",
    "WLP": "Wilayah Persekutuan Labuan",
}

TARGET_HEADERS = [
    "No.",
    "Station ID",
    "Station Name",
    "District",
    "Main Basin",
    "Sub River Basin",
    "Last Updated",
    "Water Level (m) (Graph)",
    "Threshold Normal",
    "Threshold Alert",
    "Threshold Warning",
    "Threshold Danger",
    "State"
]

# -----------------------------
# SCRAPING FUNCTION
# -----------------------------
def _scrape_state_legacy(state_name):
    """
    Legacy scraper:
    https://publicinfobanjir.water.gov.my/waterleveldata/{state_name}
    with table id="normaltable1"
    """
    from urllib.parse import quote

    base_url = "https://publicinfobanjir.water.gov.my/waterleveldata/{}"
    url = base_url.format(quote(state_name, safe=""))

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, "html.parser")
        table = soup.find("table", {"id": "normaltable1"})

        if not table:
            return {"error": f"No data table found for {state_name} (legacy URL)"}

        tbody = table.find("tbody")
        if not tbody:
            return {"error": f"No table body found for {state_name} (legacy URL)"}

        rows = tbody.find_all("tr")
        results = []

        for tr in rows:
            cells = tr.find_all("td")
            if len(cells) != 12:
                continue

            bil = cells[0].get_text(strip=True)
            if not bil.isdigit():
                continue

            # Extract text or link text for aras_air
            aras_air = cells[7].find("a")
            aras_air_text = (
                aras_air.get_text(strip=True)
                if aras_air
                else cells[7].get_text(strip=True)
            )

            data_point = {
                "state_name": state_name,
                "bil": bil,
                "id_stesen": cells[1].get_text(strip=True),
                "nama_stesen": cells[2].get_text(strip=True),
                "daerah": cells[3].get_text(strip=True),
                "lembangan": cells[4].get_text(strip=True),
                "sub_lembangan": cells[5].get_text(strip=True),
                "kemaskini_terakhir": cells[6].get_text(strip=True),
                "aras_air_m": aras_air_text,
                "ambang_normal": cells[8].get_text(strip=True),
                "ambang_waspada": cells[9].get_text(strip=True),
                "ambang_amaran": cells[10].get_text(strip=True),
                "ambang_bahaya": cells[11].get_text(strip=True),
                "scraped_timestamp": datetime.now(timezone(timedelta(hours=8))).isoformat(),
            }
            results.append(data_point)

        return {"state": state_name, "count": len(results), "data": results}

    except requests.RequestException as e:
        return {"error": f"Legacy URL request failed for {state_name}: {str(e)}"}
    except Exception as e:
        return {"error": f"Legacy URL parsing failed for {state_name}: {str(e)}"}


def _scrape_state_new(state_name):
    """
    New scraper:
    https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/
      ?state=CODE&district=ALL&station=ALL&lang=en
    """
    STATE_CODE_MAP = {
        "Perlis": "PLS",
        "Kedah": "KDH",
        "Pulau Pinang": "PNG",
        "Perak": "PRK",
        "Selangor": "SEL",
        "Wilayah Persekutuan Kuala Lumpur": "WLH",
        "Wilayah Persekutuan Putrajaya": "PTJ",
        "Negeri Sembilan": "NSN",
        "Melaka": "MLK",
        "Johor": "JHR",
        "Pahang": "PHG",
        "Terengganu": "TRG",
        "Kelantan": "KEL",
        "Sarawak": "SRK",
        "Sabah": "SAB",
        "Wilayah Persekutuan Labuan": "WLP",
    }

    state_code = STATE_CODE_MAP.get(state_name)
    if not state_code:
        return {"error": f"Unknown state name for new URL: {state_name}"}

    url = "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/"
    params = {
        "state": state_code,
        "district": "ALL",
        "station": "ALL",
        "lang": "en",
    }
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        resp = requests.get(url, params=params, headers=headers, timeout=60)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        table = soup.find("table")
        if not table:
            return {"error": f"No data table found for {state_name} (new URL)"}

        tbody = table.find("tbody") or table
        rows = tbody.find_all("tr")
        results = []

        for tr in rows:
            tds = tr.find_all("td")
            if len(tds) < 12:
                continue

            values = [td.get_text(strip=True) for td in tds]
            bil = values[0]
            if not bil.isdigit():
                continue

            data_point = {
                "state_name": state_name,
                "bil": values[0],
                "id_stesen": values[1],
                "nama_stesen": values[2],
                "daerah": values[3],
                "lembangan": values[4],
                "sub_lembangan": values[5],
                "kemaskini_terakhir": values[6],
                "aras_air_m": values[7],
                "ambang_normal": values[8],
                "ambang_waspada": values[9],
                "ambang_amaran": values[10],
                "ambang_bahaya": values[11],
                "scraped_timestamp": datetime.now(timezone(timedelta(hours=8))).isoformat(),
            }
            results.append(data_point)

        return {"state": state_name, "count": len(results), "data": results}

    except requests.RequestException as e:
        return {"error": f"New URL request failed for {state_name}: {str(e)}"}
    except Exception as e:
        return {"error": f"New URL parsing failed for {state_name}: {str(e)}"}


def scrape_state_data(state_name):
    """
    Try legacy URL first. If it fails or returns no rows, fall back to new URL.
    Output schema is identical for both.
    """
    import logging
    logger = logging.getLogger()
    
    # 1) Try legacy URL
    logger.info(f"Trying legacy API for {state_name}...")
    legacy_result = _scrape_state_legacy(state_name)

    if isinstance(legacy_result, dict) and legacy_result.get("count", 0) > 0:
        logger.info(f"Legacy API success for {state_name}: {legacy_result['count']} records")
        return legacy_result
    else:
        logger.warning(f"Legacy API failed for {state_name}: {legacy_result.get('error', 'No data')}")

    # 2) Fallback to new URL
    logger.info(f"Trying fallback API for {state_name}...")
    new_result = _scrape_state_new(state_name)

    if isinstance(new_result, dict) and new_result.get("count", 0) > 0:
        logger.info(f"Fallback API success for {state_name}: {new_result['count']} records")
        return new_result
    else:
        logger.warning(f"Fallback API also failed for {state_name}: {new_result.get('error', 'No data')}")

    # 3) Both failed / 0 rows – return combined error
    error_msg = "Unknown error"
    if isinstance(legacy_result, dict) and "error" in legacy_result:
        error_msg = f"Legacy: {legacy_result['error']}"
    if isinstance(new_result, dict) and "error" in new_result:
        error_msg = f"{error_msg}; New: {new_result['error']}"

    logger.error(f"Both APIs failed for {state_name}: {error_msg}")
    return {
        "state": state_name,
        "count": 0,
        "data": [],
        "error": error_msg,
    }


def fetch_state_water_level(state_code):
    """
    Fetch water level data for a specific state using the modern API
    """
    import pandas as pd
    from io import StringIO
    
    url = "https://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data/"
    params = {
        "state": state_code,
        "district": "ALL",
        "station": "ALL",
        "lang": "en"
    }
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5"
    }

    try:
        response = requests.get(url, params=params, headers=headers, timeout=60)
        response.raise_for_status()

        # Use pandas to read HTML table
        try:
            dfs = pd.read_html(StringIO(response.text))
            if not dfs:
                return {
                    "error": f"No tables found for state {state_code}",
                    "state_code": state_code,
                    "state_name": STATE_CODE_MAP.get(state_code, f"Unknown-{state_code}"),
                }
            
            df = dfs[0]  # Get the first (and usually only) table
            
            if df.empty:
                return {
                    "state_code": state_code,
                    "state_name": STATE_CODE_MAP.get(state_code, f"Unknown-{state_code}"),
                    "count": 0,
                    "data": [],
                    "message": "No water level data available",
                }
            
            # Add state information to each row
            df["state_code"] = state_code
            df["state_name"] = STATE_CODE_MAP.get(state_code, f"Unknown-{state_code}")
            df["scraped_timestamp"] = datetime.now(timezone(timedelta(hours=8))).isoformat()
            
            # Convert DataFrame to list of dictionaries
            records = df.to_dict('records')
            
            return {
                "state_code": state_code,
                "state_name": STATE_CODE_MAP.get(state_code, f"Unknown-{state_code}"),
                "count": len(records),
                "data": records,
            }
            
        except ValueError as ve:
            return {
                "error": f"Failed to parse HTML table for state {state_code}: {str(ve)}",
                "state_code": state_code,
                "state_name": STATE_CODE_MAP.get(state_code, f"Unknown-{state_code}"),
            }

    except requests.RequestException as e:
        return {
            "error": f"Request failed for state {state_code}: {str(e)}",
            "state_code": state_code,
            "state_name": STATE_CODE_MAP.get(state_code, f"Unknown-{state_code}"),
        }
    except Exception as e:
        return {
            "error": f"Processing failed for state {state_code}: {str(e)}",
            "state_code": state_code,
            "state_name": STATE_CODE_MAP.get(state_code, f"Unknown-{state_code}"),
        }


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


def upload_to_obs(content_str, logger):
    """
    Upload the CSV to OBS.
    Returns the object key.
    """
    if not (OBS_AK and OBS_SK):
        raise RuntimeError("OBS_AK and OBS_SK environment variables must be set")

    obs_client = ObsClient(
        access_key_id=OBS_AK,
        secret_access_key=OBS_SK,
        server=OBS_ENDPOINT,
    )

    MYT = timezone(timedelta(hours=8))
    timestamp = datetime.now(MYT).strftime("%Y%m%d%H%M%S")

    # Upload with timestamp - match CDM job configuration
    object_key = f"{OBS_FOLDER}/{WATERLEVEL_FILENAME}_{timestamp}.csv"
    logger.info(f"[OBS] Uploading CSV to bucket={OBS_BUCKET}, key={object_key}")
    resp = obs_client.putContent(OBS_BUCKET, object_key, content_str)
    if resp.status >= 300:
        raise RuntimeError(
            f"OBS upload failed: status={resp.status}, "
            f"errorCode={getattr(resp, 'errorCode', '')}, "
            f"errorMessage={getattr(resp, 'errorMessage', '')}"
        )

    logger.info("[OBS] Upload successful")
    return object_key


# -----------------------------
# MAIN HANDLER
# -----------------------------
def handler(event, context):
    """
    Main entry point for Huawei FunctionGraph.
    - Scrapes water level data for all states.
    - Uploads CSV to OBS bucket.
    - Triggers CDM job for migration.
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    try:
        logger.info("Starting water level pipeline execution...")
        
        # STEP 1: Scrape water level data
        logger.info("Step 1: Scraping water level data...")
        all_items = []
        
        # Use all Malaysian states for comprehensive coverage  
        state_names = [
            "Johor", "Kedah", "Kelantan", "Melaka", "Negeri Sembilan", "Pahang",
            "Pulau Pinang", "Perak", "Perlis", "Selangor", "Terengganu", "Sabah", 
            "Sarawak", "Wilayah Persekutuan Kuala Lumpur", "Wilayah Persekutuan Labuan", 
            "Wilayah Persekutuan Putrajaya"
        ]
        
        for state_name in state_names:
            logger.info(f"Fetching data for {state_name}...")
            
            result = scrape_state_data(state_name)
            
            if "error" in result:
                logger.warning(f"Warning: {result['error']}")
                continue
                
            if "data" in result and result["data"]:
                all_items.extend(result["data"])
                logger.info(f"Successfully scraped {result['count']} records for {state_name}")
        
        if not all_items:
            raise RuntimeError("No water level data found from any state")
        
        logger.info(f"Total records scraped: {len(all_items)}")
        
        # STEP 2: Convert to CSV
        logger.info("Step 2: Converting data to CSV...")
        csv_body = items_to_csv(all_items)
        
        # STEP 3: Upload to OBS
        logger.info("Step 3: Uploading CSV to OBS...")
        object_key = upload_to_obs(csv_body, logger)
        
        # STEP 4: Start CDM job
        logger.info("Step 4: Triggering CDM migration job...")
        job_name = os.getenv("CDM_JOB_NAME", "waterlevel_functiongraph_trigger")
        cdm_result = start_cdm_job(job_name, logger, context)
        
        logger.info("Pipeline completed successfully")
        
        result = {
            "rows_scraped": len(all_items),
            "bucket": OBS_BUCKET,
            "object_key": object_key,
            "cdm_job": cdm_result,
        }
        
        return {
            "statusCode": 200,
            "body": json.dumps(result),
        }
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        
        return {
            "statusCode": 500,
            "body": json.dumps({
                "success": False,
                "error": str(e),
                "timestamp": datetime.now(timezone(timedelta(hours=8))).isoformat(),
            }),
        }


# -----------------------------
# TESTING/DEBUG MODE
# -----------------------------
if __name__ == "__main__":
    """
    Local testing mode - run this script directly to test functionality
    """
    print("=" * 50)
    print("NAHRIM Water Level Pipeline - Test Mode")
    print("=" * 50)
    
    # Test environment variable loading
    print("\n1. Checking environment variables...")
    required_vars = ["OBS_AK", "OBS_SK", "OBS_BUCKET", "OBS_ENDPOINT", "OBS_FOLDER"]
    optional_vars = ["CDM_ENDPOINT", "CDM_CLUSTER_ID", "CDM_JOB_NAME"]
    
    for var in required_vars:
        value = os.getenv(var)
        status = "✓ SET" if value else "✗ MISSING"
        print(f"  {var}: {status}")
    
    for var in optional_vars:
        value = os.getenv(var)
        status = "✓ SET" if value else "- NOT SET"
        print(f"  {var}: {status}")
    
    # Test single state scraping
    print("\n2. Testing single state scraping (Johor)...")
    test_result = scrape_state_data("Johor")
    
    if "error" in test_result:
        print(f"  ✗ Error: {test_result['error']}")
    else:
        print(f"  ✓ Success: Found {test_result['count']} records for {test_result['state']}")
        if test_result.get('data'):
            print(f"  Sample fields: {list(test_result['data'][0].keys())}")
    
    # Test full scraping (only if single state test passed)
    if "error" not in test_result and test_result['count'] > 0:
        print("\n3. Testing full data scraping...")
        # Test the full pipeline approach
        print("  Testing data aggregation from multiple states...")
        all_items = []
        test_states = ["Johor", "Selangor", "Perlis"]  # Test first 3 states
        for state_name in test_states:
            state_result = scrape_state_data(state_name)
            if "data" in state_result and state_result["data"]:
                all_items.extend(state_result["data"])
        
        if all_items:
            print(f"  ✓ Success: {len(all_items)} total records from test states")
            csv_content = items_to_csv(all_items)
            print(f"  CSV content generated: {len(csv_content)} characters")
        else:
            print(f"  ✗ No data collected from test states")
    else:
        print("\n3. Skipping full scraping test due to single state test failure")
    
    print("\n" + "=" * 50)
    print("Test completed. Use handler() function for full pipeline execution.")
    print("=" * 50)
