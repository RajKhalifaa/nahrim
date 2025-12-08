# -*- coding: utf-8 -*-
"""
FunctionGraph Handler for Malaysia Water Level Data (JPS)
Saves results to Huawei Cloud OBS as CSV
"""
import os
import json
import csv
import io
import requests
from bs4 import BeautifulSoup
from urllib.parse import quote
from datetime import datetime
from obs import ObsClient  # Huawei OBS SDK

# -----------------------------
# OBS config from environment
# -----------------------------
OBS_ENDPOINT = os.getenv("OBS_ENDPOINT")
OBS_BUCKET = os.getenv("OBS_BUCKET")
OBS_OBJECT_PREFIX = os.getenv("OBS_FOLDER") or "waterlevel"

OBS_AK = os.getenv("OBS_AK")  # Access Key
OBS_SK = os.getenv("OBS_SK")  # Secret Key

# Default states if event doesn't specify
DEFAULT_STATE_LIST = [
    "Johor",
    "Selangor",
    "Pahang",
    "Negeri Sembilan",
    "Perlis",
    "Kedah",
]


# -----------------------------
# Scraper (your original version)
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
    (this matches your Water_level.ipynb)
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
    # 1) Try legacy URL
    legacy_result = _scrape_state_legacy(state_name)

    if isinstance(legacy_result, dict) and legacy_result.get("count", 0) > 0:
        return legacy_result

    # 2) Fallback to new URL
    new_result = _scrape_state_new(state_name)

    if isinstance(new_result, dict) and new_result.get("count", 0) > 0:
        # Optionally, you could log legacy_result["error"] somewhere
        return new_result

    # 3) Both failed / 0 rows – return combined error
    error_msg = "Unknown error"
    if isinstance(legacy_result, dict) and "error" in legacy_result:
        error_msg = f"Legacy: {legacy_result['error']}"
    if isinstance(new_result, dict) and "error" in new_result:
        error_msg = f"{error_msg}; New: {new_result['error']}"

    return {
        "state": state_name,
        "count": 0,
        "data": [],
        "error": error_msg,
    }


# -----------------------------
# Save ALL results as CSV to OBS
# -----------------------------
def save_to_obs(data, obs_config):
    """
    Flatten data["results"] and upload as CSV to OBS.
    Always writes header row even if there are 0 records.
    """
    try:
        obs_client = ObsClient(
            access_key_id=OBS_AK,
            secret_access_key=OBS_SK,
            server=OBS_ENDPOINT,
        )

        # Flatten all records from results[*]["data"]
        rows = []
        for state_result in data.get("results", []):
            if isinstance(state_result, dict) and isinstance(
                state_result.get("data"), list
            ):
                rows.extend(state_result["data"])

        # Fixed header so you always get columns, even when rows = []
        fieldnames = [
            "state_name",
            "bil",
            "id_stesen",
            "nama_stesen",
            "daerah",
            "lembangan",
            "sub_lembangan",
            "kemaskini_terakhir",
            "aras_air_m",
            "ambang_normal",
            "ambang_waspada",
            "ambang_amaran",
            "ambang_bahaya",
        ]

        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()  # <- header always
        if rows:
            writer.writerows(rows)  # <- only data if available

        csv_content = output.getvalue()

        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        object_key = f"{OBS_OBJECT_PREFIX}/waterlevel_jps__{timestamp}.csv"

        resp = obs_client.putContent(
            bucketName=OBS_BUCKET,
            objectKey=object_key,
            content=csv_content,
            metadata={"Content-Type": "text/csv"},
        )

        obs_client.close()

        if resp.status < 300:
            return {
                "success": True,
                "bucket": OBS_BUCKET,
                "object_key": object_key,
                "message": "Data saved to OBS successfully",
                "row_count": len(rows),
            }
        else:
            return {
                "success": False,
                "error": f"OBS upload failed: {resp.errorCode} - {resp.errorMessage}",
            }

    except Exception as e:
        safe_key = locals().get("object_key", None)
        return {
            "success": False,
            "bucket": OBS_BUCKET,
            "object_key": safe_key,
            "error": f"OBS operation failed: {str(e)}",
        }


def get_request_id(context):
    try:
        if hasattr(context, "request_id"):
            return context.request_id
        elif hasattr(context, "getRequestId"):
            return context.getRequestId()
        else:
            return None
    except:
        return None


# -----------------------------
# FunctionGraph handler
# -----------------------------
def handler(event, context):
    """
    FunctionGraph entry point
    """
    try:
        # Parse event body (from APIG)
        if isinstance(event, str):
            event = json.loads(event)

        if "body" in event:
            body = event["body"]
            if isinstance(body, str):
                body = json.loads(body)
        else:
            body = event

        # OBS config (we rely on env vars; 'obs' in body kept for future)
        obs_config = body.get("obs", {})

        obs_settings = {
            "ak": OBS_AK,
            "sk": OBS_SK,
            "server": OBS_ENDPOINT,
            "bucket_name": OBS_BUCKET,
            "folder": OBS_OBJECT_PREFIX,
        }

        if not all(
            [
                obs_settings["ak"],
                obs_settings["sk"],
                obs_settings["server"],
                obs_settings["bucket_name"],
            ]
        ):
            return {
                "statusCode": 400,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps(
                    {
                        "success": False,
                        "error": "Missing OBS configuration. Required: OBS_AK, OBS_SK, OBS_ENDPOINT, OBS_BUCKET",
                    }
                ),
            }

        # Determine which states to scrape
        if "state" in body:
            states_to_scrape = [body["state"]]
        elif "states" in body:
            states_to_scrape = body["states"]
        else:
            states_to_scrape = DEFAULT_STATE_LIST

        # Scrape each state
        results = []
        for state in states_to_scrape:
            res = scrape_state_data(state)
            results.append(res)

        timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
        request_id = get_request_id(context)

        final_data = {
            "success": True,
            "timestamp": timestamp,
            "request_id": request_id,
            "results": results,
        }

        # Save to OBS as CSV
        obs_result = save_to_obs(final_data, obs_settings)

        response_body = {
            "success": True,
            "message": "Water level data scraped and saved successfully",
            "timestamp": timestamp,
            "request_id": request_id,
            "states_processed": len(states_to_scrape),
            "total_records": sum(r.get("count", 0) for r in results if "count" in r),
            "obs_upload": obs_result,
            "preview": results[:2] if len(results) > 2 else results,
        }

        return {
            "statusCode": 200,
            "isBase64Encoded": False,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(response_body, ensure_ascii=False),
        }

    except Exception as e:
        import traceback

        return {
            "statusCode": 500,
            "isBase64Encoded": False,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(
                {
                    "success": False,
                    "error": str(e),
                    "traceback": traceback.format_exc(),
                }
            ),
        }
