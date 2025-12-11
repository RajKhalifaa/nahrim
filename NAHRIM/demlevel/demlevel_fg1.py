# -*- coding: utf-8 -*-
"""
FunctionGraph Handler for Malaysia Dam/Reservoir Data (SPAN)
Saves results to Huawei Cloud OBS (CSV) and triggers a CDM job.
"""
import os
import json
import csv
import io
import logging
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from obs import ObsClient  # Huawei OBS SDK

# -------------------- OBS CONFIG --------------------

OBS_ENDPOINT = os.getenv("OBS_ENDPOINT")
OBS_BUCKET = os.getenv("OBS_BUCKET")
OBS_OBJECT_PREFIX = os.getenv("OBS_FOLDER")
OBS_FILENAME = "empangan"
OBS_AK = os.getenv("OBS_AK")  # Access Key
OBS_SK = os.getenv("OBS_SK")  # Secret Key

# -------------------- CDM CONFIG --------------------

PROJECT_ID = os.getenv("PROJECT_ID")
CDM_CLUSTER_ID = os.getenv("CDM_CLUSTER_ID")
CDM_ENDPOINT = os.getenv(
    "CDM_ENDPOINT",
    "https://cdm.my-kualalumpur-1.alphaedge.tmone.com.my",
)
CDM_JOB_NAME = os.getenv("CDM_JOB_NAME", "demlevel_functiongraph_trigger")

# State IDs mapping (based on SPAN War Room)
STATE_MAPPING = {
    1: "Johor",
    2: "Kedah",
    3: "Kelantan",
    4: "Labuan",
    5: "Melaka",
    6: "Negeri Sembilan",
    7: "Pahang",
    8: "Perak",
    9: "Perlis",
    10: "Pulau Pinang",
    11: "Selangor",
    12: "Terengganu",
}

# Color to status mapping
COLOR_MAP = {
    "green": "Paras Normal",
    "orange": "Paras Waspada",
    "yellow": "Paras Amaran",
    "red": "Paras Kritikal",
}


def extract_colour_and_label(td):
    """
    Extract color and corresponding status label from table cell
    """
    span = td.find("span")
    style_source = span if span is not None else td
    style = (style_source.get("style") or "").lower()

    colour = None
    for c in COLOR_MAP.keys():
        if f"background:{c}" in style or f"background-color:{c}" in style:
            colour = c
            break

    label = COLOR_MAP.get(colour)
    return colour, label


def scrape_dam_data(state_id, state_name):
    """
    Scrape dam/reservoir data for a specific state from SPAN War Room
    """
    base_url = "https://warroom.span.gov.my/warroom/main/empangan/{}"
    url = base_url.format(state_id)

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    try:
        response = requests.get(url, headers=headers, timeout=60)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, "html.parser")

        # Find the dam table
        table = soup.find("table", {"class": "table"})

        if not table:
            return {
                "error": f"No dam table found for {state_name} (ID={state_id})",
                "state_id": state_id,
                "state_name": state_name,
            }

        # Extract header columns
        thead = table.find("thead")
        if not thead:
            return {
                "error": f"No table header found for {state_name}",
                "state_id": state_id,
                "state_name": state_name,
            }

        base_columns = [th.get_text(" ", strip=True) for th in thead.find_all("th")]

        # Extract body rows
        tbody = table.find("tbody")
        if not tbody:
            return {
                "error": f"No table body found for {state_name}",
                "state_id": state_id,
                "state_name": state_name,
            }

        rows = tbody.find_all("tr")
        results = []

        for tr in rows:
            tds = tr.find_all("td")
            if not tds:
                continue

            # Skip rows that don't have enough columns
            if len(tds) < 8:
                continue

            # Extract text values
            values = [td.get_text(" ", strip=True) for td in tds]

            # Extract color-coded status from columns 6 and 7
            semalam_colour, semalam_label = extract_colour_and_label(tds[6])
            semasa_colour, semasa_label = extract_colour_and_label(tds[7])

            # Create data record
            data_point = {}

            # Map base columns to values
            for i, col_name in enumerate(base_columns):
                if i < len(values):
                    data_point[col_name] = values[i]

            # Add extracted status labels
            data_point["Kategori Simpanan Semalam"] = semalam_label
            data_point["Kategori Simpanan Semasa"] = semasa_label
            data_point["state_id"] = state_id
            data_point["state_name"] = state_name

            results.append(data_point)

        return {
            "state_id": state_id,
            "state_name": state_name,
            "count": len(results),
            "data": results,
        }

    except requests.RequestException as e:
        return {
            "error": f"Request failed for {state_name} (ID={state_id}): {str(e)}",
            "state_id": state_id,
            "state_name": state_name,
        }
    except Exception as e:
        return {
            "error": f"Processing failed for {state_name} (ID={state_id}): {str(e)}",
            "state_id": state_id,
            "state_name": state_name,
        }


def save_to_obs(data):
    """
    Save data to Huawei Cloud OBS **as CSV**
    Returns a dict with success flag and object_key.
    """
    try:
        # Initialize OBS client
        obs_client = ObsClient(
            access_key_id=OBS_AK, secret_access_key=OBS_SK, server=OBS_ENDPOINT
        )

        # ---- flatten results and build CSV ----
        rows = []
        for state_result in data.get("results", []):
            if isinstance(state_result, dict) and isinstance(
                state_result.get("data"), list
            ):
                rows.extend(state_result["data"])

        # Build fieldnames
        if rows:
            # Try to keep a sensible column order
            extra_cols = [
                "Kategori Simpanan Semalam",
                "Kategori Simpanan Semasa",
                "state_id",
                "state_name",
            ]
            first_row = rows[0]
            base_cols = [k for k in first_row.keys() if k not in extra_cols]
            fieldnames = base_cols + [c for c in extra_cols if c in first_row]
        else:
            fieldnames = []

        output = io.StringIO()
        if fieldnames:
            writer = csv.DictWriter(output, fieldnames=fieldnames)
            writer.writeheader()
            for r in rows:
                writer.writerow(r)
        else:
            # No rows: still write an empty CSV with no header
            writer = csv.writer(output)
        csv_content = output.getvalue()
        # ---------------------------------------------

        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        object_key = f"{OBS_OBJECT_PREFIX}/{OBS_FILENAME}_{timestamp}.csv"

        # Upload to OBS
        resp = obs_client.putContent(
            bucketName=OBS_BUCKET,
            objectKey=object_key,
            content=csv_content,
            metadata={"Content-Type": "text/csv"},
        )

        # Close client
        obs_client.close()

        if resp.status < 300:
            return {
                "success": True,
                "bucket": OBS_BUCKET,
                "object_key": object_key,
                "message": "Data saved to OBS successfully",
            }
        else:
            return {
                "success": False,
                "bucket": OBS_BUCKET,
                "object_key": object_key,
                "error": f"OBS upload failed: {resp.errorCode} - {resp.errorMessage}",
            }

    except Exception as e:
        safe_object_key = locals().get("object_key", None)
        return {
            "success": False,
            "bucket": OBS_BUCKET,
            "object_key": safe_object_key,
            "error": f"OBS operation failed: {str(e)}",
        }


def get_request_id(context):
    """
    Safely extract request ID from context
    """
    try:
        if hasattr(context, "request_id"):
            return context.request_id
        elif hasattr(context, "getRequestId"):
            return context.getRequestId()
        else:
            return None
    except Exception:
        return None


def start_cdm_job(job_name, context, logger):
    """
    Call DataArts Migration (CDM) 'Start Job' API to run a job.
    Uses token from FunctionGraph context and env vars for IDs.
    """
    if not (PROJECT_ID and CDM_CLUSTER_ID):
        raise RuntimeError(
            "PROJECT_ID and CDM_CLUSTER_ID must be set as environment variables"
        )

    token = context.getToken()

    url = (
        f"{CDM_ENDPOINT}/v1.1/{PROJECT_ID}"
        f"/clusters/{CDM_CLUSTER_ID}/cdm/job/{job_name}/start"
    )

    headers = {
        "Content-Type": "application/json;charset=utf-8",
        "X-Auth-Token": token,
    }

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

    try:
        return resp.json()
    except Exception:
        return {"raw_body": resp.text}


def handler(event, context):
    """
    FunctionGraph entry point
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    try:
        # Parse event body (from APIG) or timer
        if isinstance(event, str):
            event = json.loads(event)

        # Extract body if coming from APIG trigger
        if isinstance(event, dict) and "body" in event:
            body = event["body"]
            if isinstance(body, str):
                body = json.loads(body)
        else:
            body = event if isinstance(event, dict) else {}

        # Determine which states to fetch
        state_ids_to_fetch = []

        if "state_id" in body:
            # Single state by ID
            state_ids_to_fetch = [body["state_id"]]
        elif "state" in body:
            # Single state by name - find matching ID
            state_name = body["state"]
            state_id = next(
                (
                    k
                    for k, v in STATE_MAPPING.items()
                    if v.lower() == state_name.lower()
                ),
                None,
            )
            if state_id:
                state_ids_to_fetch = [state_id]
            else:
                return {
                    "statusCode": 400,
                    "headers": {"Content-Type": "application/json"},
                    "body": json.dumps(
                        {
                            "success": False,
                            "error": f"State '{state_name}' not found in mapping",
                        }
                    ),
                }
        elif "state_ids" in body:
            # Multiple states by IDs
            state_ids_to_fetch = body["state_ids"]
        elif "states" in body:
            # Multiple states by names
            state_names = body["states"]
            for name in state_names:
                state_id = next(
                    (k for k, v in STATE_MAPPING.items() if v.lower() == name.lower()),
                    None,
                )
                if state_id:
                    state_ids_to_fetch.append(state_id)
        else:
            # Default: fetch all states
            state_ids_to_fetch = list(STATE_MAPPING.keys())

        # Scrape data from SPAN War Room
        results = []
        for state_id in state_ids_to_fetch:
            state_name = STATE_MAPPING.get(state_id, f"Unknown-{state_id}")
            result = scrape_dam_data(state_id, state_name)
            results.append(result)

        # Prepare final data (still JSON in memory)
        timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
        request_id = get_request_id(context)

        final_data = {
            "success": True,
            "source": "SPAN War Room - Suruhanjaya Perkhidmatan Air Negara",
            "data_type": "Dam/Reservoir Water Levels (Empangan)",
            "timestamp": timestamp,
            "request_id": request_id,
            "states_processed": len(state_ids_to_fetch),
            "total_dams": sum(r.get("count", 0) for r in results if "count" in r),
            "results": results,
        }

        # Save to OBS (CSV)
        obs_result = save_to_obs(final_data)
        logger.info(f"[OBS] Upload result: {obs_result}")

        if not obs_result.get("success"):
            # Do not start CDM if upload failed
            raise RuntimeError(
                f"OBS upload failed, not starting CDM: {obs_result.get('error')}"
            )

        # Start CDM job after successful upload
        cdm_job_name = CDM_JOB_NAME or "demlevel_functiongraph_trigger"
        cdm_result = start_cdm_job(cdm_job_name, context, logger)

        # Return response (JSON)
        response_body = {
            "success": True,
            "message": "Dam data scraped, saved to OBS, and CDM job triggered",
            "timestamp": timestamp,
            "request_id": request_id,
            "states_processed": len(state_ids_to_fetch),
            "total_dams": sum(r.get("count", 0) for r in results if "count" in r),
            "obs_upload": obs_result,
            "cdm_job": cdm_result,
            "preview": (results[:2] if len(results) > 2 else results),
        }

        return {
            "statusCode": 200,
            "isBase64Encoded": False,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(response_body, ensure_ascii=False),
        }

    except Exception as e:
        import traceback

        logger.error(f"[handler] Error: {e}")
        logger.error(traceback.format_exc())
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
