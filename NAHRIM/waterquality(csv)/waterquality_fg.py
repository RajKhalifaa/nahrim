# -*- coding: utf-8 -*-
"""
FunctionGraph Handler for Malaysia Water Quality Data (MyEQMS)
Saves results to Huawei Cloud OBS (CSV)
"""
import os
import json
import csv
import io
import requests
from datetime import datetime
from obs import ObsClient  # Huawei OBS SDK

OBS_ENDPOINT = os.getenv("OBS_ENDPOINT")
OBS_BUCKET = os.getenv("OBS_BUCKET")
OBS_OBJECT_PREFIX = os.getenv("OBS_FOLDER")

OBS_AK = os.getenv("OBS_AK")  # Access Key
OBS_SK = os.getenv("OBS_SK")  # Secret Key

# State IDs mapping (based on MyEQMS API)
STATE_MAPPING = {
    1: "Johor",
    2: "Kedah",
    3: "Kelantan",
    4: "Melaka",
    5: "Negeri Sembilan",
    6: "Pahang",
    7: "Pulau Pinang",
    8: "Perak",
    9: "Perlis",
    10: "Selangor",
    11: "Terengganu",
    12: "Sabah",
    13: "Sarawak",
    14: "WP Kuala Lumpur",
    15: "WP Labuan",
    16: "WP Putrajaya",
}


def fetch_state_water_quality(state_id):
    """
    Fetch water quality data (CRWQI) for a specific state from MyEQMS API
    """
    base_url = "https://eqms.doe.gov.my/api3/publicportalrqims/crwqi"
    url = f"{base_url}?stateid={state_id}"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()

        # Parse JSON response
        try:
            payload = response.json()
        except json.JSONDecodeError:
            return {
                "error": f"Failed to decode JSON for state_id={state_id}",
                "state_id": state_id,
                "state_name": STATE_MAPPING.get(state_id, f"Unknown-{state_id}"),
            }

        # Extract CRWQI data
        rows = payload.get("crwqi", [])

        if not rows:
            return {
                "state_id": state_id,
                "state_name": STATE_MAPPING.get(state_id, f"Unknown-{state_id}"),
                "count": 0,
                "data": [],
                "message": "No CRWQI data available",
            }

        # Add state_id to each record
        for record in rows:
            record["state_id"] = state_id
            record["state_name"] = STATE_MAPPING.get(state_id, f"Unknown-{state_id}")

        return {
            "state_id": state_id,
            "state_name": STATE_MAPPING.get(state_id, f"Unknown-{state_id}"),
            "count": len(rows),
            "data": rows,
        }

    except requests.RequestException as e:
        return {
            "error": f"Request failed for state_id={state_id}: {str(e)}",
            "state_id": state_id,
            "state_name": STATE_MAPPING.get(state_id, f"Unknown-{state_id}"),
        }
    except Exception as e:
        return {
            "error": f"Processing failed for state_id={state_id}: {str(e)}",
            "state_id": state_id,
            "state_name": STATE_MAPPING.get(state_id, f"Unknown-{state_id}"),
        }


def save_to_obs(data, obs_config):
    """
    Save data to Huawei Cloud OBS **as CSV**

    obs_config should contain:
    - ak: Access Key
    - sk: Secret Key
    - server: OBS endpoint (e.g., obs.ap-southeast-3.myhuaweicloud.com)
    - bucket_name: Target bucket name
    - object_key: Object key (file path in bucket)
    """
    try:
        # Initialize OBS client
        obs_client = ObsClient(
            access_key_id=OBS_AK, secret_access_key=OBS_SK, server=OBS_ENDPOINT
        )

        # ---------- NEW: flatten results → CSV ----------
        rows = []
        for state_result in data.get("results", []):
            if isinstance(state_result, dict) and isinstance(
                state_result.get("data"), list
            ):
                rows.extend(state_result["data"])

        if rows:
            # Build columns from first row
            first = rows[0]
            # Try to keep state info near the end for readability
            extra_cols = ["state_id", "state_name"]
            base_cols = [k for k in first.keys() if k not in extra_cols]
            fieldnames = base_cols + [c for c in extra_cols if c in first]
        else:
            fieldnames = []

        output = io.StringIO()
        if fieldnames:
            writer = csv.DictWriter(output, fieldnames=fieldnames)
            writer.writeheader()
            for r in rows:
                writer.writerow(r)
        else:
            writer = csv.writer(output)  # empty CSV
        csv_content = output.getvalue()
        # ------------------------------------------------

        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        object_key = f"{OBS_OBJECT_PREFIX}/waterquality_myeqms__{timestamp}.csv"

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
    except:
        return None


def handler(event, context):
    """
    FunctionGraph entry point
    """

    try:
        # Parse event body (from APIG)
        if isinstance(event, str):
            event = json.loads(event)

        # Extract body if coming from APIG trigger
        if "body" in event:
            body = event["body"]
            if isinstance(body, str):
                body = json.loads(body)
        else:
            body = event

        # Get OBS configuration (from event or environment variables)
        obs_config = body.get("obs", {})

        obs_settings = {
            "ak": OBS_AK,
            "sk": OBS_SK,
            "server": OBS_ENDPOINT,
            "bucket_name": OBS_BUCKET,
            "folder": OBS_OBJECT_PREFIX,
        }

        # Validate OBS config
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

        # Fetch data from API
        results = []
        for state_id in state_ids_to_fetch:
            result = fetch_state_water_quality(state_id)
            results.append(result)

        # Prepare final data (still JSON for response/CSV generation)
        timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
        request_id = get_request_id(context)

        final_data = {
            "success": True,
            "source": "MyEQMS - Malaysia Environmental Quality Management System",
            "data_type": "CRWQI - Continuous River Water Quality Index",
            "timestamp": timestamp,
            "request_id": request_id,
            "states_processed": len(state_ids_to_fetch),
            "total_records": sum(r.get("count", 0) for r in results if "count" in r),
            "results": results,
        }

        # Generate object key (file path in OBS) – now .csv (for consistency)
        folder = obs_settings["folder"]
        object_key = f"{folder}/waterquality_myeqms_{timestamp}.csv"
        obs_settings["object_key"] = object_key

        # Save to OBS (CSV)
        obs_result = save_to_obs(final_data, obs_settings)

        # Return response (JSON)
        response = {
            "statusCode": 200,
            "isBase64Encoded": False,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(
                {
                    "success": True,
                    "message": "Water quality data fetched and saved successfully",
                    "timestamp": timestamp,
                    "request_id": request_id,
                    "states_processed": len(state_ids_to_fetch),
                    "total_records": sum(
                        r.get("count", 0) for r in results if "count" in r
                    ),
                    "obs_upload": obs_result,
                    "preview": (
                        results[:2] if len(results) > 2 else results
                    ),  # Preview first 2 states
                },
                ensure_ascii=False,
            ),
        }

        return response

    except Exception as e:
        import traceback

        return {
            "statusCode": 500,
            "isBase64Encoded": False,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(
                {"success": False, "error": str(e), "traceback": traceback.format_exc()}
            ),
        }
