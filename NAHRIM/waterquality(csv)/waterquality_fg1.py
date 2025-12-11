# -*- coding: utf-8 -*-
"""
Automated Water Quality Pipeline (NAHRIM)
Scrape → Save CSV to OBS → Trigger CDM Migration Job
"""

import os
import json
import csv
import io
import requests
from datetime import datetime, timedelta, timezone
from bs4 import BeautifulSoup
from obs import ObsClient


# -----------------------------
# ENVIRONMENT VARIABLES
# -----------------------------
OBS_AK = os.getenv("OBS_AK")
OBS_SK = os.getenv("OBS_SK")
OBS_BUCKET = os.getenv("OBS_BUCKET")
OBS_ENDPOINT = os.getenv("OBS_ENDPOINT")
OBS_FOLDER = os.getenv("OBS_FOLDER")

WATERQUALITY_FILENAME = "waterquality_myeqms"


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
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9"
    }

    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()

        # Parse JSON response
        try:
            payload = response.json()
        except json.JSONDecodeError as json_error:
            return {
                "error": f"Failed to decode JSON for state_id={state_id}: {str(json_error)}",
                "state_id": state_id,
                "state_name": STATE_MAPPING.get(state_id, f"Unknown-{state_id}"),
            }

        # Check if API returned an error
        if "error" in payload:
            return {
                "error": f"API error for state_id={state_id}: {payload['error']}",
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

        # Add state_id and state_name to each record (only if not already present)
        for record in rows:
            # Check if API already provides STATE_ID and STATE_NAME (uppercase)
            if "STATE_ID" not in record:
                record["STATE_ID"] = state_id
            if "STATE_NAME" not in record:
                record["STATE_NAME"] = STATE_MAPPING.get(state_id, f"Unknown-{state_id}")
                
            # Also add lowercase versions for compatibility
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


def scrape_waterquality():
    """
    Scrape water quality data from all states and prepare for CSV export.
    """
    try:
        all_rows = []
        headers = set()
        
        # Iterate through all states
        for state_id in STATE_MAPPING.keys():
            print(f"Fetching data for {STATE_MAPPING[state_id]} (ID: {state_id})")
            
            result = fetch_state_water_quality(state_id)
            
            if "error" in result:
                print(f"Warning: {result['error']}")
                continue
                
            if "data" in result and result["data"]:
                # Add rows from this state
                all_rows.extend(result["data"])
                
                # Collect all unique headers
                for row in result["data"]:
                    headers.update(row.keys())
        
        if not all_rows:
            return {
                "success": False,
                "error": "No water quality data found from any state"
            }
        
        # Convert headers to sorted list for consistent CSV column order
        headers_list = sorted(list(headers))
        
        return {
            "success": True,
            "headers": headers_list,
            "rows": all_rows
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": f"Failed to scrape water quality data: {str(e)}"
        }


# -----------------------------
# SAVE TO OBS
# -----------------------------
def save_to_obs(data, obs_config):
    """
    Save data to Huawei Cloud OBS **as CSV** - exact copy from working waterquality_fg.py
    """
    try:
        # Initialize OBS client
        obs_client = ObsClient(
            access_key_id=OBS_AK, secret_access_key=OBS_SK, server=OBS_ENDPOINT
        )

        # ---------- Flatten results → CSV ----------
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

        timestamp = datetime.now(timezone(timedelta(hours=8))).strftime("%Y%m%d%H%M%S")
        object_key = f"{obs_config['folder']}/{WATERQUALITY_FILENAME}_{timestamp}.csv"

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
    
    if not headers:
        return {"success": False, "error": "No headers specified for CSV"}

    try:
        # Validate required OBS environment variables
        if not all([OBS_AK, OBS_SK, OBS_ENDPOINT, OBS_BUCKET, OBS_FOLDER]):
            return {
                "success": False, 
                "error": "Missing required OBS environment variables"
            }

        client = ObsClient(
            access_key_id=OBS_AK,
            secret_access_key=OBS_SK,
            server=OBS_ENDPOINT,
        )

        MYT = timezone(timedelta(hours=8))
        timestamp = datetime.now(MYT).strftime("%Y-%m-%d_%H-%M-%S")
        object_key = f"{OBS_FOLDER}/{WATERQUALITY_FILENAME}_{timestamp}.csv"

        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=headers)
        writer.writeheader()

        # Handle missing fields gracefully and clean data for JDBC compatibility
        for row in rows:
            # Ensure all headers exist in the row (fill with empty string if missing)
            cleaned_row = {}
            for header in headers:
                value = row.get(header, "")
                # Clean data to prevent JDBC issues
                if value is None:
                    cleaned_row[header] = ""
                elif isinstance(value, (int, float)):
                    # Convert numbers to strings to prevent type issues
                    cleaned_row[header] = str(value)
                else:
                    # Clean string values
                    clean_value = str(value).replace('\x00', '').replace('\n', ' ').replace('\r', ' ').strip()
                    # Escape quotes properly for CSV
                    clean_value = clean_value.replace('"', '""')
                    # Limit length to prevent database field overflow
                    if len(clean_value) > 255:
                        clean_value = clean_value[:255]
                    cleaned_row[header] = clean_value
            writer.writerow(cleaned_row)

        content = output.getvalue()

        resp = client.putContent(
            bucketName=OBS_BUCKET,
            objectKey=object_key,
            content=content,
            metadata={"Content-Type": "text/csv; charset=utf-8"},
        )

        client.close()

        if resp.status < 300:
            return {"success": True, "object_key": object_key, "rows_saved": len(rows)}
        else:
            return {
                "success": False,
                "error": f"OBS upload failed: {resp.errorCode} - {resp.errorMessage}",
            }

    except Exception as e:
        return {"success": False, "error": f"Failed to save to OBS: {str(e)}"}


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


# -----------------------------
# MAIN HANDLER
# -----------------------------
def handler(event, context):
    """
    Main handler for the water quality pipeline.
    """
    import logging
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    try:
        logger.info("Starting water quality pipeline execution...")
        
        # STEP 1: Scrape water quality data
        logger.info("Step 1: Scraping water quality data...")
        result = scrape_waterquality()
        if not result["success"]:
            error_msg = f"Scraping failed: {result['error']}"
            logger.error(error_msg)
            return {
                "statusCode": 500,
                "body": json.dumps({"success": False, "error": error_msg}),
            }

        headers = result["headers"]
        rows = result["rows"]
        logger.info(f"Successfully scraped {len(rows)} records with {len(headers)} fields")

        # STEP 2: Upload CSV to OBS (match working version exactly)
        logger.info("Step 2: Uploading CSV to OBS...")
        
        # Create the data structure that save_to_obs expects
        # Convert our result format to the format that save_to_obs can process
        state_results = []
        
        # Group rows back by state for the expected structure
        from collections import defaultdict
        rows_by_state = defaultdict(list)
        
        for row in rows:
            state_name = row.get("state_name", "Unknown")
            rows_by_state[state_name].append(row)
        
        # Create state results in the format save_to_obs expects
        for state_name, state_rows in rows_by_state.items():
            state_results.append({
                "state_name": state_name,
                "count": len(state_rows),
                "data": state_rows
            })
        
        # Create the exact same data structure as working version
        timestamp_for_data = datetime.now(timezone(timedelta(hours=8))).strftime("%Y%m%d%H%M%S")
        final_data = {
            "success": True,
            "source": "MyEQMS - Malaysia Environmental Quality Management System",
            "data_type": "CRWQI - Continuous River Water Quality Index", 
            "timestamp": timestamp_for_data,
            "states_processed": len(state_results),
            "total_records": len(rows),
            "results": state_results
        }
        
        # Use same save function approach as working version
        obs_settings = {
            "folder": OBS_FOLDER
        }
        
        obs_result = save_to_obs(final_data, obs_settings)
        
        if not obs_result["success"]:
            logger.error(f"OBS upload failed: {obs_result['error']}")
        else:
            logger.info(f"Successfully uploaded to OBS: {obs_result.get('object_key', 'Unknown key')}")

        # STEP 3: Trigger CDM job
        logger.info("Step 3: Triggering CDM migration job...")
        job_name = os.getenv("CDM_JOB_NAME", "waterquality_functiongraph_trigger")
        cdm_result = start_cdm_job(job_name, logger, context)
        
        logger.info("CDM job triggered successfully")

        # STEP 4: Return pipeline summary
        success = obs_result["success"]
        status_code = 200 if success else 206  # 206 = Partial Content (some steps failed)
        
        response_body = {
            "success": success,
            "message": "Water quality pipeline executed",
            "timestamp": datetime.now(timezone(timedelta(hours=8))).isoformat(),
            "rows_scraped": len(rows),
            "obs_upload": obs_result,
            "cdm_job": cdm_result,
        }
        
        print(f"Pipeline completed with status: {'SUCCESS' if success else 'PARTIAL_SUCCESS'}")
        
        return {
            "statusCode": status_code,
            "body": json.dumps(response_body, ensure_ascii=False),
        }

    except Exception as e:
        import traceback
        
        error_msg = f"Pipeline execution failed: {str(e)}"
        error_trace = traceback.format_exc()
        print(f"ERROR: {error_msg}")
        print(f"TRACE: {error_trace}")

        return {
            "statusCode": 500,
            "body": json.dumps(
                {
                    "success": False,
                    "error": error_msg,
                    "timestamp": datetime.now(timezone(timedelta(hours=8))).isoformat(),
                    "trace": error_trace,
                },
                ensure_ascii=False
            ),
        }


# -----------------------------
# TESTING/DEBUG MODE
# -----------------------------
if __name__ == "__main__":
    """
    Local testing mode - run this script directly to test functionality
    """
    print("=" * 50)
    print("NAHRIM Water Quality Pipeline - Test Mode")
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
    
    # Test single state fetch
    print("\n2. Testing single state fetch (Johor - ID: 1)...")
    test_result = fetch_state_water_quality(1)
    
    if "error" in test_result:
        print(f"  ✗ Error: {test_result['error']}")
    else:
        print(f"  ✓ Success: Found {test_result['count']} records for {test_result['state_name']}")
        if test_result['data']:
            print(f"  Sample fields: {list(test_result['data'][0].keys())}")
    
    # Test full scraping (only if single state test passed)
    if "error" not in test_result and test_result['count'] > 0:
        print("\n3. Testing full data scraping...")
        scrape_result = scrape_waterquality()
        
        if scrape_result["success"]:
            print(f"  ✓ Success: {len(scrape_result['rows'])} total records")
            print(f"  Headers: {scrape_result['headers'][:5]}..." if len(scrape_result['headers']) > 5 else f"  Headers: {scrape_result['headers']}")
        else:
            print(f"  ✗ Error: {scrape_result['error']}")
    else:
        print("\n3. Skipping full scraping test due to single state test failure")
    
    print("\n" + "=" * 50)
    print("Test completed. Use handler() function for full pipeline execution.")
    print("=" * 50)
