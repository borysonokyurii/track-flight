import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import json
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import sys
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_schiphol_flights(app_id: str, app_key: str, schedule_date: str) -> Optional[List[Dict[Any, Any]]]:
    url = "https://api.schiphol.nl/public-flights/flights"
    headers = {
        "Accept": "application/json",
        "ResourceVersion": "v4",
        "app_id": app_id,
        "app_key": app_key
    }
    
    all_flights = []
    page = 0
    
    logger.info(f"Initiating extraction process for {schedule_date} from Schiphol Public API.")
    
    while True:
        params = {
            "flightDirection": "A",
            "includedelays": "false",
            "scheduleDate": schedule_date,
            "page": page,
            "sort": "+scheduleTime"
        }

        try:
            response = requests.get(url, headers=headers, params=params, timeout=10)
            
            if response.status_code == 404:
                logger.info("End of data stream reached (404 Not Found).")
                break
                
            response.raise_for_status()
            data = response.json()
            flights = data.get("flights", [])
            
            if not flights:
                logger.info("Pagination completed. No further records found.")
                break
                
            all_flights.extend(flights)
            logger.info(f"Successfully fetched page {page} with {len(flights)} records. Cumulative count: {len(all_flights)}")
            page += 1
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch page {page}: {e}")
            if 'response' in locals() and response is not None:
                 logger.error(f"API Response Payload: {response.text}")
            break
            
    return all_flights if all_flights else None

def upload_to_minio(data: List[Dict[Any, Any]], bucket_name: str, object_name: str, minio_conn_id: str) -> bool:
    try:
        s3_hook = S3Hook(aws_conn_id=minio_conn_id)
        json_data = json.dumps(data)
        s3_hook.load_string(string_data=json_data, key=object_name, bucket_name=bucket_name, replace=True)
        return True
        
    except Exception as e:
        logger.error(f"S3 Object Upload Error: {e}")
        return False

if __name__ == "__main__":
    APP_ID = os.environ.get("SCHIPHOL_APP_ID")
    APP_KEY = os.environ.get("SCHIPHOL_APP_KEY")
    
    if not APP_ID or not APP_KEY:
        logger.error("Missing Schiphol API credentials. Required environment variables: SCHIPHOL_APP_ID, SCHIPHOL_APP_KEY.")
        exit(1)
    
    TARGET_AIRPORT = "EHAM"
    

    if len(sys.argv) > 1:
        target_date_str = sys.argv[1]
    else:
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        target_date_str = yesterday.strftime("%Y-%m-%d")
        logger.info(f"Target date missing from arguments. Defaulting to: {target_date_str}")
        
    flights_data = get_schiphol_flights(APP_ID, APP_KEY, target_date_str)
    
    if flights_data:
        file_key = f"raw/flights/{TARGET_AIRPORT}/{target_date_str}/arrivals.json"
        upload_to_minio(
            data=flights_data,
            bucket_name="flight-weather-bronze",
            object_name=file_key,
            minio_conn_id="minio_conn"
        )