from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import json
import logging
import sys
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_schiphol_weather(target_date: str) -> Optional[Dict[Any, Any]]:
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": 52.3086,
        "longitude": 4.7639,
        "start_date": target_date,
        "end_date": target_date,
        "hourly": "temperature_2m,wind_speed_10m,precipitation,weather_code",
        "timezone": "UTC"
    }
    
    logger.info(f"Initiating weather data extraction for {target_date} from Open-Meteo API.")
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        if "hourly" not in data or not data["hourly"].get("time"):
            logger.warning(f"No hourly weather data found for {target_date}.")
            return None
            
        logger.info(f"Successfully fetched weather data for {target_date}.")
        return data
        
    except requests.exceptions.RequestException as e:
        logger.error(f"API Request Failed: {e}")
        if 'response' in locals() and response is not None:
             logger.error(f"API Response Payload: {response.text}")
        return None

def upload_to_minio(data: Dict[Any, Any], bucket_name: str, object_name: str, minio_conn_id: str) -> bool:
    try:
        s3_hook = S3Hook(aws_conn_id=minio_conn_id)
        json_data = json.dumps(data)
        s3_hook.load_string(string_data=json_data, key=object_name, bucket_name=bucket_name, replace=True)
        return True
        
    except Exception as e:
        logger.error(f"MinIO Upload Error: {e}")
        return False

if __name__ == "__main__":
    TARGET_AIRPORT = "EHAM"
    
    if len(sys.argv) > 1:
        target_date_str = sys.argv[1]
    else:
        test_date = datetime.now() - timedelta(days=7)
        target_date_str = test_date.strftime("%Y-%m-%d")
        logger.info(f"Target date missing from arguments. Defaulting to 7 days prior: {target_date_str}")
        
    weather_data = get_schiphol_weather(target_date_str)
    
    if weather_data:
        file_key = f"raw/weather/{TARGET_AIRPORT}/{target_date_str}/hourly.json"
        
        success = upload_to_minio(
            data=weather_data,
            bucket_name="flight-weather-bronze",
            object_name=file_key,
            minio_conn_id="minio_conn"
        )
        
        if success:
            logger.info(f"Weather data successfully persisted to {file_key}")