import json
import logging
from typing import Dict, Any
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_json_to_postgres(
    target_date: str, 
    minio_bucket: str, 
    minio_key: str, 
    pg_conn_id: str, 
    target_table: str,
    minio_conn_id: str
) -> bool:
    logger.info(f"Initiating data transfer from MinIO ({minio_key}) to PostgreSQL table ({target_table}).")
    
    try:
        s3_hook = S3Hook(aws_conn_id=minio_conn_id)
        # s3_hook.read_key reads the object from S3 and returns a string
        file_content = s3_hook.read_key(key=minio_key, bucket_name=minio_bucket)
        json_data = json.loads(file_content)
    except Exception as e:
        logger.error(f"Failed to read object from MinIO ({minio_key}): {e}")
        return False

    conn = None
    try:
        pg_hook = PostgresHook(postgres_conn_id=pg_conn_id)
        conn = pg_hook.get_conn()
        cur = conn.cursor()
        
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                extracted_date DATE,
                raw_data JSONB,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
        cur.execute(create_table_sql)
        
        cur.execute(f"DELETE FROM {target_table} WHERE extracted_date = %s", (target_date,))
        
        insert_sql = f"INSERT INTO {target_table} (extracted_date, raw_data) VALUES (%s, %s)"
        cur.execute(insert_sql, (target_date, json.dumps(json_data)))
        
        conn.commit()
        cur.close()
        
        logger.info(f"Data load completed successfully for {target_date} into table {target_table}")
        return True
        
    except Exception as e:
        logger.error(f"PostgreSQL Write Error: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()