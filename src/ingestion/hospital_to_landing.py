import json
import logging
from datetime import datetime, timedelta, timezone

from google.cloud import bigquery, storage
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

# Cấu hình logging cơ bản
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- CONFIGURATIONS ---
PROJECT_ID = 'fpt-fresher-495407'
BUCKET_NAME = 'healthcare_bucket_longnn'
TEMP_FOLDER = 'temp/'
CONFIG_FILE_PATH = f"gs://{BUCKET_NAME}/configs/load_config.csv"

BQ_TEMP_DATASET = 'temp_dataset'
BQ_AUDIT_TABLE = f"{PROJECT_ID}.{BQ_TEMP_DATASET}.audit_log"
BQ_LOG_TABLE = f"{PROJECT_ID}.{BQ_TEMP_DATASET}.pipeline_log"
BQ_TEMP_PATH = f"{BUCKET_NAME}/temp/"

# Cấu hình PostgreSQL trong Cloud SQL
HOSPITAL_A_DB_CONFIG = {
    'url': 'jdbc:postgresql://10.76.192.3:5432/hospital_a_db',
    'driver': 'org.postgresql.Driver',
    'user': 'longnn28',
    'password': 'Pythongold@64'
}
HOSPITAL_B_DB_CONFIG = {
    'url': 'jdbc:postgresql://10.76.192.5:5432/hospital_b',
    'driver': 'org.postgresql.Driver',
    'user': 'longnn28',
    'password': 'Pythongold@64'
}

# Khởi tạo globals (sẽ được gán trong main)
spark = None
storage_client = None
bq_client = None
log_entries = []

# --- FUNCTIONS ---

def log_event(event_type, message, datasource=None, table=None):
    local_tz = timezone(timedelta(hours=7))
    log_entry = {
        'timestamp': datetime.now(timezone.utc).astimezone(local_tz).isoformat(),
        'event_type': event_type,
        'message': message,
        'datasource': datasource,
        'table': table
    }
    log_entries.append(log_entry)
    
    log_msg = f"{log_entry['timestamp']} - {event_type}: {message} (Datasource: {datasource}, Table: {table})"
    if event_type == "ERROR":
        logger.error(log_msg)
    else:
        logger.info(log_msg)

def read_config_file():
    df = spark.read.format("csv").option("header", "true").load(CONFIG_FILE_PATH)
    log_event("INFO", "Config file read successfully")
    return df

def save_logs_to_storage():
    local_tz = timezone(timedelta(hours=7))
    log_file_name = f"pipeline_log_{datetime.now(timezone.utc).astimezone(local_tz).strftime('%Y%m%d_%H%M%S')}.json"
    log_file_path = f"temp/pipeline_logs/{log_file_name}"

    json_data = json.dumps(log_entries, indent=4)
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(log_file_path)
    blob.upload_from_string(json_data, content_type='application/json')
    logger.info(f"Logs saved to {log_file_path} in Cloud Storage")

def save_logs_to_bigquery():
    if log_entries:
        log_df = spark.createDataFrame(log_entries)
        log_df.write.format('bigquery') \
            .option('table', BQ_LOG_TABLE) \
            .option('temporaryGcsBucket', BQ_TEMP_PATH) \
            .mode('append') \
            .save()
        logger.info(f"Logs saved to BigQuery table {BQ_LOG_TABLE}")
    else:
        logger.info("No logs to save to BigQuery")

def move_existing_files_to_archive(datasource, table):
    blob_list = list(storage_client.bucket(BUCKET_NAME).list_blobs(prefix=f"landing/{datasource}/{table}/"))
    existing_files = [b.name for b in blob_list if b.name.endswith('.json')]

    if not existing_files:
        log_event("INFO", f"No existing files to move for {datasource}.{table}", datasource, table)
        return
    
    local_tz = timezone(timedelta(hours=7))
    run_ts = datetime.now(timezone.utc).astimezone(local_tz).strftime('%Y%m%d_%H%M%S')
    
    for file in existing_files:
        source_blob = storage_client.bucket(BUCKET_NAME).blob(file)

        # Trích xuất ngày từ tên file
        date_part = file.split('_')[-1].split('.')[0]
        if len(date_part) >= 8:
            year, month, day = date_part[-4:], date_part[2:4], date_part[:2]
        else:
            year, month, day = "unknown", "unknown", "unknown"
            
        file_name = file.split('/')[-1]

        # Move tới archive
        archive_path = f"landing/{datasource}/archive/{table}/{year}/{month}/{day}/{run_ts}/{file_name}"
        destination_blob = storage_client.bucket(BUCKET_NAME).blob(archive_path)

        # Copy và xóa file gốc
        storage_client.bucket(BUCKET_NAME).copy_blob(source_blob, storage_client.bucket(BUCKET_NAME), archive_path)
        source_blob.delete()

        log_event("INFO", f"Moved file {file} to archive at {archive_path}", datasource, table)

def get_latest_watermark(datasource, table):
    query = f"""
        SELECT MAX(load_timestamp) AS load_timestamp
        FROM `{BQ_AUDIT_TABLE}`
        WHERE data_source = '{datasource}' AND tablename = '{table}'
    """
    query_job = bq_client.query(query)
    result = query_job.result()
    for row in result:
        if row.load_timestamp:
            return row.load_timestamp
    return '1900-01-01 00:00:00'

def extract_and_save_to_landing(datasource, table, load_type, watermark_col):
    audit_schema = StructType([
        StructField("data_source", StringType(), True),
        StructField("tablename", StringType(), True),
        StructField("load_type", StringType(), True),
        StructField("record_count", LongType(), True),
        StructField("load_timestamp", TimestampType(), True),
        StructField("status", StringType(), True),
    ])

    try:
        # Chuẩn hóa load_type
        load_type_norm = str(load_type).strip().lower()

        # Chuẩn hóa watermark_col
        watermark_col = None if watermark_col is None else str(watermark_col).strip()

        # Chỉ lấy watermark nếu là incremental
        if load_type_norm == "incremental":
            last_watermark = get_latest_watermark(datasource, table)
        else:
            last_watermark = None

        log_event(
            "INFO",
            f"Starting data extraction for {datasource}.{table} with load type {load_type_norm} and last watermark {last_watermark}",
            datasource,
            table
        )

        # Build query
        if load_type_norm == "full":
            query = f"(SELECT * FROM public.{table}) AS src"

        elif load_type_norm == "incremental":
            if not watermark_col:
                raise ValueError(f"Missing watermark column for incremental table {datasource}.{table}")

            query = f"""
                (
                    SELECT *
                    FROM public.{table}
                    WHERE {watermark_col} > TIMESTAMP '{last_watermark}'
                ) AS src
            """

        else:
            raise ValueError(f"Invalid load_type: {load_type}")

        db_config = HOSPITAL_A_DB_CONFIG if datasource == 'hospital_a' else HOSPITAL_B_DB_CONFIG

        df = spark.read.format("jdbc") \
            .option("url", db_config['url']) \
            .option("driver", db_config['driver']) \
            .option("user", db_config['user']) \
            .option("password", db_config['password']) \
            .option("dbtable", query) \
            .load()

        log_event("INFO", f"Data extraction completed for {datasource}.{table}", datasource, table)

        local_tz = timezone(timedelta(hours=7))
        today = datetime.now(timezone.utc).astimezone(local_tz).strftime('%d%m%Y')
        json_file_path = f"landing/{datasource}/{table}/{table}_{today}.json"

        # Tính record count trước
        record_count = df.count()

        if record_count > 0:
            bucket = storage_client.bucket(BUCKET_NAME)
            blob = bucket.blob(json_file_path)
            blob.upload_from_string(
                df.toPandas().to_json(orient='records', lines=True),
                content_type='application/json'
            )
            log_event("SUCCESS", f"Data saved to Cloud Storage at {json_file_path}", datasource, table)
        else:
            log_event("INFO", f"No new data found for {datasource}.{table}. Skipping file upload.", datasource, table)

        audit_timestamp = datetime.now(timezone.utc).astimezone(local_tz)

        # Thêm entry vào audit log
        audit_df = spark.createDataFrame(
            [(
                datasource,
                table,
                load_type_norm,
                int(record_count),
                audit_timestamp,
                "SUCCESS"
            )],
            schema=audit_schema
        )

        audit_df.write.format("bigquery") \
            .option("table", BQ_AUDIT_TABLE) \
            .option("temporaryGcsBucket", BQ_TEMP_PATH) \
            .mode("append") \
            .save()

        log_event("INFO", f"Audit log updated for {datasource}.{table}", datasource, table)

    except Exception as e:
        log_event("ERROR", f"Error processing {datasource}.{table}: {str(e)}", datasource, table)


def main():
    global spark, storage_client, bq_client

    # Khởi tạo SparkSession
    spark = SparkSession.builder \
        .appName("HospitalDataIngestion") \
        .getOrCreate()
        
    # Khởi tạo kết nối tới Cloud Storage và BigQuery
    storage_client = storage.Client()
    bq_client = bigquery.Client()

    logger.info("Bắt đầu luồng ingestion từ Hospital sang Landing")

    # Xử lý dữ liệu cho từng bảng theo config
    config_df = read_config_file()
    for row in config_df.collect():
        if row['is_active'] == '1':
            src = row['datasource']
            table_name = row['tablename']
            load_type = row['loadtype']
            watermark = row['watermark']
            
            logger.info("-" * 60)
            logger.info(f"Processing: {src}.{table_name}")
            move_existing_files_to_archive(src, table_name)
            extract_and_save_to_landing(src, table_name, load_type, watermark)

    save_logs_to_storage()
    save_logs_to_bigquery()
    logger.info("Hoàn tất luồng ingestion.")

if __name__ == "__main__":
    main()
