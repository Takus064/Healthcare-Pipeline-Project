import csv
import io
import logging
from datetime import datetime, timezone, timedelta
from google.cloud import bigquery, storage

# Cấu hình logging cơ bản
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Thông tin project
PROJECT_ID = 'fpt-fresher-495407'
BUCKET_NAME = 'healthcare_bucket_longnn'
BRONZE_DATASET = 'bronze_dataset'


def get_external_table_name(datasource: str, table: str) -> str:
    """
    External table dùng để đọc current batch file từ GCS Landing.
    """
    return f"ext_{datasource}_{table}"


def get_bronze_table_name(datasource: str, table: str) -> str:
    """
    Native Bronze table lưu dữ liệu append từ Landing.
    """
    return f"{datasource}_{table}"


def create_or_replace_external_table(
    bq_client: bigquery.Client,
    bucket_name: str,
    datasource: str,
    table: str,
    project_id: str = PROJECT_ID,
    bronze_dataset: str = BRONZE_DATASET,
) -> str:
    """
    Tạo hoặc replace external table đọc dữ liệu JSON hiện tại trong GCS Landing.

    Ví dụ:
    gs://healthcare_bucket_longnn/landing/hospital_a/patients/*.json
    → fpt-fresher-495407.bronze_dataset.ext_hospital_a_patients
    """
    ext_table_name = get_external_table_name(datasource, table)
    ext_table_id = f"{project_id}.{bronze_dataset}.{ext_table_name}"

    source_uri = f"gs://{bucket_name}/landing/{datasource}/{table}/*.json"

    table_obj = bigquery.Table(ext_table_id)

    external_config = bigquery.ExternalConfig("NEWLINE_DELIMITED_JSON")
    external_config.source_uris = [source_uri]
    external_config.autodetect = True

    table_obj.external_data_configuration = external_config

    bq_client.delete_table(ext_table_id, not_found_ok=True)
    bq_client.create_table(table_obj)

    return ext_table_id


def create_bronze_table_if_not_exists(
    bq_client: bigquery.Client,
    datasource: str,
    table: str,
    project_id: str = PROJECT_ID,
    bronze_dataset: str = BRONZE_DATASET,
) -> str:
    """
    Tạo native Bronze table nếu chưa tồn tại.
    Schema được lấy từ external table + thêm metadata columns.
    """
    ext_table_name = get_external_table_name(datasource, table)
    bronze_table_name = get_bronze_table_name(datasource, table)

    ext_table = f"`{project_id}.{bronze_dataset}.{ext_table_name}`"
    bronze_table = f"`{project_id}.{bronze_dataset}.{bronze_table_name}`"

    sql = f"""
    CREATE TABLE IF NOT EXISTS {bronze_table}
    PARTITION BY DATE(_bronze_loaded_at)
    AS
    SELECT
      t.*,
      CAST(NULL AS STRING) AS _source_system,
      CAST(NULL AS STRING) AS _source_table,
      CAST(NULL AS STRING) AS _ingestion_run_id,
      CAST(NULL AS TIMESTAMP) AS _bronze_loaded_at,
      CAST(NULL AS STRING) AS _record_hash
    FROM {ext_table} AS t
    WHERE FALSE
    """

    bq_client.query(sql).result()

    return f"{project_id}.{bronze_dataset}.{bronze_table_name}"


def append_current_batch_to_bronze(
    bq_client: bigquery.Client,
    datasource: str,
    table: str,
    run_id: str,
    project_id: str = PROJECT_ID,
    bronze_dataset: str = BRONZE_DATASET,
) -> str:
    """
    Append current batch từ external table vào native Bronze table.

    Có delete theo run_id trước khi insert để tránh duplicate nếu rerun cùng run_id.
    """
    ext_table_name = get_external_table_name(datasource, table)
    bronze_table_name = get_bronze_table_name(datasource, table)

    ext_table = f"`{project_id}.{bronze_dataset}.{ext_table_name}`"
    bronze_table = f"`{project_id}.{bronze_dataset}.{bronze_table_name}`"

    delete_sql = f"""
    DELETE FROM {bronze_table}
    WHERE _source_system = '{datasource}'
      AND _source_table = '{table}'
      AND _ingestion_run_id = '{run_id}'
    """

    insert_sql = f"""
    INSERT INTO {bronze_table}
    SELECT
      t.*,
      '{datasource}' AS _source_system,
      '{table}' AS _source_table,
      '{run_id}' AS _ingestion_run_id,
      CURRENT_TIMESTAMP() AS _bronze_loaded_at,
      TO_HEX(SHA256(TO_JSON_STRING(t))) AS _record_hash
    FROM {ext_table} AS t
    """

    bq_client.query(delete_sql).result()
    bq_client.query(insert_sql).result()

    return f"{project_id}.{bronze_dataset}.{bronze_table_name}"


def load_landing_to_bronze(
    bq_client: bigquery.Client,
    bucket_name: str,
    datasource: str,
    table: str,
    run_id: str,
    project_id: str = PROJECT_ID,
    bronze_dataset: str = BRONZE_DATASET,
) -> dict:
    """
    Hàm orchestration tổng:
    1. Tạo external table đọc Landing current batch
    2. Tạo Bronze native table nếu chưa có
    3. Append current batch vào Bronze
    """
    ext_table_id = create_or_replace_external_table(
        bq_client=bq_client,
        bucket_name=bucket_name,
        datasource=datasource,
        table=table,
        project_id=project_id,
        bronze_dataset=bronze_dataset,
    )

    bronze_table_id = create_bronze_table_if_not_exists(
        bq_client=bq_client,
        datasource=datasource,
        table=table,
        project_id=project_id,
        bronze_dataset=bronze_dataset,
    )

    append_current_batch_to_bronze(
        bq_client=bq_client,
        datasource=datasource,
        table=table,
        run_id=run_id,
        project_id=project_id,
        bronze_dataset=bronze_dataset,
    )

    return {
        "external_table": ext_table_id,
        "bronze_table": bronze_table_id,
        "datasource": datasource,
        "table": table,
        "run_id": run_id,
        "status": "SUCCESS",
    }


def main():
    """Hàm main thực thi toàn bộ luồng xử lý"""
    # Khởi tạo kết nối tới Cloud Storage và BigQuery
    storage_client = storage.Client()
    bq_client = bigquery.Client()

    # Sinh run_id nội bộ cho metadata Bronze
    local_tz = timezone(timedelta(hours=7))
    run_id = datetime.now(timezone.utc).astimezone(local_tz).strftime('%Y%m%d_%H%M%S')
    
    logger.info(f"START: run_id = {run_id}")
    logger.info("-" * 60)

    # Đọc config từ GCS
    try:
        config_blob = storage_client.bucket(BUCKET_NAME).blob('configs/load_config.csv')
        config_content = config_blob.download_as_text()
        reader = csv.DictReader(io.StringIO(config_content))
    except Exception as e:
        logger.error(f"Lỗi khi đọc file config từ GCS: {e}")
        return

    results = []
    errors = []

    for row in reader:
        if row.get('is_active') != '1':
            continue

        datasource = row.get('datasource')
        table = row.get('tablename')
        
        if not datasource or not table:
            continue

        logger.info(f"RUNNING: {datasource}.{table} ...")
        try:
            result = load_landing_to_bronze(
                bq_client=bq_client,
                bucket_name=BUCKET_NAME,
                datasource=datasource,
                table=table,
                run_id=run_id
            )
            results.append(result)
            logger.info(f"OK: {datasource}.{table} -> {result['bronze_table']}")
        except Exception as e:
            errors.append({'datasource': datasource, 'table': table, 'error': str(e)})
            logger.error(f"ERROR: {datasource}.{table}: {e}")

    logger.info("-" * 60)
    logger.info(f"DONE: Thành công: {len(results)}/{len(results)+len(errors)} bảng")
    
    if errors:
        logger.error("Các bảng lỗi:")
        for err in errors:
            logger.error(f"  - {err['datasource']}.{err['table']}: {err['error']}")


if __name__ == "__main__":
    main()
