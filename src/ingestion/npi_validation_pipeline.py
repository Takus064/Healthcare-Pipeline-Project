import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from datetime import datetime, timedelta

# --- CONFIGURATIONS ---
PROJECT_ID = 'fpt-fresher-495407'
BUCKET_NAME = 'healthcare_bucket_longnn'

# Lấy ngày từ tham số dòng lệnh (mặc định là hôm nay nếu chạy test)
# Trong Airflow bạn sẽ truyền: --date={{ ds }}
if len(sys.argv) > 1:
    exec_date = sys.argv[1]
else:
    exec_date = datetime.now().strftime("%Y-%m-%d")

# Tính ngày hôm trước cho Change Detection
prev_date_obj = datetime.strptime(exec_date, "%Y-%m-%d") - timedelta(days=1)
prev_date = prev_date_obj.strftime("%Y-%m-%d")

# Paths
NPI_VALIDATION_PATH = f'gs://{BUCKET_NAME}/landing/npi_validation/dt={exec_date}/'
NPI_SNAPSHOT_PATH = f'gs://{BUCKET_NAME}/landing/npi_snapshot/dt={exec_date}/'
NPI_CHANGE_PATH = f'gs://{BUCKET_NAME}/landing/npi_change_detection/dt={exec_date}/'
PREV_SNAPSHOT_PATH = f'gs://{BUCKET_NAME}/landing/npi_snapshot/dt={prev_date}/'

# Cloud SQL Configs
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

npi_api_schema = StructType([
    StructField('npi_found', BooleanType(), True),
    StructField('npi_first_name', StringType(), True),
    StructField('npi_last_name', StringType(), True),
    StructField('npi_organization_name', StringType(), True),
    StructField('npi_position', StringType(), True),
    StructField('npi_last_updated', StringType(), True),
    StructField('enumeration_type', StringType(), True),
    StructField('refreshed_at', StringType(), True)
])

spark = SparkSession.builder.appName("NPI Validation Pipeline").getOrCreate()

def extract_provider_data():
    query = "(SELECT * FROM public.providers) AS src"
    
    def read_db(config, sys_name):
        return spark.read.format('jdbc') \
            .option('url', config['url']).option('driver', config['driver']) \
            .option('user', config['user']).option('password', config['password']) \
            .option('dbtable', query).load() \
            .withColumn('_source_system', F.lit(sys_name))

    df_a = read_db(HOSPITAL_A_DB_CONFIG, 'hospital_a')
    df_b = read_db(HOSPITAL_B_DB_CONFIG, 'hospital_b')
    
    df = df_a.unionByName(df_b) # Dùng unionByName an toàn hơn
    return df.withColumnsRenamed({
        'firstname': 'internal_firstname',
        'lastname': 'internal_lastname',
        'specialization': 'internal_specialization',
        'deptid': 'internal_deptid',
        'npi': 'internal_npi'
    })

def call_npi_api(npi):
    import requests
    from datetime import datetime
    default_return = (False, None, None, None, None, None, None, None)
    if not npi: return default_return
    try:
        url = f'https://npiregistry.cms.hhs.gov/api/?version=2.1&number={npi}'
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data.get('result_count', 0) > 0:
                result = data['results'][0]
                basic = result.get('basic', {})
                enum_type = result.get('enumeration_type')
                
                if enum_type == 'NPI-1':
                    first_name, last_name, org_name = basic.get('first_name'), basic.get('last_name'), None
                    tax = result.get('taxonomies', [])
                    position = tax[0].get('desc') if tax else None
                else:
                    first_name = basic.get('authorized_official_first_name')
                    last_name = basic.get('authorized_official_last_name')
                    org_name, position = basic.get('organization_name'), basic.get('authorized_official_title_or_position')

                return (True, first_name, last_name, org_name, position, 
                        basic.get('last_updated'), enum_type, 
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    except: pass
    return default_return

def process_npi_partition(rows):
    """
    Xử lý API theo từng Partition để tận dụng Multithreading và Connection Pooling.
    """
    import requests
    from concurrent.futures import ThreadPoolExecutor
    from datetime import datetime

    session = requests.Session()
    rows_list = list(rows)
    
    def fetch_data(row):
        npi = row.npi_id
        default_return = (npi, False, None, None, None, None, None, None, None)
        if not npi: return default_return
        
        try:
            url = f'https://npiregistry.cms.hhs.gov/api/?version=2.1&number={npi}'
            response = session.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data.get('result_count', 0) > 0:
                    result = data['results'][0]
                    basic = result.get('basic', {})
                    enum_type = result.get('enumeration_type')
                    
                    if enum_type == 'NPI-1':
                        first_name, last_name, org_name = basic.get('first_name'), basic.get('last_name'), None
                        tax = result.get('taxonomies', [])
                        position = tax[0].get('desc') if tax else None
                    else:
                        first_name = basic.get('authorized_official_first_name')
                        last_name = basic.get('authorized_official_last_name')
                        org_name, position = basic.get('organization_name'), basic.get('authorized_official_title_or_position')

                    return (npi, True, first_name, last_name, org_name, position, 
                            basic.get('last_updated'), enum_type, 
                            datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        except Exception:
            pass
        return default_return

    # Sử dụng 10 threads mỗi partition
    with ThreadPoolExecutor(max_workers=10) as executor:
        return list(executor.map(fetch_data, rows_list))

def detect_changes(current_snapshot_df):
    try:
        prev_df = spark.read.json(PREV_SNAPSHOT_PATH)
        comparison = current_snapshot_df.alias("curr").join(
            prev_df.alias("prev"), 
            F.col("curr.npi_id") == F.col("prev.npi_id"), 
            "left"
        )
        return comparison.select(
            "curr.*",
            F.when(F.col("prev.npi_id").isNull(), "NEW")
             .when(F.col("curr.npi_last_updated") != F.col("prev.npi_last_updated"), "UPDATED")
             .otherwise("UNCHANGED").alias("change_type")
        )
    except Exception:
        return current_snapshot_df.withColumn("change_type", F.lit("NEW"))

def main():
    print(f"--- Bắt đầu Pipeline NPI Validation cho ngày: {exec_date} ---")
    
    # 1. Extract
    print("[1/4] Đang lấy dữ liệu từ Cloud SQL...")
    df_internal = extract_provider_data()
    
    # 2. Enrich & Validate
    print("[2/4] Đang gọi CMS API (Song song hóa qua mapPartitions)...")
    distinct_npi = df_internal.select(F.col('internal_npi').alias('npi_id')).distinct()
    
    # Chuyển qua RDD để dùng mapPartitions
    rdd_enriched = distinct_npi.rdd.mapPartitions(process_npi_partition)
    
    # Tạo Schema mới bao gồm npi_id_key để map
    schema_with_key = npi_api_schema.add("npi_id_key", StringType(), False)
    enriched_df = spark.createDataFrame(rdd_enriched, schema=schema_with_key)
    enriched_df = enriched_df.withColumnRenamed("npi_id_key", "npi_id")
    
    # Join lại với dữ liệu nội bộ
    validation_df = df_internal.join(enriched_df, df_internal.internal_npi == enriched_df.npi_id, "left")
    validation_df = validation_df.withColumn('name_match', 
        (F.upper(F.col('internal_firstname')) == F.upper(F.col('npi_first_name'))) &
        (F.upper(F.col('internal_lastname')) == F.upper(F.col('npi_last_name')))
    ).withColumn('validation_status', 
        F.when(F.col('npi_found') & F.col('name_match'), "VALID")
         .when(F.col('npi_found') & ~F.col('name_match'), "NAME_MISMATCH")
         .otherwise("NPI_NOT_FOUND")
    )
    
    # 3. Save
    print(f"[3/4] Đang lưu kết quả Validation và Snapshot vào GCS...")
    validation_df.write.mode("overwrite").json(NPI_VALIDATION_PATH)
    enriched_df.write.mode("overwrite").json(NPI_SNAPSHOT_PATH)
    
    # 4. Change Detection
    print("[4/4] Đang thực hiện Change Detection so với ngày hôm trước...")
    change_df = detect_changes(enriched_df)
    change_df.write.mode("overwrite").json(NPI_CHANGE_PATH)

    print(f"✅ Pipeline hoàn thành xuất sắc cho ngày: {exec_date}")

if __name__ == "__main__":
    main()
