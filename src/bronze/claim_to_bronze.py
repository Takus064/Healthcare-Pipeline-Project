import json
import logging

from google.cloud import bigquery, storage
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


# Project
PROJECT_ID = 'fpt-fresher-495407'

# Cấu hình layer bronze và temp Bigquery
BQ_TEMP_DATASET = 'temp_dataset'
BQ_BRONZE_DATASET = 'bronze_dataset'
BQ_AUDIT_TABLE = f"{PROJECT_ID}.{BQ_TEMP_DATASET}.audit_log"
BQ_LOG_TABLE = f"{PROJECT_ID}.{BQ_TEMP_DATASET}.pipeline_log"

# Cấu hình Cloud Storage (folder temp và landing)
BUCKET_NAME = 'healthcare_bucket_longnn'
BQ_TEMP_PATH = f"gs://{BUCKET_NAME}/temp/"
BQ_CLAIM_PATH = f"gs://{BUCKET_NAME}/landing/claims/*.csv"

# Spark session
spark = SparkSession.builder \
    .appName("claim_to_landing") \
    .getOrCreate()

claims_df = spark.read.csv(f'{BQ_CLAIM_PATH}',header = True, inferSchema=True)

claims_df = claims_df.withColumn('_source_system', F.when(F.input_file_name().contains('hospital1'), 'hospital_a').otherwise('hospital_b'))

claims_df = claims_df.dropDuplicates()

claims_df.write.format('bigquery') \
    .option('table', f'{PROJECT_ID}.{BQ_BRONZE_DATASET}.claims_data') \
    .option('temporaryGCSBucket', BQ_TEMP_PATH) \
    .mode('overwrite') \
    .save()





