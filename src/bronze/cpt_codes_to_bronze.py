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
BQ_CPT_PATH = f"gs://{BUCKET_NAME}/landing/cptcodes/*.csv"

# Spark session
spark = SparkSession.builder \
    .appName("cpt_codes_to_landing") \
    .getOrCreate()

cpt_df = spark.read.csv(f'{BQ_CPT_PATH}',header = True, inferSchema=True)

cpt_df = cpt_df.dropDuplicates()

cpt_df.write.format('bigquery') \
    .option('table', f'{PROJECT_ID}.{BQ_BRONZE_DATASET}.cpt_codes') \
    .option('temporaryGCSBucket', BQ_TEMP_PATH) \
    .mode('overwrite') \
    .save()





