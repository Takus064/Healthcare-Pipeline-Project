from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.google.cloud.operators.dataproc import DataprocStartClusterOperator, DataprocStopClusterOperator, DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta
import pendulum

# Config
project_id = 'fpt-fresher-495407'
region = 'asia-southeast1'
cluster_name = 'healthcare-cluster'
composer_bucket = 'asia-southeast1-healthcare--c5ed87e7-bucket'

gcs_job_hospital_to_landing = f'gs://{composer_bucket}/data/ingestion/hospital_to_landing.py'
gcs_npi_validation_pipeline = f'gs://{composer_bucket}/data/ingestion/npi_validation_pipeline.py'
gcs_job_claim_to_bronze = f'gs://{composer_bucket}/data/bronze/claim_to_bronze.py'
gcs_job_cpt_codes_to_bronze = f'gs://{composer_bucket}/data/bronze/cpt_codes_to_bronze.py'

def pyspark_job(main_python_file_uri: str) -> dict:
    return {
        'reference': {'project_id': project_id},
        'placement': {'cluster_name': cluster_name},
        'pyspark_job': {
            'main_python_file_uri': main_python_file_uri,
            'properties': {
                # Cân bằng lại: 1g là mức tối thiểu an toàn cho cụm yếu
                'spark.driver.memory': '1g',
                'spark.executor.memory': '1g',
                'spark.executor.cores': '1',
                'spark.executor.instances': '1',
                # Bật Dynamic Allocation để linh hoạt hơn
                'spark.dynamicAllocation.enabled': 'true',
                'spark.dynamicAllocation.initialExecutors': '1',
                'spark.dynamicAllocation.minExecutors': '1',
                'spark.dynamicAllocation.maxExecutors': '2'
            }
        }
    }



pyspark_job_hospital_to_landing = pyspark_job(gcs_job_hospital_to_landing)
pyspark_job_claim_to_bronze = pyspark_job(gcs_job_claim_to_bronze)
pyspark_job_cpt_codes_to_bronze = pyspark_job(gcs_job_cpt_codes_to_bronze)
pyspark_job_npi_validation = pyspark_job(gcs_npi_validation_pipeline)

default_args = {
    'owner': 'LongNN28',
    'email': ['longnn28@fpt.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id = 'healthcare_etl_pipeline',
    default_args = default_args,
    schedule = '@daily',
    start_date = pendulum.datetime(2026, 1, 1, tz='Asia/Ho_Chi_Minh'),
    catchup=False,
    tags=['healthcare','dataproc','bigquery'],
    description = 'Thực thi luồng ETL, bao gồm chạy Dataproc Cluster và SQL commands'
) as dag:
    start_cluster = DataprocStartClusterOperator(
        task_id = 'start_cluster',
        project_id = project_id,
        region = region,
        cluster_name = cluster_name,
    )

    pyspark_claim_to_bronze = DataprocSubmitJobOperator(
        task_id = 'pyspark_claim_to_bronze',
        job = pyspark_job_claim_to_bronze,
        region = region,
        project_id = project_id
    )

    pyspark_cpt_codes_to_bronze = DataprocSubmitJobOperator(
        task_id = 'pyspark_cpt_codes_to_bronze',
        job = pyspark_job_cpt_codes_to_bronze,
        region = region,
        project_id = project_id
    )

    pyspark_npi_validation_task = DataprocSubmitJobOperator(
        task_id = 'pyspark_npi_validation',
        job = pyspark_job_npi_validation,
        region = region,
        project_id = project_id
    )

    pyspark_hospital_to_landing = DataprocSubmitJobOperator(
        task_id = 'pyspark_hospital_to_landing',
        job = pyspark_job_hospital_to_landing,
        region = region,
        project_id = project_id
    )

    # --- SILVER LAYER ---
    silver_01_dept = BigQueryInsertJobOperator(
        task_id='silver_01_departments',
        configuration={
            "query": {
                "query": "{% include 'sql/silver/01_departments.sql' %}",
                "useLegacySql": False,
            }
        },
        location=region
    )

    silver_02_cpt_codes = BigQueryInsertJobOperator(
        task_id='silver_02_cpt_codes',
        configuration={
            "query": {
                "query": "{% include 'sql/silver/02_cpt_codes.sql' %}",
                "useLegacySql": False,
            }
        },
        location=region
    )

    silver_03_claim_data = BigQueryInsertJobOperator(
        task_id='silver_03_claim_data',
        configuration={
            "query": {
                "query": "{% include 'sql/silver/03_claim_data.sql' %}",
                "useLegacySql": False,
            }
        },
        location=region
    )

    silver_04_providers = BigQueryInsertJobOperator(
        task_id='silver_04_providers',
        configuration={
            "query": {
                "query": "{% include 'sql/silver/04_providers.sql' %}",
                "useLegacySql": False,
            }
        },
        location=region
    )

    silver_05_patients = BigQueryInsertJobOperator(
        task_id='silver_05_patients',
        configuration={
            "query": {
                "query": "{% include 'sql/silver/05_patients.sql' %}",
                "useLegacySql": False,
            }
        },
        location=region
    )

    silver_06_encounters = BigQueryInsertJobOperator(
        task_id='silver_06_encounters',
        configuration={
            "query": {
                "query": "{% include 'sql/silver/06_encounters.sql' %}",
                "useLegacySql": False,
            }
        },
        location=region
    )

    silver_07_transactions = BigQueryInsertJobOperator(
        task_id='silver_07_transactions',
        configuration={
            "query": {
                "query": "{% include 'sql/silver/07_transactions.sql' %}",
                "useLegacySql": False,
            }
        },
        location=region
    )

    # --- GOLD LAYER ---
    gold_01_provider_charge = BigQueryInsertJobOperator(
        task_id='gold_01_provider_charge',
        configuration={
            "query": {
                "query": "{% include 'sql/gold/01_provider_department_charge.sql' %}",
                "useLegacySql": False,
            }
        },
        location=region
    )

    gold_02_patient_history = BigQueryInsertJobOperator(
        task_id='gold_02_patient_history',
        configuration={
            "query": {
                "query": "{% include 'sql/gold/02_patient_history.sql' %}",
                "useLegacySql": False,
            }
        },
        location=region
    )

    gold_03_provider_performance = BigQueryInsertJobOperator(
        task_id='gold_03_provider_performance',
        configuration={
            "query": {
                "query": "{% include 'sql/gold/03_provider_performance.sql' %}",
                "useLegacySql": False,
            }
        },
        location=region
    )

    gold_04_department_analytics = BigQueryInsertJobOperator(
        task_id='gold_04_department_analytics',
        configuration={
            "query": {
                "query": "{% include 'sql/gold/04_department_analytics.sql' %}",
                "useLegacySql": False,
            }
        },
        location=region
    )

    gold_05_financial_metrics = BigQueryInsertJobOperator(
        task_id='gold_05_financial_metrics',
        configuration={
            "query": {
                "query": "{% include 'sql/gold/05_financial_metrics.sql' %}",
                "useLegacySql": False,
            }
        },
        location=region
    )

    # --- CLUSTER MANAGEMENT ---
    stop_cluster = DataprocStopClusterOperator(
        task_id='stop_cluster',
        project_id=project_id,
        region=region,
        cluster_name=cluster_name,
        trigger_rule=TriggerRule.ALL_DONE
    )

    # --- DEPENDENCIES ---
    start_cluster >> pyspark_hospital_to_landing >> pyspark_npi_validation_task
    pyspark_npi_validation_task >> [pyspark_claim_to_bronze, pyspark_cpt_codes_to_bronze]
    [pyspark_claim_to_bronze, pyspark_cpt_codes_to_bronze] >> silver_01_dept
    
    silver_01_dept >> silver_02_cpt_codes >> silver_03_claim_data >> silver_04_providers >> \
    silver_05_patients >> silver_06_encounters >> silver_07_transactions
    
    silver_07_transactions >> [
        gold_01_provider_charge, 
        gold_02_patient_history, 
        gold_03_provider_performance, 
        gold_04_department_analytics, 
        gold_05_financial_metrics
    ]
    
    [
        gold_01_provider_charge, 
        gold_02_patient_history, 
        gold_03_provider_performance, 
        gold_04_department_analytics, 
        gold_05_financial_metrics
    ] >> stop_cluster

