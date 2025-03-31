from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
import os
import datetime as dt
from pathlib import Path
from batch_data_pipeline_app.ingestion.main import DataIngestionBatchMetadata
from cfg.resource_paths import PROJECT_DIR, BATCH_APP_ROOT, BATCH_INGEST_SPARK_APP_SUBPATH

BATCH_INGEST_SPARK_APP_PATH = Path(PROJECT_DIR, BATCH_APP_ROOT, BATCH_INGEST_SPARK_APP_SUBPATH)

dag_id = os.path.basename(__file__).replace('.py', '')
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

ingest_batch_metadata = DataIngestionBatchMetadata()
ingest_batch_metadata.get_etl_jobs_from_conf()

with DAG(dag_id=dag_id, default_args=default_args) as dag:
    
    t_get_etl_jobs = PythonOperator(task_id='t_get_etl_jobs',
                                    python_callable=ingest_batch_metadata.get_etl_jobs,
                                    execution_timeout=dt.timedelta(minutes=5))
    
    t_get_req_spark_jars = PythonOperator(task_id='t_get_req_spark_jars',
                                          python_callable=ingest_batch_metadata.get_req_spark_jars,
                                          execution_timeout=dt.timedelta(minutes=5))
    
    t_submit_spark_jobs = SparkSubmitOperator \
                              .partial(task_id='t_submit_spark_jobs',
                                       application=str(BATCH_INGEST_SPARK_APP_PATH),
                                       conn_id='spark_standalone',
                                       deploy_mode='client',
                                       execution_timeout=dt.timedelta(minutes=15)) \
                              .expand_kwargs(t_get_etl_jobs.output.zip(t_get_req_spark_jars.output) \
                                                 .map(lambda spark_job_inputs: {
                                                         "application_args": [spark_job_inputs[0]],
                                                         "jars": spark_job_inputs[1]}))

    t_get_etl_jobs >> t_get_req_spark_jars >> t_submit_spark_jobs