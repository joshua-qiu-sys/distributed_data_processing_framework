from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
import os
import data_pipeline_app.ingestion.main as ing

dag_id = os.path.basename(__file__).replace('.py', '')
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

with DAG(dag_id=dag_id, default_args=default_args) as dag:
    
    t_get_etl_jobs = PythonOperator(task_id='t_get_etl_jobs',
                                    python_callable=ing.get_etl_jobs)
    
    t_get_req_spark_jars = PythonOperator.partial(task_id='t_get_req_spark_jars',
                                                  python_callable=ing.get_req_spark_jars) \
                               .expand(op_args=t_get_etl_jobs.output.map(lambda etl_id: [etl_id]))

    t_get_etl_jobs >> t_get_req_spark_jars