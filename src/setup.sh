#!/bin/bash

export PROJECT_DIR=/home/joshuaqiu/Projects/pyspark_etl_framework

export PYSPARK_PYTHON=$(which python3.12)
export PYSPARK_DRIVER_PYTHON=$(which python3.12)

echo "PYSPARK_PYTHON is set to: $PYSPARK_PYTHON"
echo "PYSPARK_DRIVER_PYTHON is set to: $PYSPARK_DRIVER_PYTHON"

export AIRFLOW_HOME="${PROJECT_DIR}/src/job_orchestration/airflow"

echo "AIRFLOW_HOME is set to: $AIRFLOW_HOME"

airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin

echo "Created Airflow admin role"