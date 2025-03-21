#!/bin/bash

. $SPARK_HOME/bin/spark-submit \
    --master spark://localhost:7077 \
    --deploy-mode client \
    --jars /home/joshuaqiu/Projects/pyspark_etl_framework/jars/postgresql/postgresql-42.7.5.jar \
    /home/joshuaqiu/Projects/pyspark_etl_framework/src/data_pipeline_app/ingestion/main.py