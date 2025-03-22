#!/bin/bash

if [[ $# -ne 2 || "$1" != "--jars" ]]; then
    USAGE="USAGE: spark_submit_ingestion_app.sh requires 2 arguments \"--jars\" and a comma-delimited string of Spark JARS"
    echo $USAGE
    exit 1
fi

SPARK_JARS="$2"
echo "Spark jars used for spark submit: $SPARK_JARS"

. $SPARK_HOME/bin/spark-submit \
    --master spark://localhost:7077 \
    --deploy-mode client \
    --jars "$SPARK_JARS" \
    /home/joshuaqiu/Projects/pyspark_etl_framework/src/data_pipeline_app/ingestion/main.py