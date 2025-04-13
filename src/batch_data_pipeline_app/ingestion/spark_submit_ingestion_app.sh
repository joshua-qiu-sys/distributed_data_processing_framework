#!/bin/bash

if [[ $# -ne 4 || "$1" != "--jars" || "$3" != "--etl-id" ]]; then
    USAGE="USAGE: spark_submit_ingestion_app.sh requires 4 arguments \"--jars\", a comma-delimited string of Spark JARS, \"--etl-id\" and the etl id"
    echo $USAGE
    exit 1
fi

SPARK_JARS="$2"
echo "Spark jars used for spark submit: $SPARK_JARS"
ETL_ID="$4"
echo "ETL ID used for spark submit: $ETL_ID"

. $SPARK_HOME/bin/spark-submit \
    --master spark://localhost:7077 \
    --deploy-mode client \
    --jars "$SPARK_JARS" \
    /home/joshuaqiu/Projects/distributed_data_processing_framework/src/batch_data_pipeline_app/ingestion/main.py \
    $ETL_ID