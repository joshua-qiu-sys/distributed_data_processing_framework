#!/bin/bash

CLUSTER_ID=$($CONFLUENT_KAFKA_HOME/bin/kafka-storage random-uuid)
echo "Cluster ID: $CLUSTER_ID"
$CONFLUENT_KAFKA_HOME/bin/kafka-storage format -t "$CLUSTER_ID" -c /home/joshuaqiu/Projects/pyspark_etl_framework/cfg/infra/kafka_cluster/brokers/kraft_managed/controller1.properties
$CONFLUENT_KAFKA_HOME/bin/kafka-storage format -t "$CLUSTER_ID" -c /home/joshuaqiu/Projects/pyspark_etl_framework/cfg/infra/kafka_cluster/brokers/kraft_managed/controller2.properties
$CONFLUENT_KAFKA_HOME/bin/kafka-storage format -t "$CLUSTER_ID" -c /home/joshuaqiu/Projects/pyspark_etl_framework/cfg/infra/kafka_cluster/brokers/kraft_managed/controller3.properties