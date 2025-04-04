#!/bin/bash

CLUSTER_ID=$($CONFLUENT_KAFKA_HOME/bin/kafka-storage random-uuid)
echo "Cluster ID: $CLUSTER_ID"
$CONFLUENT_KAFKA_HOME/bin/kafka-storage format -t "$CLUSTER_ID" -c /home/joshuaqiu/Projects/pyspark_etl_framework/cfg/infra/kafka_cluster/kraft_managed/controllers/controller1.properties
$CONFLUENT_KAFKA_HOME/bin/kafka-storage format -t "$CLUSTER_ID" -c /home/joshuaqiu/Projects/pyspark_etl_framework/cfg/infra/kafka_cluster/kraft_managed/controllers/controller2.properties
$CONFLUENT_KAFKA_HOME/bin/kafka-storage format -t "$CLUSTER_ID" -c /home/joshuaqiu/Projects/pyspark_etl_framework/cfg/infra/kafka_cluster/kraft_managed/controllers/controller3.properties
$CONFLUENT_KAFKA_HOME/bin/kafka-storage format -t "$CLUSTER_ID" -c /home/joshuaqiu/Projects/pyspark_etl_framework/cfg/infra/kafka_cluster/kraft_managed/brokers/broker1.properties
$CONFLUENT_KAFKA_HOME/bin/kafka-storage format -t "$CLUSTER_ID" -c /home/joshuaqiu/Projects/pyspark_etl_framework/cfg/infra/kafka_cluster/kraft_managed/brokers/broker2.properties
$CONFLUENT_KAFKA_HOME/bin/kafka-storage format -t "$CLUSTER_ID" -c /home/joshuaqiu/Projects/pyspark_etl_framework/cfg/infra/kafka_cluster/kraft_managed/brokers/broker3.properties