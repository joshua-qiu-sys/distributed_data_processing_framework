PROJECT_DIR = '/home/joshuaqiu/Projects/pyspark_etl_framework'

LOG_CONF_PATH = 'cfg/logging/log.conf'

BATCH_APP_CONF_ROOT = 'cfg/batch_data_pipeline_app'
BATCH_INGEST_CONF_SUBPATH = 'ingestion'
BATCH_INGEST_ETL_JOBS_CONF_SUBPATH = 'etl_jobs.yml'
BATCH_INGEST_SRC_TO_TGT_CONF_SUBPATH = 'src_to_target.yml'
BATCH_INGEST_SRC_DATA_VALIDATION_CONF_SUBPATH = 'src_data_validation.yml'
BATCH_INGEST_SPARK_APP_CONF_SUBPATH = 'spark_app.yml'
BATCH_TRANSFORM_CONF_SUBPATH = 'transformation'
BATCH_DISTRIBUTE_CONF_SUBPATH = 'distribution'

STREAM_APP_CONF_ROOT = 'cfg/streaming_data_pipeline_app'
STREAM_INGEST_CONF_SUBPATH = 'ingestion'
STREAM_INGEST_KAFKA_CONSUMER_CONF_SUBPATH = 'kafka_consumer.yml'

BATCH_APP_ROOT = 'src/batch_data_pipeline_app'
BATCH_INGEST_SPARK_APP_SUBPATH = 'ingestion/main.py'

DATA_PRODUCERS_CONF_ROOT = 'cfg/data_producers'
RAND_DATA_GEN_CONF_SUBPATH = 'random_data_generator'
RAND_DATA_GEN_KAFKA_PRODUCER_CONF_SUBPATH = 'kafka_producer.yml'

INFRA_CONF_ROOT = 'cfg/infra'
KAFKA_CLUSTER_CONF_SUBPATH = 'kafka_cluster/cluster.properties'

CONNECTORS_CONF_ROOT = 'cfg/connectors'
POSTGRES_CONNECTOR_CONF_SUBPATH = 'postgresql/db_conn.properties'

JARS_CONF_PATH = 'cfg/jars/spark_jars.conf'

JARS_ROOT = 'jars'
POSTGRES_JAR_SUBPATH = 'postgresql/postgresql-42.7.5.jar'