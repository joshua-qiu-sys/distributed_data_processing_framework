export SPARK_MASTER_URL=spark://localhost:7077

export SPARK_MASTER_HOST=localhost
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080

export SPARK_MASTER_CORES=2
export SPARK_MASTER_MEMORY=8G

export SPARK_WORKER_PORT=8881
export SPARK_WORKER_WEBUI_PORT=8081

export SPARK_WORKER_CORES=10
export SPARK_WORKER_MEMORY=24G

export SPARK_WORKER_OPTS="-Dspark.shuffle.service.enabled=true -Dspark.shuffle.service.port=7337"