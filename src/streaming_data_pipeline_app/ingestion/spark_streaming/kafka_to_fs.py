from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, min, max, count, approx_count_distinct
from pyspark.sql.avro.functions import from_avro
from confluent_kafka.schema_registry import SchemaRegistryClient

def main():

    schema_registry_client_conf = {
        'url': 'http://localhost:8081'
    }
    schema_registry_client = SchemaRegistryClient(conf=schema_registry_client_conf)
    schema_res = schema_registry_client.get_latest_version(subject_name='ConsumerGood-value')
    schema_str = schema_res.schema.schema_str

    spark = SparkSession.builder.appName('kafka_to_fs_pyspark_app') \
        .config('spark.sql.shuffle.partitions', '3') \
        .getOrCreate()

    sdf = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9093,localhost:8093,localhost:7093") \
        .option("subscribe", "uncatg_landing_zone") \
        .load()
    
    sdf_parsed = sdf.selectExpr('key', 'substring(value, 6, length(value) - 5) as parsed_value') \
        .select(col('key').cast('string').alias('key'),
                from_avro(col('parsed_value'), schema_str).alias('parsed_value')) \
        .select(col('key'), col('parsed_value.*'))
    
    sdf_windowed = sdf_parsed.withWatermark('txn_timestamp', '5 seconds') \
        .groupBy(window('txn_timestamp', '10 seconds'), 'retailer') \
        .agg(count('item').alias('item_count'),
             approx_count_distinct('item').alias('distinct_item_count'),
             min('price').alias('min_price_observed'),
             max('price').alias('max_price_observed'))
    
    sdf_output = sdf_windowed.select(sdf_windowed.window.start.cast('string').alias('window_start'),
                                     sdf_windowed.window.end.cast('string').alias('window_end'),
                                     'retailer', 'item_count', 'distinct_item_count', 'min_price_observed', 'max_price_observed')

    strm_query = sdf_output \
        .writeStream \
        .format("csv") \
        .option("path", '/home/joshuaqiu/Projects/distributed_data_processing_framework/data/processed/consumer_good_items') \
        .option("checkpointLocation", 'tmp/spark/checkpoints/kafka_to_fs') \
        .outputMode("append") \
        .partitionBy('window_start') \
        .trigger(processingTime='500 milliseconds') \
        .start()
    
    strm_query.awaitTermination()

if __name__ == '__main__':
    main()