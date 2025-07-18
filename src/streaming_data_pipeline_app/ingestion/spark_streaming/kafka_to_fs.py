from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName('kafka_to_fs_pyspark_app').getOrCreate()

    sdf = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9093,localhost:8093,localhost:7093") \
        .option("subscribe", "uncatg_landing_zone") \
        .option("includeHeaders", "true") \
        .load()
    
    sdf = sdf.selectExpr('CAST(key as string)', 'CAST(value as string)', 'headers')

    strm_query = sdf \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .start()
    
    strm_query.awaitTermination()

if __name__ == '__main__':
    main()