from pyspark.sql import SparkSession

class PysparkSessionBuilder:
    def __init__(self, app_name: str):
        self.app_name = app_name

    def get_or_create_spark_session(self, **kwargs) -> SparkSession:
        spark_builder = SparkSession \
            .builder \
            .appName(self.app_name)
        
        kwargs.update({
            'spark.sql.shuffle.partitions': 20,
            'spark.driver.memory': '8g',
            'spark.executor.memory': '8g',
            'spark.executor.cores': '2',
            'spark.executor.instances': '3'
        })
        
        for k, v in kwargs.items():
            spark_builder = spark_builder.config(k, v)

        try:
            spark = spark_builder.getOrCreate()
        except Exception as e:
            raise Exception(f'Failed to get or create Spark session: {str(e)}')

        return spark
    
    def get_app_name(self) -> str:
        return self.app_name

if __name__ == '__main__':
    spark_session_builder = PysparkSessionBuilder(app_name='Pyspark App')
    spark = spark_session_builder.get_or_create_spark_session()