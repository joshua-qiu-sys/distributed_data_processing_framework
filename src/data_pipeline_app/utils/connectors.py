from pyspark.sql import SparkSession, DataFrame
from data_pipeline_app.utils.pyspark_session_builder import PysparkSessionBuilder

class LocalFileConnector:
    def __init__(self, spark: SparkSession, file_path: str):
        self.spark = spark
        self.file_path = file_path

    def read_file(self) -> DataFrame:
        try:
            df = self.spark.read.parquet(self.file_path)
        except Exception as e:
            raise Exception(f'Failed to read file {self.file_path}: {str(e)}')
        return df
    
    def get_file_path(self) -> str:
        return self.file_path
    
if __name__ == '__main__':
    spark_session_builder = PysparkSessionBuilder(app_name='Pyspark App')
    spark = spark_session_builder.get_or_create_spark_session()
    file_connector = LocalFileConnector(spark=spark, file_path='data/raw/dataset1')
    df = file_connector.read_file()