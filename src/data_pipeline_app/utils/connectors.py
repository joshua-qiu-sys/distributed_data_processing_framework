from pyspark.sql import SparkSession, DataFrame
from typing import Dict
from pathlib import Path
from abc import ABC, abstractmethod
from data_pipeline_app.utils.pyspark_session_builder import PysparkSessionBuilder
from data_pipeline_app.utils.cfg_reader import IniCfgReader
from cfg.resource_paths import CONNECTORS_CONF_PATH, POSTGRES_CONNECTOR_CONF_SUBPATH

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
    
class ConnectionInterface(ABC):
    @abstractmethod
    def connect(self):
        raise NotImplementedError

class PostgreSQLConnector(ConnectionInterface):
    def __init__(self, spark: SparkSession, db_conn_details: Dict):
        self.spark = spark
        self.db_conn_details = db_conn_details

    def connect(self):
        pass

    def read_db_table_as_df(self, db_input_loc: Dict = None) -> DataFrame:

        read_props = self.db_conn_details.copy()
        read_props.update(db_input_loc)
        print(f'read props: {read_props}')
        print(f'read props: {read_props.items()}')

        df_reader = self.spark.read \
            .format("jdbc")
        
        for k, v in read_props.items():
            df_reader = df_reader.option(k, v)

        df = df_reader.load()
        return df

    def write_df_to_db_table(self, df: DataFrame) -> None:
        pass
    
if __name__ == '__main__':
    spark_session_builder = PysparkSessionBuilder(app_name='Pyspark App')
    spark = spark_session_builder.get_or_create_spark_session()
    file_connector = LocalFileConnector(spark=spark, file_path='data/raw/dataset1')
    df = file_connector.read_file()

    postgres_conf_path = Path(CONNECTORS_CONF_PATH, POSTGRES_CONNECTOR_CONF_SUBPATH)
    postgres_db_conn_cfg = IniCfgReader().read_cfg(file_path=postgres_conf_path)['DEFAULT']
    postgres_db_conn_details = {
        'url': postgres_db_conn_cfg['jdbc_url'],
        'schema': postgres_db_conn_cfg['schema'],
        'user': postgres_db_conn_cfg['user'],
        'password': postgres_db_conn_cfg['password']
    }
    postgres_connector = PostgreSQLConnector(spark=spark, db_conn_details=postgres_db_conn_details)
    postgres_db_input_loc = {
        'schema': 'dev',
        'dbtable': 'dev.test_table',
        'driver': 'org.postgresql.Driver'
    }
    df_postgres_table = postgres_connector.read_db_table_as_df(db_input_loc=postgres_db_input_loc)
    df_postgres_table.show()