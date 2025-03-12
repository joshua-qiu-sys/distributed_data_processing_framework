from pyspark.sql import SparkSession, DataFrame
from typing import Dict
from pathlib import Path
from abc import ABC, abstractmethod
from data_pipeline_app.utils.pyspark_session_builder import PysparkSessionBuilder
from data_pipeline_app.utils.cfg_reader import IniCfgReader
from cfg.resource_paths import CONNECTORS_CONF_PATH, POSTGRES_CONNECTOR_CONF_SUBPATH

class LocalFileConnector:
    def __init__(self, spark: SparkSession = None):
        self.spark = spark

    def read_file_as_df(self, file_path: Path, file_type: str = 'parquet', read_props: Dict = None) -> DataFrame:

        if file_type not in ['parquet', 'csv', 'json', 'orc']:
            raise ValueError(f'Cannot read file. Unknown file type: {file_type}')
        
        if self.spark is None:
            raise Exception('LocalFileConnector must be instantiated with a SparkSession object')

        df_reader = self.spark.read
        if read_props is not None:
            for k, v in read_props.items():
                df_reader = df_reader.option(k, v)

        try:
            match file_type:
                case 'parquet':
                    df = df_reader.parquet(file_path)
                case 'csv':
                    df= df_reader.csv(file_path)
                case 'json':
                    df = df_reader.json(file_path)
                case 'orc':
                    df = df_reader.orc(file_path)
        except Exception as e:
            raise Exception(f'Failed to read file {file_path}: {str(e)}')
        
        return df
    
    def write_df_to_file(self,
                         df: DataFrame, file_path: Path, file_type: str = 'parquet',
                         write_mode: str = 'overwrite', write_props: Dict = None) -> None:

        if file_type not in ['parquet', 'csv', 'json', 'orc']:
            raise ValueError(f'Cannot write DataFrame to file. Unknown file type: {file_type}')
        
        df_writer = df.write.mode(write_mode)
        if write_props is not None:
            for k, v in write_props.items():
                df_writer = df_writer.option(k, v)

        try:
            match file_type:
                case 'parquet':
                    df_writer.parquet(file_path)
                case 'csv':
                    df_writer.csv(file_path)
                case 'json':
                    df_writer.json(file_path)
                case 'orc':
                    df_writer.orc(file_path)
        except Exception as e:
            raise Exception(f'Failed to write DataFrame to file {file_path}: {str(e)}')
        
        return df
    
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
    file_connector = LocalFileConnector(spark=spark)
    df = file_connector.read_file_as_df(file_path='data/raw/dataset1', file_type='parquet')

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