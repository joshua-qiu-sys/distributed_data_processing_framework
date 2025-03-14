from pyspark.sql import SparkSession, DataFrame
from typing import Dict, Optional, Union
from pathlib import Path
from abc import ABC, abstractmethod
from data_pipeline_app.utils.pyspark_session_builder import PysparkSessionBuilder
from data_pipeline_app.utils.cfg_reader import IniCfgReader
from cfg.resource_paths import CONNECTORS_CONF_PATH, POSTGRES_CONNECTOR_CONF_SUBPATH

class LocalFileConnector:
    def __init__(self, spark: Optional[SparkSession] = None):
        self.spark = spark

    def read_file_as_df(self,
                        file_path: Path,
                        file_type: str = 'parquet',
                        read_props: Optional[Dict[str, Union[str, int]]] = None) -> DataFrame:

        if file_type not in ['parquet', 'csv', 'json', 'orc']:
            raise ValueError(f'Cannot read file. Unknown file type: {file_type}')
        
        if self.spark is None:
            raise Exception('LocalFileConnector must be instantiated with a SparkSession object to read file')

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
                         df: DataFrame,
                         file_path: Path,
                         file_type: str = 'parquet',
                         write_mode: str = 'overwrite',
                         write_props: Optional[Dict[str, Union[str, int]]] = None) -> None:

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
    def __init__(self, db_conn_details: Dict[str, str], spark: Optional[SparkSession] = None):
        self.spark = spark
        self.db_conn_details = db_conn_details

        try:
            self.connect()
        except ConnectionError:
            raise ConnectionError('PostgreSQLConnector could not establish a connection with the provided database connection details')

    def connect(self):
        pass

    def read_db_table_as_df(self, read_props: Dict[str, Union[str, int]]) -> DataFrame:

        if self.spark is None:
            raise Exception('PostgreSQLConnector must be instantiated with a SparkSession object to read database table')
        
        upd_read_props = self.db_conn_details.copy()
        upd_read_props.update(read_props)

        df_reader = self.spark.read.format('jdbc')
        
        for k, v in upd_read_props.items():
            df_reader = df_reader.option(k, v)

        df = df_reader.load()
        return df

    def write_df_to_db_table(self, df: DataFrame, write_props: Dict[str, Union[str, int]], write_mode: str = 'append') -> None:
        
        upd_write_props = self.db_conn_details.copy()
        upd_write_props.update(write_props)

        df_writer = df.write.format('jdbc')

        for k, v in upd_write_props.items():
            df_writer = df_writer.option(k, v)

        df_writer.mode(write_mode).save()
    
if __name__ == '__main__':
    spark_session_builder = PysparkSessionBuilder(app_name='Pyspark App')
    spark = spark_session_builder.get_or_create_spark_session()
    file_connector = LocalFileConnector(spark=spark)
    df = file_connector.read_file_as_df(file_path='data/raw/dataset1', file_type='parquet')
    df.show()

    postgres_conf_path = Path(CONNECTORS_CONF_PATH, POSTGRES_CONNECTOR_CONF_SUBPATH)
    postgres_db_conn_cfg = IniCfgReader().read_cfg(file_path=postgres_conf_path)['DEFAULT']
    print(f'Postgres DB connection cfg:\n{postgres_db_conn_cfg}')
    postgres_db_conn_details = {
        'url': postgres_db_conn_cfg['jdbc_url'],
        'driver': postgres_db_conn_cfg['driver'],
        'schema': postgres_db_conn_cfg['schema'],
        'user': postgres_db_conn_cfg['user'],
        'password': postgres_db_conn_cfg['password']
    }
    postgres_connector = PostgreSQLConnector(spark=spark, db_conn_details=postgres_db_conn_details)
    postgres_db_table_read_props = {
        'schema': 'dev',
        'dbtable': 'dev.test_table'
    }
    df_postgres_table = postgres_connector.read_db_table_as_df(read_props=postgres_db_table_read_props)
    df_postgres_table.show()

    df_write_to_postgres = spark.createDataFrame(data=[{'person': 'John', 'age': 20}, {'person': 'James', 'age': 30}])
    postgres_connector.write_df_to_db_table(df=df_write_to_postgres, write_props={'schema': 'dev', 'dbtable': 'dev.person_table'}, write_mode='overwrite')
    df_write_to_postgres.show()