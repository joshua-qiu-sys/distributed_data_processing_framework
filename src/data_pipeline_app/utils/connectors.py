from pyspark.sql import SparkSession, DataFrame
from typing import Any, Dict, Optional, Union
from pathlib import Path
from abc import ABC, abstractmethod
from data_pipeline_app.utils.pyspark_session_builder import PysparkSessionBuilder
from data_pipeline_app.utils.cfg_reader import IniCfgReader
from cfg.resource_paths import CONNECTORS_CONF_ROOT, POSTGRES_CONNECTOR_CONF_SUBPATH

POSTGRES_CONF_PATH = Path(CONNECTORS_CONF_ROOT, POSTGRES_CONNECTOR_CONF_SUBPATH)

class AbstractConnector(ABC):
    @abstractmethod
    def read_from_source(self, **kwargs) -> DataFrame:
        raise NotImplementedError
    
    @abstractmethod
    def write_to_sink(self, df: DataFrame, **kwargs) -> None:
        raise NotImplementedError

class LocalFileConnector(AbstractConnector):
    def __init__(self, spark: Optional[SparkSession] = None):
        self.spark = spark

    def read_from_source(self, **kwargs) -> DataFrame:
        df = self.read_file_as_df(**kwargs)
        return df
    
    def write_to_sink(self, df: DataFrame, **kwargs) -> None:
        self.write_df_to_file(df=df, **kwargs)

    def read_file_as_df(self,
                        file_path: Path,
                        file_type: str = 'parquet',
                        read_props: Optional[Dict[str, Union[str, int, float]]] = None) -> DataFrame:

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
                         write_props: Optional[Dict[str, Union[str, int, float]]] = None) -> None:

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
    
class ExternalConnectionInterface(ABC):
    @abstractmethod
    def connect(self):
        raise NotImplementedError

class PostgreSQLConnector(AbstractConnector, ExternalConnectionInterface):
    def __init__(self,
                 db_conf_path: Optional[Path] = POSTGRES_CONF_PATH,
                 db_conf_conn_id: Optional[str] = 'DEFAULT',
                 db_conn_details: Optional[Dict[str, str]] = None,
                 spark: Optional[SparkSession] = None):
        
        self.spark = spark
        if db_conn_details is None:
            self.db_conf_path = db_conf_path
            self.db_conf_conn_id = db_conf_conn_id
            self.db_conn_details = IniCfgReader().read_cfg(file_path=db_conf_path, interpolation=None)[db_conf_conn_id]
        
        else:
            self.db_conf_path = None
            self.db_conf_conn_id = None
            self.db_conn_details = db_conn_details

        try:
            self.connect()
        except ConnectionError:
            raise ConnectionError('PostgreSQLConnector could not establish a connection with the provided database connection details')

    def connect(self):
        pass

    def set_db_conf_path(self, db_conf_path: Path) -> None:
        self.db_conf_path = db_conf_path
        self.db_conn_details = IniCfgReader().read_cfg(file_path=self.db_conf_path, interpolation=None)[self.db_conf_conn_id]

    def set_db_conf_conn_id(self, db_conf_conn_id: str) -> None:
        self.db_conf_conn_id = db_conf_conn_id
        self.db_conn_details = IniCfgReader().read_cfg(file_path=self.db_conf_path, interpolation=None)[self.db_conf_conn_id]

    def read_from_source(self, **kwargs) -> DataFrame:
        df = self.read_db_table_as_df(**kwargs)
        return df
    
    def write_to_sink(self, df: DataFrame, **kwargs):
        self.write_df_to_db_table(df=df, **kwargs)

    def read_db_table_as_df(self, db_conf_conn_id: str, schema: str, db_table: str, read_props: Dict[str, Union[str, int, float]] = None) -> DataFrame:

        if self.spark is None:
            raise Exception('PostgreSQLConnector must be instantiated with a SparkSession object to read database table')
        
        if db_conf_conn_id != self.db_conf_conn_id:
            self.set_db_conf_conn_id(db_conf_conn_id)
        
        upd_read_props = self.db_conn_details.copy()
        upd_read_props.update({
            'schema': schema,
            'dbtable': db_table
        })
        if read_props is not None:
            upd_read_props.update(read_props)

        df_reader = self.spark.read.format('jdbc')
        
        for k, v in upd_read_props.items():
            df_reader = df_reader.option(k, v)

        df = df_reader.load()
        return df

    def write_df_to_db_table(self,
                             df: DataFrame,
                             db_conf_conn_id: str,
                             schema: str,
                             db_table: str,
                             write_mode: str = 'append',
                             write_props: Dict[str, Union[str, int, float]] = None) -> None:
        
        if db_conf_conn_id != self.db_conf_conn_id:
            self.set_db_conf_conn_id(db_conf_conn_id)
        
        upd_write_props = self.db_conn_details.copy()
        upd_write_props.update({
            'schema': schema,
            'dbtable': db_table,
        })
        if write_props is not None:
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

    postgres_connector = PostgreSQLConnector(spark=spark)
    df_postgres_table = postgres_connector.read_db_table_as_df(db_conf_conn_id='DEFAULT', schema='dev', db_table='dev.test_table')
    df_postgres_table.show()

    df_write_to_postgres = spark.createDataFrame(data=[{'person': 'John', 'age': 20}, {'person': 'James', 'age': 30}])
    postgres_connector.write_df_to_db_table(df=df_write_to_postgres, db_conf_conn_id='DEFAULT', schema='dev', db_table='dev.test_table', write_mode='overwrite')
    df_write_to_postgres.show()