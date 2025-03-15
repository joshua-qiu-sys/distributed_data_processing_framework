from pyspark.sql import SparkSession
from typing import Dict, Optional
from abc import ABC
from data_pipeline_app.utils.pyspark_session_builder import PysparkSessionBuilder
from data_pipeline_app.utils.connectors import AbstractConnector, LocalFileConnector, PostgreSQLConnector
from data_pipeline_app.utils.cfg_reader import YamlCfgReader

ACCEPTED_CONNECTOR_TYPES = ['local_file', 'postgres']

class AbstractConnectorHandler(ABC):
    def __init__(self, direction: str, raw_connector_cfg: Dict):
        if direction not in ['source', 'sink']:
            raise ValueError('Direction can only be one of "source" or "sink"')
        
        self.direction = direction
        self.raw_connector_cfg = raw_connector_cfg
        self.processed_connector_cfg = None

        if 'connector_type' not in self.raw_connector_cfg.keys():
            raise ValueError(f'Connector type not found in config')
        
        self.connector_type = self.raw_connector_cfg['connector_type']
        
        if self.connector_type not in ACCEPTED_CONNECTOR_TYPES:
            raise ValueError(f'Unrecognised connector type in config: {self.connector_type}')
        
    def set_direction(self, direction: str) -> None:
        if direction not in ['source', 'sink']:
            raise ValueError('Direction can only be one of "source" or "sink"')
        self.direction = direction
    
    def set_raw_connector_cfg(self, raw_connector_cfg: Dict) -> None:
        self.raw_connector_cfg = raw_connector_cfg

        if 'connector_type' not in self.raw_connector_cfg.keys():
            raise ValueError(f'Connector type not found in config')
        
        self._set_connector_type(connector_type=self.raw_connector_cfg['connector_type'])
    
    def set_processed_connector_cfg(self, processed_connector_cfg: Dict) -> None:
        self.processed_connector_cfg = processed_connector_cfg

    def _set_connector_type(self, connector_type: str) -> None:
        self.connector_type = connector_type

    def get_direction(self) -> str:
        return self.direction
    
    def get_connector_type(self) -> str:
        return self.connector_type
    
    def get_raw_connector_cfg(self) -> Dict:
        return self.raw_connector_cfg
    
    def get_processed_connector_cfg(self) -> Optional[Dict]:
        return self.processed_connector_cfg

class ConnectorCfgHandler(AbstractConnectorHandler):
    def __init__(self, direction: str, raw_connector_cfg: Dict):
        super().__init__(direction=direction, raw_connector_cfg=raw_connector_cfg)

    def prepare_processed_cfg_for_connector(self) -> Dict:
        
        match self.connector_type:
            case 'local_file':
                return self.prepare_processed_cfg_for_local_file_connector()
            case 'postgres':
                return self.prepare_processed_cfg_for_postgres_connector()

    def prepare_processed_cfg_for_local_file_connector(self) -> Dict:

        if self.direction == 'source':
            processed_connector_cfg = {
                'file_path': self.raw_connector_cfg['file_path'],
                'file_type': self.raw_connector_cfg['file_type'],
                'read_props': self.raw_connector_cfg['read_props']
            }

        elif self.direction == 'sink':
            processed_connector_cfg = {
                'file_path': self.raw_connector_cfg['file_path'],
                'file_type': self.raw_connector_cfg['file_type'],
                'write_mode': self.raw_connector_cfg['write_mode'],
                'write_props': self.raw_connector_cfg['write_props']
            }

        super().set_processed_connector_cfg(processed_connector_cfg=processed_connector_cfg)

        return processed_connector_cfg

    def prepare_processed_cfg_for_postgres_connector(self) -> Dict:
        
        if self.direction == 'source':
            processed_connector_cfg = {
                'db_conf_conn_id': self.raw_connector_cfg['conn_id'],
                'schema': self.raw_connector_cfg['schema'],
                'db_table': self.raw_connector_cfg['db_table'],
                'read_props': self.raw_connector_cfg['read_props']
            }

        elif self.direction == 'sink':
            processed_connector_cfg = {
                'db_conf_conn_id': self.raw_connector_cfg['conn_id'],
                'schema': self.raw_connector_cfg['schema'],
                'db_table': self.raw_connector_cfg['db_table'],
                'write_mode': self.raw_connector_cfg['write_mode'],
                'write_props': self.raw_connector_cfg['write_props']
            }

        super().set_processed_connector_cfg(processed_connector_cfg=processed_connector_cfg)

        return processed_connector_cfg
    
class ConnectorSelectionHandler(AbstractConnectorHandler):
    def __init__(self, spark: SparkSession, direction: str, raw_connector_cfg: Dict):
        super().__init__(direction=direction, raw_connector_cfg=raw_connector_cfg)
        self.spark = spark
        self.connector_cfg_handler = None

    def get_processed_connector_cfg(self) -> Dict:
        connector_cfg_handler = ConnectorCfgHandler(direction=self.direction, raw_connector_cfg=self.raw_connector_cfg)
        processed_connector_cfg = connector_cfg_handler.prepare_processed_cfg_for_connector()
        super().set_processed_connector_cfg(processed_connector_cfg=processed_connector_cfg)
        return processed_connector_cfg
    
    def get_req_connector(self) -> AbstractConnector:
        
        match self.connector_type:
            case 'local_file':
                connector = LocalFileConnector(spark=self.spark)
            case 'postgres':
                connector = PostgreSQLConnector(spark=self.spark)
        return connector

if __name__ == '__main__':

    etl_id = 'ingest~dataset1'

    yml_cfg_reader = YamlCfgReader()
    src_to_tgt_cfg = yml_cfg_reader.read_cfg(file_path='cfg/data_pipeline_app/01_ingestion/src_to_target.yml')[etl_id]

    spark_session_builder = PysparkSessionBuilder(app_name='Pyspark App')
    spark = spark_session_builder.get_or_create_spark_session()

    src_conn_select_handler = ConnectorSelectionHandler(spark=spark, direction='source', raw_connector_cfg=src_to_tgt_cfg['src'])
    processed_src_conn_cfg = src_conn_select_handler.get_processed_connector_cfg()
    src_connector = src_conn_select_handler.get_req_connector()
    print(f'Processed source connector config: {processed_src_conn_cfg}')
    df = src_connector.read_from_source(**processed_src_conn_cfg)
    df.show()
    
    target_conn_select_handler = ConnectorSelectionHandler(spark=spark, direction='sink', raw_connector_cfg=src_to_tgt_cfg['target'])
    processed_target_conn_cfg = target_conn_select_handler.get_processed_connector_cfg()
    target_connector = target_conn_select_handler.get_req_connector()
    print(f'Processed target connector config: {processed_target_conn_cfg}')
    df = spark.createDataFrame(data=[{'person': 'John', 'age': 20}, {'person': 'James', 'age': 30}])
    df.show()
    target_connector.write_to_sink(df=df, **processed_target_conn_cfg)
