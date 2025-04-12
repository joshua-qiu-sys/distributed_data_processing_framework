from typing import Dict, Optional
from batch_data_pipeline_app.batch_utils.pyspark_app_initialisers import PysparkAppCfgHandler, PysparkSessionBuilder
from batch_data_pipeline_app.batch_utils.connectors import AbstractConnector, LocalFileConnector, PostgreSQLConnector
from src.utils.constructors import AbstractFactory, AbstractFactoryRegistry
from src.utils.cfg_management import YamlCfgReader, AbstractCfgHandler

ACCEPTED_CONNECTOR_TYPES = {
    'local_file': LocalFileConnector,
    'postgres': PostgreSQLConnector
}

class ConnectorCfgHandler(AbstractCfgHandler):
    def __init__(self, direction: str, raw_connector_cfg: Dict):
        if direction not in ['source', 'sink']:
            raise ValueError('Direction can only be one of "source" or "sink"')
        
        self.direction = direction
        self.raw_connector_cfg = raw_connector_cfg
        self.processed_connector_cfg = None

        if 'connector_type' not in self.raw_connector_cfg.keys():
            raise ValueError(f'Connector type not found in config')
        
        self.connector_type = self.raw_connector_cfg['connector_type']
        
        if self.connector_type not in ACCEPTED_CONNECTOR_TYPES.keys():
            raise ValueError(f'Unrecognised connector type in config: {self.connector_type}')
        
    def set_direction(self, direction: str) -> None:
        if direction not in ['source', 'sink']:
            raise ValueError('Direction can only be either "source" or "sink"')
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
    
    def process_cfg(self) -> Dict:
        return self.prepare_processed_cfg_for_connector()

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

        self.set_processed_connector_cfg(processed_connector_cfg=processed_connector_cfg)

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

        self.set_processed_connector_cfg(processed_connector_cfg=processed_connector_cfg)

        return processed_connector_cfg
    
class ConnectorFactoryRegistry(AbstractFactoryRegistry):
    def __init__(self):
        super().__init__()

    def lookup_registry(self, connector_type: str) -> AbstractConnector:
        if not self.is_registered(connector_type=connector_type):
            return KeyError(f'Connector type "{connector_type}" not found in factory registry')
        connector = self._registry[connector_type]
        return connector

    def is_registered(self, connector_type: str) -> bool:
        return connector_type in self._registry.keys()

    def register(self, connector_type: str, connector: AbstractConnector) -> None:
        if not self.is_registered(connector_type=connector_type):
            self._registry[connector_type] = connector

    def register_defaults(self, default_connector_dict: Dict[str, AbstractConnector]) -> None:
        for connector_type, connector in default_connector_dict.items():
            self.register(connector_type=connector_type, connector=connector)

    def deregister(self, connector_type: str) -> None:
        if not self.is_registered(connector_type=connector_type):
            return KeyError(f'Connector type "{connector_type}" not found in factory registry')
        self._registry.pop(connector_type)
        
    def reset_registry(self):
        self._registry.clear()
    
class ConnectorFactory(AbstractFactory):
    def __init__(self, factory_registry: ConnectorFactoryRegistry):
        super().__init__(factory_registry)

    def create(self, connector_type: str) -> AbstractConnector:
        if not self.factory_registry.is_registered(connector_type=connector_type):
            raise KeyError(f'Connector type "{connector_type}" not found in factory registry')
        connector_cls = self.factory_registry.lookup_registry(connector_type=connector_type)
        connector = connector_cls()
        return connector

if __name__ == '__main__':

    etl_id = 'ingest~dataset1'

    yml_cfg_reader = YamlCfgReader()
    src_to_tgt_cfg = yml_cfg_reader.read_cfg(file_path='cfg/batch_data_pipeline_app/ingestion/src_to_target.yml')[etl_id]

    spark_app_cfg_handler = PysparkAppCfgHandler(spark_app_conf_section=etl_id)
    spark_app_props = spark_app_cfg_handler.get_app_props()
    spark_session_builder = PysparkSessionBuilder(app_name='Pyspark App', app_props=spark_app_props)
    spark = spark_session_builder.get_or_create_spark_session()

    connector_factory_registry = ConnectorFactoryRegistry()
    connector_factory_registry.register_defaults(default_connector_dict=ACCEPTED_CONNECTOR_TYPES)
    connector_factory = ConnectorFactory(factory_registry=connector_factory_registry)

    src_connector_cfg_handler = ConnectorCfgHandler(direction='source', raw_connector_cfg=src_to_tgt_cfg['src'])
    src_connector_type = src_connector_cfg_handler.get_connector_type()
    src_processed_conn_cfg = src_connector_cfg_handler.process_cfg()
    src_connector = connector_factory.create(connector_type=src_connector_type)
    print(f'Processed source connector config: {src_processed_conn_cfg}')
    df = src_connector.read_from_source(spark=spark, **src_processed_conn_cfg)
    df.show()
    
    target_connector_cfg_handler = ConnectorCfgHandler(direction='sink', raw_connector_cfg=src_to_tgt_cfg['target'])
    target_connector_type = target_connector_cfg_handler.get_connector_type()
    target_processed_conn_cfg = target_connector_cfg_handler.process_cfg()
    target_connector = connector_factory.create(connector_type=target_connector_type)
    print(f'Processed target connector config: {target_processed_conn_cfg}')
    df = spark.createDataFrame(data=[{'person': 'John', 'age': 20}, {'person': 'James', 'age': 30}])
    df.show()
    target_connector.write_to_sink(df=df, **target_processed_conn_cfg)