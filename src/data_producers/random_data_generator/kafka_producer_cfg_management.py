from confluent_kafka.serialization import SerializationContext, MessageField
from typing import Dict, Optional, Union, Any
from pathlib import Path
import logging
from src.data_producers.random_data_generator.schema_registry_connector_management import SchemaRegistryConnectorFactoryRegistry, SchemaRegistryConnectorFactory
from src.data_producers.random_data_generator.schema_management import SchemaHandler
from src.data_producers.random_data_generator.serialisations import KafkaMsgConverter
from src.data_producers.random_data_generator.consumer_good_generator import ConsumerGood
from src.utils.cfg_management import BaseCfgReader, YamlCfgReader, IniCfgReader, AbstractCfgHandler, AbstractCfgManager
from src.data_producers.random_data_generator.schema_registry_connector_management import ACCEPTED_SCHEMA_REGISTRIES
from cfg.resource_paths import INFRA_CONF_ROOT, KAFKA_CLUSTER_CONF_SUBPATH, \
                               DATA_PRODUCERS_CONF_ROOT, RAND_DATA_GEN_CONF_SUBPATH, RAND_DATA_GEN_KAFKA_PRODUCER_CONF_SUBPATH, RAND_DATA_GEN_KAFKA_PRODUCER_TOPIC_CONF_SUBPATH, \
                               RAND_DATA_GEN_KAFKA_PRODUCER_MSG_SERIAL_CONF_SUBPATH, RAND_DATA_GEN_KAFKA_PRODUCER_SCHEMA_REGISTRY_CONN_CONF_SUBPATH

KAFKA_CLUSTER_CONF_PATH = Path(INFRA_CONF_ROOT, KAFKA_CLUSTER_CONF_SUBPATH)
RAND_DATA_GEN_KAFKA_PRODUCER_CONF_PATH = Path(DATA_PRODUCERS_CONF_ROOT, RAND_DATA_GEN_CONF_SUBPATH, RAND_DATA_GEN_KAFKA_PRODUCER_CONF_SUBPATH)
RAND_DATA_GEN_KAFKA_PRODUCER_TOPIC_CONF_PATH = Path(DATA_PRODUCERS_CONF_ROOT, RAND_DATA_GEN_CONF_SUBPATH, RAND_DATA_GEN_KAFKA_PRODUCER_TOPIC_CONF_SUBPATH)
RAND_DATA_GEN_KAFKA_PRODUCER_SCHEMA_REGISTRY_CONN_CONF_PATH = Path(DATA_PRODUCERS_CONF_ROOT, RAND_DATA_GEN_CONF_SUBPATH, RAND_DATA_GEN_KAFKA_PRODUCER_SCHEMA_REGISTRY_CONN_CONF_SUBPATH)
RAND_DATA_GEN_KAFKA_PRODUCER_MSG_SERIAL_CONF_PATH = Path(DATA_PRODUCERS_CONF_ROOT, RAND_DATA_GEN_CONF_SUBPATH, RAND_DATA_GEN_KAFKA_PRODUCER_MSG_SERIAL_CONF_SUBPATH)

logger = logging.getLogger(f'random_data_generator.{__name__}')

ACCEPTED_MSG_DATACLASSES = {
    'ConsumerGood': ConsumerGood
}

class KafkaProducerTopicCfgReader(BaseCfgReader):
    def __init__(self):
        super().__init__()

    def read_producer_topic_cfg(self,
                                producer_topic_conf_path: Path = RAND_DATA_GEN_KAFKA_PRODUCER_TOPIC_CONF_PATH) -> Dict[str, str]:
        
        yml_cfg_reader = YamlCfgReader()
        producer_topic_cfg = yml_cfg_reader.read_cfg(file_path=producer_topic_conf_path)
        return producer_topic_cfg

class KafkaProducerPropsCfgReader(BaseCfgReader):
    def __init__(self):
        super().__init__()

    def read_producer_props_cfg(self,
                                producer_props_conf_path: Path = RAND_DATA_GEN_KAFKA_PRODUCER_CONF_PATH,
                                kafka_cluster_conf_path: Optional[Path] = KAFKA_CLUSTER_CONF_PATH,
                                kafka_cluster_conf_section: Optional[str] = 'DEFAULT') -> Dict[str, Union[str, int]]:
        
        yml_cfg_reader = YamlCfgReader()
        ini_cfg_reader = IniCfgReader()
        if kafka_cluster_conf_path is None:
            producer_props_cfg = yml_cfg_reader.read_cfg(file_path=producer_props_conf_path)
            return producer_props_cfg
        else:
            kafka_cluster_cfg = ini_cfg_reader.read_cfg(file_path=kafka_cluster_conf_path, interpolation=None)[kafka_cluster_conf_section]
            rendered_producer_props_cfg = yml_cfg_reader.read_jinja_templated_cfg(file_path=producer_props_conf_path,
                                                                                  cfg_vars=kafka_cluster_cfg)
            return rendered_producer_props_cfg
        
class KafkaProducerSchemaRegistryConnCfgReader(BaseCfgReader):
    def __init__(self):
        super().__init__()

    def read_producer_schema_registry_conn_cfg(self,
                                               producer_schema_registry_conn_conf_path: str = RAND_DATA_GEN_KAFKA_PRODUCER_SCHEMA_REGISTRY_CONN_CONF_PATH,
                                               kafka_cluster_conf_path: Optional[Path] = KAFKA_CLUSTER_CONF_PATH,
                                               kafka_cluster_conf_section: Optional[str] = 'DEFAULT') -> Dict:
        
        yml_cfg_reader = YamlCfgReader()
        ini_cfg_reader = IniCfgReader()

        if kafka_cluster_conf_path is None:
            producer_schema_registry_conn_cfg = yml_cfg_reader.read_cfg(file_path=producer_schema_registry_conn_conf_path)
            return producer_schema_registry_conn_cfg
        
        else:
            kafka_cluster_cfg = ini_cfg_reader.read_cfg(file_path=kafka_cluster_conf_path, interpolation=None)[kafka_cluster_conf_section]
            rendered_producer_schema_registry_conn_cfg = yml_cfg_reader.read_jinja_templated_cfg(file_path=producer_schema_registry_conn_conf_path,
                                                                                                 cfg_vars=kafka_cluster_cfg)
            return rendered_producer_schema_registry_conn_cfg

class KafkaProducerMsgSerialisationCfgReader(BaseCfgReader):
    def __init__(self):
        super().__init__()

    def read_unrendered_msg_serialisation_cfg(self,
                                              producer_msg_serialisation_conf_path: Path = RAND_DATA_GEN_KAFKA_PRODUCER_MSG_SERIAL_CONF_PATH) -> Dict:
        
        yml_cfg_reader = YamlCfgReader()
        unrendered_producer_msg_serialisation_cfg = yml_cfg_reader.read_unrendered_jinja_templated_cfg(file_path=producer_msg_serialisation_conf_path)
        return unrendered_producer_msg_serialisation_cfg

class KafkaProducerMsgSerialisationCfgHandler(AbstractCfgHandler):
    def __init__(self):
        super().__init__()
    
    @classmethod
    def _get_msg_dataclass(cls, msg_dataclass_name: str) -> Any:
        if msg_dataclass_name in ACCEPTED_MSG_DATACLASSES.keys():
            return ACCEPTED_MSG_DATACLASSES[msg_dataclass_name]
        else:
            return None

    def process_cfg(self,
                    producer_topic_cfg: Dict[str, str],
                    producer_schema_registry_conn_cfg: Dict,
                    unrendered_producer_msg_serialisation_cfg: Dict,
                    producer_msg_serialisation_conf_path: Path = RAND_DATA_GEN_KAFKA_PRODUCER_MSG_SERIAL_CONF_PATH) -> Dict:
        
        processed_cfg = self.process_producer_msg_serialisation_cfg(producer_topic_cfg=producer_topic_cfg,
                                                                    producer_schema_registry_conn_cfg=producer_schema_registry_conn_cfg,
                                                                    unrendered_producer_msg_serialisation_cfg=unrendered_producer_msg_serialisation_cfg,
                                                                    producer_msg_serialisation_conf_path=producer_msg_serialisation_conf_path)
        return processed_cfg
            
    def process_producer_msg_serialisation_cfg(self,
                                               producer_topic_cfg: Dict[str, str],
                                               producer_schema_registry_conn_cfg: Dict,
                                               unrendered_producer_msg_serialisation_cfg: Dict,
                                               producer_msg_serialisation_conf_path: Path) -> Dict:
        
        if 'schema_details' in unrendered_producer_msg_serialisation_cfg['key_serialisation'].keys() \
            or 'schema_details' in unrendered_producer_msg_serialisation_cfg['val_serialisation'].keys():

            schema_registry_type = producer_schema_registry_conn_cfg['schema_registry_connector']['type']
            schema_registry_client_conf = producer_schema_registry_conn_cfg['schema_registry_connector']['schema_registry_client_conf']

            schema_registry_connector_factory_registry = SchemaRegistryConnectorFactoryRegistry()
            schema_registry_connector_factory_registry.register_defaults(ACCEPTED_SCHEMA_REGISTRIES)
            schema_registry_connector_factory = SchemaRegistryConnectorFactory(factory_registry=schema_registry_connector_factory_registry)
            schema_registry_connector = schema_registry_connector_factory.create(schema_registry_type=schema_registry_type)

            schema_registry_client = schema_registry_connector.get_schema_registry_client(schema_registry_client_conf=schema_registry_client_conf)
            client = schema_registry_client.get_client()

            if 'schema_details' in unrendered_producer_msg_serialisation_cfg['key_serialisation'].keys():
                key_schema_details = unrendered_producer_msg_serialisation_cfg['key_serialisation']['schema_details']
                key_schema_handler = SchemaHandler(schema_registry_client=schema_registry_client,
                                                   schema_details=key_schema_details)
                key_schema = key_schema_handler.get_schema()
            else:
                val_schema_details = unrendered_producer_msg_serialisation_cfg['val_serialisation']['schema_details']
                val_schema_handler = SchemaHandler(schema_registry_client=schema_registry_client,
                                                   schema_details=val_schema_details)
                val_schema = val_schema_handler.get_schema()

            if 'kafka_msg_converter' in unrendered_producer_msg_serialisation_cfg['key_serialisation'].keys():
                msg_key_dataclass_name = unrendered_producer_msg_serialisation_cfg['key_serialisation']['kafka_msg_converter']['msg_dataclass_name']
                msg_key_dataclass = self.__class__._get_msg_dataclass(msg_dataclass_name=msg_key_dataclass_name)

                kafka_msg_key_converter = KafkaMsgConverter(msg_dataclass=msg_key_dataclass)
                kafka_msg_key_from_dict_callable = kafka_msg_key_converter.get_kafka_msg_from_dict_callable()
                kafka_msg_key_to_dict_callable = kafka_msg_key_converter.get_kafka_msg_to_dict_callable()
            else:
                msg_val_dataclass_name = unrendered_producer_msg_serialisation_cfg['val_serialisation']['kafka_msg_converter']['msg_dataclass_name']
                msg_val_dataclass = self.__class__._get_msg_dataclass(msg_dataclass_name=msg_val_dataclass_name)

                kafka_msg_val_converter = KafkaMsgConverter(msg_dataclass=msg_val_dataclass)
                kafka_msg_val_from_dict_callable = kafka_msg_val_converter.get_kafka_msg_from_dict_callable()
                kafka_msg_val_to_dict_callable = kafka_msg_val_converter.get_kafka_msg_to_dict_callable()

            rendered_producer_msg_serialisation_cfg = unrendered_producer_msg_serialisation_cfg.copy()
            if 'schema_registry_client' in unrendered_producer_msg_serialisation_cfg['key_serialisation']['key_serialiser'].keys():
                rendered_producer_msg_serialisation_cfg['key_serialisation']['key_serialiser']['schema_registry_client'] = client
            
            if 'schema_registry_client' in unrendered_producer_msg_serialisation_cfg['val_serialisation']['val_serialiser'].keys():
                rendered_producer_msg_serialisation_cfg['val_serialisation']['val_serialiser']['schema_registry_client'] = client

            if 'schema_str' in unrendered_producer_msg_serialisation_cfg['key_serialisation']['key_serialiser'].keys():
                rendered_producer_msg_serialisation_cfg['key_serialisation']['key_serialiser']['schema_str'] = key_schema

            if 'schema_str' in unrendered_producer_msg_serialisation_cfg['val_serialisation']['val_serialiser'].keys():
                rendered_producer_msg_serialisation_cfg['val_serialisation']['val_serialiser']['schema_str'] = val_schema

            if 'to_dict' in unrendered_producer_msg_serialisation_cfg['key_serialisation']['key_serialiser'].keys():
                rendered_producer_msg_serialisation_cfg['key_serialisation']['key_serialiser']['to_dict'] = kafka_msg_key_to_dict_callable

            if 'from_dict' in unrendered_producer_msg_serialisation_cfg['key_serialisation']['key_deserialiser'].keys():
                rendered_producer_msg_serialisation_cfg['key_serialisation']['key_deserialiser']['from_dict'] = kafka_msg_key_from_dict_callable

            if 'to_dict' in unrendered_producer_msg_serialisation_cfg['val_serialisation']['val_serialiser'].keys():
                rendered_producer_msg_serialisation_cfg['val_serialisation']['val_serialiser']['to_dict'] = kafka_msg_val_to_dict_callable

            if 'from_dict' in unrendered_producer_msg_serialisation_cfg['val_serialisation']['val_deserialiser'].keys():
                rendered_producer_msg_serialisation_cfg['val_serialisation']['val_deserialiser']['from_dict'] = kafka_msg_val_from_dict_callable

            if 'key_serialisation' in unrendered_producer_msg_serialisation_cfg['key_serialisation'].keys():
                if 'ctx' in unrendered_producer_msg_serialisation_cfg['key_serialisation']['key_serialisation'].keys():
                    rendered_producer_msg_serialisation_cfg['key_serialisation']['key_serialisation']['ctx'] = SerializationContext(producer_topic_cfg['topic_name'], MessageField.KEY)
                    
            if 'val_serialisation' in unrendered_producer_msg_serialisation_cfg['val_serialisation'].keys():
                if 'ctx' in unrendered_producer_msg_serialisation_cfg['val_serialisation']['val_serialisation'].keys():
                    rendered_producer_msg_serialisation_cfg['val_serialisation']['val_serialisation']['ctx'] = SerializationContext(producer_topic_cfg['topic_name'], MessageField.VALUE)

            if 'kafka_msg_converter' in unrendered_producer_msg_serialisation_cfg['key_serialisation'].keys():
                rendered_producer_msg_serialisation_cfg['key_serialisation']['kafka_msg_converter'] = msg_key_dataclass

            if 'kafka_msg_converter' in unrendered_producer_msg_serialisation_cfg['val_serialisation'].keys():
                rendered_producer_msg_serialisation_cfg['val_serialisation']['kafka_msg_converter'] = msg_val_dataclass

            return rendered_producer_msg_serialisation_cfg
        
class KafkaProducerCfgManager(AbstractCfgManager):
    def __init__(self,
                 producer_topic_cfg: Dict[str, str],
                 producer_props_cfg: Dict[str, Union[str, int]],
                 producer_schema_registry_conn_cfg: Dict,
                 producer_msg_serialisation_cfg: Dict):
        
        self.producer_topic_cfg = producer_topic_cfg
        self.producer_props_cfg = producer_props_cfg
        self.producer_schema_registry_conn_cfg = producer_schema_registry_conn_cfg
        self.producer_msg_serialisation_cfg = producer_msg_serialisation_cfg

    def get_producer_topic_cfg(self) -> Dict[str, str]:
        return self.producer_topic_cfg
    
    def get_producer_props_cfg(self) -> Dict[str, Union[str, int]]:
        return self.producer_props_cfg
    
    def get_producer_schema_registry_conn_cfg(self) -> Dict:
        return self.producer_schema_registry_conn_cfg
    
    def get_producer_msg_serialisation_cfg(self) -> Dict:
        return self.producer_msg_serialisation_cfg

if __name__ == '__main__':
    producer_props_cfg_reader = KafkaProducerPropsCfgReader()
    producer_props_cfg = producer_props_cfg_reader.read_producer_props_cfg()
    print(f'Producer properties: {producer_props_cfg}')

    producer_topic_cfg_reader = KafkaProducerTopicCfgReader()
    producer_topic_cfg = producer_topic_cfg_reader.read_producer_topic_cfg()
    print(f'Producer topic properties: {producer_topic_cfg}')

    producer_schema_registry_conn_cfg_reader = KafkaProducerSchemaRegistryConnCfgReader()
    producer_schema_registry_conn_cfg = producer_schema_registry_conn_cfg_reader.read_producer_schema_registry_conn_cfg()
    print(f'Producer schema registry connector properties: {producer_schema_registry_conn_cfg}')

    producer_msg_serialisation_cfg_reader = KafkaProducerMsgSerialisationCfgReader()
    unrendered_producer_msg_serialisation_cfg = producer_msg_serialisation_cfg_reader.read_unrendered_msg_serialisation_cfg()
    print(f'Unrendered producer msg serialisation properties: {unrendered_producer_msg_serialisation_cfg}')

    producer_msg_serialisation_cfg_handler = KafkaProducerMsgSerialisationCfgHandler()
    processed_producer_msg_serialisation_cfg = producer_msg_serialisation_cfg_handler.process_cfg(producer_topic_cfg=producer_topic_cfg,
                                                                                                  producer_schema_registry_conn_cfg=producer_schema_registry_conn_cfg,
                                                                                                  unrendered_producer_msg_serialisation_cfg=unrendered_producer_msg_serialisation_cfg)
    print(f'Processed producer msg serialisation properties: {processed_producer_msg_serialisation_cfg}')

    kafka_producer_cfg_manager = KafkaProducerCfgManager(producer_topic_cfg=producer_topic_cfg,
                                                         producer_props_cfg=producer_props_cfg,
                                                         producer_schema_registry_conn_cfg=producer_schema_registry_conn_cfg,
                                                         producer_msg_serialisation_cfg=processed_producer_msg_serialisation_cfg)
    print(f'Kafka producer config manager - producer properties: {kafka_producer_cfg_manager.get_producer_props_cfg()}')
    print(f'Kafka producer config manager - producer topic properties: {kafka_producer_cfg_manager.get_producer_topic_cfg()}')
    print(f'Kafka producer config manager - producer schema registry connection properties: {kafka_producer_cfg_manager.get_producer_schema_registry_conn_cfg()}')
    print(f'Kafka producer config manager - producer msg serialisation properties: {kafka_producer_cfg_manager.get_producer_msg_serialisation_cfg()}')
