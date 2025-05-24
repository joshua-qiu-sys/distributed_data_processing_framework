from typing import Dict, Optional, Union, Callable
from pathlib import Path
import logging
from src.utils.cfg_management import BaseCfgReader, YamlCfgReader, IniCfgReader
from cfg.resource_paths import STREAM_APP_CONF_ROOT, STREAM_INGEST_CONF_SUBPATH, STREAM_INGEST_KAFKA_CONSUMER_CONF_SUBPATH, \
                               INFRA_CONF_ROOT, KAFKA_CLUSTER_CONF_SUBPATH

STREAM_INGEST_KAFKA_CONSUMER_CONF_PATH = Path(STREAM_APP_CONF_ROOT, STREAM_INGEST_CONF_SUBPATH, STREAM_INGEST_KAFKA_CONSUMER_CONF_SUBPATH)
KAFKA_CLUSTER_CONF_PATH = Path(INFRA_CONF_ROOT, KAFKA_CLUSTER_CONF_SUBPATH)

logger = logging.getLogger(f'streaming_ingestion_app.{__name__}')

class KafkaConsumerCfgReader(BaseCfgReader):
    def __init__(self):
        super().__init__()

    def read_consumer_props_cfg(self,
                                consumer_props_conf_path: Path = STREAM_INGEST_KAFKA_CONSUMER_CONF_PATH,
                                kafka_cluster_conf_path: Optional[Path] = KAFKA_CLUSTER_CONF_PATH,
                                kafka_cluster_conf_section: Optional[str] = 'DEFAULT') -> Dict[str, Union[str, int, Callable]]:
        
        yml_cfg_reader = YamlCfgReader()
        ini_cfg_reader = IniCfgReader()
        if kafka_cluster_conf_path is None:
            consumer_props_cfg = yml_cfg_reader.read_cfg(file_path=consumer_props_conf_path)
            return consumer_props_cfg
        else:
            kafka_cluster_cfg = ini_cfg_reader.read_cfg(file_path=kafka_cluster_conf_path, interpolation=None)[kafka_cluster_conf_section]
            cfg_vars = kafka_cluster_cfg
            rendered_consumer_props_cfg = yml_cfg_reader.read_jinja_templated_cfg(file_path=consumer_props_conf_path,
                                                                                  cfg_vars=cfg_vars)
            return rendered_consumer_props_cfg
        
if __name__ == '__main__':
    
    consumer_cfg_reader = KafkaConsumerCfgReader()
    consumer_props_cfg = consumer_cfg_reader.read_consumer_props_cfg()
    print(f'Consumer properties: {consumer_props_cfg}')