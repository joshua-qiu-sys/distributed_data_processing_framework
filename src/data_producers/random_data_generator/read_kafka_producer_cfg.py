from typing import Dict, Optional, Union
from pathlib import Path
import logging
from src.utils.cfg_reader import BaseCfgReader, YamlCfgReader, IniCfgReader
from cfg.resource_paths import DATA_PRODUCERS_CONF_ROOT, RAND_DATA_GEN_CONF_SUBPATH, \
                               RAND_DATA_GEN_KAFKA_PRODUCER_CONF_SUBPATH, INFRA_CONF_ROOT, KAFKA_CLUSTER_CONF_SUBPATH

RAND_DATA_GEN_KAFKA_PRODUCER_CONF_PATH = Path(DATA_PRODUCERS_CONF_ROOT, RAND_DATA_GEN_CONF_SUBPATH, RAND_DATA_GEN_KAFKA_PRODUCER_CONF_SUBPATH)
KAFKA_CLUSTER_CONF_PATH = Path(INFRA_CONF_ROOT, KAFKA_CLUSTER_CONF_SUBPATH)

logger = logging.getLogger(f'random_data_generator.{__name__}')

class KafkaProducerCfgReader(BaseCfgReader):
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
        
if __name__ == '__main__':
    producer_cfg_reader = KafkaProducerCfgReader()
    producer_props_cfg = producer_cfg_reader.read_producer_props_cfg()
    print(f'Producer properties: {producer_props_cfg}')