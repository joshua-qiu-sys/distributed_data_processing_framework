import yaml
from pathlib import Path
from typing import Dict
import logging
from cfg.resource_paths import APP_CONF_ROOT, INGEST_CONF_SUBPATH, INGEST_SRC_TO_TGT_CONF_SUBPATH, INGEST_SRC_DATA_VALIDATION_CONF_SUBPATH

SRC_TO_TGT_CONF_PATH = Path(APP_CONF_ROOT, INGEST_CONF_SUBPATH, INGEST_SRC_TO_TGT_CONF_SUBPATH)
SRC_DATA_VALIDATION_CONF_PATH = Path(APP_CONF_ROOT, INGEST_CONF_SUBPATH, INGEST_SRC_DATA_VALIDATION_CONF_SUBPATH)

logger = logging.getLogger(f'pyspark_ingestion_app.{__name__}')

class IngestionCfgReader:
    def __init__(self,
                 src_to_tgt_conf_path: Path = SRC_TO_TGT_CONF_PATH,
                 src_data_vald_conf_path: Path = SRC_DATA_VALIDATION_CONF_PATH):
        
        self.src_to_tgt_conf_path = src_to_tgt_conf_path
        self.src_data_vald_conf_path = src_data_vald_conf_path

    def read_src_to_tgt_cfg(self) -> Dict:
        with open(self.src_to_tgt_conf_path) as f:
            try:
                src_to_tgt_cfg = yaml.safe_load(f)
                return src_to_tgt_cfg
            except yaml.YAMLError as e:
                raise Exception(f'Error encountered while parsing YAML config {self.src_to_tgt_conf_path} : {str(e)}')
    
    def read_src_data_vald_cfg(self) -> Dict:
        with open(self.src_data_vald_conf_path) as f:
            try:
                src_data_vald_cfg = yaml.safe_load(f)
                return src_data_vald_cfg
            except yaml.YAMLError as e:
                raise Exception(f'Error encountered while parsing YAML config {self.src_data_vald_conf_path} : {str(e)}')
            
if __name__ == '__main__':
    ingest_cfg_reader = IngestionCfgReader()
    src_to_tgt_cfg = ingest_cfg_reader.read_src_to_tgt_cfg()
    src_data_vald_cfg = ingest_cfg_reader.read_src_data_vald_cfg()