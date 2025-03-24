from pathlib import Path
from typing import Dict, List
import logging
from data_pipeline_app.utils.cfg_reader import YamlCfgReader
from cfg.resource_paths import APP_CONF_ROOT, INGEST_CONF_SUBPATH, INGEST_ETL_JOBS_CONF_SUBPATH, INGEST_SRC_TO_TGT_CONF_SUBPATH, INGEST_SRC_DATA_VALIDATION_CONF_SUBPATH

INGEST_ETL_JOBS_CONF_PATH = Path(APP_CONF_ROOT, INGEST_CONF_SUBPATH, INGEST_ETL_JOBS_CONF_SUBPATH)
SRC_TO_TGT_CONF_PATH = Path(APP_CONF_ROOT, INGEST_CONF_SUBPATH, INGEST_SRC_TO_TGT_CONF_SUBPATH)
SRC_DATA_VALIDATION_CONF_PATH = Path(APP_CONF_ROOT, INGEST_CONF_SUBPATH, INGEST_SRC_DATA_VALIDATION_CONF_SUBPATH)

logger = logging.getLogger(f'pyspark_ingestion_app.{__name__}')

class IngestionCfgReader(YamlCfgReader):
    def __init__(self):
        super().__init__()

    def read_etl_jobs_cfg(self, etl_jobs_conf_path: Path = INGEST_ETL_JOBS_CONF_PATH) -> List[str]:
        cfg = super().read_cfg(file_path=etl_jobs_conf_path)
        return cfg

    def read_src_to_tgt_cfg(self, src_to_tgt_conf_path: Path = SRC_TO_TGT_CONF_PATH) -> Dict:
        cfg = super().read_cfg(file_path=src_to_tgt_conf_path)
        return cfg
    
    def read_src_data_vald_cfg(self, src_data_vald_conf_path: Path = SRC_DATA_VALIDATION_CONF_PATH) -> Dict:
        cfg = super().read_cfg(file_path=src_data_vald_conf_path)
        return cfg
            
if __name__ == '__main__':
    ingest_cfg_reader = IngestionCfgReader()
    ingest_etl_jobs_cfg = ingest_cfg_reader.read_etl_jobs_cfg()
    src_to_tgt_cfg = ingest_cfg_reader.read_src_to_tgt_cfg()
    src_data_vald_cfg = ingest_cfg_reader.read_src_data_vald_cfg()
    print(f'Ingestion ETL jobs cfg: {ingest_etl_jobs_cfg}')
    print(f'Source to target cfg: {src_to_tgt_cfg}')
    print(f'Source data validation cfg: {src_data_vald_cfg}')