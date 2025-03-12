from typing import Dict
from pathlib import Path
import yaml
import configparser
from configparser import Interpolation
from abc import ABC, abstractmethod

class CfgReader(ABC):
    @abstractmethod
    def read_cfg(self, file_path: Path):
        raise NotImplementedError

class IniCfgReader(CfgReader):
    def __init__(self):
        pass
    
    def read_cfg(self, file_path: Path, interpolation: Interpolation = None) -> Dict:
        cfg_parser = configparser.ConfigParser(interpolation=interpolation)
        cfg_parser.read_file(open(file_path))
        return cfg_parser
    
class YamlCfgReader(CfgReader):
    def __init__(self):
        pass

    def read_cfg(self, file_path: Path) -> Dict:
        with open(file_path) as f:
            try:
                cfg = yaml.safe_load(f)
                return cfg
            except yaml.YAMLError as e:
                raise Exception(f'Error encountered while parsing YAML config {file_path} : {str(e)}')
            
if __name__ == '__main__':
    yml_cfg_reader = YamlCfgReader()
    cfg = yml_cfg_reader.read_cfg('cfg/data_pipeline_app/01_ingestion/src_to_target.yml')