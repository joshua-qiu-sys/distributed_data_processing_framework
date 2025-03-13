from typing import Dict, Optional
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
    
    def read_cfg(self, file_path: Path, interpolation: Optional[Interpolation] = None) -> Dict:
        cfg_parser = configparser.ConfigParser(interpolation=interpolation)
        cfg_parser.read_file(open(file_path))

        cfg = {}
        default_cfg = {}
        for k, v in cfg_parser.defaults().items():
            default_cfg[k] = v
        if default_cfg:
            cfg["DEFAULT"] = default_cfg

        for section in cfg_parser.sections():
            section_cfg = {}
            for k, v in cfg_parser.items(section):
                section_cfg[k] = v
            cfg[section] = section_cfg

        return cfg
    
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
    ini_cfg_reader = IniCfgReader()
    ini_cfg = ini_cfg_reader.read_cfg(file_path='cfg/logging/log.conf')
    ini_cfg = ini_cfg_reader.read_cfg(file_path='abc.properties')
    print(f'Ini cfg:\n{ini_cfg}')
    yml_cfg_reader = YamlCfgReader()
    yml_cfg = yml_cfg_reader.read_cfg(file_path='cfg/data_pipeline_app/01_ingestion/src_to_target.yml')
    print(f'Yml cfg:\n{yml_cfg}')