from typing import Dict, Optional, Union
from pathlib import Path
import yaml
from configparser import ConfigParser, Interpolation
from jinja2 import Template, TemplateError
from abc import ABC, abstractmethod

class AbstractCfgReader(ABC):
    @abstractmethod
    def read_file(self, file_path: Path):
        raise NotImplementedError
    
    @abstractmethod
    def read_cfg(self, file_path: Path):
        raise NotImplementedError
    
class BaseCfgReader(AbstractCfgReader):
    def __init__(self):
        pass

    def read_file(self, file_path: Path) -> str:
        try:
            with open(file_path) as f:
                cfg = f.read()
                return cfg
        except FileNotFoundError as e:
            raise FileNotFoundError(f'File {file_path} not found: {str(e)}')
        except IOError as e:
            raise IOError(f'IO error occurred while reading the file {file_path}: {str(e)}')
        
    def read_cfg(self, file_path: Path) -> Dict:
        pass

class IniCfgReader(BaseCfgReader):
    def __init__(self):
        super().__init__()

    def parse_cfg(self, cfg_parser: ConfigParser) -> Dict:

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

    def read_file(self, file_path: Path) -> str:
        return super().read_file(file_path=file_path)
    
    def read_cfg(self, file_path: Path, interpolation: Optional[Interpolation]) -> Dict:
        cfg_parser = ConfigParser(interpolation=interpolation)
        cfg_parser.read_file(open(file_path))
        cfg = self.parse_cfg(cfg_parser=cfg_parser)
        return cfg
    
    def read_jinja_templated_cfg(self,
                                 file_path: Path,
                                 interpolation: Optional[Interpolation],
                                 cfg_vars: Dict[str, Union[str, int, float]]) -> Dict:
        
        cfg = self.read_file(file_path=file_path)
        templated_cfg = Template(cfg)
        rendered_cfg = templated_cfg.render(cfg_vars)
        cfg_parser = ConfigParser(interpolation=interpolation)
        cfg_parser.read_string(rendered_cfg)
        cfg = self.parse_cfg(cfg_parser=cfg_parser)
        return cfg
    
class YamlCfgReader(BaseCfgReader):
    def __init__(self):
        super().__init__()

    def read_file(self, file_path: Path) -> str:
        return super().read_file(file_path=file_path)

    def read_cfg(self, file_path: Path) -> Dict:
        try:
            with open(file_path) as f:
                cfg = yaml.safe_load(f)
                return cfg
        except yaml.YAMLError as e:
            raise yaml.YAMLError(f'Error encountered while parsing YAML config {file_path} : {str(e)}')
            
    def read_jinja_templated_cfg(self, file_path: Path, cfg_vars: Dict[str, Union[str, int, float]]) -> Dict:
        cfg = self.read_file(file_path=file_path)
        
        try:
            templated_cfg = Template(cfg)
            rendered_cfg = templated_cfg.render(cfg_vars)
            cfg = yaml.safe_load(rendered_cfg)
            return cfg
        except TemplateError as e:
            raise TemplateError(f'Error encountered while templating YAML config {file_path} : {str(e)}')
            
if __name__ == '__main__':
    ini_cfg_reader = IniCfgReader()
    ini_cfg = ini_cfg_reader.read_cfg(file_path='cfg/logging/log.conf', interpolation=None)
    print(f'Ini cfg:\n{ini_cfg}')
    yml_cfg_reader = YamlCfgReader()
    yml_cfg = yml_cfg_reader.read_cfg(file_path='cfg/batch_data_pipeline_app/01_ingestion/src_to_target.yml')
    print(f'Yml cfg:\n{yml_cfg}')

    jinja_templated_ini_cfg = ini_cfg_reader.read_jinja_templated_cfg(file_path='cfg/jars/spark_jars.conf', interpolation=None, cfg_vars={'postgres_jar_path': '/root/postgres_jar'})
    print(f'Jinja templated ini cfg:\n{jinja_templated_ini_cfg}')
    jinja_templated_yml_cfg = yml_cfg_reader.read_jinja_templated_cfg(file_path='cfg/batch_data_pipeline_app/01_ingestion/spark_app.yml', cfg_vars={'postgres': '/root/postgres_jar', 'postgre': '/root/postgres_jar'})
    print(f'Jinja templated yml cfg:\n{jinja_templated_yml_cfg}')