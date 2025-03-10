from typing import Dict
from pathlib import Path
import configparser
from configparser import Interpolation

class CfgReader:
    def __init__(self, file_path: Path):
        self.file_path = file_path
    
    def read_cfg(self, interpolation: Interpolation = None) -> Dict:
        cfg_parser = configparser.ConfigParser(interpolation=interpolation)
        cfg_parser.read_file(open(self.file_path))
        return cfg_parser