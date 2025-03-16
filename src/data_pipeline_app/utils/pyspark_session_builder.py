from pyspark.sql import SparkSession
import os
from pathlib import Path
from typing import Dict, Optional, Union
from data_pipeline_app.utils.cfg_reader import IniCfgReader, YamlCfgReader
from cfg.resource_paths import APP_CONF_ROOT, INGEST_CONF_SUBPATH, INGEST_SPARK_APP_CONF_SUBPATH, JARS_ROOT, POSTGRES_JAR_SUBPATH, JARS_CONF_PATH

SPARK_APP_CONF_PATH = Path(APP_CONF_ROOT, INGEST_CONF_SUBPATH, INGEST_SPARK_APP_CONF_SUBPATH)
POSTGRES_JAR_PATH = Path(JARS_ROOT, POSTGRES_JAR_SUBPATH)

class PysparkSessionBuilder:
    def __init__(self, app_name: str):
        self.app_name = app_name
        self.spark_app_props = None

    def get_app_name(self) -> str:
        return self.app_name
    
    def get_spark_app_props(self) -> Dict[str, Union[str, int, float]]:
        return self.spark_app_props
    
    def _get_spark_jar_paths_from_conf(self) -> Dict[str, str]:
        spark_jar_path_dict = {
            'postgres_jar_path': POSTGRES_JAR_PATH
        }
        return spark_jar_path_dict
    
    def _get_rendered_app_props_from_conf(self,
                                          spark_app_conf_path: Path,
                                          spark_jars_conf_path: Path,
                                          spark_jar_conf_section: str = 'DEFAULT',
                                          spark_app_conf_section: str = 'default',
                                          spark_jar_path_dict: Dict[str, str] = None) -> Dict[str, Union[str, int, float]]:

        ini_cfg_reader = IniCfgReader()
        if spark_jar_path_dict is None:
            spark_jar_path_dict = self._get_spark_jar_paths_from_conf()
        rendered_spark_jar_path_dict = ini_cfg_reader.read_jinja_templated_cfg(file_path=spark_jars_conf_path, interpolation=None, cfg_vars=spark_jar_path_dict)[spark_jar_conf_section]

        yml_cfg_reader = YamlCfgReader()
        rendered_app_props = yml_cfg_reader.read_jinja_templated_cfg(file_path=spark_app_conf_path, cfg_vars=rendered_spark_jar_path_dict)[spark_app_conf_section]
        
        return rendered_app_props

    def get_or_create_spark_session(self,
                                    spark_app_conf_path: Path = SPARK_APP_CONF_PATH,
                                    spark_jars_conf_path: Path = JARS_CONF_PATH,
                                    spark_app_conf_etl_id: Optional[str] = 'default',
                                    spark_jar_path_dict: Dict[str, str] = None,
                                    spark_app_props: Optional[Dict[str, Union[str, int, float]]] = None) -> SparkSession:
        
        spark_builder = SparkSession.builder.appName(self.app_name)
        
        if spark_app_props is None:
            spark_app_props = self._get_rendered_app_props_from_conf(spark_app_conf_path=spark_app_conf_path,
                                                                     spark_jars_conf_path=spark_jars_conf_path,
                                                                     spark_app_conf_section=spark_app_conf_etl_id,
                                                                     spark_jar_path_dict=spark_jar_path_dict)
        
        self.spark_app_props = spark_app_props
        
        for k, v in spark_app_props.items():
            spark_builder = spark_builder.config(k, v)

        try:
            spark = spark_builder.getOrCreate()
        except Exception as e:
            raise Exception(f'Failed to get or create Spark session: {str(e)}')

        return spark

if __name__ == '__main__':
    spark_session_builder = PysparkSessionBuilder(app_name='Pyspark App')
    spark = spark_session_builder.get_or_create_spark_session(spark_app_conf_path=SPARK_APP_CONF_PATH, spark_jars_conf_path=JARS_CONF_PATH, spark_app_conf_etl_id='default')
    spark_app_name = spark_session_builder.get_app_name()
    spark_app_props = spark_session_builder.get_spark_app_props()
    print(f'Spark application name: {spark_app_name}')
    print(f'Spark application properties:\n{spark_app_props}')