import pytest
from typing import Dict, Union
import os
from pathlib import Path
import yaml
from configparser import ConfigParser
from jinja2 import Template
from data_pipeline_app.utils.pyspark_session_builder import PysparkSessionBuilder

@pytest.fixture(scope='module')
def spark_session_builder() -> PysparkSessionBuilder:
    spark_session_builder = PysparkSessionBuilder(app_name='Pyspark Test App')
    return spark_session_builder

@pytest.fixture(scope='module')
def spark_session_conf(request) -> Dict[str, Union[str, Path]]:

    test_spark_app_conf = """
        default:

            "spark.jars": {{ postgres }}

            "spark.driver.cores": 1
            "spark.driver.memory": 7g
            "spark.driver.memoryOverhead": 1g
            
            "spark.executor.cores": 2
            "spark.executor.memory": 7g
            "spark.executor.memoryOverhead": 1g

        ingest~dataset100:

            "spark.jars": {{ postgres }}

            "spark.driver.cores": 1
            "spark.driver.memory": 4g
            "spark.driver.memoryOverhead": 1g

            "spark.executor.cores": 1
            "spark.executor.memory": 4g
            "spark.executor.memoryOverhead": 1g
    """

    test_spark_app_conf_etl_id = 'ingest~dataset100'

    test_spark_jars_conf = """
        [DEFAULT]
        postgres = {{ postgres_jar_path }}
    """

    test_spark_jars_conf_section = 'DEFAULT'

    test_spark_jar_path_dict = {
        'postgres_jar_path': 'jars/postgresql/postgresql-42.7.5.jar'
    }

    tmp_dir = Path('tmp')
    if not os.path.exists(tmp_dir):
        os.makedirs(tmp_dir)

    test_spark_app_conf_path = Path(tmp_dir, 'spark_app_conf.yml')
    test_spark_jars_conf_path = Path(tmp_dir, 'spark_jars.conf')

    with open(test_spark_app_conf_path, 'w') as f:
        f.write(test_spark_app_conf)

    with open(test_spark_jars_conf_path, 'w') as f:
        f.write(test_spark_jars_conf)

    def cleanup():
        os.remove(test_spark_app_conf_path)
        os.remove(test_spark_jars_conf_path)

    request.addfinalizer(cleanup)

    conf = {
        'spark_app_conf_etl_id': test_spark_app_conf_etl_id,
        'spark_app_conf': test_spark_app_conf,
        'spark_jars_conf': test_spark_jars_conf,
        'spark_jars_conf_section': test_spark_jars_conf_section,
        'spark_jar_path_dict': test_spark_jar_path_dict,
        'spark_app_conf_path': test_spark_app_conf_path,
        'spark_jars_conf_path': test_spark_jars_conf_path
    }

    return conf

def test_get_or_create_spark_session(spark_session_builder, spark_session_conf):

    spark_app_conf_etl_id = spark_session_conf['spark_app_conf_etl_id']
    spark_app_conf = spark_session_conf['spark_app_conf']
    spark_jars_conf = spark_session_conf['spark_jars_conf']
    spark_jars_conf_section = spark_session_conf['spark_jars_conf_section']
    spark_jar_path_dict = spark_session_conf['spark_jar_path_dict']
    spark_app_conf_path = spark_session_conf['spark_app_conf_path']
    spark_jars_conf_path = spark_session_conf['spark_jars_conf_path']

    spark = spark_session_builder.get_or_create_spark_session(spark_app_conf_path=spark_app_conf_path,
                                                              spark_jars_conf_path=spark_jars_conf_path,
                                                              spark_app_conf_etl_id=spark_app_conf_etl_id,
                                                              spark_jar_path_dict=spark_jar_path_dict)

    with open(spark_jars_conf_path) as f:
        raw_spark_jars_conf = f.read()
    templated_spark_jars_conf = Template(raw_spark_jars_conf)
    rendered_spark_jars_conf = templated_spark_jars_conf.render(spark_jar_path_dict)

    spark_jar_conf_parser = ConfigParser(interpolation=None)
    spark_jar_conf_parser.read_string(rendered_spark_jars_conf)

    spark_jar_conf = {}
    spark_jar_default_cfg = {}
    for k, v in spark_jar_conf_parser.defaults().items():
        spark_jar_default_cfg[k] = v
    if spark_jar_default_cfg:
        spark_jar_conf["DEFAULT"] = spark_jar_default_cfg

    for section in spark_jar_conf_parser.sections():
        section_cfg = {}
        for k, v in spark_jar_conf_parser.items(section):
            section_cfg[k] = v
        spark_jar_conf[section] = section_cfg

    spark_jar_conf = spark_jar_conf[spark_jars_conf_section]

    with open(spark_app_conf_path) as f:
        raw_spark_app_conf = f.read()
    templated_spark_app_conf = Template(raw_spark_app_conf)
    rendered_spark_app_conf = templated_spark_app_conf.render(spark_jar_conf)
    expected_spark_app_conf = yaml.safe_load(rendered_spark_app_conf)[spark_app_conf_etl_id]
    expected_spark_app_conf = {k: str(v) for k, v in expected_spark_app_conf.items()}
    print(f'Expected spark configuration\n{expected_spark_app_conf}')

    actual_spark_app_conf_options = spark.sparkContext.getConf().getAll()
    actual_spark_app_conf = {}
    for k, v in actual_spark_app_conf_options:
        actual_spark_app_conf[k] = v
    actual_spark_app_conf = {k: v for k, v in actual_spark_app_conf.items() if k in expected_spark_app_conf.keys()}
    print(f'Actual spark configuration:\n{actual_spark_app_conf}')

    assert actual_spark_app_conf == expected_spark_app_conf