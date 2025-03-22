import pytest
import os
from pathlib import Path
from collections import namedtuple
from data_pipeline_app.utils.pyspark_app_initialisers import PysparkAppCfg, PysparkSessionBuilder

SparkSessionConfFixture = namedtuple('SparkSessionConfFixture',
                                     ['spark_app_conf_etl_id', 'spark_app_conf', 'spark_jars_conf',
                                      'spark_jars_conf_section', 'spark_jar_path_dict', 'spark_app_conf_path',
                                      'spark_jars_conf_path', 'expected'])

@pytest.fixture(scope='module')
def spark_session_conf(request) -> SparkSessionConfFixture:

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

    test_spark_app_conf_expected = {
        "spark.jars": "jars/postgresql/postgresql-42.7.5.jar",
        "spark.driver.cores": "1",
        "spark.driver.memory": "4g",
        "spark.driver.memoryOverhead": "1g",
        "spark.executor.cores": "1",
        "spark.executor.memory": "4g",
        "spark.executor.memoryOverhead": "1g"
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

    spark_session_conf_fixture = SparkSessionConfFixture(spark_app_conf_etl_id=test_spark_app_conf_etl_id,
                                                         spark_app_conf=test_spark_app_conf,
                                                         spark_jars_conf=test_spark_jars_conf,
                                                         spark_jars_conf_section=test_spark_jars_conf_section,
                                                         spark_jar_path_dict=test_spark_jar_path_dict,
                                                         spark_app_conf_path=test_spark_app_conf_path,
                                                         spark_jars_conf_path=test_spark_jars_conf_path,
                                                         expected=test_spark_app_conf_expected)

    def cleanup():
        os.remove(test_spark_app_conf_path)
        os.remove(test_spark_jars_conf_path)

    request.addfinalizer(cleanup)

    return spark_session_conf_fixture

def test_get_or_create_spark_session(spark_session_conf: SparkSessionConfFixture):

    spark_app_conf_etl_id = spark_session_conf.spark_app_conf_etl_id
    spark_app_conf = spark_session_conf.spark_app_conf
    spark_jars_conf = spark_session_conf.spark_jars_conf
    spark_jars_conf_section = spark_session_conf.spark_jars_conf_section
    spark_jar_path_dict = spark_session_conf.spark_jar_path_dict
    spark_app_conf_path = spark_session_conf.spark_app_conf_path
    spark_jars_conf_path = spark_session_conf.spark_jars_conf_path
    spark_app_conf_expected = spark_session_conf.expected

    spark_app_cfg = PysparkAppCfg(spark_app_conf_path=spark_app_conf_path,
                                  spark_jars_conf_path=spark_jars_conf_path,
                                  spark_app_conf_section=spark_app_conf_etl_id,
                                  spark_jar_conf_section=spark_jars_conf_section,
                                  spark_jar_path_dict=spark_jar_path_dict)
    spark_app_props = spark_app_cfg.get_app_props()
    spark_session_builder = PysparkSessionBuilder(app_name='Pyspark Test App', app_props=spark_app_props)
    spark = spark_session_builder.get_or_create_spark_session()

    actual_spark_app_conf_options = spark.sparkContext.getConf().getAll()
    actual_spark_app_conf = {}
    for k, v in actual_spark_app_conf_options:
        actual_spark_app_conf[k] = v
    actual_spark_app_conf = {k: v for k, v in actual_spark_app_conf.items() if k in spark_app_conf_expected.keys()}
    expected_spark_app_conf = spark_app_conf_expected
    assert actual_spark_app_conf == expected_spark_app_conf