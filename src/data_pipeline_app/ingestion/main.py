from pyspark.sql import SparkSession, DataFrame
from typing import List, Dict, Optional
from abc import ABC
import sys
from data_pipeline_app.ingestion.read_ingestion_cfg import IngestionCfgReader
from data_pipeline_app.utils.connector_handlers import ConnectorSelectionHandler
from data_pipeline_app.utils.pyspark_app_initialisers import PysparkAppCfg, PysparkSessionBuilder
from data_pipeline_app.utils.data_validation import DatasetValidation
from data_pipeline_app.utils.data_pipeline import AbstractDataPipeline
from data_pipeline_app.utils.application_logger import ApplicationLogger
        
class DataIngestionPipeline(AbstractDataPipeline):
    def __init__(self, spark: SparkSession, etl_id: str, src_to_tgt_cfg: Dict, src_data_vald_cfg: Dict = None, phases: Optional[List[str]] = ['extraction', 'validation', 'load']):
        self.spark = spark
        self.etl_id = etl_id
        self.src_to_tgt_cfg = src_to_tgt_cfg
        self.data_vald_cfg = src_data_vald_cfg
        self.dataset = self.src_to_tgt_cfg['src']['dataset_name']
        if 'extraction' not in phases or 'load' not in phases:
            raise ValueError('Data pipeline phases must contain "extraction" and "load"')
        else:    
            self.phases = phases
        self.df = None

    def execute_pipeline(self) -> None:
        self.extract()
        if 'validation' in self.phases:
            self.validate()
        self.load()

    def extract(self) -> DataFrame:
        src_conn_select_handler = ConnectorSelectionHandler(direction='source', raw_connector_cfg=self.src_to_tgt_cfg['src'])
        processed_src_conn_cfg = src_conn_select_handler.get_processed_connector_cfg()
        src_connector = src_conn_select_handler.get_req_connector()
        df = src_connector.read_from_source(spark=self.spark, **processed_src_conn_cfg)
        return df
    
    def validate(self) -> DatasetValidation:
        validations = [vald for vald in self.src_data_vald_cfg['req_validations'].keys()]
        primary_key_cols = self.src_data_vald_cfg['req_validations']['primary_key_validation']['cols_to_check']
        non_nullable_cols = self.src_data_vald_cfg['req_validations']['null_validation']['cols_to_check']
        
        dataset_vald = DatasetValidation(spark=self.spark,
                                         df=self.df,
                                         vald_types=validations,
                                         primary_key_cols=primary_key_cols,
                                         non_nullable_cols=non_nullable_cols)
        dataset_vald.perform_req_validations()
        return dataset_vald

    def load(self) -> None:
        if self.df is None:
            raise Exception('Cannot load data to a sink for a DataFrame object that does not exist')
        else:
            target_conn_select_handler = ConnectorSelectionHandler(direction='sink', raw_connector_cfg=self.src_to_tgt_cfg['target'])
            processed_target_conn_cfg = target_conn_select_handler.get_processed_connector_cfg()
            target_connector = target_conn_select_handler.get_req_connector()
            target_connector.write_to_sink(df=self.df, **processed_target_conn_cfg)

class DataIngestionBatchMetadata:
    def __init__(self, etl_jobs: List[str] = None):
        if etl_jobs is None:
            self.get_etl_jobs_from_conf()
        else:
            self.etl_jobs = etl_jobs

    def set_etl_jobs(self, etl_jobs: List[str]) -> None:
        self.etl_jobs = etl_jobs

    def get_etl_jobs(self) -> List[str]:
        return self.etl_jobs

    def get_etl_jobs_from_conf(self) -> List[str]:
        ingest_cfg_reader = IngestionCfgReader()
        ingest_etl_jobs = ingest_cfg_reader.read_etl_jobs_cfg()
        self.set_etl_jobs(etl_jobs=ingest_etl_jobs)
        return ingest_etl_jobs

    def get_req_spark_jars(self, etl_jobs_list: List[str] = None) -> List[Optional[str]]:
        req_spark_jar_list = []
        if etl_jobs_list is None:
            etl_jobs_list = self.etl_jobs
        for etl_job in etl_jobs_list:
            spark_app_cfg = PysparkAppCfg(spark_app_conf_section=etl_job)
            spark_app_props = spark_app_cfg.get_app_props()
            if 'spark.jars' in spark_app_props.keys():
                req_spark_jar = spark_app_props['spark.jars']
            else:
                req_spark_jar = None
            req_spark_jar_list.append(req_spark_jar)
        return req_spark_jar_list

def get_etl_jobs() -> List[str]:
    ingest_cfg_reader = IngestionCfgReader()
    ingest_etl_jobs = ingest_cfg_reader.read_etl_jobs_cfg()
    return ingest_etl_jobs

def get_req_spark_jars(etl_id: str) -> Optional[str]:
    spark_app_cfg = PysparkAppCfg(spark_app_conf_section=etl_id)
    spark_app_props = spark_app_cfg.get_app_props()
    if 'spark.jars' in spark_app_props.keys():
        req_spark_jars = spark_app_props['spark.jars']
    else:
        req_spark_jars = None
    return req_spark_jars

def ingest(etl_id: str = 'ingest~dataset1', **kwargs):

    app_logger = ApplicationLogger(log_app_name='pyspark_ingestion_app', log_conf_section='INGESTION')
    logger = app_logger.get_logger()
    logger.info(f'Created application logger for application {app_logger.get_log_app_name()}')

    ingest_cfg_reader = IngestionCfgReader()
    src_to_tgt_cfg = ingest_cfg_reader.read_src_to_tgt_cfg()[etl_id]
    logger.info(f'Successfully read source to target configurations: {src_to_tgt_cfg}')
    src_data_vald_cfg = ingest_cfg_reader.read_src_data_vald_cfg()[etl_id]
    logger.info(f'Successfully read source data validation configurations: {src_data_vald_cfg}')
    
    spark_app_name = 'Pyspark App'
    spark_app_cfg = PysparkAppCfg(spark_app_conf_section=etl_id)
    spark_app_props = spark_app_cfg.get_app_props()
    spark_session_builder = PysparkSessionBuilder(app_name=spark_app_name, app_props=spark_app_props)
    spark = spark_session_builder.get_or_create_spark_session()
    logger.info(f'Created Spark session for {spark_app_name}')

    dataset = src_to_tgt_cfg['src']['dataset_name']

    src_conn_select_handler = ConnectorSelectionHandler(direction='source', raw_connector_cfg=src_to_tgt_cfg['src'])
    processed_src_conn_cfg = src_conn_select_handler.get_processed_connector_cfg()
    src_connector = src_conn_select_handler.get_req_connector()
    logger.info(f'Processed source connector config: {processed_src_conn_cfg}')
    df = src_connector.read_from_source(spark=spark, **processed_src_conn_cfg)
    df.show()
    logger.info(f'Loaded file {dataset} into Dataframe')
    logger.info(f'{df.take(10)}')

    validations = [vald for vald in src_data_vald_cfg['req_validations'].keys()]
    logger.info(f'Validations to perform: {validations}')
    primary_key_cols = src_data_vald_cfg['req_validations']['primary_key_validation']['cols_to_check']
    logger.info(f'Primary key cols: {primary_key_cols}')
    non_nullable_cols = src_data_vald_cfg['req_validations']['null_validation']['cols_to_check']
    logger.info(f'Non-nullable cols: {non_nullable_cols}')
    dataset_vald = DatasetValidation(spark=spark,
                                     df=df,
                                     vald_types=validations,
                                     primary_key_cols=primary_key_cols,
                                     non_nullable_cols=non_nullable_cols)
    dataset_vald.perform_req_validations()
    logger.info(f'Performed required validations for {dataset}')
    for vald_checker in dataset_vald.get_vald_checker_list():
        vald_type = vald_checker.get_vald_type()
        vald_passed = vald_checker.get_vald_result().get_vald_passed()
        df_vald_result_dataset_level = vald_checker.get_vald_result().get_df_vald_result_dataset_level()
        df_vald_result_record_level = vald_checker.get_vald_result().get_df_vald_result_record_level()
        logger.info(f'Validation checker for {vald_type}')
        logger.info(f'Validation passed: {vald_passed}')
        logger.info(f'Validation dataframe - dataset level: {df_vald_result_dataset_level.show()}')
        if df_vald_result_record_level is not None:
            logger.info(f'Validation dataframe - record level: {df_vald_result_record_level.collect()}')

    target_conn_select_handler = ConnectorSelectionHandler(direction='sink', raw_connector_cfg=src_to_tgt_cfg['target'])
    processed_target_conn_cfg = target_conn_select_handler.get_processed_connector_cfg()
    target_connector = target_conn_select_handler.get_req_connector()
    logger.info(f'Processed target connector config: {processed_target_conn_cfg}')
    target_connector.write_to_sink(df=df, **processed_target_conn_cfg)
    logger.info(f'Loaded DataFrame into target')

if __name__ == '__main__':
    etl_id = sys.argv[1]
    ingest(etl_id=etl_id)
    