from pyspark.sql import SparkSession, DataFrame
from typing import List, Dict, Optional
import sys
import logging
from batch_data_pipeline_app.ingestion.ingestion_cfg_management import IngestionCfgReader, DatasetIngestionCfgManager
from batch_data_pipeline_app.batch_utils.connector_management import ConnectorCfgHandler, ConnectorFactoryRegistry, ConnectorFactory
from batch_data_pipeline_app.batch_utils.pyspark_app_initialisers import PysparkAppCfgHandler, PysparkSessionBuilder
from batch_data_pipeline_app.batch_utils.data_validation import DatasetValidation
from src.utils.data_pipeline import AbstractDataPipeline
from src.utils.application_logger import ApplicationLogger

from batch_data_pipeline_app.batch_utils.connector_management import ACCEPTED_CONNECTOR_TYPES

logger = logging.getLogger('batch_ingestion_app')
        
class DatasetIngestionPipeline(AbstractDataPipeline):
    def __init__(self,
                 spark: SparkSession,
                 etl_id: str,
                 ingest_cfg_manager: DatasetIngestionCfgManager,
                 connector_factory: ConnectorFactory,
                 phases: Optional[List[str]] = ['extraction', 'validation', 'load']):
        
        self.spark = spark
        self.etl_id = etl_id
        self.ingest_cfg_manager = ingest_cfg_manager
        self.src_connector_cfg_handler = ConnectorCfgHandler(direction='source', raw_connector_cfg=self.ingest_cfg_manager.get_src_to_tgt_cfg()['src'])
        self.target_connector_cfg_handler = ConnectorCfgHandler(direction='sink', raw_connector_cfg=self.ingest_cfg_manager.get_src_to_tgt_cfg()['target'])
        self.connector_factory = connector_factory
        self.dataset = self.ingest_cfg_manager.get_src_to_tgt_cfg()['src']['dataset_name']
        if 'extraction' not in phases or 'load' not in phases:
            raise ValueError('Data pipeline phases must contain "extraction" and "load"')
        else:    
            self.phases = phases
        self.df = None
        self.dataset_vald = None

    def _set_df(self, df: DataFrame) -> None:
        self.df = df

    def _set_dataset_vald(self, dataset_vald: DatasetValidation) -> None:
        self.dataset_vald = dataset_vald

    def get_dataset_vald(self) -> Optional[DatasetValidation]:
        return self.dataset_vald
    
    def get_dataset_name(self) -> str:
        return self.dataset

    def execute_pipeline(self) -> None:
        self.extract()
        if 'validation' in self.phases:
            self.validate()
        self.load()

    def extract(self) -> DataFrame:
        src_connector_type = self.src_connector_cfg_handler.get_connector_type()
        src_processed_conn_cfg = self.src_connector_cfg_handler.process_cfg()
        logger.info(f'Processed source connector config: {src_processed_conn_cfg}')
        src_connector = self.connector_factory.create(connector_type=src_connector_type)
        df = src_connector.read_from_source(spark=self.spark, **src_processed_conn_cfg)
        df.show(10)
        logger.info(f'Loaded file {self.dataset} into Dataframe')
        logger.info(f'{df.take(10)}')
        self._set_df(df=df)
        return df
    
    def validate(self) -> DatasetValidation:
        src_data_vald_cfg = self.ingest_cfg_manager.get_src_data_vald_cfg()
        validations = [vald for vald in src_data_vald_cfg['req_validations'].keys()]
        logger.info(f'Validations to perform: {validations}')
        primary_key_cols = src_data_vald_cfg['req_validations']['primary_key_validation']['cols_to_check']
        logger.info(f'Primary key cols: {primary_key_cols}')
        non_nullable_cols = src_data_vald_cfg['req_validations']['null_validation']['cols_to_check']
        logger.info(f'Non-nullable cols: {non_nullable_cols}')
        
        dataset_vald = DatasetValidation(spark=self.spark,
                                         df=self.df,
                                         vald_types=validations,
                                         primary_key_cols=primary_key_cols,
                                         non_nullable_cols=non_nullable_cols)
        dataset_vald.perform_req_validations()
        logger.info(f'Performed required validations for {self.dataset}')

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

        self._set_dataset_vald(dataset_vald=dataset_vald)
        
        return dataset_vald

    def load(self) -> None:
        if self.df is None:
            raise Exception('Cannot load data to a sink from a DataFrame object that does not exist')
        else:
            target_connector_type = self.target_connector_cfg_handler.get_connector_type()
            target_processed_conn_cfg = self.target_connector_cfg_handler.process_cfg()
            target_connector = connector_factory.create(connector_type=target_connector_type)
            logger.info(f'Processed target connector config: {target_processed_conn_cfg}')
            target_connector.write_to_sink(df=self.df, **target_processed_conn_cfg)
            logger.info(f'Loaded DataFrame into target')

class BatchIngestionMetadata:
    def __init__(self, etl_jobs: List[str] = None):
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
            spark_app_cfg_handler = PysparkAppCfgHandler(spark_app_conf_section=etl_job)
            spark_app_props = spark_app_cfg_handler.get_app_props()
            if 'spark.jars' in spark_app_props.keys():
                req_spark_jar = spark_app_props['spark.jars']
            else:
                req_spark_jar = None
            req_spark_jar_list.append(req_spark_jar)
        return req_spark_jar_list

if __name__ == '__main__':
    etl_id = sys.argv[1]

    app_logger = ApplicationLogger(log_app_name='pyspark_ingestion_app', log_conf_section='INGESTION')
    logger = app_logger.get_logger()
    logger.info(f'Created application logger for application {app_logger.get_log_app_name()}')

    ingest_cfg_reader = IngestionCfgReader()
    src_to_tgt_cfg = ingest_cfg_reader.read_src_to_tgt_cfg()[etl_id]
    logger.info(f'Successfully read source to target configurations: {src_to_tgt_cfg}')
    src_data_vald_cfg = ingest_cfg_reader.read_src_data_vald_cfg()[etl_id]
    logger.info(f'Successfully read source data validation configurations: {src_data_vald_cfg}')
    
    spark_app_name = 'Pyspark App'
    spark_app_cfg_handler = PysparkAppCfgHandler(spark_app_conf_section=etl_id)
    spark_app_props = spark_app_cfg_handler.get_app_props()
    spark_session_builder = PysparkSessionBuilder(app_name=spark_app_name, app_props=spark_app_props)
    spark = spark_session_builder.get_or_create_spark_session()
    logger.info(f'Created Spark session for {spark_app_name}')

    ingest_cfg_manager = DatasetIngestionCfgManager(src_to_tgt_cfg=src_to_tgt_cfg, src_data_vald_cfg=src_data_vald_cfg)
    logger.info(f'Created dataset ingestion config manager')

    connector_factory_registry = ConnectorFactoryRegistry()
    connector_factory_registry.register_defaults(default_connector_dict=ACCEPTED_CONNECTOR_TYPES)
    logger.info(f'Created connector factory registry with accepted connector types: {ACCEPTED_CONNECTOR_TYPES}')
    connector_factory = ConnectorFactory(factory_registry=connector_factory_registry)
    logger.info(f'Created connector factory')

    data_ingest_pipeline = DatasetIngestionPipeline(spark=spark,
                                                 etl_id=etl_id,
                                                 ingest_cfg_manager=ingest_cfg_manager,
                                                 connector_factory=connector_factory,
                                                 phases=['extraction', 'validation', 'load'])
    dataset = data_ingest_pipeline.get_dataset_name()
    data_ingest_pipeline.execute_pipeline()
    logger.info(f'Successfully executed data ingestion pipeline for ETL job {etl_id} and dataset {dataset}')
    