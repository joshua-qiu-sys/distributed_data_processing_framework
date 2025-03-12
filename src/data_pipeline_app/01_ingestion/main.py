import itertools
from read_ingestion_cfg import IngestionCfgReader
from data_pipeline_app.utils.connectors import LocalFileConnector
from data_pipeline_app.utils.pyspark_session_builder import PysparkSessionBuilder
from data_pipeline_app.utils.data_validation import DatasetValidation, DatasetValidationChecker, DatasetValidationResult
from data_pipeline_app.utils.application_logger import ApplicationLogger

class DatasetIngestion:
    def __init__(self):
        pass

def main():

    app_logger = ApplicationLogger(log_app_name='pyspark_ingestion_app', log_conf_section='INGESTION')
    logger = app_logger.get_logger()
    logger.info(f'Created application logger for application {app_logger.get_log_app_name()}')

    spark_session_builder = PysparkSessionBuilder(app_name='Pyspark Ingestion App')
    spark = spark_session_builder.get_or_create_spark_session()
    logger.info(f'Created Spark session for {spark_session_builder.get_app_name()}')

    # target state --> dataset_name should be derived from the event json received
    dataset_name = 'dataset1'

    ingest_cfg_reader = IngestionCfgReader()
    src_to_tgt_cfg = ingest_cfg_reader.read_src_to_tgt_cfg()[dataset_name]
    logger.info(f'Successfully read source to target configurations: {src_to_tgt_cfg}')
    src_data_vald_cfg = ingest_cfg_reader.read_src_data_vald_cfg()[dataset_name]
    logger.info(f'Successfully read source data validation configurations: {src_data_vald_cfg}')
    
    file_path = 'data/raw/dataset1'
    file_connector = LocalFileConnector(spark=spark)
    df = file_connector.read_file_as_df(file_path=file_path, file_type='parquet')
    logger.info(f'Loaded file {file_path} into Dataframe')
    logger.info(f'{df.take(10)}')

    validations = [vald for vald in src_data_vald_cfg['req_validations'].keys()]
    logger.info(f'Validations to perform: {validations}')
    primary_key_cols = src_data_vald_cfg['req_validations']['primary_key_validation']['cols_to_check']
    logger.info(f'Primary key cols: {primary_key_cols}')
    non_nullable_cols = primary_key_cols = src_data_vald_cfg['req_validations']['null_validation']['cols_to_check']
    logger.info(f'Non-nullable cols: {non_nullable_cols}')
    dataset_vald = DatasetValidation(spark=spark,
                                     df=df,
                                     vald_types=validations,
                                     primary_key_cols=primary_key_cols,
                                     non_nullable_cols=non_nullable_cols)
    dataset_vald.perform_req_validations()
    logger.info(f'Performed required validations for {file_path}')
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

    file_connector.write_df_to_file(df=df, file_path='data/processed/dataset1', file_type='csv', write_mode='overwrite')

if __name__ == '__main__':
    main()
    