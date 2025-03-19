from data_pipeline_app.ingestion.read_ingestion_cfg import IngestionCfgReader
from data_pipeline_app.utils.connector_handlers import ConnectorSelectionHandler
from data_pipeline_app.utils.pyspark_session_builder import PysparkSessionBuilder
from data_pipeline_app.utils.data_validation import DatasetValidation
from data_pipeline_app.utils.application_logger import ApplicationLogger

class DatasetIngestion:
    def __init__(self):
        pass

def ingest():

    app_logger = ApplicationLogger(log_app_name='pyspark_ingestion_app', log_conf_section='INGESTION')
    logger = app_logger.get_logger()
    logger.info(f'Created application logger for application {app_logger.get_log_app_name()}')

    spark_session_builder = PysparkSessionBuilder(app_name='Pyspark Ingestion App')
    spark = spark_session_builder.get_or_create_spark_session()
    logger.info(f'Created Spark session for {spark_session_builder.get_app_name()}')

    # target state --> etl id should be fetched from the event json received and used for fetching other etl details
    etl_id = 'ingest~dataset1'

    ingest_cfg_reader = IngestionCfgReader()
    src_to_tgt_cfg = ingest_cfg_reader.read_src_to_tgt_cfg()[etl_id]
    logger.info(f'Successfully read source to target configurations: {src_to_tgt_cfg}')
    src_data_vald_cfg = ingest_cfg_reader.read_src_data_vald_cfg()[etl_id]
    logger.info(f'Successfully read source data validation configurations: {src_data_vald_cfg}')

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
    non_nullable_cols = primary_key_cols = src_data_vald_cfg['req_validations']['null_validation']['cols_to_check']
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
    ingest()
    