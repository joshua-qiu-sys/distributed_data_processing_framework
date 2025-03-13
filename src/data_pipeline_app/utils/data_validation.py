from pyspark.sql import SparkSession, DataFrame, Row
import pyspark.sql.functions as F
import datetime as dt
from typing import List, Dict
from data_pipeline_app.utils.pyspark_session_builder import PysparkSessionBuilder
from data_pipeline_app.utils.connectors import LocalFileConnector

class DatasetValidationResult:
    def __init__(self,
                 spark: SparkSession,
                 vald_type: str,
                 vald_passed: bool,
                 df_vald_result_dataset_level: DataFrame,
                 df_vald_result_record_level: DataFrame):
        
        self.spark = spark
        self.vald_type = vald_type
        self.vald_passed = vald_passed
        self.df_vald_result_dataset_level = df_vald_result_dataset_level
        self.df_vald_result_record_level = df_vald_result_record_level

    def get_vald_type(self) -> str:
        return self.vald_type
    
    def get_vald_passed(self) -> bool:
        return self.vald_passed
    
    def get_df_vald_result_dataset_level(self) -> DataFrame:
        return self.df_vald_result_dataset_level
    
    def get_df_vald_result_record_level(self) -> DataFrame:
        return self.df_vald_result_record_level

class DatasetValidationChecker:
    def __init__(self,
                 spark: SparkSession,
                 df: DataFrame,
                 vald_type: str,
                 computed_df_metrics: Dict = None,
                 primary_key_cols: List[str] = None,
                 non_nullable_cols: List[str] = None):
        
        self.spark = spark
        self.df = df
        self.vald_type = vald_type
        self.computed_df_metrics = computed_df_metrics
        self.primary_key_cols = primary_key_cols
        self.non_nullable_cols = non_nullable_cols
        self.vald_result = None
        self._df_count = None

    def _set_computed_df_metrics(self) -> None:
        self._df_count = self.computed_df_metrics['count']

    def perform_validation(self) -> DatasetValidationResult:
        match self.vald_type:
            case 'primary_key_validation':
                if self.primary_key_cols is None or len(self.primary_key_cols) == 0:
                    raise ValueError('Primary key columns cannot be empty for primary key validation')
                self._set_computed_df_metrics()
                return self.perform_primary_key_validation(self.primary_key_cols)
            case 'null_validation':
                if self.non_nullable_cols is None or len(self.non_nullable_cols) == 0:
                    raise ValueError('Non-nullable columns cannot be empty for null validation')
                self._set_computed_df_metrics()
                return self.perform_null_validation(self.non_nullable_cols)
            case _:
                raise ValueError(f'Unknown validation type: {self.vald_type}')
            
    def perform_primary_key_validation(self, primary_key_cols: List[str]) -> DatasetValidationResult:
        curr_dt = dt.datetime.now()
        df_vald_result_record_level = self.df.groupBy(primary_key_cols).count().filter(F.col('count') > 1) \
            .withColumn('validation_type', F.lit(self.vald_type)) \
            .withColumn('observed_record_pk', F.to_json(F.struct(primary_key_cols))) \
            .withColumn('expected_count', F.lit(0)) \
            .withColumn('observed_count', F.col('count')) \
            .withColumn('ts', F.lit(curr_dt)) \
            .select(['validation_type', 'observed_record_pk', 'expected_count', 'observed_count', 'ts'])
        
        if df_vald_result_record_level.count() > 0:
            vald_passed = False
            err_record_count = df_vald_result_record_level.filter(F.col('observed_count')).agg(F.sum(F.col('observed_count')))
            vald_result_dataset_level_data = [
                Row(validation_type=self.vald_type, passed=False, total_records=self._df_count, error_records=err_record_count, ts=curr_dt)
            ]
        else:
            vald_passed = True
            df_vald_result_record_level = None
            vald_result_dataset_level_data = [
                Row(validation_type=self.vald_type, passed=True, total_records=self._df_count, error_records=0, ts=curr_dt),
            ]
        df_vald_result_dataset_level = self.spark.createDataFrame(data=vald_result_dataset_level_data)
        vald_result = DatasetValidationResult(spark=self.spark, vald_type=self.vald_type, vald_passed=vald_passed,
                                              df_vald_result_dataset_level=df_vald_result_dataset_level,
                                              df_vald_result_record_level=df_vald_result_record_level)
        self.set_vald_result(vald_result=vald_result)
        return vald_result

    def perform_null_validation(self, non_nullable_cols: List[str]) -> DatasetValidationResult:
        curr_dt = dt.datetime.now()

        null_cond = ' or '.join([f'{col} is null' for col in non_nullable_cols])
        df_vald_result_record_level = self.df.filter(null_cond) \
            .withColumn('validation_type', F.lit(self.vald_type)) \
            .withColumn('observed_record_pk', F.to_json(F.struct(self.primary_key_cols))) \
            .withColumn('expected_count', F.lit(0)) \
            .withColumn('observed_count', F.lit(1)) \
            .withColumn('ts', F.lit(curr_dt)) \
            .select(['validation_type', 'observed_record_pk', 'expected_count', 'observed_count', 'ts'])
        
        if df_vald_result_record_level.count() > 0:
            vald_passed = False
            err_record_count = df_vald_result_record_level.filter(F.col('observed_count')).agg(F.sum(F.col('observed_count')))
            vald_result_dataset_level_data = [
                Row(validation_type=self.vald_type, passed=False, total_records=self._df_count, error_records=err_record_count, ts=curr_dt)
            ]
        else:
            vald_passed = True
            df_vald_result_record_level = None
            vald_result_dataset_level_data = [
                Row(validation_type=self.vald_type, passed=True, total_records=self._df_count, error_records=0, ts=curr_dt),
            ]
        df_vald_result_dataset_level = self.spark.createDataFrame(data=vald_result_dataset_level_data)
        vald_result = DatasetValidationResult(spark=self.spark, vald_type=self.vald_type, vald_passed=vald_passed,
                                              df_vald_result_dataset_level=df_vald_result_dataset_level,
                                              df_vald_result_record_level=df_vald_result_record_level)
        self.set_vald_result(vald_result=vald_result)
        return vald_result
    
    def get_df(self) -> DataFrame:
        return self.df
    
    def get_vald_type(self) -> str:
        return self.vald_type
    
    def get_vald_result(self) -> DatasetValidationResult:
        return self.vald_result
    
    def set_vald_result(self, vald_result: DatasetValidationResult) -> None:
        self.vald_result = vald_result

class DatasetValidation:
    def __init__(self,
                 spark: SparkSession,
                 df: DataFrame,
                 vald_types: List[str],
                 primary_key_cols: List[str] = None,
                 non_nullable_cols: List[str] = None):
        self.spark = spark
        self.df = df
        self.vald_types = vald_types
        self.primary_key_cols = primary_key_cols
        self.non_nullable_cols = non_nullable_cols
        self.vald_checker_list = None
        self._df_count = None

    def _compute_df_metrics(self) -> Dict:
        self._df_count = self.df.count()
        return {
            'count': self._df_count
        }

    def perform_req_validations(self) -> None:
        computed_df_metrics = self._compute_df_metrics()
        self.vald_checker_list = [
            DatasetValidationChecker(
                spark=self.spark,
                df=self.df,
                vald_type=self.vald_types[idx],
                computed_df_metrics=computed_df_metrics,
                primary_key_cols=self.primary_key_cols,
                non_nullable_cols=self.non_nullable_cols) for idx, vald_type in enumerate(self.vald_types)
        ]
        for vald_checker in self.vald_checker_list:
            vald_checker.perform_validation()

    def get_df(self) -> DataFrame:
        return self.df
    
    def get_vald_types(self) -> List[str]:
        return self.vald_types
    
    def get_vald_checker_list(self) -> List[DatasetValidationChecker]:
        return self.vald_checker_list

if __name__ == '__main__':
    spark_session_builder = PysparkSessionBuilder(app_name='Pyspark App')
    spark = spark_session_builder.get_or_create_spark_session()
    file_connector = LocalFileConnector(spark=spark, file_path='data/raw/dataset1')
    df = file_connector.read_file()
    validations = ['primary_key_validation', 'null_validation']
    primary_key_cols = ['product_id']
    non_nullable_cols = ['item', 'quantity']
    dataset_vald = DatasetValidation(spark=spark,
                                     df=df,
                                     vald_types=validations,
                                     primary_key_cols=primary_key_cols,
                                     non_nullable_cols=non_nullable_cols)
    dataset_vald.perform_req_validations()
    for vald_checker in dataset_vald.get_vald_checker_list():
        vald_type = vald_checker.get_vald_type()
        vald_passed = vald_checker.get_vald_result().get_vald_passed()
        df_vald_result_dataset_level = vald_checker.get_vald_result().get_df_vald_result_dataset_level()
        df_vald_result_record_level = vald_checker.get_vald_result().get_df_vald_result_record_level()
        print(f'Validation checker for {vald_type}')
        print(f'Validation passed: {vald_passed}')
        print(f'Validation dataframe - dataset level: {df_vald_result_dataset_level.show()}')
        if df_vald_result_record_level is not None:
            print(f'Validation dataframe - record level: {df_vald_result_record_level.show()}')