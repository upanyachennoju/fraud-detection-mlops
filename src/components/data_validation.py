import os
import pandas as pd
import json
from pandas import DataFrame
from src.logger import logging
from src.utils.main_utils import read_yaml_file
from src.constants import SCHEMA_FILE_PATH
from src.entity.artifact_entity import DataIngestionArtifact, DataValidationArtifact
from src.entity.config_entity import DataValidationConfig

class DataValidation:
    def __init__(self, data_ingestion_artifact: DataIngestionArtifact, data_validation_config: DataValidationConfig):
        self.data_ingestion_artifact = data_ingestion_artifact
        self.data_validation_config = data_validation_config
        self._schema_config = read_yaml_file(file_path=SCHEMA_FILE_PATH)
    
    def is_column_exist(self, df:DataFrame):
        dataframe_cols = df.columns
        missing_numerical_cols = []
        missing_categorical_cols = []
        for col in self._schema_config['raw_data_schema']['numerical_columns']:
            if col not in dataframe_cols:
                missing_numerical_cols.append(col)
        
        if len(missing_numerical_cols)>0:
            logging.info(f"Missing numerical cols: {missing_numerical_cols}")

        for col in self._schema_config['raw_data_schema']['categorical_columns']:
            if col not in dataframe_cols:
                missing_categorical_cols.append(col)
        
        if len(missing_categorical_cols)>0:
            logging.info(f"Missing categorical cols: {missing_categorical_cols}")

        return False if len(missing_numerical_cols) > 0 or len(missing_categorical_cols) > 0 else True
    
    @staticmethod
    def read_data(file_path):
        return pd.read_csv(file_path)
    
    def initiate_data_validation(self):
        validation_error_msg = ""
        logging.info("Data validation is starting")
        df = self.read_data(file_path=self.data_ingestion_artifact.data_file_path)
        
        df.columns = df.columns.str.strip()
        if "_id" in df.columns:
            df = df.drop(columns=["_id"])
        status = self.is_column_exist(df)

        if not status:
            validation_error_msg += "Missing required columns. "
        else:
            logging.info(f"All numerical/categorical columns are present in the dataset: {status}")

        validation_status = len(validation_error_msg) == 0
        data_validation_artifact = DataValidationArtifact(validation_status=validation_status, 
                                                          validated_data_path=self.data_validation_config.validation_report_file_path,
                                                          message=validation_error_msg)
        
        report_dir = os.path.dirname(self.data_validation_config.validation_report_file_path)
        os.makedirs(report_dir, exist_ok=True)

        validation_report = {
            "validation_status": validation_status,
            "status": status
        }

        with open(self.data_validation_config.validation_report_file_path, "w") as report_file:
            json.dump(validation_report, report_file, indent=4)
        
        logging.info("Data validation artifact is created and saved into JSON file")
        logging.info(f"Data validation artifact: {data_validation_artifact}")
        return data_validation_artifact