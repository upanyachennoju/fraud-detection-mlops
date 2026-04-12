import pandas as pd
import numpy as np
from sklearn.preprocessing import RobustScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from entity.artifact_entity import DataIngestionArtifact, DataTransformationArtifact, DataValidationArtifact
from entity.config_entity import DataTransformationConfig
from constants import SCHEMA_FILE_PATH
from src.utils.main_utils import read_yaml_file, save_object, save_numpy_array_data
from logger import logging

class DataTransformation:
    def __init__(self, data_ingestion_artifact: DataIngestionArtifact, 
                 data_transformation_config: DataTransformationConfig,
                 data_validation_artifact: DataValidationArtifact):
        
        self.data_ingestion_artifact = data_ingestion_artifact
        self.data_transformation_config = data_transformation_config
        self.data_validation_artifact = data_validation_artifact
        self._schema_config = read_yaml_file(file_path=SCHEMA_FILE_PATH)

    @staticmethod
    def read_data(file_path):
        return pd.read_csv(file_path)
    
    def get_data_transformer(self):
        logging.info("Entered get_data_transformer method in Data transformation class")
        scaler = RobustScaler()
        ohe = OneHotEncoder(handle_unknown='ignore')
        logging.info("Transformers are initialized: RobustScaler, OneHotEncoder")

        num_feats = self._schema_config["raw_data_schema"]["numerical_columns"]
        cat_feats = self._schema_config["raw_data_schema"]["categorical_columns"]
        logging.info("Cols loaded from schema")

        preprocessor = ColumnTransformer(transformers=[
            ("RobustScaler", scaler, num_feats),
            ("One Hot Encoder", ohe, cat_feats)
        ], remainder="passthrough")

        pipeline = Pipeline(steps=[("Preprocessor", preprocessor)])
        logging.info("Final Pipeline is Ready")
        logging.info("Exited the get_data_transformer method of DataTransformation class.")
        
        return pipeline
    
    def _split_date_column(self, df: pd.DataFrame):
        df["TransactionDate"] = pd.to_datetime(df["TransactionDate"])
        df["year"] = df["TransactionDate"].dt.year
        df["month"] = df["TransactionDate"].dt.month
        df["day"] = df["TransactionDate"].dt.day
        df["hour"] = df["TransactionDate"].dt.hour
        df = df.drop(columns=["TransactionDate", "PreviousTransactionDate"])

        return df

    
    def initiate_data_transformation(self):
        logging.info("Data Transformation started")
        if not self.data_validation_artifact.validation_status:
            raise Exception(self.data_validation_artifact.message)
        
        df = self.read_data(file_path=self.data_ingestion_artifact.data_file_path)
        extracted_df = self._split_date_column(df)
        logging.info("Extracted the date and time.")

        pipeline = self.get_data_transformer()
        transformed_df = pipeline.fit_transform(extracted_df)
        logging.info("Transformed the data successfully")

        save_object(self.data_transformation_config.transformed_object_file_path, pipeline)
        save_numpy_array_data(self.data_transformation_config.transformed_file_path, transformed_df)
        logging.info("Transformed data is saved")

        data_transformation_artifact = DataTransformationArtifact(
            transformed_data_path=self.data_transformation_config.transformed_file_path,
            preprocessor_path=self.data_transformation_config.transformed_object_file_path)
        
        return data_transformation_artifact