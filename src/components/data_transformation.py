import pandas as pd
import numpy as np
import os
from sklearn.preprocessing import RobustScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from src.entity.artifact_entity import DataIngestionArtifact, DataTransformationArtifact, DataValidationArtifact
from src.entity.config_entity import DataTransformationConfig
from src.constants import SCHEMA_FILE_PATH
from src.utils.main_utils import read_yaml_file, save_object, save_numpy_array_data
from src.logger import logging

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
    
    def get_data_transformer(self, df):
        logging.info("Entered get_data_transformer method in Data transformation class")
        schema = self._schema_config["processed_data_schema"]

        num_feats = [c for c in schema["numerical_columns"] if c in df.columns]
        cat_feats = [c for c in schema["categorical_columns"] if c in df.columns]
        logging.info("Cols loaded from schema")

        preprocessor = ColumnTransformer([
            ("num", RobustScaler(), num_feats),
            ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), cat_feats)
        ])

        pipeline = Pipeline(steps=[("Preprocessor", preprocessor)])
        logging.info("Final Pipeline is Ready")
        logging.info("Exited the get_data_transformer method of DataTransformation class.")

        return pipeline
    
    def _split_date_column(self, df: pd.DataFrame):
        df['TransactionDate'] = pd.to_datetime(df['TransactionDate'])
        df['PreviousTransactionDate'] = pd.to_datetime(df['PreviousTransactionDate'])

        df['TransactionYear'] = df['TransactionDate'].dt.year
        df['TransactionMonth'] = df['TransactionDate'].dt.month
        df['TransactionDay'] = df['TransactionDate'].dt.day
        df['TransactionHour'] = df['TransactionDate'].dt.hour + df['TransactionDate'].dt.minute / 60 + df['TransactionDate'].dt.second / 3600

        df['PrevTransactionYear'] = df['PreviousTransactionDate'].dt.year
        df['PrevTransactionMonth'] = df['PreviousTransactionDate'].dt.month
        df['PrevTransactionDay'] = df['PreviousTransactionDate'].dt.day
        df['PrevTransactionHour'] = df['PreviousTransactionDate'].dt.hour + df['PreviousTransactionDate'].dt.minute / 60 + df['PreviousTransactionDate'].dt.second / 3600

        df = df.drop(columns=["TransactionDate", "PreviousTransactionDate"])

        return df

    def _drop_id_column(self, df):
        logging.info("Dropping 'id' columns")
        drop_cols = self._schema_config['raw_data_schema']['drop_columns']
        cols_to_drop = [col for col in drop_cols if col in df.columns]
        df = df.drop(cols_to_drop, axis=1)
        return df
    
    def initiate_data_transformation(self):
        logging.info("Data Transformation started")
        if not self.data_validation_artifact.validation_status:
            raise Exception(self.data_validation_artifact.message)
        
        df = self.read_data(file_path=self.data_ingestion_artifact.data_file_path)
        extracted_df = self._split_date_column(df)
        extracted_df = self._drop_id_column(extracted_df)
        logging.info("Extracted the date and time, dropped id columns.")

        pipeline = self.get_data_transformer(extracted_df)
        transformed_arr = pipeline.fit_transform(extracted_df)
        logging.info("Transformed the data successfully")

        
        transformed_arr = np.array(transformed_arr)
        logging.info(f"Transformed data's dtype: {transformed_arr.dtype}")

        os.makedirs(os.path.dirname(self.data_transformation_config.transformed_file_path),exist_ok=True)
        os.makedirs(os.path.dirname(self.data_transformation_config.transformed_object_file_path), exist_ok=True)

        save_object(self.data_transformation_config.transformed_object_file_path, pipeline)
        save_numpy_array_data(self.data_transformation_config.transformed_file_path, transformed_arr)
        logging.info("Transformed data is saved")

        data_transformation_artifact = DataTransformationArtifact(
            transformed_data_path=self.data_transformation_config.transformed_file_path,
            preprocessor_path=self.data_transformation_config.transformed_object_file_path)
        
        return data_transformation_artifact