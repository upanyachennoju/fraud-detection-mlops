import os

from pandas import DataFrame
from sklearn.model_selection import train_test_split

from src.entity.config_entity import DataIngestionConfig
from src.entity.artifact_entity import DataIngestionArtifact
from src.logger import logging
from src.data_access.proj1_data import project1_data

class DataIngestion:
    def __init__(self, data_ingestion_config: DataIngestionConfig=DataIngestionConfig()):
        self.data_ingestion_config = data_ingestion_config
    
    def export_data_into_feature_store(self):
        logging.info(f"Exporting data from mongodb")
        my_data = project1_data()
        dataframe = my_data.export_collection_as_dataframe(collection_name=self.data_ingestion_config.collection_name)            
        logging.info(f"Shape of dataframe: {dataframe.shape}")
        feature_store_file_path  = self.data_ingestion_config.feature_store_file_path
        dir_path = os.path.dirname(feature_store_file_path)
        os.makedirs(dir_path,exist_ok=True)
        logging.info(f"Saving exported data into feature store file path: {feature_store_file_path}")
        dataframe.to_csv(feature_store_file_path,index=False,header=True)
        return dataframe

    def initiate_data_ingestion(self):
        df = self.export_data_into_feature_store()
        logging.info("Got the data from mongodb")
        logging.info("Exited the inititate data ingestion method")
        data_ingestion_artifact = DataIngestionArtifact(data_file_path=self.data_ingestion_config.feature_store_file_path)
        logging.info(f"Data ingestion artifact: {data_ingestion_artifact}")
        return data_ingestion_artifact
    