import os
import numpy as np
from sklearn.ensemble import IsolationForest
from src.logger import logging
from src.utils.main_utils import load_numpy_array_data, load_object, save_object
from src.entity.config_entity import ModelTrainerConfig
from src.entity.artifact_entity import DataTransformationArtifact, ModelTrainerArtifact
from src.entity.estimator import MyModel


class ModelTrainer:
    def __init__(self, data_transformation_artifact: DataTransformationArtifact, model_trainer_config: ModelTrainerConfig):
        self.data_transformation_artifact = data_transformation_artifact
        self.model_trainer_config = model_trainer_config

    def train_model(self, data: np.array):
        logging.info("Training an Isolation forest.")
        model = IsolationForest()
        logging.info("Model training going on...")
        model.fit(data)
        return model
    
    def initiate_model_trainer(self):
        logging.info("Entered the initiate model trainer method in Model Trainer class.")
        data_arr = load_numpy_array_data(file_path=self.data_transformation_artifact.transformed_data_path)
        logging.info("Loaded the data")

        model = self.train_model(data_arr)
        preprocessing_obj = load_object(file_path=self.data_transformation_artifact.preprocessor_path)
        logging.info("Preprocessing object is loaded")
        my_model = MyModel(preprocessing_obj, model)
        save_object(self.model_trainer_config.trained_model_file_path, my_model)
        logging.info("Saved final model object that includes both preprocessing and the trained model")

        model_trainer_artifact = ModelTrainerArtifact(model_path=self.model_trainer_config.trained_model_file_path)
        logging.info(f"Model trainer artifact: {model_trainer_artifact}")
        return model_trainer_artifact

    