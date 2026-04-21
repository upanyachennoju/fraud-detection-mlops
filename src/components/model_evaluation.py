import pandas as pd
import numpy as np
from src.entity.artifact_entity import DataTransformationArtifact, ModelTrainerArtifact, ModelEvaluationArtifact
from src.entity.config_entity import ModelEvaluationConfig
from src.logger import logging
from src.entity.estimator import MyModel
from src.utils.main_utils import load_numpy_array_data, load_object

class ModelEvaluation:
    def __init__(self, model_eval_config: ModelEvaluationConfig ,data_transformation_artifact: DataTransformationArtifact, model_trainer_artifact: ModelTrainerArtifact):
        self.data_transformation_artifact = data_transformation_artifact
        self.model_trainer_artifact = model_trainer_artifact
        self.model_eval_config = model_eval_config

    def initiate_model_evaluation(self):
        logging.info("Starting model evaluation")

        # load data
        data_arr = load_numpy_array_data(
            self.data_transformation_artifact.transformed_data_path
        )

        model: MyModel = load_object(self.model_trainer_artifact.model_path) # type: ignore

        # predict
        y_pred = model.predict(data_arr)
        scores = model.decision_function(data_arr)

        # evaluation metrics
        anomaly_ratio = np.mean(y_pred == -1)
        score_min = float(scores.min())
        score_max = float(scores.max())

        logging.info(f"Anomaly ratio: {anomaly_ratio}")
        logging.info(f"Score range: {score_min} to {score_max}")

        # top anomalies
        top_idx = np.argsort(scores)[:10]
        logging.info(f"Top anomaly indices: {top_idx}")
        s3_model_path = self.model_eval_config.s3_model_key_path

        model_evaluation_artifact = ModelEvaluationArtifact(
                                        anomaly_ratio=anomaly_ratio, 
                                        score_min=score_min, 
                                        score_max=score_max,
                                        s3_model_path=s3_model_path,
                                        trained_model_path=self.model_trainer_artifact.model_path)

        logging.info(f"Model evaluation artifact: {model_evaluation_artifact}")

        return model_evaluation_artifact
        