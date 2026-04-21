from src.cloud_storage.aws_storage import SimpleS3Storage
from src.logger import logging
from src.entity.artifact_entity import ModelEvaluationArtifact, ModelPusherArtifact
from src.entity.config_entity import ModelPusherConfig
from src.entity.s3_estimator import Project1Estimator


class ModelPusher:
    def __init__(self, model_evaluation_artifact: ModelEvaluationArtifact, model_pusher_config: ModelPusherConfig):
        self.s3 = SimpleS3Storage
        self.model_evaluation_artifact = model_evaluation_artifact
        self.model_pusher_config = model_pusher_config
        self.project1_estimator = Project1Estimator(bucket_name=model_pusher_config.bucket_name, model_path=model_pusher_config.s3_model_key_path)


    def initiate_model_pusher(self):
        logging.info("Uploading artifacts folder to s3 bucket")
            
        logging.info("Uploading new model to S3 bucket....")
        self.project1_estimator.save_model(from_file=self.model_evaluation_artifact.trained_model_path)
        model_pusher_artifact = ModelPusherArtifact(model_pushed_path=self.model_pusher_config.s3_model_key_path)

        logging.info("Uploaded artifacts folder to s3 bucket")
        logging.info(f"Model pusher artifact: [{model_pusher_artifact}]")
        logging.info("Exited initiate_model_pusher method of ModelTrainer class")

        return model_pusher_artifact
        