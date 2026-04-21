from dataclasses import dataclass

# 1. Data Ingestion
@dataclass
class DataIngestionArtifact:
    data_file_path: str


# 2. Data Validation
@dataclass
class DataValidationArtifact:
    validation_status: bool
    message: str
    validated_data_path: str


# 3. Data Transformation
@dataclass
class DataTransformationArtifact:
    transformed_data_path: str
    preprocessor_path: str


# 4. Model Trainer
@dataclass
class ModelTrainerArtifact:
    model_path: str


# 5. Model Evaluation
@dataclass
class ModelEvaluationArtifact:
    anomaly_ratio: float
    score_min: float
    score_max: float
    s3_model_path:str 
    trained_model_path:str


# 6. Model Pusher
@dataclass
class ModelPusherArtifact:
    model_pushed_path: str