import pandas as pd
from src.cloud_storage.aws_storage import SimpleS3Storage
from src.entity.estimator import MyModel

class Project1Estimator:
    def __init__(self, bucket_name, model_path):
        self.bucket_name = bucket_name
        self.model_path = model_path
        self.s3 = SimpleS3Storage(bucket_name=bucket_name)
        self.loaded_model:MyModel = None

    def load_model(self):
        return self.s3.load_model(self.model_path)
    
    def save_model(self, from_file):
        self.s3.upload_model(file_path=from_file, key=self.model_path)

    def predict(self, dataframe: pd.DataFrame):
        if self.loaded_model == None:
            self.loaded_model = self.load_model()
        return self.loaded_model.predict(dataframe=dataframe)

