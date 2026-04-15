import boto3
import pickle
from src.logger import logging

class SimpleS3Storage:
    def __init__(self, bucket_name):
        self.s3 = boto3.client("s3")
        self.bucket = bucket_name

    def upload_model(self, file_path, key):
        self.s3.upload_file(file_path, self.bucket, key)

    def download_model(self, key, file_path):
        self.s3.download_file(self.bucket, key, file_path)

    def load_model(self, key):
        self.download_model(key, "temp.pkl")
        with open("temp.pkl", "rb") as f:
            return pickle.load(f)