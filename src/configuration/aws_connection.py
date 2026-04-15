import os
from dotenv import load_dotenv
import boto3
from src.constants import REGION_NAME as region_name

load_dotenv()

class S3Client:
    s3_client = None
    s3_resource = None
    def __init__(self):
        if S3Client.s3_client==None or S3Client.s3_resource==None:
            __access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
            __secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
            if __access_key_id is None:
                raise Exception("Environment variable AWS access key is not set.")
            if __secret_access_key is None:
                raise Exception("Environment variabel AWS Secret access key is not set.")
            S3Client.s3_resource = boto3.resource('s3', aws_access_key_id=__access_key_id, aws_secret_access_key=__secret_access_key, region_name=region_name)
            S3Client.s3_client = boto3.client('s3', aws_access_key_id=__access_key_id, aws_secret_access_key=__secret_access_key, region_name=region_name)

        self.s3_client = S3Client.s3_client
        self.s3_resource = S3Client.s3_resource
        