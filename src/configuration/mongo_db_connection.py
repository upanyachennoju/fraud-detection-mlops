import os
from dotenv import load_dotenv
import pymongo
import certifi
from src.logger import logging
from src.constants import DATABASE_NAME

load_dotenv()

ca = certifi.where()

class MongoDBClient():
    def __init__(self, database_name: str = DATABASE_NAME):
        mongodb_url = os.getenv("MONGODB_CONNECTION_URL")
        MongoDBClient.client = pymongo.MongoClient(mongodb_url, tlsCAFile=ca)
        self.client = MongoDBClient.client
        self.database = self.client[database_name]
        self.database_name = database_name
        logging.info("MongoDB connection was successfull.")
