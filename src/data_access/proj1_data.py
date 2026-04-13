import sys
import pandas as pd
import numpy as np
from src.configuration.mongo_db_connection import MongoDBClient
from src.constants import DATABASE_NAME
from typing import Optional

class project1_data():
    def __init__(self):
        self.mongo_client = MongoDBClient(database_name=DATABASE_NAME)

    def export_collection_as_dataframe(self, collection_name: str, database_name: Optional[str] = None):
        
        collection = self.mongo_client.database[collection_name]
        print("Fetching data from MongoDB")
        df = pd.DataFrame(list(collection.find()))
        print(f"Data fetched with len: {len(df)}")
        return df


