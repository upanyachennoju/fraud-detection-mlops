import pandas as pd
from sklearn.pipeline import Pipeline
from src.logger import logging

class MyModel:
    def __init__(self, preprocessing_object, trained_model_object):
        self.preprocessing_object = preprocessing_object
        self.trained_model_object = trained_model_object

    def predict(self, dataframe: pd.DataFrame):
        logging.info("Started prediction process")
        transformed_feature = self.preprocessing_object.transform(dataframe)
        predictions = self.trained_model_object.predict(transformed_feature)
        return predictions
    