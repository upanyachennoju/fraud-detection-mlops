import pandas as pd
import numpy as np
from sklearn.pipeline import Pipeline
from src.logger import logging

class MyModel:
    def __init__(self, preprocessing_object, trained_model_object):
        self.preprocessing_object = preprocessing_object
        self.trained_model_object = trained_model_object

    def predict(self, dataframe):
        logging.info("Started prediction process")
        if isinstance(dataframe, np.ndarray):
            transformed_feature = dataframe
        else:
            transformed_feature = self.preprocessing_object.transform(dataframe)
        predictions = self.trained_model_object.predict(transformed_feature)
        return predictions
    
    def decision_function(self, dataframe):
        # If input is already a numpy array (pre-transformed), use it directly
        if isinstance(dataframe, np.ndarray):
            transformed_feature = dataframe
        else:
            # If input is a DataFrame, apply preprocessing
            transformed_feature = self.preprocessing_object.transform(dataframe)
        return self.trained_model_object.decision_function(transformed_feature)