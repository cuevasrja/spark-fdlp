from pyspark.sql import SparkSession, DataFrame
from utils.model_methods import load_model, save_model, analyze_model
from utils.session import create_spark_session
from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.naive_bayes import GaussianNB
import numpy as np
import pandas as pd
from utils.processing import convert_to_vectors

class ClassifyPopularity:
    """
    Classify the popularity of a song using Logistic Regression.
    """

    def __init__(self, filepath: str, model_name: str = "logistic_regression"):
        """
        Initialize the ClassifyPopularity class.
        :param model_name: The name of the model to use.
        """
        self.model_name: str = model_name
        self.model: LogisticRegression|LinearRegression|GaussianNB = load_model(model_name)
        self.session: SparkSession = create_spark_session("ClassifyPopularity")
        self.dataset: DataFrame = self.session.read.option("header", "true").option("inferSchema", "true").csv(filepath)
            
    def predict(self):
        """
        predict the model to the dataset.
        """
        # Convert the dataset to a Pandas DataFrame

        self.dataset.sort("id").explain()

        # df: pd.DataFrame = self.dataset.toPandas()
        
        # x: np.ndarray
        # y: np.ndarray
        # # Separate features and target variable
        # x, y = convert_to_vectors(df)
        
        # # Train the model
        # predictions = self.model.predict(x, y)

        # # Analyze the model
        # analyze_model(self.model, y, predictions)
        
        # # Save the model
        # save_model(self.model, self.model_name)