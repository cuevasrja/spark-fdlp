from pyspark.sql import SparkSession, DataFrame
from utils.model_methods import load_model, save_model, analyze_model
from utils.session import create_spark_session
from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.naive_bayes import GaussianNB
import numpy as np
import pandas as pd
from utils.processing import convert_to_vectors, casting

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
            
    def dataframe_method(self):
        """
        Classify the popularity of a song using Logistic Regression.
        """

        # Only keep the columns:
        # track_name,
        # acousticness,
        # energy,
        # loudness,
        # instrumentalness,
        # album_popularity,
        # artist_popularity
        # and the target variable (popularity)
        # df = df[["acousticness", "energy", "loudness", "instrumentalness", "album_popularity", "artist_popularity", "popularity"]]
        df: DataFrame = self.dataset.select("acousticness", "energy", "loudness", "instrumentalness", "album_popularity", "artist_popularity", "popularity")
        tracks: DataFrame = self.dataset.select("track_name")

        # Remove rows with missing values
        df = df.na.drop()

        # Convert the dataset to a Pandas DataFrame
        df_p: pd.DataFrame = df.toPandas()

        # Convert the columns to the correct types
        df_p = casting(df_p)

        x: np.ndarray
        y: np.ndarray
        # Separate features and target variable
        x, y = convert_to_vectors(df_p)
        
        # Train the model
        predictions = self.model.predict(x)

        # Analyze the model
        analyze_model(f"{self.model_name}-dataframe-spark", y, predictions)
        
        # Save the model
        save_model(self.model, self.model_name)

        # Convert the predictions to a Spark DataFrame with the track names
        predictions_df = pd.DataFrame(predictions, columns=["predictions"])
        predictions_df["track_name"] = tracks.toPandas()

        # Convert the Pandas DataFrame to a Spark DataFrame
        predictions_df = self.session.createDataFrame(predictions_df)
        predictions_df = predictions_df.select("track_name", "predictions")

        # Save the predictions to a CSV file
        predictions_df.write.mode("overwrite").option("header", "true").csv("out/predictions-dataframe")
        print(f"\033[92;1mCSV file saved\033[0m")

    def sql_method(self):
        """
        Classify the popularity of a song using Logistic Regression.
        """
        # Create a temporary view of the dataset
        self.dataset.createOrReplaceTempView("songs")

        # SQL query to select the relevant columns
        query = """
            SELECT acousticness, energy, loudness, instrumentalness, album_popularity, artist_popularity, popularity
            FROM songs
        """

        # Execute the SQL query and remove rows with missing values
        df: DataFrame = self.session.sql(query).na.drop()

        # Get the track names
        tracks: DataFrame = self.session.sql("SELECT track_name FROM songs")

        # Convert the dataset to a Pandas DataFrame
        df_p: pd.DataFrame = df.toPandas()

        # Convert the columns to the correct types
        df_p = casting(df_p)

        x: np.ndarray
        y: np.ndarray
        # Separate features and target variable
        x, y = convert_to_vectors(df_p)

        # Train the model
        predictions = self.model.predict(x)

        # Analyze the model
        analyze_model(f"{self.model_name}-sql-spark", y, predictions)
        
        # Save the model
        save_model(self.model, self.model_name)

        # Convert the predictions to a Spark DataFrame
        predictions_df = pd.DataFrame(predictions, columns=["predictions"])
        predictions_df["track_name"] = tracks.toPandas()

        # Convert the Pandas DataFrame to a Spark DataFrame
        predictions_df = self.session.createDataFrame(predictions_df)
        predictions_df = predictions_df.select("track_name", "predictions")

        # Save the predictions to a CSV file
        predictions_df.write.mode("overwrite").option("header", "true").csv("out/predictions-sql")
        print(f"\033[92;1mCSV file saved\033[0m")

    def stop_session(self):
        """
        Stop the Spark session.
        """
        self.session.stop()
        print(f"\033[92;1mSpark session stopped\033[0m")