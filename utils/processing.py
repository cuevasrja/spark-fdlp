import numpy as np
import pandas as pd
from typing import Tuple

def convert_to_vectors(df: pd.DataFrame, target: str = "popularity") -> Tuple[np.ndarray, np.ndarray]:
    """
    Converts a DataFrame to feature vectors and target variable.

    Args:
        df (pd.DataFrame): The DataFrame to convert.
        target (str): The name of the target variable column.

    Returns:
        Tuple[np.ndarray, np.ndarray]: A tuple containing the feature vectors and target variable.
    """
    # Separate features and target variable
    x = df.drop(columns=[target]).values
    # Normalize the feature vectors
    x = (x - np.mean(x, axis=0)) / np.std(x, axis=0)

    y = df[target].values

    return x, y

def casting(df: pd.DataFrame) -> pd.DataFrame:
    """
    Casts the DataFrame columns to appropriate data types.

    Args:
        df (pd.DataFrame): The DataFrame to cast.

    Returns:
        pd.DataFrame: The casted DataFrame.
    """
    # Cast columns to float:
    # acousticness,
    # energy,
    # loudness,
    # instrumentalness
    df['acousticness'] = df['acousticness'].astype(float)
    df['energy'] = df['energy'].astype(float)
    df['loudness'] = df['loudness'].astype(float)
    df['instrumentalness'] = df['instrumentalness'].astype(float)

    # Cast columns to int:
    # album_popularity,
    # artist_popularity
    # popularity
    df['album_popularity'] = df['album_popularity'].astype(int)
    df['artist_popularity'] = df['artist_popularity'].astype(int)
    df['popularity'] = df['popularity'].astype(int)

    return df
