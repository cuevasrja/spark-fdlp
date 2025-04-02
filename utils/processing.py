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