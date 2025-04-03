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
    # instrumentalness,
    # danceability,
    # liveness,
    # speechiness,
    # valence,
    # tempo
    df['acousticness'] = df['acousticness'].astype(float)
    df['energy'] = df['energy'].astype(float)
    df['loudness'] = df['loudness'].astype(float)
    df['instrumentalness'] = df['instrumentalness'].astype(float)
    df['danceability'] = df['danceability'].astype(float)
    df['liveness'] = df['liveness'].astype(float)
    df['speechiness'] = df['speechiness'].astype(float)
    df['valence'] = df['valence'].astype(float)
    df['tempo'] = df['tempo'].astype(float)

    # Cast columns to int:
    # album_popularity,
    # artist_popularity,
    # popularity,
    # duration,
    # explicit,
    # key,
    # mode,
    # time_signature,
    # followers,
    # track_number,
    # disc_number,
    # is_playable
    df['album_popularity'] = df['album_popularity'].astype(int)
    df['artist_popularity'] = df['artist_popularity'].astype(int)
    df['popularity'] = df['popularity'].astype(int)
    df['duration'] = df['duration'].astype(int)
    df['explicit'] = df['explicit'].astype(int)
    df['key'] = df['key'].astype(int)
    df['mode'] = df['mode'].astype(int)
    df['time_signature'] = df['time_signature'].astype(int)
    df['followers'] = df['followers'].astype(int)
    df['track_number'] = df['track_number'].astype(int)
    df['disc_number'] = df['disc_number'].astype(int)
    df['is_playable'] = df['is_playable'].astype(int)

    # Cast columns to str:
    # id,
    # track_name,
    # artist_name,
    # genre_id,
    # audio_feature_id,
    # preview_url,
    # album_name.
    # album_group,
    # album_type,
    # release_date,
    df['id'] = df['id'].astype(str)
    df['track_name'] = df['track_name'].astype(str)
    df['artist_name'] = df['artist_name'].astype(str)
    df['genre_id'] = df['genre_id'].astype(str)
    df['audio_feature_id'] = df['audio_feature_id'].astype(str)
    df['preview_url'] = df['preview_url'].astype(str)
    df['album_name'] = df['album_name'].astype(str)
    df['album_group'] = df['album_group'].astype(str)
    df['album_type'] = df['album_type'].astype(str)
    df['release_date'] = df['release_date'].astype(str)

    return df
