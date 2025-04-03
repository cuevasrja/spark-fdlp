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
    if 'acousticness' in df.columns:
        df['acousticness'] = df['acousticness'].astype(float)
    if 'energy' in df.columns:
        df['energy'] = df['energy'].astype(float)
    if 'loudness' in df.columns:
        df['loudness'] = df['loudness'].astype(float)
    if 'instrumentalness' in df.columns:
        df['instrumentalness'] = df['instrumentalness'].astype(float)
    if 'danceability' in df.columns:
        df['danceability'] = df['danceability'].astype(float)
    if 'liveness' in df.columns:
        df['liveness'] = df['liveness'].astype(float)
    if 'speechiness' in df.columns:
        df['speechiness'] = df['speechiness'].astype(float)
    if 'valence' in df.columns:
        df['valence'] = df['valence'].astype(float)
    if 'tempo' in df.columns:
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
    if 'album_popularity' in df.columns:
        df['album_popularity'] = df['album_popularity'].astype(float)
    if 'artist_popularity' in df.columns:
        df['artist_popularity'] = df['artist_popularity'].astype(int)
    if 'popularity' in df.columns:
        df['popularity'] = df['popularity'].astype(int)
    if 'duration' in df.columns:
        df['duration'] = df['duration'].astype(int)
    if 'explicit' in df.columns:
        df['explicit'] = df['explicit'].astype(int)
    if 'key' in df.columns:
        df['key'] = df['key'].astype(int)
    if 'mode' in df.columns:
        df['mode'] = df['mode'].astype(int)
    if 'time_signature' in df.columns:
        df['time_signature'] = df['time_signature'].astype(int)
    if 'followers' in df.columns:
        df['followers'] = df['followers'].astype(int)
    if 'track_number' in df.columns:
        df['track_number'] = df['track_number'].astype(int)
    if 'disc_number' in df.columns:
        df['disc_number'] = df['disc_number'].astype(int)
    if 'is_playable' in df.columns:
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
    if 'id' in df.columns:
        df['id'] = df['id'].astype(str)
    if 'track_name' in df.columns:
        df['track_name'] = df['track_name'].astype(str)
    if 'artist_name' in df.columns:
        df['artist_name'] = df['artist_name'].astype(str)
    if 'genre_id' in df.columns:
        df['genre_id'] = df['genre_id'].astype(str)
    if 'audio_feature_id' in df.columns:
        df['audio_feature_id'] = df['audio_feature_id'].astype(str)
    if 'preview_url' in df.columns:
        df['preview_url'] = df['preview_url'].astype(str)
    if 'album_name' in df.columns:
        df['album_name'] = df['album_name'].astype(str)
    if 'album_group' in df.columns:
        df['album_group'] = df['album_group'].astype(str)
    if 'album_type' in df.columns:
        df['album_type'] = df['album_type'].astype(str)
    if 'release_date' in df.columns:
        df['release_date'] = df['release_date'].astype(str)

    return df
