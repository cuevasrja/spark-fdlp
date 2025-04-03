#!/usr/bin/env python3

import matplotlib.pyplot as plt
import fireducks.pandas as pd
import sqlite3
import seaborn as sns
import sys

n: int = 100000
if len(sys.argv) >= 2:
    try:
        n = int(sys.argv[1])
    except ValueError:
        print(f"Invalid argument: {sys.argv[1]}. Using default value: {n}")

query = f"""SELECT
    DISTINCT(t.id),
    af.acousticness,
    af.danceability,
    af.energy,
    af.instrumentalness,
    af.liveness,
    af.loudness,
    af.speechiness,
    af.tempo,
    af.valence,
    t.duration,
    t.explicit,
    af.key,
    af.mode,
    af.time_signature,
    artists.followers,
    albums.popularity AS album_popularity,
    artists.popularity AS artist_popularity,
    t.popularity
FROM tracks AS t
LEFT JOIN audio_features AS af ON t.audio_feature_id = af.id
LEFT JOIN r_albums_tracks AS rat ON t.id = rat.track_id
LEFT JOIN albums ON rat.album_id = albums.id
LEFT JOIN r_track_artist AS rta ON t.id = rta.track_id
LEFT JOIN artists ON rta.artist_id = artists.id
LEFT JOIN r_artist_genre AS genres ON artists.id = genres.artist_id
LIMIT {n};"""

# Create a connection to the database
conn = sqlite3.connect('spotify.sqlite')
cursor = conn.cursor()

conn.text_factory = lambda b: b.decode(errors = 'ignore')

# Función para forzar la decodificación en UTF-8
def force_utf8(df):
    for col in df.select_dtypes(include=[object]).columns:
        df[col] = df[col].apply(lambda x: x.encode('utf-8', errors='ignore').decode('utf-8', errors='ignore') if isinstance(x, str) else x)
    return df

print("\033[93;1mLoading data from database...\033[0m")
tracks: pd.DataFrame = pd.read_sql_query(query, conn)
# Drop the first column
tracks = tracks.drop(columns=["id"])
print("Force UTF-8\n")
tracks = force_utf8(tracks)

# Close the connection
conn.close()


print(f"Tracks: \033[92;1m{tracks.shape}\033[0m")
# Remove rows with NaN values
print("Removing NaN values")
tracks = tracks.dropna()
print(f"Tracks after removing NaN values: \033[92;1m{tracks.shape}\033[0m\n")

# Find the correlation between the features and the target variable
correlation = tracks.corr()

# Save the correlation matrix as a CSV file
correlation.to_csv('correlation_matrix.csv', index=True)

# Create a heatmap of the correlation matrix
plt.figure(figsize=(12, 8))
sns.heatmap(correlation, annot=True, cmap='coolwarm', fmt='.2f')
plt.title('Correlation Matrix')
plt.savefig('correlation_matrix.png')
plt.close()
# Print the correlation matrix
print("Correlation matrix:")
print(correlation)