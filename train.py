#!/usr/bin/env python3

import pandas as pd
import sqlite3
import numpy as np
from sklearn.metrics import confusion_matrix
from utils.processing import convert_to_vectors
from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.naive_bayes import GaussianNB
import os
import joblib
from utils.model_methods import lr_model_path, gnb_model_path, linear_model_path, create_report, save_confusion_matrix

query = f"""SELECT
    DISTINCT(t.id),
    af.acousticness,
    af.energy,
    af.loudness,
    af.instrumentalness,
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
ORDER BY RANDOM()
LIMIT {100000};"""

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

x, y = convert_to_vectors(tracks, target="popularity")

# Split the data into training and testing sets
train_size = int(0.8 * len(x))
x_train, x_test = x[:train_size], x[train_size:]
y_train, y_test = y[:train_size], y[train_size:]

# Create the neural network
print("\033[93;1mCreating models...\033[0m")

if not os.path.exists("models"):
    os.makedirs("models")

lr: LogisticRegression = None
if not os.path.exists(lr_model_path):
    print("\033[93;1mCreating Logistic Regression...\033[0m")
    lr: LogisticRegression = LogisticRegression(max_iter=10000, solver='liblinear')
else:
    print("\033[94;1mLoading Logistic Regression...\033[0m")
    lr: LogisticRegression = joblib.load(lr_model_path)

gnb: GaussianNB = None
if not os.path.exists(gnb_model_path):
    print("\033[93;1mCreating Gaussian Naive Bayes...\033[0m")
    gnb: GaussianNB = GaussianNB()
else:
    print("\033[94;1mLoading Gaussian Naive Bayes...\033[0m")
    gnb: GaussianNB = joblib.load(gnb_model_path)

linear: LinearRegression = None
if not os.path.exists(linear_model_path):
    print("\033[93;1mCreating Linear Regression...\033[0m")
    linear: LinearRegression = LinearRegression()
else:
    print("\033[94;1mLoading Linear Regression...\033[0m")
    linear: LinearRegression = joblib.load(linear_model_path)

print("Training Logistic Regression")
lr.fit(x_train, y_train)
print("Training Gaussian Naive Bayes")
gnb.fit(x_train, y_train)
print("Training Linear Regression")
linear.fit(x_train, y_train)

print("Testing Logistic Regression")
lr_predictions: np.ndarray = lr.predict(x_test)
print("Testing Gaussian Naive Bayes")
gnb_predictions: np.ndarray = gnb.predict(x_test)
print("Testing Linear Regression")
linear_predictions: np.ndarray = linear.predict(x_test)
# Round the predictions to the nearest integer in linear regression
linear_predictions = np.round(linear_predictions).astype(int)
# Clip the predictions to be between 0 and 100
linear_predictions = np.clip(linear_predictions, 0, 100)

# Calculate the mean squared error
lr_mse = np.mean((lr_predictions - y_test) ** 2)
gnb_mse = np.mean((gnb_predictions - y_test) ** 2)
linear_mse = np.mean((linear_predictions - y_test) ** 2)
print(f"Logistic Regression Mean Squared Error: \033[94;1m{lr_mse}\033[0m")
print(f"Gaussian Naive Bayes Mean Squared Error: \033[94;1m{gnb_mse}\033[0m")
print(f"Linear Regression Mean Squared Error: \033[94;1m{linear_mse}\033[0m\n")

# Calculate the accuracy
lr_accuracy = np.mean(lr_predictions == y_test)
gnb_accuracy = np.mean(gnb_predictions == y_test)
linear_accuracy = np.mean(linear_predictions == y_test)
print(f"Logistic Regression Accuracy: \033[94;1m{lr_accuracy}\033[0m")
print(f"Gaussian Naive Bayes Accuracy: \033[94;1m{gnb_accuracy}\033[0m")
print(f"Linear Regression Accuracy: \033[94;1m{linear_accuracy}\033[0m")

print()

cm = confusion_matrix(y_test, lr_predictions)
save_confusion_matrix(cm, "logistic_regression")
create_report("logistic_regression", cm, lr_accuracy, lr_mse)

cm = confusion_matrix(y_test, gnb_predictions)
save_confusion_matrix(cm, "gaussian_naive_bayes")
create_report("gaussian_naive_bayes", cm, gnb_accuracy, gnb_mse)

cm = confusion_matrix(y_test, linear_predictions)
save_confusion_matrix(cm, "linear_regression")
create_report("linear_regression", cm, linear_accuracy, linear_mse)

print()

# Save the model
print("\033[93;1mSaving models...\033[0m")

if os.path.exists(lr_model_path):
    os.remove(lr_model_path)
if os.path.exists(gnb_model_path):
    os.remove(gnb_model_path)
if os.path.exists(linear_model_path):
    os.remove(linear_model_path)

# Save the models
params = lr.get_params()
joblib.dump(lr, lr_model_path)
joblib.dump(gnb, gnb_model_path)
joblib.dump(linear, linear_model_path)
print(f"Logistic Regression model saved as \033[96m{lr_model_path}\033[0m")
print(f"Gaussian Naive Bayes model saved as \033[96m{gnb_model_path}\033[0m")
print(f"Linear Regression model saved as \033[96m{linear_model_path}\033[0m")
print("\033[92;1mModels saved successfully!\033[0m")
print("\033[92;1mTraining completed!\033[0m")
print("\033[92;1mGoodbye!\033[0m")