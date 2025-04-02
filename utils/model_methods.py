import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.naive_bayes import GaussianNB
import os
import joblib

lr_model_path = "models/logistic_regression_model.pkl"
gnb_model_path = "models/gaussian_naive_bayes_model.pkl"
linear_model_path = "models/linear_model.pkl"

def load_lr_model() -> LogisticRegression:
    """
    Load the Logistic Regression model from a file.
    If the model does not exist, create a new one.
    """
    if not os.path.exists(lr_model_path):
        print("\033[94;1mLoading Logistic Regression...\033[0m")
        lr = LogisticRegression()
    else:
        print("\033[93;1mCreating Logistic Regression...\033[0m")
        lr = joblib.load(lr_model_path)
    return lr

def load_gnb_model() -> GaussianNB:
    """
    Load the Gaussian Naive Bayes model from a file.
    If the model does not exist, create a new one.
    """
    if not os.path.exists(gnb_model_path):
        print("\033[94;1mLoading Gaussian Naive Bayes...\033[0m")
        gnb = GaussianNB()
    else:
        print("\033[93;1mCreating Gaussian Naive Bayes...\033[0m")
        gnb = joblib.load(gnb_model_path)
    return gnb

def load_linear_model() -> LinearRegression:
    """
    Load the Linear Regression model from a file.
    If the model does not exist, create a new one.
    """
    if not os.path.exists(linear_model_path):
        print("\033[94;1mLoading Linear Regression...\033[0m")
        linear = LinearRegression()
    else:
        print("\033[93;1mCreating Linear Regression...\033[0m")
        linear = joblib.load(linear_model_path)
    return linear

def load_model(model_name: str) -> LogisticRegression|GaussianNB|LinearRegression:
    """
    Load the model from a file.
    If the model does not exist, create a new one.
    """
    if model_name == "logistic_regression":
        return load_lr_model()
    elif model_name == "gaussian_naive_bayes":
        return load_gnb_model()
    elif model_name == "linear_regression":
        return load_linear_model()
    else:
        raise ValueError(f"Unknown model name: {model_name}")

def save_model(model: LogisticRegression|GaussianNB|LinearRegression, model_path: str):
    """
    Save the model to a file.
    """
    if not os.path.exists("models"):
        os.makedirs("models")

    if os.path.exists(model_path):
        os.remove(model_path)
        print(f"\033[93;1mModel already exists. Replacing it...\033[0m")

    joblib.dump(model, model_path)
    print(f"\033[92;1mModel saved to {model_path}\033[0m")

def create_report(model_name: str, cm: np.ndarray, accuracy: float, mse: float):
    """
    Create a file report for the model.
    """
    report_path = f"reports/{model_name}_report.txt"
    if not os.path.exists("reports"):
        os.makedirs("reports")

    with open(report_path, "w") as report_file:
        report_file.write(f"Model: {model_name}\n")
        report_file.write(f"Accuracy: {accuracy}\n")
        report_file.write(f"Mean Squared Error: {mse}\n")
        report_file.write(f"Confusion Matrix (as DataFrame):\n")
        cm_df = pd.DataFrame(cm)
        report_file.write(cm_df.to_string(index=True))


    print(f"\033[92;1mReport saved to {report_path}\033[0m")

def save_confusion_matrix(cm: pd.DataFrame, model_name: str):
    """
    Save the confusion matrix as a PNG file.
    """
    if not os.path.exists("confusion_matrix"):
        os.makedirs("confusion_matrix")

    # Check if the confusion matrix already exists as a PNG file
    if os.path.exists(f"confusion_matrix/confusion_matrix_{model_name}.png"):
        os.remove(f"confusion_matrix/confusion_matrix_{model_name}.png")
        print(f"\033[93;1mConfusion matrix already exists. Replacing it...\033[0m")

    cm_def = pd.DataFrame(
        cm
    )
    sns.heatmap(cm_def, annot=True, fmt="d", cmap="Blues")
    plt.title(f"Confusion Matrix for {model_name}")
    plt.xlabel("Predicted")
    plt.ylabel("True")
    plt.savefig(f"confusion_matrix/confusion_matrix_{model_name}.png")
    plt.close()
    print(f"\033[92;1mConfusion matrix saved to confusion_matrix/confusion_matrix_{model_name}.png\033[0m")