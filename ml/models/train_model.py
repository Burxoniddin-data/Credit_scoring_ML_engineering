import pandas as pd
import joblib
import mlflow
import mlflow.sklearn
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, f1_score
from mlflow_utils.tracking import start_run

def train_model(input_path="/tmp/features.csv", model_path="/tmp/model.pkl"):

    df = pd.read_csv(input_path)
    df = df.dropna()

    X = df[['debt_to_income', 'missed_ratio']]
    y = df['default_risk']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

    model = RandomForestClassifier(
        n_estimators=100,
        max_depth=5,
        random_state=42
    )

    with start_run("random_forest_training"):
        mlflow.log_param("model", "RandomForest")
        mlflow.log_param("n_estimators", 100)
        mlflow.log_param("max_depth", 5)

        model.fit(X_train, y_train)

        preds = model.predict(X_test)

        acc = accuracy_score(y_test, preds)
        f1 = f1_score(y_test, preds)

        mlflow.log_metric("accuracy", acc)
        mlflow.log_metric("f1_score", f1)

        joblib.dump(model, model_path)

        mlflow.sklearn.log_model(model, "model")

        mlflow.log_artifact(input_path, artifact_path="data")

    return model_path