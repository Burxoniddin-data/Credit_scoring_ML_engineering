import mlflow
import os

def setup_mlflow():
    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
    mlflow.set_experiment("credit_score_model")

def start_run(run_name="training"):
    setup_mlflow()
    return mlflow.start_run(run_name=run_name)