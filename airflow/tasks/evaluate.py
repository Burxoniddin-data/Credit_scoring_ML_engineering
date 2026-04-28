import pandas as pd
from ml.models.evaluate import evaluate_model

def run_evaluation(**context):
    ti = context["ti"]
    predictions_path = ti.xcom_pull(task_ids="predict")
    df = pd.read_csv(predictions_path)
    metrics = evaluate_model(df)
    print("Evaluation:", metrics)

    return metrics