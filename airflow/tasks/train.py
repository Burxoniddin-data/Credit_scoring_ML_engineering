from ml.models.train_model import train_model

def run_training(**context):
    ti = context["ti"]
    params = context["params"]
    features_path = ti.xcom_pull(task_ids="feature_engineering")
    model_path = params.get("model_path", "/tmp/model.pkl")

    result = train_model(
        input_path=features_path,
        model_path=model_path
    )
    return result