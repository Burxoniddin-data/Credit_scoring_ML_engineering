from ml.models.predict_model import predict_model

def run_prediction(**context):
    ti = context["ti"]
    params = context["params"]
    model_path = ti.xcom_pull(task_ids="train")
    features_path = ti.xcom_pull(task_ids="feature_engineering")

    output_path = params.get("predictions_path", "/tmp/predictions.csv")
    result = predict_model(
        model_path=model_path,
        input_path=features_path,
        output_path=output_path
    )

    return result