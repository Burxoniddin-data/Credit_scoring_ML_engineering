from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.tasks.feature_engineering import run_feature_engineering
from airflow.tasks.train import run_training
from airflow.tasks.predict import run_prediction
from airflow.tasks.evaluate import run_evaluation

default_args = {
    "owner": "ml-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="credit_ml_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    default_args=default_args,
    params={
        "features_path": "/tmp/features.csv",
        "model_path": "/tmp/model.pkl",
        "predictions_path": "/tmp/predictions.csv",
    }

) as dag:

    feature_engineering = PythonOperator(
        task_id="feature_engineering",
        python_callable=run_feature_engineering,
        provide_context=True
    )

    train = PythonOperator(
        task_id="train",
        python_callable=run_training,
        provide_context=True
    )

    predict = PythonOperator(
        task_id="predict",
        python_callable=run_prediction,
        provide_context=True
    )

    evaluate = PythonOperator(
        task_id="evaluate",
        python_callable=run_evaluation,
        provide_context=True
    )

    feature_engineering >> train >> predict >> evaluate