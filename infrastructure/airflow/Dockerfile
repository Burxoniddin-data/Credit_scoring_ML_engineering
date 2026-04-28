FROM apache/airflow:3.2.0

USER root

RUN pip install --no-cache-dir \
    psycopg2-binary \
    pandas \
    scikit-learn \
    faker \
    mlflow

USER airflow