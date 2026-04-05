FROM python:3.11-slim

WORKDIR /app

COPY kafka_consumer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY kafka_consumer/consumer.py .
COPY feature_store/schema.sql .

CMD ["python", "-u", "consumer.py"]