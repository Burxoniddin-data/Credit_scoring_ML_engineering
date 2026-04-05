"""
register_connector.py
=====================
Registers the Debezium CDC connector via its REST API.
Works on Windows, Mac, and Linux — no bash needed.

Usage:
    python register_connector.py
"""

import json
import time
import urllib.request
import urllib.error
import os

DEBEZIUM_URL = os.getenv("DEBEZIUM_URL", "http://localhost:8083")
CONNECTOR_NAME = "credit-postgres-connector"
CONNECTOR_CONFIG = {
    "name": CONNECTOR_NAME,
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": "debezium",
        "database.password": "debezium_pass",
        "database.dbname": "credit_db",
        "plugin.name": "pgoutput",
        "topic.prefix": "credit",
        "table.include.list": (
            "public.users,"
            "public.accounts,"
            "public.loan_applications,"
            "public.loans,"
            "public.payment_events,"
            "public.credit_inquiries"
        ),
        "slot.name": "credit_debezium_slot",
        "publication.name": "credit_publication",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter.schemas.enable": "false",
        "transforms": "unwrap",
        "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
        "transforms.unwrap.drop.tombstones": "false",
        "transforms.unwrap.delete.handling.mode": "rewrite",
    }
}


def wait_for_debezium():
    print("Waiting for Debezium to be ready...")
    for attempt in range(1, 31):
        try:
            req = urllib.request.urlopen(f"{DEBEZIUM_URL}/connectors", timeout=5)
            req.read()
            print(f"Debezium is up!")
            return
        except Exception as e:
            print(f"  Not ready yet (attempt {attempt}/30): {e}")
            time.sleep(3)
    raise RuntimeError("Debezium did not become ready in time.")


def delete_existing_connector():
    try:
        req = urllib.request.Request(
            f"{DEBEZIUM_URL}/connectors/{CONNECTOR_NAME}",
            method="DELETE"
        )
        urllib.request.urlopen(req, timeout=10)
        print(f"Deleted existing connector: {CONNECTOR_NAME}")
        time.sleep(2)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            pass  # doesn't exist yet, that's fine
        else:
            raise


def register_connector():
    payload = json.dumps(CONNECTOR_CONFIG).encode("utf-8")
    req = urllib.request.Request(
        f"{DEBEZIUM_URL}/connectors",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    try:
        response = urllib.request.urlopen(req, timeout=10)
        result = json.loads(response.read())
        print(f"\nConnector registered successfully!")
        print(f"  Name:  {result['name']}")
        print(f"\nKafka topics being created:")
        for table in CONNECTOR_CONFIG["config"]["table.include.list"].split(","):
            topic = f"credit.{table.strip()}"
            print(f"  {topic}")
        print(f"\nKafka UI:        http://localhost:8080")
        print(f"Debezium status: {DEBEZIUM_URL}/connectors/{CONNECTOR_NAME}/status")
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"Failed to register connector (HTTP {e.code}): {body}")
        raise


def main():
    wait_for_debezium()
    delete_existing_connector()
    register_connector()


if __name__ == "__main__":
    main()