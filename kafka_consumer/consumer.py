"""
Credit Score ML - Kafka CDC Consumer
======================================
Reads every change event from the 6 Debezium topics and writes
them into the dwh schema (feature store) in Postgres.

Key concepts explained:
  - Consumer group: Kafka tracks which messages each "group" has read.
    If this consumer restarts, Kafka resumes from where it left off.
  - UPSERT: INSERT ... ON CONFLICT DO UPDATE — handles both new records
    (CDC 'c' events) and updates (CDC 'u' events) with one SQL statement.
  - Batch commits: we collect N messages before committing to Postgres,
    which is much faster than committing one at a time.
  - Dead letter queue (DLQ): messages that fail to process go to a
    separate table instead of crashing the consumer.
"""

import os
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

import psycopg2
import psycopg2.extras
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# ── Logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("consumer")

# ── Config ───────────────────────────────────────────────────
KAFKA_BOOTSTRAP    = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_GROUP_ID     = os.getenv("KAFKA_GROUP_ID", "credit-score-consumer")
KAFKA_AUTO_OFFSET  = os.getenv("KAFKA_AUTO_OFFSET", "earliest")  # start from beginning
BATCH_SIZE         = int(os.getenv("BATCH_SIZE", 50))             # commit every N messages
POLL_TIMEOUT_MS    = int(os.getenv("POLL_TIMEOUT_MS", 1000))

DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname":   os.getenv("POSTGRES_DB", "credit_db"),
    "user":     os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
}

# Debezium topic names — must match connector.json prefix + table name
TOPICS = [
    "credit.public.users",
    "credit.public.accounts",
    "credit.public.loans",
    "credit.public.loan_applications",
    "credit.public.payment_events",
    "credit.public.credit_inquiries",
]


# ── Database connection ──────────────────────────────────────

def connect_db() -> psycopg2.extensions.connection:
    for attempt in range(1, 11):
        try:
            conn = psycopg2.connect(**DB_CONFIG, cursor_factory=psycopg2.extras.RealDictCursor)
            conn.autocommit = False
            log.info("Connected to PostgreSQL (feature store)")
            return conn
        except psycopg2.OperationalError as e:
            log.warning(f"DB not ready ({attempt}/10): {e}")
            time.sleep(5)
    raise RuntimeError("Cannot connect to PostgreSQL")


def ensure_dwh_schema(conn) -> None:
    """Run the feature store DDL if tables don't exist yet."""
    ddl_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    if not os.path.exists(ddl_path):
        # Schema file might be mounted separately; skip if not found
        log.warning(f"schema.sql not found at {ddl_path}, skipping DDL")
        return
    with open(ddl_path) as f:
        ddl = f.read()
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()
    log.info("DWH schema ready")


# ── Kafka connection ─────────────────────────────────────────

def connect_kafka() -> KafkaConsumer:
    for attempt in range(1, 11):
        try:
            consumer = KafkaConsumer(
                *TOPICS,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=KAFKA_GROUP_ID,
                auto_offset_reset=KAFKA_AUTO_OFFSET,
                # Don't auto-commit offsets — we commit manually after DB write
                # This guarantees we don't "lose" a message if the DB write fails
                enable_auto_commit=False,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
                key_deserializer=lambda k: json.loads(k.decode("utf-8")) if k else None,
                max_poll_records=BATCH_SIZE,
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
            )
            log.info(f"Connected to Kafka: {KAFKA_BOOTSTRAP}")
            log.info(f"Subscribed to topics: {TOPICS}")
            return consumer
        except NoBrokersAvailable:
            log.warning(f"Kafka not ready ({attempt}/10), retrying in 5s...")
            time.sleep(5)
    raise RuntimeError("Cannot connect to Kafka")


# ── UPSERT handlers — one per topic ─────────────────────────
#
# Each function receives:
#   cur  — Postgres cursor
#   data — the flat JSON dict from Debezium (after ExtractNewRecordState)
#   op   — the CDC operation: 'c' (create), 'u' (update), 'd' (delete), 'r' (snapshot read)
#
# We use INSERT ... ON CONFLICT (pk) DO UPDATE SET ...
# This handles both INSERT ('c','r') and UPDATE ('u') with one query.
# For DELETE ('d'), we mark the record rather than physically deleting it.

def handle_users(cur, data: dict, op: str, cdc_ts: str) -> None:
    if op == 'd':
        # Soft-delete: mark with cdc_operation = 'd'
        cur.execute(
            "UPDATE dwh.users SET cdc_operation = 'd', dwh_updated_at = NOW() WHERE user_id = %s",
            (data.get("user_id"),)
        )
        return

    cur.execute("""
        INSERT INTO dwh.users (
            user_id, first_name, last_name, email,
            date_of_birth, employment_status, monthly_income, city,
            cdc_operation, cdc_ts, dwh_updated_at
        ) VALUES (
            %(user_id)s, %(first_name)s, %(last_name)s, %(email)s,
            %(date_of_birth)s, %(employment_status)s, %(monthly_income)s, %(city)s,
            %(op)s, %(cdc_ts)s, NOW()
        )
        ON CONFLICT (user_id) DO UPDATE SET
            first_name        = EXCLUDED.first_name,
            last_name         = EXCLUDED.last_name,
            email             = EXCLUDED.email,
            employment_status = EXCLUDED.employment_status,
            monthly_income    = EXCLUDED.monthly_income,
            city              = EXCLUDED.city,
            cdc_operation     = EXCLUDED.cdc_operation,
            cdc_ts            = EXCLUDED.cdc_ts,
            dwh_updated_at    = NOW()
    """, {**data, "op": op, "cdc_ts": cdc_ts})


def handle_accounts(cur, data: dict, op: str, cdc_ts: str) -> None:
    if op == 'd':
        cur.execute(
            "UPDATE dwh.accounts SET cdc_operation = 'd', dwh_updated_at = NOW() WHERE account_id = %s",
            (data.get("account_id"),)
        )
        return

    cur.execute("""
        INSERT INTO dwh.accounts (
            account_id, user_id, account_type, balance,
            is_active, opened_at, cdc_operation, cdc_ts
        ) VALUES (
            %(account_id)s, %(user_id)s, %(account_type)s, %(balance)s,
            %(is_active)s, %(opened_at)s, %(op)s, %(cdc_ts)s
        )
        ON CONFLICT (account_id) DO UPDATE SET
            balance        = EXCLUDED.balance,
            is_active      = EXCLUDED.is_active,
            cdc_operation  = EXCLUDED.cdc_operation,
            cdc_ts         = EXCLUDED.cdc_ts,
            dwh_updated_at = NOW()
    """, {**data, "op": op, "cdc_ts": cdc_ts})


def handle_loans(cur, data: dict, op: str, cdc_ts: str) -> None:
    if op == 'd':
        cur.execute(
            "UPDATE dwh.loans SET cdc_operation = 'd', dwh_updated_at = NOW() WHERE loan_id = %s",
            (data.get("loan_id"),)
        )
        return

    cur.execute("""
        INSERT INTO dwh.loans (
            loan_id, application_id, user_id, principal, outstanding,
            monthly_payment, next_due_date, payments_made, payments_missed,
            is_active, started_at, cdc_operation, cdc_ts
        ) VALUES (
            %(loan_id)s, %(application_id)s, %(user_id)s, %(principal)s, %(outstanding)s,
            %(monthly_payment)s, %(next_due_date)s, %(payments_made)s, %(payments_missed)s,
            %(is_active)s, %(started_at)s, %(op)s, %(cdc_ts)s
        )
        ON CONFLICT (loan_id) DO UPDATE SET
            outstanding      = EXCLUDED.outstanding,
            payments_made    = EXCLUDED.payments_made,
            payments_missed  = EXCLUDED.payments_missed,
            is_active        = EXCLUDED.is_active,
            next_due_date    = EXCLUDED.next_due_date,
            cdc_operation    = EXCLUDED.cdc_operation,
            cdc_ts           = EXCLUDED.cdc_ts,
            dwh_updated_at   = NOW()
    """, {**data, "op": op, "cdc_ts": cdc_ts})


def handle_loan_applications(cur, data: dict, op: str, cdc_ts: str) -> None:
    if op == 'd':
        cur.execute(
            "UPDATE dwh.loan_applications SET cdc_operation = 'd', dwh_updated_at = NOW() WHERE application_id = %s",
            (data.get("application_id"),)
        )
        return

    cur.execute("""
        INSERT INTO dwh.loan_applications (
            application_id, user_id, loan_type, requested_amount,
            approved_amount, interest_rate, term_months, status,
            applied_at, decision_at, cdc_operation, cdc_ts
        ) VALUES (
            %(application_id)s, %(user_id)s, %(loan_type)s, %(requested_amount)s,
            %(approved_amount)s, %(interest_rate)s, %(term_months)s, %(status)s,
            %(applied_at)s, %(decision_at)s, %(op)s, %(cdc_ts)s
        )
        ON CONFLICT (application_id) DO UPDATE SET
            approved_amount = EXCLUDED.approved_amount,
            interest_rate   = EXCLUDED.interest_rate,
            status          = EXCLUDED.status,
            decision_at     = EXCLUDED.decision_at,
            cdc_operation   = EXCLUDED.cdc_operation,
            cdc_ts          = EXCLUDED.cdc_ts,
            dwh_updated_at  = NOW()
    """, {**data, "op": op, "cdc_ts": cdc_ts})


def handle_payment_events(cur, data: dict, op: str, cdc_ts: str) -> None:
    # Payment events are immutable - we only INSERT, never update
    # If we see the same event_id twice (consumer restart), we ignore it
    cur.execute("""
        INSERT INTO dwh.payment_events (
            event_id, user_id, loan_id, account_id, event_type,
            amount, status, description, event_timestamp, cdc_operation, cdc_ts
        ) VALUES (
            %(event_id)s, %(user_id)s, %(loan_id)s, %(account_id)s, %(event_type)s,
            %(amount)s, %(status)s, %(description)s, %(event_timestamp)s, %(op)s, %(cdc_ts)s
        )
        ON CONFLICT (event_id) DO NOTHING
    """, {**data, "op": op, "cdc_ts": cdc_ts})


def handle_credit_inquiries(cur, data: dict, op: str, cdc_ts: str) -> None:
    cur.execute("""
        INSERT INTO dwh.credit_inquiries (
            inquiry_id, user_id, inquiry_type, requested_by,
            purpose, inquired_at, cdc_operation, cdc_ts
        ) VALUES (
            %(inquiry_id)s, %(user_id)s, %(inquiry_type)s, %(requested_by)s,
            %(purpose)s, %(inquired_at)s, %(op)s, %(cdc_ts)s
        )
        ON CONFLICT (inquiry_id) DO NOTHING
    """, {**data, "op": op, "cdc_ts": cdc_ts})


# ── Router: dispatch message to the right handler ────────────

# Maps Kafka topic → handler function
TOPIC_HANDLERS = {
    "credit.public.users":             handle_users,
    "credit.public.accounts":          handle_accounts,
    "credit.public.loans":             handle_loans,
    "credit.public.loan_applications": handle_loan_applications,
    "credit.public.payment_events":    handle_payment_events,
    "credit.public.credit_inquiries":  handle_credit_inquiries,
}


def write_dlq(conn, topic: str, raw_value: Any, error: str) -> None:
    """
    Dead Letter Queue — failed messages go here instead of crashing.
    You can inspect dwh.consumer_dlq to debug parsing errors.
    """
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS dwh.consumer_dlq (
                    id          SERIAL PRIMARY KEY,
                    topic       VARCHAR(200),
                    raw_value   TEXT,
                    error_msg   TEXT,
                    failed_at   TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            cur.execute("""
                INSERT INTO dwh.consumer_dlq (topic, raw_value, error_msg)
                VALUES (%s, %s, %s)
            """, (topic, json.dumps(raw_value), error))
        conn.commit()
    except Exception as e:
        log.error(f"Failed to write to DLQ: {e}")


def process_message(cur, topic: str, data: dict) -> bool:
    """
    Extract op + cdc_ts from the Debezium message and call the right handler.
    Returns True on success, False on error.

    Debezium message format (after ExtractNewRecordState transform):
      {
        "user_id": "...",
        "email": "...",
        ...all fields...,
        "__op": "c",          ← operation: c/u/d/r
        "__deleted": false,
        "__source_ts_ms": 1710000000000  ← epoch ms when change happened
      }
    """
    handler = TOPIC_HANDLERS.get(topic)
    if not handler:
        log.warning(f"No handler for topic: {topic}")
        return True  # skip unknown topics

    # Extract CDC metadata fields that Debezium adds
    op = data.pop("__op", "c") or "c"
    deleted = data.pop("__deleted", False)
    source_ts_ms = data.pop("__source_ts_ms", None)
    # Remove other Debezium metadata fields we don't need
    for meta_key in ["__table", "__schema", "__db", "__lsn", "__txId", "__ts_ms"]:
        data.pop(meta_key, None)

    if deleted:
        op = "d"

    # Convert source timestamp from ms epoch to ISO string
    cdc_ts = None
    if source_ts_ms:
        cdc_ts = datetime.fromtimestamp(source_ts_ms / 1000, tz=timezone.utc).isoformat()

    # Call the appropriate handler
    handler(cur, data, op, cdc_ts)
    return True


# ── Main consumer loop ───────────────────────────────────────

def run_consumer() -> None:
    log.info("=" * 55)
    log.info("Credit Score ML - Kafka Consumer starting")
    log.info(f"  Kafka:      {KAFKA_BOOTSTRAP}")
    log.info(f"  Group ID:   {KAFKA_GROUP_ID}")
    log.info(f"  Batch size: {BATCH_SIZE}")
    log.info("=" * 55)

    db_conn  = connect_db()
    ensure_dwh_schema(db_conn)
    consumer = connect_kafka()

    messages_processed = 0
    messages_failed    = 0
    batch_buffer       = []   # collect messages before committing

    log.info("Listening for CDC events...")

    try:
        while True:
            # poll() returns a dict: {TopicPartition -> [ConsumerRecord, ...]}
            raw_messages = consumer.poll(timeout_ms=POLL_TIMEOUT_MS)

            if not raw_messages:
                # No new messages — commit any pending batch
                if batch_buffer:
                    _commit_batch(db_conn, consumer, batch_buffer)
                    messages_processed += len(batch_buffer)
                    batch_buffer = []
                continue

            for tp, records in raw_messages.items():
                for record in records:
                    if record.value is None:
                        # Tombstone message (Kafka delete marker) — skip
                        continue

                    try:
                        with db_conn.cursor() as cur:
                            process_message(cur, record.topic, dict(record.value))
                        batch_buffer.append(record)

                    except Exception as e:
                        log.error(f"Error processing {record.topic} offset {record.offset}: {e}")
                        messages_failed += 1
                        write_dlq(db_conn, record.topic, record.value, str(e))
                        db_conn.rollback()
                        # Don't add to batch_buffer — skip this message

            # Commit batch when it's full
            if len(batch_buffer) >= BATCH_SIZE:
                _commit_batch(db_conn, consumer, batch_buffer)
                messages_processed += len(batch_buffer)
                log.info(
                    f"Batch committed: {messages_processed} total processed, "
                    f"{messages_failed} failed"
                )
                batch_buffer = []

    except KeyboardInterrupt:
        log.info("Consumer stopped by user")
    finally:
        if batch_buffer:
            _commit_batch(db_conn, consumer, batch_buffer)
        consumer.close()
        db_conn.close()
        log.info(f"Consumer shut down. Total processed: {messages_processed}, failed: {messages_failed}")


def _commit_batch(
    db_conn: psycopg2.extensions.connection,
    consumer: KafkaConsumer,
    batch: list
) -> None:
    """
    Commit Postgres transaction first, then Kafka offsets.
    Order matters: if Postgres commits but Kafka offset commit fails,
    we'll reprocess those messages — but ON CONFLICT DO NOTHING handles duplicates.
    If Kafka offsets commit but Postgres fails — we'd lose data.
    So: DB first, Kafka second.
    """
    db_conn.commit()
    consumer.commit()


# ── Entry point ──────────────────────────────────────────────

if __name__ == "__main__":
    run_consumer()