#!/usr/bin/env python3
"""
verify_pipeline.py
==================
Run this from your host machine to verify the full pipeline is working:
  Generator → Postgres → Debezium → Kafka → Consumer → DWH (feature store)

Usage:
    python verify_pipeline.py
    python verify_pipeline.py --watch   # refresh every 3 seconds
"""

import argparse
import time
import os
import psycopg2
import psycopg2.extras

DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname":   os.getenv("POSTGRES_DB", "credit_db"),
    "user":     os.getenv("POSTGRES_USER", "burxoniddin"),
    "password": os.getenv("POSTGRES_PASSWORD", "Burxoniddin12345"),
}


def check(conn):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

        # ── Row counts: source vs DWH ─────────────────────────
        cur.execute("""
            SELECT
                (SELECT COUNT(*) FROM public.users)                    AS src_users,
                (SELECT COUNT(*) FROM dwh.users)                       AS dwh_users,
                (SELECT COUNT(*) FROM public.accounts)                 AS src_accounts,
                (SELECT COUNT(*) FROM dwh.accounts)                    AS dwh_accounts,
                (SELECT COUNT(*) FROM public.loans)                    AS src_loans,
                (SELECT COUNT(*) FROM dwh.loans)                       AS dwh_loans,
                (SELECT COUNT(*) FROM public.payment_events)           AS src_payments,
                (SELECT COUNT(*) FROM dwh.payment_events)              AS dwh_payments,
                (SELECT COUNT(*) FROM public.credit_inquiries)         AS src_inquiries,
                (SELECT COUNT(*) FROM dwh.credit_inquiries)            AS dwh_inquiries
        """)
        counts = cur.fetchone()

        print("\n" + "═" * 52)
        print("  PIPELINE HEALTH CHECK")
        print("═" * 52)
        print(f"  {'Table':<22} {'Source':>8}  {'DWH':>8}  {'Lag':>6}")
        print("  " + "─" * 48)
        for table in ['users', 'accounts', 'loans', 'payments', 'inquiries']:
            src = counts[f'src_{table}']
            dwh = counts[f'dwh_{table}']
            lag = src - dwh
            status = "✓" if lag == 0 else f"⚠ {lag}"
            print(f"  {table:<22} {src:>8}  {dwh:>8}  {status:>6}")

        # ── Latest payment events in DWH ──────────────────────
        cur.execute("""
            SELECT event_type, amount, event_timestamp, cdc_operation
            FROM dwh.payment_events
            ORDER BY dwh_inserted_at DESC
            LIMIT 5
        """)
        latest = cur.fetchall()
        print("\n  Latest events in DWH (payment_events):")
        print("  " + "─" * 48)
        for row in latest:
            ts = str(row['event_timestamp'])[:19]
            print(f"  [{row['cdc_operation']}] {row['event_type']:<18} ${row['amount']:>9.2f}  {ts}")

        # ── CDC lag: how far behind is the DWH? ───────────────
        cur.execute("""
            SELECT
                MAX(event_timestamp)                           AS latest_src_event,
                (SELECT MAX(event_timestamp) FROM dwh.payment_events) AS latest_dwh_event
            FROM public.payment_events
        """)
        lag_row = cur.fetchone()
        if lag_row['latest_src_event'] and lag_row['latest_dwh_event']:
            lag_sec = (lag_row['latest_src_event'] - lag_row['latest_dwh_event']).total_seconds()
            print(f"\n  CDC lag: {lag_sec:.1f} seconds")
        else:
            print("\n  CDC lag: N/A (no data yet)")

        # ── Dead letter queue ─────────────────────────────────
        cur.execute("""
            SELECT COUNT(*) AS dlq_count
            FROM information_schema.tables
            WHERE table_schema = 'dwh' AND table_name = 'consumer_dlq'
        """)
        if cur.fetchone()['dlq_count'] > 0:
            cur.execute("SELECT COUNT(*) AS n FROM dwh.consumer_dlq")
            dlq = cur.fetchone()['n']
            if dlq > 0:
                print(f"\n  ⚠  Dead Letter Queue: {dlq} failed messages")
                print("     Run: SELECT * FROM dwh.consumer_dlq ORDER BY failed_at DESC LIMIT 5;")

        print("═" * 52)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--watch", action="store_true", help="Refresh every 3 seconds")
    args = parser.parse_args()

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True

    try:
        if args.watch:
            print("Watching pipeline (Ctrl+C to stop)...")
            while True:
                check(conn)
                time.sleep(3)
        else:
            check(conn)
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()