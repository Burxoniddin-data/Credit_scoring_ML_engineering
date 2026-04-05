-- ============================================================
--  Credit Score ML - Feature Store Schema
--  This is a SEPARATE schema (dwh) inside the same Postgres.
--  In production you'd use a dedicated warehouse (Redshift, BigQuery),
--  but for this project one Postgres with two schemas keeps it simple.
--
--  The Kafka consumer reads CDC events from Kafka topics and
--  UPSERTs (INSERT or UPDATE) into these tables.
--
--  Why UPSERT? Because CDC sends both INSERTs and UPDATEs.
--  If a user's income changes, we get an UPDATE event — we want
--  to reflect that in the feature store, not insert a duplicate.
-- ============================================================

CREATE SCHEMA IF NOT EXISTS dwh;

-- ============================================================
--  TABLE: dwh.users
--  Mirror of public.users, kept in sync via CDC.
--  We add derived columns useful for feature engineering.
-- ============================================================
CREATE TABLE IF NOT EXISTS dwh.users (
    user_id             UUID PRIMARY KEY,
    first_name          VARCHAR(50),
    last_name           VARCHAR(50),
    email               VARCHAR(100),
    date_of_birth       DATE,
    employment_status   VARCHAR(20),
    monthly_income      NUMERIC(12,2),
    city                VARCHAR(50),
    cdc_operation       VARCHAR(1),   -- c=create, u=update, d=delete
    cdc_ts              TIMESTAMPTZ,  -- when the change happened in source DB
    dwh_updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
--  TABLE: dwh.accounts
--  Bank accounts snapshot. Balance is always the latest value.
-- ============================================================
CREATE TABLE IF NOT EXISTS dwh.accounts (
    account_id          UUID PRIMARY KEY,
    user_id             UUID NOT NULL,
    account_type        VARCHAR(20),
    balance             NUMERIC(14,2),
    is_active           BOOLEAN,
    opened_at           TIMESTAMPTZ,

    cdc_operation       VARCHAR(1),
    cdc_ts              TIMESTAMPTZ,
    dwh_updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
--  TABLE: dwh.loans
--  Live loan state. We track outstanding, missed payments, etc.
-- ============================================================
CREATE TABLE IF NOT EXISTS dwh.loans (
    loan_id             UUID PRIMARY KEY,
    application_id      UUID,
    user_id             UUID NOT NULL,
    principal           NUMERIC(14,2),
    outstanding         NUMERIC(14,2),
    monthly_payment     NUMERIC(12,2),
    next_due_date       DATE,
    payments_made       INTEGER,
    payments_missed     INTEGER,
    is_active           BOOLEAN,
    started_at          TIMESTAMPTZ,

    cdc_operation       VARCHAR(1),
    cdc_ts              TIMESTAMPTZ,
    dwh_updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
--  TABLE: dwh.loan_applications
--  Full history of all loan applications and their outcomes.
-- ============================================================
CREATE TABLE IF NOT EXISTS dwh.loan_applications (
    application_id      UUID PRIMARY KEY,
    user_id             UUID NOT NULL,
    loan_type           VARCHAR(20),
    requested_amount    NUMERIC(14,2),
    approved_amount     NUMERIC(14,2),
    interest_rate       NUMERIC(5,2),
    term_months         INTEGER,
    status              VARCHAR(20),
    applied_at          TIMESTAMPTZ,
    decision_at         TIMESTAMPTZ,

    cdc_operation       VARCHAR(1),
    cdc_ts              TIMESTAMPTZ,
    dwh_updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
--  TABLE: dwh.payment_events
--  Append-only ledger of every payment event.
--  Unlike the other tables, we INSERT only (not upsert) because
--  payment events are immutable — they don't get updated.
-- ============================================================
CREATE TABLE IF NOT EXISTS dwh.payment_events (
    event_id            UUID PRIMARY KEY,
    user_id             UUID NOT NULL,
    loan_id             UUID,
    account_id          UUID,
    event_type          VARCHAR(30),
    amount              NUMERIC(12,2),
    status              VARCHAR(20),
    description         TEXT,
    event_timestamp     TIMESTAMPTZ,

    cdc_operation       VARCHAR(1),
    cdc_ts              TIMESTAMPTZ,
    dwh_inserted_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
--  TABLE: dwh.credit_inquiries
--  Append-only log of credit checks.
-- ============================================================
CREATE TABLE IF NOT EXISTS dwh.credit_inquiries (
    inquiry_id          UUID PRIMARY KEY,
    user_id             UUID NOT NULL,
    inquiry_type        VARCHAR(20),
    requested_by        VARCHAR(100),
    purpose             VARCHAR(50),
    inquired_at         TIMESTAMPTZ,

    cdc_operation       VARCHAR(1),
    cdc_ts              TIMESTAMPTZ,
    dwh_inserted_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
--  TABLE: dwh.consumer_offsets
--  Tracks which Kafka messages we've already processed.
--  This gives us exactly-once semantics — if the consumer
--  crashes and restarts, it knows where to resume.
-- ============================================================
CREATE TABLE IF NOT EXISTS dwh.consumer_offsets (
    topic               VARCHAR(200),
    partition_id        INTEGER,
    last_offset         BIGINT NOT NULL DEFAULT -1,
    last_processed_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (topic, partition_id)
);

-- ============================================================
--  INDEXES for fast feature queries
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_dwh_accounts_user         ON dwh.accounts(user_id);
CREATE INDEX IF NOT EXISTS idx_dwh_loans_user            ON dwh.loans(user_id);
CREATE INDEX IF NOT EXISTS idx_dwh_loans_active          ON dwh.loans(user_id, is_active);
CREATE INDEX IF NOT EXISTS idx_dwh_loan_apps_user        ON dwh.loan_applications(user_id);
CREATE INDEX IF NOT EXISTS idx_dwh_loan_apps_status      ON dwh.loan_applications(user_id, status);
CREATE INDEX IF NOT EXISTS idx_dwh_payments_user         ON dwh.payment_events(user_id);
CREATE INDEX IF NOT EXISTS idx_dwh_payments_ts           ON dwh.payment_events(user_id, event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_dwh_payments_type         ON dwh.payment_events(user_id, event_type);
CREATE INDEX IF NOT EXISTS idx_dwh_inquiries_user        ON dwh.credit_inquiries(user_id);
CREATE INDEX IF NOT EXISTS idx_dwh_inquiries_ts          ON dwh.credit_inquiries(user_id, inquired_at DESC);

-- Grant access to the main user
GRANT ALL ON SCHEMA dwh TO burxoniddin;
GRANT ALL ON ALL TABLES IN SCHEMA dwh TO burxoniddin;