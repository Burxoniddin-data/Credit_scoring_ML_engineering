-- ============================================================
--  Credit Score ML - PostgreSQL Schema
--  This is the OLTP (operational) database - the "source of truth"
--  that Debezium will watch for changes via CDC.
-- ============================================================

-- Enable UUID generation (built into Postgres 13+)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================
--  TABLE: users
--  Represents bank customers. Each user will get a credit score.
-- ============================================================
CREATE TABLE IF NOT EXISTS users (
    user_id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    first_name      VARCHAR(50)  NOT NULL,
    last_name       VARCHAR(50)  NOT NULL,
    email           VARCHAR(100) NOT NULL UNIQUE,
    date_of_birth   DATE         NOT NULL,
    national_id     VARCHAR(20)  NOT NULL UNIQUE,     -- passport / SSN equivalent
    phone           VARCHAR(20),
    address         TEXT,
    city            VARCHAR(50),
    employment_status VARCHAR(20) NOT NULL             -- employed, self_employed, unemployed, retired
        CHECK (employment_status IN ('employed','self_employed','unemployed','retired')),
    monthly_income  NUMERIC(12,2) NOT NULL DEFAULT 0, -- in USD
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ============================================================
--  TABLE: accounts
--  A user can have one or more bank accounts (checking/savings).
--  We track balance here - this changes frequently.
-- ============================================================
CREATE TABLE IF NOT EXISTS accounts (
    account_id      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id         UUID         NOT NULL REFERENCES users(user_id),
    account_type    VARCHAR(20)  NOT NULL
        CHECK (account_type IN ('checking','savings')),
    account_number  VARCHAR(20)  NOT NULL UNIQUE,
    balance         NUMERIC(14,2) NOT NULL DEFAULT 0,
    currency        VARCHAR(3)   NOT NULL DEFAULT 'USD',
    is_active       BOOLEAN      NOT NULL DEFAULT TRUE,
    opened_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ============================================================
--  TABLE: loan_applications
--  Tracks every loan request made by a user.
--  Status changes over time (pending → approved/rejected → active → closed).
--  This is gold for credit scoring - a history of credit behavior.
-- ============================================================
CREATE TABLE IF NOT EXISTS loan_applications (
    application_id  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id         UUID         NOT NULL REFERENCES users(user_id),
    loan_type       VARCHAR(20)  NOT NULL
        CHECK (loan_type IN ('personal','mortgage','auto','student','credit_card')),
    requested_amount NUMERIC(14,2) NOT NULL,
    approved_amount  NUMERIC(14,2),                   -- NULL if not yet approved
    interest_rate    NUMERIC(5,2),                    -- e.g. 12.50 means 12.5% per year
    term_months      INTEGER      NOT NULL,            -- loan duration in months
    purpose          TEXT,
    status           VARCHAR(20)  NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending','approved','rejected','active','closed','defaulted')),
    applied_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    decision_at      TIMESTAMPTZ,                     -- when bank decided
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ============================================================
--  TABLE: loans
--  Once a loan_application is approved, a loan record is created.
--  This tracks the live loan: outstanding balance, next due date.
-- ============================================================
CREATE TABLE IF NOT EXISTS loans (
    loan_id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    application_id   UUID         NOT NULL UNIQUE REFERENCES loan_applications(application_id),
    user_id          UUID         NOT NULL REFERENCES users(user_id),
    principal        NUMERIC(14,2) NOT NULL,          -- original loan amount approved
    outstanding      NUMERIC(14,2) NOT NULL,          -- what's still owed
    monthly_payment  NUMERIC(12,2) NOT NULL,          -- required payment each month
    next_due_date    DATE         NOT NULL,
    payments_made    INTEGER      NOT NULL DEFAULT 0,
    payments_missed  INTEGER      NOT NULL DEFAULT 0,
    is_active        BOOLEAN      NOT NULL DEFAULT TRUE,
    started_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ============================================================
--  TABLE: payment_events
--  The heartbeat of the system. Every transaction goes here:
--  loan repayments, account deposits/withdrawals.
--  This table grows fastest - perfect for CDC streaming.
-- ============================================================
CREATE TABLE IF NOT EXISTS payment_events (
    event_id        UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id         UUID         NOT NULL REFERENCES users(user_id),
    loan_id         UUID         REFERENCES loans(loan_id),    -- NULL if not loan-related
    account_id      UUID         REFERENCES accounts(account_id),
    event_type      VARCHAR(30)  NOT NULL
        CHECK (event_type IN (
            'loan_payment',       -- regular monthly payment
            'missed_payment',     -- payment was due but not made
            'late_payment',       -- made but after due date
            'early_payoff',       -- paid off before term ended
            'deposit',            -- money coming into account
            'withdrawal',         -- money leaving account
            'transfer_in',
            'transfer_out'
        )),
    amount          NUMERIC(12,2) NOT NULL,
    currency        VARCHAR(3)   NOT NULL DEFAULT 'USD',
    status          VARCHAR(20)  NOT NULL DEFAULT 'completed'
        CHECK (status IN ('pending','completed','failed','reversed')),
    description     TEXT,
    event_timestamp TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ============================================================
--  TABLE: credit_inquiries
--  Every time someone checks a user's credit (applying for a loan,
--  renting an apartment, etc). Too many inquiries = red flag.
-- ============================================================
CREATE TABLE IF NOT EXISTS credit_inquiries (
    inquiry_id      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id         UUID         NOT NULL REFERENCES users(user_id),
    inquiry_type    VARCHAR(20)  NOT NULL
        CHECK (inquiry_type IN ('hard','soft')),       -- hard = affects score, soft = doesn't
    requested_by    VARCHAR(100),                      -- bank or company name
    purpose         VARCHAR(50),
    inquired_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ============================================================
--  INDEXES
--  We index foreign keys and columns we'll filter/join on often.
--  This speeds up Debezium reads and feature engineering queries.
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_accounts_user_id        ON accounts(user_id);
CREATE INDEX IF NOT EXISTS idx_loan_apps_user_id       ON loan_applications(user_id);
CREATE INDEX IF NOT EXISTS idx_loan_apps_status        ON loan_applications(status);
CREATE INDEX IF NOT EXISTS idx_loans_user_id           ON loans(user_id);
CREATE INDEX IF NOT EXISTS idx_payment_events_user_id  ON payment_events(user_id);
CREATE INDEX IF NOT EXISTS idx_payment_events_ts       ON payment_events(event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_payment_events_type     ON payment_events(event_type);
CREATE INDEX IF NOT EXISTS idx_credit_inquiries_user   ON credit_inquiries(user_id);

-- ============================================================
--  LOGICAL REPLICATION
--  Required for Debezium CDC. This tells Postgres to write enough
--  detail in its WAL (Write-Ahead Log) for Debezium to capture
--  full row changes (not just diffs).
-- ============================================================
ALTER TABLE users             REPLICA IDENTITY FULL;
ALTER TABLE accounts          REPLICA IDENTITY FULL;
ALTER TABLE loan_applications REPLICA IDENTITY FULL;
ALTER TABLE loans             REPLICA IDENTITY FULL;
ALTER TABLE payment_events    REPLICA IDENTITY FULL;
ALTER TABLE credit_inquiries  REPLICA IDENTITY FULL;

-- Create a dedicated replication user for Debezium
-- (password set in docker-compose via env var, here we just grant rights)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'debezium') THEN
        CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD 'debezium_pass';
    END IF;
END
$$;

GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;