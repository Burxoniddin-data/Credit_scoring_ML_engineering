CREATE TABLE IF NOT EXISTS users (
    user_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255) UNIQUE,
    date_of_birth DATE,
    phone VARCHAR(20),
    address TEXT,
    city VARCHAR(100),
    employment_status VARCHAR(50),
    monthly_income NUMERIC(12,2),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS accounts (
    account_id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(user_id),
    account_type VARCHAR(20),
    account_number VARCHAR(30) UNIQUE,
    balance NUMERIC(12,2) DEFAULT 0,
    is_active BOOLEAN DEFAULT TRUE,
    opened_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS loan_applications (
    application_id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(user_id),
    loan_type VARCHAR(20),
    requested_amount NUMERIC(12,2),
    approved_amount NUMERIC(12,2),
    interes t_rate NUMERIC(5,2),
    term_months INTEGER,
    purpose TEXT,
    status VARCHAR(20) DEFAULT 'pending',
    applied_at TIMESTAMPTZ DEFAULT NOW(),
    decision_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS loans (
    loan_id SERIAL PRIMARY KEY,
    application_id INTEGER REFERENCES loan_applications(application_id),
    user_id INTEGER REFERENCES users(user_id),
    principal NUMERIC(12,2),
    outstanding NUMERIC(12,2),
    monthly_payment NUMERIC(12,2),
    next_due_date DATE,
    payments_made INTEGER DEFAULT 0,
    payments_missed INTEGER DEFAULT 0,
    is_active BOOLEAN DEFAULT TRUE,
    started_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS payment_events (
    event_id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(user_id),
    loan_id INTEGER REFERENCES loans(loan_id),
    account_id INTEGER REFERENCES accounts(account_id),
    event_type VARCHAR(30),
    amount NUMERIC(12,2),
    status VARCHAR(20) DEFAULT 'completed',
    description TEXT,
    event_timestamp TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS credit_inquiries (
    inquiry_id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(user_id),
    inquiry_type VARCHAR(10),
    requested_by VARCHAR(100),
    purpose TEXT,
    inquired_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS predictions (
    prediction_id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(user_id),
    credit_score FLOAT,
    model_version VARCHAR(50),
    predicted_at TIMESTAMPTZ DEFAULT NOW()
);