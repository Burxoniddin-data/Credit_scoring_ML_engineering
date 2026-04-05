"""
Credit Score ML - Continuous Data Generator
============================================
Mimics a live banking system by inserting realistic data into Postgres
24/7. Think of this as the "production database" that never sleeps.

How it works:
  1. On first run, it seeds N users with accounts and initial loans.
  2. Then it enters an infinite loop that simulates daily banking activity:
     - Users make loan payments (or miss them)
     - New users sign up
     - New loan applications come in
     - Account deposits and withdrawals happen
     - Credit inquiries arrive

This feeds Debezium's CDC pipeline with a continuous stream of changes.
"""

import os
import time
import random
import logging
import uuid
from datetime import date, timedelta, datetime

import psycopg2
from psycopg2.extras import RealDictCursor
from faker import Faker

# ── Logging setup ────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("generator")

# ── Faker: generates realistic names, emails, addresses ─────
fake = Faker()
Faker.seed(42)  # reproducible for dev, remove in prod

# ── Configuration from environment variables ─────────────────
DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname":   os.getenv("POSTGRES_DB", "postgres"),
    "user":     os.getenv("POSTGRES_USER", "burxoniddin"),
    "password": os.getenv("POSTGRES_PASSWORD", "Burxoniddin12345"),
}

SEED_USERS       = int(os.getenv("SEED_USERS", 200))       # initial users to create
TICK_INTERVAL    = float(os.getenv("TICK_SECONDS", 2.0))   # seconds between activity ticks
NEW_USERS_CHANCE = float(os.getenv("NEW_USERS_CHANCE", 0.1))  # 10% chance per tick to add a new user


# ── Helpers ──────────────────────────────────────────────────

def connect() -> psycopg2.extensions.connection:
    """Connect to Postgres with retry logic (container might be starting)."""
    for attempt in range(1, 11):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            conn.autocommit = False
            log.info("Connected to PostgreSQL")
            return conn
        except psycopg2.OperationalError as e:
            log.warning(f"DB not ready (attempt {attempt}/10): {e}")
            time.sleep(5)
    raise RuntimeError("Could not connect to PostgreSQL after 10 attempts")


def random_employment() -> str:
    return random.choices(
        ['employed', 'self_employed', 'unemployed', 'retired'],
        weights=[60, 20, 10, 10]
    )[0]


def random_income(employment: str) -> float:
    """Income varies by employment type - affects creditworthiness."""
    ranges = {
        'employed':      (2000, 15000),
        'self_employed': (1000, 20000),
        'unemployed':    (0, 800),
        'retired':       (800, 4000),
    }
    lo, hi = ranges[employment]
    return round(random.uniform(lo, hi), 2)


def random_loan_type() -> str:
    return random.choices(
        ['personal', 'mortgage', 'auto', 'student', 'credit_card'],
        weights=[35, 15, 20, 15, 15]
    )[0]


def random_loan_amount(loan_type: str, income: float) -> float:
    """Loan amount is loosely tied to income - realistic banking behavior."""
    multipliers = {
        'personal':    (1, 5),
        'mortgage':    (10, 40),
        'auto':        (3, 10),
        'student':     (2, 8),
        'credit_card': (0.5, 3),
    }
    lo_m, hi_m = multipliers[loan_type]
    amount = income * random.uniform(lo_m, hi_m)
    return round(min(amount, 500_000), 2)  # cap at 500k


# ── Core creation functions ──────────────────────────────────

def create_user(cur) -> dict:
    """Insert one realistic user and return their data."""
    dob = fake.date_of_birth(minimum_age=18, maximum_age=75)
    employment = random_employment()
    income = random_income(employment)

    cur.execute("""
        INSERT INTO users
            (first_name, last_name, email, date_of_birth, national_id,
             phone, address, city, employment_status, monthly_income)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING user_id, monthly_income, employment_status
    """, (
        fake.first_name(),
        fake.last_name(),
        fake.unique.email(),
        dob,
        fake.unique.bothify(text='??########'),   # e.g. AB12345678
        fake.phone_number()[:20],
        fake.street_address(),
        fake.city(),
        employment,
        income,
    ))
    return cur.fetchone()


def create_account(cur, user_id: str, initial_balance: float) -> dict:
    """Give a user a checking account with some starting balance."""
    acct_num = fake.bothify(text='####-####-####-####')
    cur.execute("""
        INSERT INTO accounts
            (user_id, account_type, account_number, balance)
        VALUES (%s, 'checking', %s, %s)
        RETURNING account_id
    """, (user_id, acct_num, initial_balance))
    return cur.fetchone()


def create_loan_application(cur, user_id: str, income: float) -> dict | None:
    """
    Apply for a loan. Not all users apply right away.
    Returns the application dict or None if skipped.
    """
    if random.random() > 0.7:  # 70% chance a new user applies for a loan
        return None

    loan_type = random_loan_type()
    amount    = random_loan_amount(loan_type, income)
    term      = random.choice([12, 24, 36, 48, 60, 84, 120])  # months

    cur.execute("""
        INSERT INTO loan_applications
            (user_id, loan_type, requested_amount, term_months, purpose, status)
        VALUES (%s, %s, %s, %s, %s, 'pending')
        RETURNING application_id, requested_amount, term_months, loan_type
    """, (user_id, loan_type, amount, term, fake.sentence(nb_words=5)))
    return cur.fetchone()


def approve_loan(cur, app: dict, income: float) -> None:
    """
    Simulate bank deciding on a pending application.
    Approval logic: employed users with income > 1500 are usually approved.
    """
    # Decide: approve or reject
    approval_chance = 0.75 if income > 1500 else 0.30
    approved = random.random() < approval_chance

    status = 'approved' if approved else 'rejected'

    # Convert Decimal from DB to float BEFORE multiplication
    requested_amount = float(app['requested_amount'])
    approved_amount = round(requested_amount * random.uniform(0.8, 1.0), 2) if approved else None
    interest_rate = round(random.uniform(4.5, 24.0), 2) if approved else None

    cur.execute("""
                UPDATE loan_applications
                SET status          = %s,
                    approved_amount = %s,
                    interest_rate   = %s,
                    decision_at     = NOW(),
                    updated_at      = NOW()
                WHERE application_id = %s
                """, (status, approved_amount, interest_rate, app['application_id']))

    # If approved, create the live loan record
    if approved:
        # monthly payment formula (all operands are now float or int)
        monthly = round(
            (approved_amount * (interest_rate / 100 / 12)) /
            (1 - (1 + interest_rate / 100 / 12) ** (-app['term_months'])),
            2
        )
        next_due = date.today() + timedelta(days=30)

        cur.execute("""
                    INSERT INTO loans
                    (application_id, user_id, principal, outstanding,
                     monthly_payment, next_due_date)
                    SELECT application_id, user_id, %s, %s, %s, %s
                    FROM loan_applications
                    WHERE application_id = %s
                    """, (approved_amount, approved_amount, monthly, next_due, app['application_id']))

        # Also update the application status to 'active'
        cur.execute("""
                    UPDATE loan_applications
                    SET status     = 'active',
                        updated_at = NOW()
                    WHERE application_id = %s
                    """, (app['application_id'],))

        log.debug(f"Loan approved: ${approved_amount} @ {interest_rate}% for {app['term_months']}mo")

def make_payment(cur, loan: dict, user_id: str, account_id: str) -> None:
    """
    Simulate a monthly loan payment. Sometimes it's missed (bad!),
    sometimes late, sometimes on time.
    """
    roll = random.random()

    if roll < 0.05:
        # 5% chance: MISSED payment - worst for credit score
        event_type = 'missed_payment'
        amount = 0
        cur.execute("""
            UPDATE loans
            SET payments_missed = payments_missed + 1,
                next_due_date   = next_due_date + INTERVAL '1 month',
                updated_at      = NOW()
            WHERE loan_id = %s
        """, (loan['loan_id'],))

    elif roll < 0.15:
        # 10% chance: LATE payment - hurts but not as bad
        event_type = 'late_payment'
        amount = loan['monthly_payment']
        _reduce_outstanding(cur, loan, amount)

    else:
        # 85% chance: on-time payment - good!
        event_type = 'loan_payment'
        amount = loan['monthly_payment']
        _reduce_outstanding(cur, loan, amount)

    cur.execute("""
        INSERT INTO payment_events
            (user_id, loan_id, account_id, event_type, amount, description)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        user_id,
        loan['loan_id'],
        account_id,
        event_type,
        amount,
        f"Monthly payment for loan {loan['loan_id']}"
    ))


def _reduce_outstanding(cur, loan: dict, amount: float) -> None:
    """Reduce outstanding balance and advance the next due date."""
    # Convert Decimal to float
    outstanding = float(loan['outstanding'])
    new_outstanding = max(0, outstanding - amount)
    is_still_active = new_outstanding > 0

    cur.execute("""
        UPDATE loans
        SET outstanding      = %s,
            payments_made    = payments_made + 1,
            next_due_date    = next_due_date + INTERVAL '1 month',
            is_active        = %s,
            updated_at       = NOW()
        WHERE loan_id = %s
    """, (new_outstanding, is_still_active, loan['loan_id']))

    if not is_still_active:
        cur.execute("""
            UPDATE loan_applications SET status = 'closed', updated_at = NOW()
            WHERE application_id = %s
        """, (loan['application_id'],))
        log.debug(f"Loan {loan['loan_id']} fully paid off!")

def make_account_activity(cur, account_id: str, user_id: str) -> None:
    """Random deposits and withdrawals - simulates daily banking."""
    is_deposit = random.random() < 0.55
    event_type = 'deposit' if is_deposit else 'withdrawal'
    amount = round(random.uniform(50, 3000), 2)

    # Make sure account doesn't go negative
    if not is_deposit:
        cur.execute("SELECT balance FROM accounts WHERE account_id = %s", (account_id,))
        row = cur.fetchone()
        if row and row['balance'] < amount:
            # Convert Decimal to float before multiplication
            balance = float(row['balance'])
            amount = max(10, round(balance * 0.3, 2))

    # Update account balance – delta is float, balance is Decimal; DB handles conversion
    delta = amount if is_deposit else -amount
    cur.execute("""
        UPDATE accounts
        SET balance    = balance + %s,
            updated_at = NOW()
        WHERE account_id = %s
    """, (delta, account_id))

    cur.execute("""
        INSERT INTO payment_events
            (user_id, account_id, event_type, amount, description)
        VALUES (%s, %s, %s, %s, %s)
    """, (user_id, account_id, event_type, amount, fake.sentence(nb_words=4)))

def add_credit_inquiry(cur, user_id: str) -> None:
    """Simulate a credit check - e.g., applying for a card or loan."""
    inquiry_type = random.choices(['hard', 'soft'], weights=[40, 60])[0]
    cur.execute("""
        INSERT INTO credit_inquiries (user_id, inquiry_type, requested_by, purpose)
        VALUES (%s, %s, %s, %s)
    """, (
        user_id,
        inquiry_type,
        fake.company(),
        random.choice(['loan_application', 'credit_card', 'rental', 'employment_check'])
    ))


# ── Seeding ──────────────────────────────────────────────────

def seed_initial_data(conn) -> None:
    """
    Create the initial population of users, accounts, and loans.
    This runs once on first start (checks if users table is empty).
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT COUNT(*) as cnt FROM users")
        count = cur.fetchone()['cnt']

    if count > 0:
        log.info(f"Database already seeded with {count} users. Skipping seed.")
        return

    log.info(f"Seeding database with {SEED_USERS} initial users...")

    for i in range(SEED_USERS):
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Create user
                user = create_user(cur)
                user_id = str(user['user_id'])
                income  = float(user['monthly_income'])

                # Give them an account with some savings
                initial_balance = round(income * random.uniform(0.5, 6.0), 2)
                acct = create_account(cur, user_id, initial_balance)
                acct_id = str(acct['account_id'])

                # Maybe apply for a loan
                app = create_loan_application(cur, user_id, income)
                if app:
                    # Immediately decide on the application (simulate history)
                    approve_loan(cur, app, income)

                # Add some historical credit inquiries (0–4 per user)
                for _ in range(random.randint(0, 4)):
                    add_credit_inquiry(cur, user_id)

                # Simulate some payment history (0–12 past payments)
                conn.commit()

                # Now fetch active loans and simulate history
                with conn.cursor(cursor_factory=RealDictCursor) as cur2:
                    cur2.execute("""
                        SELECT l.*, la.application_id
                        FROM loans l
                        JOIN loan_applications la USING(application_id)
                        WHERE l.user_id = %s AND l.is_active = TRUE
                    """, (user_id,))
                    loans = cur2.fetchall()

                for loan in loans:
                    n_payments = random.randint(0, 12)
                    with conn.cursor(cursor_factory=RealDictCursor) as cur3:
                        for _ in range(n_payments):
                            make_payment(cur3, loan, user_id, acct_id)
                    conn.commit()

            if (i + 1) % 50 == 0:
                log.info(f"  Seeded {i + 1}/{SEED_USERS} users")

        except Exception as e:
            log.error(f"Error seeding user {i}: {e}")
            conn.rollback()

    log.info("Seeding complete!")


# ── Main simulation loop ─────────────────────────────────────

def simulation_tick(conn) -> None:
    """
    One tick of the simulation. Called every TICK_INTERVAL seconds.
    Picks random users and simulates banking activity for them.
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:

        # ── 1. Maybe add a new user ──────────────────────────
        if random.random() < NEW_USERS_CHANCE:
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as c:
                    user = create_user(c)
                    user_id = str(user['user_id'])
                    income  = float(user['monthly_income'])
                    initial_balance = round(income * random.uniform(0.5, 3.0), 2)
                    acct = create_account(c, user_id, initial_balance)
                    acct_id = str(acct['account_id'])
                    app = create_loan_application(c, user_id, income)
                    if app:
                        approve_loan(c, app, income)
                conn.commit()
                log.info(f"New user joined: {user_id[:8]}...")
            except Exception as e:
                log.error(f"Error creating new user: {e}")
                conn.rollback()

        # ── 2. Process pending loan applications ─────────────
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("""
                    SELECT la.*, u.monthly_income
                    FROM loan_applications la
                    JOIN users u USING(user_id)
                    WHERE la.status = 'pending'
                    ORDER BY RANDOM()
                    LIMIT 5
                """)
                pending = c.fetchall()

            for app in pending:
                with conn.cursor(cursor_factory=RealDictCursor) as c:
                    approve_loan(c, app, float(app['monthly_income']))
                conn.commit()
        except Exception as e:
            log.error(f"Error processing pending loans: {e}")
            conn.rollback()

        # ── 3. Simulate loan payments for active loans ────────
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("""
                    SELECT l.*, la.application_id,
                           a.account_id
                    FROM loans l
                    JOIN loan_applications la USING(application_id)
                    JOIN accounts a ON a.user_id = l.user_id
                    WHERE l.is_active = TRUE
                      AND l.next_due_date <= CURRENT_DATE + INTERVAL '5 days'
                    ORDER BY RANDOM()
                    LIMIT 10
                """)
                due_loans = c.fetchall()

            for loan in due_loans:
                with conn.cursor(cursor_factory=RealDictCursor) as c:
                    make_payment(c, loan, str(loan['user_id']), str(loan['account_id']))
                conn.commit()
                log.debug(f"Payment processed for loan {str(loan['loan_id'])[:8]}")

        except Exception as e:
            log.error(f"Error processing loan payments: {e}")
            conn.rollback()

        # ── 4. Random account activity ────────────────────────
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("""
                    SELECT account_id, user_id FROM accounts
                    WHERE is_active = TRUE
                    ORDER BY RANDOM() LIMIT 8
                """)
                accounts = c.fetchall()

            for acct in accounts:
                with conn.cursor(cursor_factory=RealDictCursor) as c:
                    make_account_activity(c, str(acct['account_id']), str(acct['user_id']))
                conn.commit()

        except Exception as e:
            log.error(f"Error in account activity: {e}")
            conn.rollback()

        # ── 5. Occasional new loan applications ───────────────
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("""
                    SELECT u.user_id, u.monthly_income
                    FROM users u
                    WHERE NOT EXISTS (
                        SELECT 1 FROM loan_applications la
                        WHERE la.user_id = u.user_id AND la.status = 'pending'
                    )
                    ORDER BY RANDOM() LIMIT 3
                """)
                potential_borrowers = c.fetchall()

            for u in potential_borrowers:
                if random.random() < 0.15:  # 15% chance they apply
                    with conn.cursor(cursor_factory=RealDictCursor) as c:
                        create_loan_application(c, str(u['user_id']), float(u['monthly_income']))
                    conn.commit()

        except Exception as e:
            log.error(f"Error creating new loan apps: {e}")
            conn.rollback()

        # ── 6. Random credit inquiries ────────────────────────
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("SELECT user_id FROM users ORDER BY RANDOM() LIMIT 3")
                users = c.fetchall()

            for u in users:
                if random.random() < 0.2:
                    with conn.cursor(cursor_factory=RealDictCursor) as c:
                        add_credit_inquiry(c, str(u['user_id']))
                    conn.commit()

        except Exception as e:
            log.error(f"Error adding credit inquiries: {e}")
            conn.rollback()


# ── Entry point ──────────────────────────────────────────────

def main():
    log.info("=" * 50)
    log.info("Credit Score ML - Data Generator starting")
    log.info(f"  Seed users:    {SEED_USERS}")
    log.info(f"  Tick interval: {TICK_INTERVAL}s")
    log.info("=" * 50)

    conn = connect()

    # Step 1: seed the database if empty
    seed_initial_data(conn)

    # Step 2: run the simulation forever
    tick = 0
    log.info("Starting continuous simulation loop...")
    while True:
        tick += 1
        try:
            simulation_tick(conn)
            if tick % 30 == 0:
                log.info(f"Simulation tick #{tick} — still running")
        except psycopg2.OperationalError:
            log.error("Lost DB connection, reconnecting...")
            conn = connect()
        except Exception as e:
            log.error(f"Unexpected error on tick {tick}: {e}")

        time.sleep(TICK_INTERVAL)


if __name__ == "__main__":
    main()