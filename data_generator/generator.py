import time
from db_postgresql.insert import *
from db_postgresql.queries import *
from data_generator.utils import *
from db_postgresql.connection import get_connection
from psycopg2.extras import RealDictCursor

def create_full_user(cur):
    employment = random_employment_status()
    income = random_monthly_income(employment)

    user = insert_user(cur, (
        fake.first_name(),
        fake.last_name(),
        fake.unique.email(),
        fake.date_of_birth(minimum_age=18, maximum_age=70),
        fake.phone_number(),
        fake.address(),
        fake.city(),
        employment,
        income
    ))

    user_id = user["user_id"]

    account = insert_account(
        cur,
        user_id,
        fake.bothify(text="####-####"),
        round(income * random.uniform(0.5, 3), 2)
    )
    loan_type = random_loan_type()

    app = insert_loan_application(
        cur,
        user_id,
        loan_type,
        random_loan_amount(loan_type, income),
        random.choice([12, 24, 36]),
        fake.sentence()
    )

    loan = approve_loan(cur, {
        "application_id": app['application_id'],
        "requested_amount": app['requested_amount'],
        "term_months": app['term_months'],
        "user_id": user_id
    }, income)
    return user_id


def seed(conn, n):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        for _ in range(n):
            create_full_user(cur)
    conn.commit()

def simulation_tick(conn):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        if random.random() < 0.1:
            create_full_user(cur)
        loans = get_active_loans(cur)
        for loan in loans:
            make_payment(cur, loan)
        cur.execute("SELECT account_id, user_id FROM accounts ORDER BY RANDOM() LIMIT 5")
        accounts = cur.fetchall()
        for acc in accounts:
            account_activity(cur, acc)
        cur.execute("SELECT user_id FROM users ORDER BY RANDOM() LIMIT 3")
        users = cur.fetchall()
        for u in users:
            if random.random() < 0.3:
                add_inquiry(cur, u['user_id'])
    conn.commit()

def run():
    conn = get_connection()
    # n = int(input("Number of users you want to create: "))
    seed(conn, 2000)
    while True:
        try:
            simulation_tick(conn)
            time.sleep(2)
        except Exception as e:
            import traceback
            traceback.print_exc()
            conn.rollback()