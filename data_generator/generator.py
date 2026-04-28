import time
from db_postgresql.insert import *
from db_postgresql.queries import *
from data_generator.utils import *
from db_postgresql.connection import get_connection

def create_full_user(cur):
    employment = random_employment_status()
    income = random_monthly_income(employment)

    user = insert_user(cur, (
        fake.first_name(),
        fake.last_name(),
        fake.unique.email(),
        fake.date_of_birth(18, 75),
        fake.phone_number(),
        fake.address(),
        fake.city(),
        employment,
        income
    ))

    user_id = user[0]

    account = insert_account(
        cur,
        user_id,
        fake.bothify(text="####-####"),
        round(income * random.uniform(0.5, 3), 2)
    )

    app = insert_loan_application(
        cur,
        user_id,
        random_loan_type(),
        random_loan_amount(random_loan_type(), income),
        random.choice([12, 24, 36]),
        fake.sentence()
    )

    loan = approve_loan(cur, {
        "application_id": app[0],
        "requested_amount": app[1],
        "term_months": app[2],
        "user_id": user_id
    }, income)

    return user_id


def seed(conn, n):
    print("Seeding...")
    with conn.cursor() as cur:
        for _ in range(n):
            create_full_user(cur)
    conn.commit()


def simulation_tick(conn):
    with conn.cursor() as cur:

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
                add_inquiry(cur, u[0])
    conn.commit()

def run():
    conn = get_connection()
    n = int(input("Number of users you want to create: "))
    seed(conn, n)
    print("Simulation started...")
    while True:
        try:
            simulation_tick(conn)
            time.sleep(2)
        except Exception as e:
            print("Error:", e)
            conn.rollback()