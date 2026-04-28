import random
from datetime import date, timedelta
from db_postgresql.queries import *

def approve_loan(cur, app, income):
    approval = random.random() < (0.7 if income > 1500 else 0.3)
    if not approval:
        update_loan_application(cur, app['application_id'], 'rejected', None, None)
        return None

    amount = float(app['requested_amount'])
    interest = round(random.uniform(5, 25), 2)

    monthly = round(
        (amount * (interest/100/12)) /
        (1 - (1 + interest/100/12) ** (-app['term_months'])),
        2)
    next_due = date.today() + timedelta(days=30)
    update_loan_application(cur, app['application_id'], 'approved', amount, interest)
    loan = insert_loan(
        cur,
        app['application_id'],
        app['user_id'],
        amount,
        monthly,
        next_due
    )
    return loan


def make_payment(cur, loan):
    roll = random.random()
    if roll < 0.1:
        event = 'missed_payment'
        amount = 0
        return
    amount = float(loan['monthly_payment'])
    new_outstanding = max(0, float(loan['outstanding']) - amount)
    update_loan_payment(
        cur,
        loan['loan_id'],
        new_outstanding,
        new_outstanding > 0
    )
    insert_payment_event(
        cur,
        loan['user_id'],
        loan['loan_id'],
        loan['account_id'],
        'loan_payment',
        amount
    )


def account_activity(cur, account):
    import random

    deposit = random.random() < 0.6
    amount = round(random.uniform(50, 2000), 2)

    delta = amount if deposit else -amount

    update_account_balance(cur, account['account_id'], delta)

    insert_account_event(
        cur,
        account['user_id'],
        account['account_id'],
        'deposit' if deposit else 'withdrawal',
        amount,
        "random activity"
    )


def add_inquiry(cur, user_id):
    import random
    from faker import Faker
    fake = Faker()

    insert_credit_inquiry(
        cur,
        user_id,
        random.choice(['hard', 'soft']),
        fake.company(),
        random.choice(['loan', 'credit_card'])
    )