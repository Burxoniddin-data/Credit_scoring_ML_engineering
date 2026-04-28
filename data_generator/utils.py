import random
from faker import Faker

fake = Faker()
def random_employment_status():
    return random.choices(
        ['employed', 'self_employed', 'unemployed', 'retired'],
        weights=[60, 20, 10, 10]
    )[0]

def random_monthly_income(employment_status):
    ranges = {
        'employed':      (2000, 15000),
        'self_employed': (1000, 20000),
        'unemployed':    (0, 800),
        'retired':       (800, 4000),
    }
    low, high = ranges[employment_status]
    return round(random.uniform(low, high), 2)

def random_loan_type():
    return random.choices(
        ['personal', 'mortgage', 'auto', 'student', 'credit_card'],
        weights=[35, 15, 20, 15, 15]
    )[0]

def random_loan_amount(loan_type, monthly_income):
    multipliers = {
        'personal':    (1, 5),
        'mortgage':    (10, 40),
        'auto':        (3, 10),
        'student':     (2, 8),
        'credit_card': (0.5, 3),
    }
    low_mult, high_mult = multipliers[loan_type]
    amount = monthly_income * random.uniform(low_mult, high_mult)
    return round(min(amount, 500_000), 2)

def random_term_months():
    return random.choice([12, 24, 36, 48, 60, 84, 120])

def calculate_monthly_payment(principal, annual_rate, term_months):
    if annual_rate == 0 or term_months == 0:
        return round(principal / term_months, 2) if term_months else principal
    monthly_rate = annual_rate / 100 / 12
    payment = principal * (monthly_rate * (1 + monthly_rate) ** term_months) / \
              ((1 + monthly_rate) ** term_months - 1)
    return round(payment, 2)