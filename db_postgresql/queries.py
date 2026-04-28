def insert_user(cur, data):
    cur.execute("""
        INSERT INTO users (first_name, last_name, email, date_of_birth,
            phone, address, city, employment_status, monthly_income)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING user_id, monthly_income
    """, data)
    return cur.fetchone()

def insert_account(cur, user_id, account_number, balance):
    cur.execute("""
        INSERT INTO accounts (user_id, account_type, account_number, balance)
        VALUES (%s, 'checking', %s, %s)
        RETURNING account_id
    """, (user_id, account_number, balance))
    return cur.fetchone()

def insert_loan_application(cur, user_id, loan_type, amount, term, purpose):
    cur.execute("""
        INSERT INTO loan_applications
        (user_id, loan_type, requested_amount, term_months, purpose)
        VALUES (%s,%s,%s,%s,%s)
        RETURNING application_id, requested_amount, term_months
    """, (user_id, loan_type, amount, term, purpose))
    return cur.fetchone()

def update_loan_application(cur, app_id, status, approved_amount, interest):
    cur.execute("""
        UPDATE loan_applications
        SET status=%s, approved_amount=%s, interest_rate=%s,
            decision_at=NOW(), updated_at=NOW()
        WHERE application_id=%s
    """, (status, approved_amount, interest, app_id))

def insert_loan(cur, app_id, user_id, amount, monthly, next_due):
    cur.execute("""
        INSERT INTO loans
        (application_id, user_id, principal, outstanding, monthly_payment, next_due_date)
        VALUES (%s,%s,%s,%s,%s,%s)
        RETURNING loan_id
    """, (app_id, user_id, amount, amount, monthly, next_due))
    return cur.fetchone()

def update_loan_payment(cur, loan_id, outstanding, is_active):
    cur.execute("""
        UPDATE loans
        SET outstanding=%s,
            payments_made = payments_made + 1,
            next_due_date = next_due_date + INTERVAL '1 month',
            is_active=%s,
            updated_at=NOW()
        WHERE loan_id=%s
    """, (outstanding, is_active, loan_id))

def insert_payment_event(cur, user_id, loan_id, account_id, event_type, amount):
    cur.execute("""
        INSERT INTO payment_events
        (user_id, loan_id, account_id, event_type, amount, description)
        VALUES (%s,%s,%s,%s,%s,%s)
    """, (user_id, loan_id, account_id, event_type, amount,
          f"{event_type} for loan {loan_id}"))

def insert_account_event(cur, user_id, account_id, event_type, amount, desc):
    cur.execute("""
        INSERT INTO payment_events
        (user_id, account_id, event_type, amount, description)
        VALUES (%s,%s,%s,%s,%s)
    """, (user_id, account_id, event_type, amount, desc))

def update_account_balance(cur, account_id, delta):
    cur.execute("""
        UPDATE accounts
        SET balance = balance + %s,
            updated_at = NOW()
        WHERE account_id=%s
    """, (delta, account_id))

def insert_credit_inquiry(cur, user_id, inquiry_type, company, purpose):
    cur.execute("""
        INSERT INTO credit_inquiries
        (user_id, inquiry_type, requested_by, purpose)
        VALUES (%s,%s,%s,%s)
    """, (user_id, inquiry_type, company, purpose))

def get_active_loans(cur):
    cur.execute("""
        SELECT l.*, a.account_id
        FROM loans l
        JOIN accounts a ON a.user_id = l.user_id
        WHERE l.is_active = TRUE
        ORDER BY RANDOM()
        LIMIT 10
    """)
    return cur.fetchall()