import pandas as pd
from db_postgresql.connection import get_connection

def build_features(output_path="/tmp/features.csv"):
    conn = get_connection()
    query = """
    SELECT 
        u.user_id,
        u.monthly_income,
        l.loan_id,
        l.outstanding,
        l.payments_made,
        l.payments_missed,
        l.is_active
    FROM users u
    LEFT JOIN loans l ON u.user_id = l.user_id
    """

    df = pd.read_sql(query, conn)

    df['outstanding'] = df['outstanding'].fillna(0)
    df['payments_made'] = df['payments_made'].fillna(0)
    df['payments_missed'] = df['payments_missed'].fillna(0)
    df['debt_to_income'] = df['outstanding'] / (df['monthly_income'] + 1)
    df['missed_ratio'] = df['payments_missed'] / (df['payments_made'] + 1)
    df['default_risk'] = (df['payments_missed'] > 2).astype(int)

    df.to_csv(output_path, index=False)

    return output_path