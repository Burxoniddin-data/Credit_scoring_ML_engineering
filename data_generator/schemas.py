from dataclasses import dataclass
from datetime import datetime, date
from typing import Optional

@dataclass
class User:
    first_name: str
    last_name: str
    email: str
    date_of_birth: date
    phone: str
    address: str
    city: str
    employment_status: str
    monthly_income: float
    created_at: datetime

@dataclass
class Account:
    user_id: int
    account_type: str
    account_number: str
    balance: float
    is_active: bool
    opened_at: datetime

@dataclass
class LoanApplication:
    user_id: int
    loan_type: str
    requested_amount: float
    approved_amount: Optional[float]
    interest_rate: float
    term_months: int
    purpose: str
    status: str
    applied_at: datetime
    decision_at: Optional[datetime]

@dataclass
class Loan:
    application_id: int
    user_id: int
    principal: float
    outstanding: float
    monthly_payment: float
    interest_rate: float
    next_due_date: date
    payments_made: int
    payments_missed: int
    is_active: bool
    started_at: datetime

@dataclass
class PaymentEvent:
    user_id: int
    loan_id: int
    account_id: int
    event_type: str
    amount: float
    status: str
    description: str
    event_timestamp: datetime

@dataclass
class CreditInquiry:
    user_id: int
    inquiry_type: str
    requested_by: str
    purpose: str
    inquired_at: datetime

@dataclass
class Prediction:
    user_id: int
    credit_score: float
    model_version: str
    predicted_at: datetime