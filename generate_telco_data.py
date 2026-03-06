# ============================================================
# generate_telco_data.py
#
# Synthetic Telco dataset generator for PostgreSQL.
#
# Summary:
#   - Inserts predefined service plans
#   - Generates 10,000 customers
#   - Creates subscription lifecycles (including upgrades/downgrades)
#   - Produces weekly usage summaries
#   - Generates monthly billing records
#   - Simulates customer support activity
#
# Run: python generate_telco_data.py
# Dependencies: psycopg2-binary, faker, numpy
# ============================================================

import random
import logging
from datetime import date, timedelta
from faker import Faker
import numpy as np
import psycopg2
from psycopg2.extras import execute_batch


# ------------------------------------------------------------
# Logging configuration
# Used to monitor progress during execution
# ------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
log = logging.getLogger(__name__)


# ------------------------------------------------------------
# Database connection settings
# Adjust if your environment differs
# ------------------------------------------------------------
DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "telco_db",
    "user":     "telco_user",
    "password": "telco_pass"
}

# Data generation parameters
NUM_CUSTOMERS    = 10000
START_DATE       = date(2024, 1, 1)
END_DATE         = date(2024, 6, 30)
CHURN_RATE       = 0.12
PLAN_CHANGE_RATE = 0.15
LATE_PAYMENT_RATE = 0.15
BAD_DEBT_RATE    = 0.05


# Faker setup for synthetic data
# Seeds ensure reproducible results
fake = Faker()
random.seed(42)
np.random.seed(42)


# ============================================================
# Database Connection
# ============================================================

def get_connection():
    """
    Establish and return a PostgreSQL connection.
    Raises an exception if the connection fails.
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        log.info("Connected to PostgreSQL successfully")
        return conn
    except Exception as e:
        log.error(f"Failed to connect to PostgreSQL: {e}")
        log.error("Ensure the database service is running.")
        raise


# ============================================================
# STEP 1: Service Plans
# Predefined product catalog
# ============================================================

SERVICE_PLANS = [
    ("Basic Prepaid",      "prepaid",  9.99,  5.0,   100, 100),
    ("Standard Prepaid",   "prepaid",  19.99, 15.0,  300, 300),
    ("Premium Prepaid",    "prepaid",  34.99, 50.0,  -1,  -1),
    ("Standard Postpaid",  "postpaid", 29.99, 30.0,  500, 500),
    ("Premium Postpaid",   "postpaid", 49.99, 100.0, -1,  -1),
]

# Probability distribution for initial plan selection (must sum to 1.0)
PLAN_WEIGHTS = [0.30, 0.28, 0.15, 0.20, 0.07]


def insert_service_plans(conn):
    """Insert predefined service plans into source_db.service_plans."""
    log.info("Inserting service plans...")

    sql = """
        INSERT INTO source_db.service_plans
            (plan_name, plan_type, monthly_fee, data_limit_gb,
             call_minutes_limit, sms_limit)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
    """

    with conn.cursor() as cur:
        execute_batch(cur, sql, SERVICE_PLANS)
    conn.commit()

    with conn.cursor() as cur:
        cur.execute("SELECT plan_id, plan_name, monthly_fee FROM source_db.service_plans ORDER BY plan_id")
        plans = cur.fetchall()

    return plans


# ============================================================
# STEP 2: Customers
# Customer profile generation
# ============================================================

SRI_LANKA_CITIES = [
    "Colombo", "Kandy", "Galle", "Negombo", "Jaffna",
    "Matara", "Kurunegala", "Anuradhapura", "Ratnapura", "Badulla"
]

# Weighted distribution across cities
CITY_WEIGHTS = [0.30, 0.12, 0.10, 0.08, 0.07, 0.07, 0.08, 0.07, 0.06, 0.05]


def generate_customers():
    """
    Generate synthetic customer records.
    Returns list of tuples ready for bulk insert.
    """
    log.info(f"Generating {NUM_CUSTOMERS} customers...")
    customers = []

    for i in range(1, NUM_CUSTOMERS + 1):
        customer_id = f"CUST-{i:05d}"

        dob = fake.date_of_birth(minimum_age=18, maximum_age=70)

        reg_date = fake.date_between(
            start_date=date(2023, 7, 1),
            end_date=START_DATE
        )

        customers.append((
            customer_id,
            fake.first_name(),
            fake.last_name(),
            fake.email() if random.random() > 0.02 else None,
            fake.numerify("07########"),
            dob,
            random.choice(["Male", "Female"]),
            fake.address().replace('\n', ', ')[:200],
            random.choices(SRI_LANKA_CITIES, weights=CITY_WEIGHTS)[0],
            "Sri Lanka",
            reg_date,
            "active"
        ))

    return customers


def insert_customers(conn, customers):
    """Bulk insert customers into source_db.customers."""
    log.info("Inserting customers...")

    sql = """
        INSERT INTO source_db.customers
            (customer_id, first_name, last_name, email, phone_number,
             date_of_birth, gender, address, city, country,
             registration_date, customer_status)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    with conn.cursor() as cur:
        execute_batch(cur, sql, customers, page_size=500)
    conn.commit()


# ============================================================
# STEP 3: Subscriptions
# Plan assignments and lifecycle simulation
# ============================================================

def generate_subscriptions(conn, customers, plans):
    """
    Assign initial plans and simulate upgrades, downgrades,
    and churn behaviour.
    """
    log.info("Generating subscriptions...")

    plan_ids = [p[0] for p in plans]
    subscriptions = []
    sub_id = 1
    customer_current_sub = {}

    plan_changers = set(
        random.sample([c[0] for c in customers],
                      int(NUM_CUSTOMERS * PLAN_CHANGE_RATE))
    )

    churners = set(
        random.sample([c[0] for c in customers],
                      int(NUM_CUSTOMERS * CHURN_RATE))
    )

    for customer in customers:
        customer_id = customer[0]
        reg_date = customer[10]
        initial_plan_id = random.choices(plan_ids, weights=PLAN_WEIGHTS)[0]

        end_date = None
        status = "active"

        if customer_id in churners:
            end_date = fake.date_between(
                start_date=START_DATE + timedelta(days=30),
                end_date=END_DATE
            )
            status = "cancelled"

        subscriptions.append((
            sub_id, customer_id, initial_plan_id,
            max(reg_date, START_DATE), end_date, status
        ))

        customer_current_sub[customer_id] = {
            "sub_id": sub_id,
            "plan_id": initial_plan_id,
            "start": max(reg_date, START_DATE),
            "end": end_date or END_DATE
        }

        sub_id += 1

    sql = """
        INSERT INTO source_db.customer_subscriptions
            (subscription_id, customer_id, plan_id, start_date,
             end_date, subscription_status)
        VALUES (%s,%s,%s,%s,%s,%s)
    """

    with conn.cursor() as cur:
        execute_batch(cur, sql, subscriptions, page_size=500)
    conn.commit()

    return customer_current_sub, churners


# ============================================================
# STEP 4: Service Usage
# Weekly aggregated usage simulation
# ============================================================

PLAN_LIMITS = {
    1: {"data": 5.0,   "calls": 100, "sms": 100},
    2: {"data": 15.0,  "calls": 300, "sms": 300},
    3: {"data": 50.0,  "calls": 999, "sms": 999},
    4: {"data": 30.0,  "calls": 500, "sms": 500},
    5: {"data": 100.0, "calls": 999, "sms": 999},
}


def generate_usage(conn, customer_current_sub):
    """Generate weekly usage summaries."""
    log.info("Generating usage records...")

    usage_records = []
    usage_id = 1

    current_date = START_DATE
    weeks = []
    while current_date <= END_DATE:
        weeks.append(current_date)
        current_date += timedelta(days=7)

    for customer_id, sub_info in customer_current_sub.items():
        sub_id  = sub_info["sub_id"]
        plan_id = sub_info["plan_id"]
        sub_end = sub_info["end"]

        limits = PLAN_LIMITS.get(plan_id, PLAN_LIMITS[1])

        for week_start in weeks:
            if week_start > sub_end:
                break

            data_used = round((limits["data"] / 4) * random.uniform(0.2, 1.1), 3)
            calls_used = int((limits["calls"] / 4) * random.uniform(0.2, 1.1))
            sms_used = int((limits["sms"] / 4) * random.uniform(0.2, 1.1))

            usage_records.append((
                usage_id, customer_id, sub_id,
                week_start, data_used, calls_used, sms_used, 0.0
            ))
            usage_id += 1

    sql = """
        INSERT INTO source_db.service_usage
            (usage_id, customer_id, subscription_id, usage_date,
             data_used_gb, call_minutes_used, sms_used, overage_charges)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """

    with conn.cursor() as cur:
        execute_batch(cur, sql, usage_records, page_size=500)
    conn.commit()

    return usage_records

def generate_billing(conn, customer_current_sub, plans):
    """
    Generate monthly billing records.
    Some customers pay late, some never pay.
    """
    log.info("Generating billing events...")

    # Build a quick lookup: plan_id → monthly_fee
    plan_fees = {p[0]: float(p[2]) for p in plans}

    billing_records = []
    billing_id = 1

    months = [
        (date(2024, 1, 1),  date(2024, 1, 31)),
        (date(2024, 2, 1),  date(2024, 2, 29)),
        (date(2024, 3, 1),  date(2024, 3, 31)),
        (date(2024, 4, 1),  date(2024, 4, 30)),
        (date(2024, 5, 1),  date(2024, 5, 31)),
        (date(2024, 6, 1),  date(2024, 6, 30)),
    ]

    for customer_id, sub_info in customer_current_sub.items():
        sub_id   = sub_info["sub_id"]
        plan_id  = sub_info["plan_id"]
        sub_end  = sub_info["end"]
        base_fee = plan_fees.get(plan_id, 19.99)

        for period_start, period_end in months:
            if period_start > sub_end:
                break

            billing_date = period_end + timedelta(days=1)
            amount_due   = round(base_fee + random.uniform(-2, 5), 2)

            rand = random.random()
            if rand < BAD_DEBT_RATE:
                amount_paid    = 0.0
                payment_date   = None
                payment_status = "overdue"
                payment_method = None

            elif rand < BAD_DEBT_RATE + LATE_PAYMENT_RATE:
                days_late      = random.randint(15, 45)
                payment_date   = billing_date + timedelta(days=days_late)
                amount_paid    = amount_due
                payment_status = "paid"
                payment_method = random.choice(["credit_card", "bank_transfer", "cash"])

            else:
                days_to_pay    = random.randint(1, 14)
                payment_date   = billing_date + timedelta(days=days_to_pay)
                amount_paid    = amount_due
                payment_status = "paid"
                payment_method = random.choice(["credit_card", "bank_transfer", "cash"])

            billing_records.append((
                billing_id, customer_id, sub_id,
                billing_date, period_start, period_end,
                amount_due, amount_paid, payment_date,
                payment_status, payment_method
            ))
            billing_id += 1

    sql = """
        INSERT INTO source_db.billing_events
            (billing_id, customer_id, subscription_id, billing_date,
             billing_period_start, billing_period_end,
             amount_due, amount_paid, payment_date,
             payment_status, payment_method)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    chunk_size = 5000
    for i in range(0, len(billing_records), chunk_size):
        chunk = billing_records[i:i+chunk_size]
        with conn.cursor() as cur:
            execute_batch(cur, sql, chunk, page_size=500)
        conn.commit()
        log.info(f"  Billing: inserted {min(i+chunk_size, len(billing_records))}/{len(billing_records)}")

    log.info(f"  ✓ Total billing records: {len(billing_records)}")


def generate_support_tickets(conn, customer_current_sub, churners):
    """
    Generate support tickets.
    Churners raise more tickets.
    """
    log.info("Generating support tickets...")

    tickets    = []
    ticket_id  = 1

    issue_types   = ["billing", "technical", "plan_change", "other"]
    issue_weights = [0.35, 0.30, 0.20, 0.15]

    for customer_id, sub_info in customer_current_sub.items():
        sub_start = sub_info["start"]
        sub_end   = sub_info["end"]

        if customer_id in churners:
            num_tickets = random.choices([0,1,2,3], weights=[0.1,0.3,0.4,0.2])[0]
        else:
            num_tickets = random.choices([0,1,2], weights=[0.70,0.25,0.05])[0]

        for _ in range(num_tickets):
            created = fake.date_between(start_date=sub_start, end_date=sub_end)

            if random.random() > 0.15:
                status   = "resolved"
                resolved = created + timedelta(days=random.randint(1, 7))
                score    = random.choices([1,2,3,4,5], weights=[0.05,0.10,0.25,0.40,0.20])[0]
            else:
                status   = random.choice(["open", "escalated"])
                resolved = None
                score    = None

            tickets.append((
                ticket_id, customer_id, created,
                random.choices(issue_types, weights=issue_weights)[0],
                random.choice(["low", "medium", "high"]),
                status, resolved, score
            ))
            ticket_id += 1

    sql = """
        INSERT INTO source_db.support_tickets
            (ticket_id, customer_id, created_date, issue_type,
             priority, status, resolved_date, satisfaction_score)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """

    with conn.cursor() as cur:
        execute_batch(cur, sql, tickets, page_size=500)
    conn.commit()

    log.info(f"  ✓ Total support tickets: {len(tickets)}")


def update_churned_customers(conn, churners):
    """Update customer_status to churned"""
    log.info(f"Updating {len(churners)} churned customers...")

    sql = """
        UPDATE source_db.customers
        SET customer_status = 'churned', updated_at = CURRENT_TIMESTAMP
        WHERE customer_id = %s
    """

    with conn.cursor() as cur:
        execute_batch(cur, sql, [(c,) for c in churners], page_size=500)
    conn.commit()
    log.info(f"  ✓ Marked {len(churners)} customers as churned")


# ============================================================
# Main Execution Flow
# ============================================================

def main():
    log.info("TELCO DATA GENERATION STARTED")

    conn = get_connection()

    try:
        plans = insert_service_plans(conn)
        customers = generate_customers()
        insert_customers(conn, customers)
        customer_current_sub, churners = generate_subscriptions(
            conn, customers, plans
        )
        generate_usage(conn, customer_current_sub)
        generate_billing(conn, customer_current_sub, plans)
        generate_support_tickets(conn, customer_current_sub, churners)
        update_churned_customers(conn, churners)

        log.info("=" * 60)
        log.info("TELCO DATA GENERATION COMPLETED!")
        log.info(f"  Customers:    {NUM_CUSTOMERS:,}")
        log.info(f"  Churned:      {int(NUM_CUSTOMERS * CHURN_RATE):,}")
        log.info("=" * 60)

    except Exception as e:
        log.error(f"Error during data generation: {e}")
        conn.rollback()
        raise

    finally:
        conn.close()
        log.info("Database connection closed.")


if __name__ == "__main__":
    main()

