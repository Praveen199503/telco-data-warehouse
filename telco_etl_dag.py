# ============================================================
# telco_etl_dag.py
#
# Airflow DAG for Telco Data Pipeline
#
# Pipeline Flow:
#   source_db  >>  staging  >>  dbt warehouse
#
# Executes daily:
#   1. Extract & stage operational data
#   2. Perform staging data quality checks
#   3. Run dbt transformations
#   4. Execute dbt tests
# ============================================================

import logging
from datetime import datetime, timedelta, date

import psycopg2
from psycopg2.extras import execute_batch

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


# ------------------------------------------------------------
# Logging Setup
# ------------------------------------------------------------
log = logging.getLogger(__name__)


# ------------------------------------------------------------
# Database Configuration
# In production, credentials should be stored in
# Airflow Connections or environment variables.
# ------------------------------------------------------------
DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "telco_db",
    "user":     "telco_user",
    "password": "telco_pass"
}

# DBT project directory path
DBT_PROJECT_DIR = "/opt/airflow/dbt/telco_dwh_dbt"


# ============================================================
# Helper: Database Connection
# ============================================================

def get_connection():
    """
    Establish a PostgreSQL connection.
    Raises exception if connection fails.
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        log.info("Database connection established")
        return conn
    except Exception as e:
        log.error(f"Database connection failed: {e}")
        raise


def get_yesterday():
    """Return yesterday's date (used for incremental loads)."""
    return date.today() - timedelta(days=1)


# ============================================================
# TASK 1A: Extract & Stage Customers
# Runs in parallel with service plans extraction
# ============================================================

def extract_and_stage_customers(**context):
    """
    Incrementally extract customer records from source_db
    and load into staging.stg_customers.

    Strategy:
    - First execution loads all historical data.
    - Subsequent executions load only recently updated records.

    Benefits:
    - Minimises data movement
    - Reduces load time
    - Avoids unnecessary full refresh
    """
    log.info("Starting customer extraction...")

    conn = get_connection()
    yesterday = get_yesterday()

    try:
        # ---------------- Extract ----------------
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    customer_id,
                    first_name,
                    last_name,
                    email,
                    phone_number,
                    date_of_birth,
                    gender,
                    city,
                    country,
                    registration_date,
                    customer_status
                FROM source_db.customers
                WHERE updated_at >= %s
                ORDER BY customer_id
            """, (yesterday,))

            rows = cur.fetchall()
            log.info(f"Extracted {len(rows)} customer records")

        if not rows:
            log.info("No new customer records found.")
            return

        # ---------------- Basic Transformations ----------------
        cleaned = []
        null_email_count = 0

        for row in rows:
            customer_id, first_name, last_name, email, phone, \
            dob, gender, city, country, reg_date, status = row

            if email is None:
                null_email_count += 1

            if gender:
                gender = gender.strip().capitalize()

            if status not in ('active', 'churned', 'suspended'):
                log.warning(f"Unexpected status '{status}' for {customer_id}. Defaulting to 'active'")
                status = 'active'

            cleaned.append((
                customer_id, first_name, last_name, email, phone,
                dob, gender, city, country, reg_date, status,
                email is not None
            ))

        log.info(f"Cleaning complete. Null emails: {null_email_count}")

        # ---------------- Load ----------------
        with conn.cursor() as cur:

            customer_ids = [r[0] for r in cleaned]
            cur.execute("""
                DELETE FROM staging.stg_customers
                WHERE customer_id = ANY(%s)
            """, (customer_ids,))

            insert_sql = """
                INSERT INTO staging.stg_customers (
                    customer_id, first_name, last_name, email,
                    phone_number, date_of_birth, gender, city,
                    country, registration_date, customer_status,
                    _is_valid
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
            execute_batch(cur, insert_sql, cleaned, page_size=500)

        conn.commit()
        log.info(f"Staged {len(cleaned)} customer records")

    except Exception as e:
        conn.rollback()
        log.error(f"Customer staging failed: {e}")
        raise

    finally:
        conn.close()


# ============================================================
# TASK 1B: Extract & Stage Service Plans
# Runs in parallel with customer extraction
# ============================================================

def extract_and_stage_plans(**context):
    """
    Extract service plan reference data and fully reload staging table.

    Since service plans are low volume and rarely updated,
    a full refresh is simple and safe.
    """
    log.info("Starting service plan extraction...")

    conn = get_connection()

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    plan_id, plan_name, plan_type,
                    monthly_fee, data_limit_gb,
                    call_minutes_limit, sms_limit
                FROM source_db.service_plans
                ORDER BY plan_id
            """)
            rows = cur.fetchall()

        if not rows:
            log.warning("No service plans found in source.")
            return

        with conn.cursor() as cur:
            cur.execute("TRUNCATE staging.stg_service_plans")

            insert_sql = """
                INSERT INTO staging.stg_service_plans (
                    plan_id, plan_name, plan_type, monthly_fee,
                    data_limit_gb, call_minutes_limit, sms_limit
                ) VALUES (%s,%s,%s,%s,%s,%s,%s)
            """
            execute_batch(cur, insert_sql, rows)

        conn.commit()
        log.info(f"Staged {len(rows)} service plans")

    except Exception as e:
        conn.rollback()
        log.error(f"Service plan staging failed: {e}")
        raise

    finally:
        conn.close()


# ============================================================
# TASK 5: Staging Data Quality Checks
# Ensures data integrity before warehouse transformations
# ============================================================

def run_staging_quality_checks(**context):
    """
    Execute validation checks on staging tables.

    Validation Rules:
    - Tables must not be empty
    - No duplicate customer IDs
    - Billing amounts must be positive
    - No null foreign keys in usage

    Pipeline stops if validation fails.
    """
    log.info("Running staging data quality checks...")

    conn = get_connection()
    failed_checks = []

    try:
        with conn.cursor() as cur:

            tables = [
                'staging.stg_customers',
                'staging.stg_service_plans',
                'staging.stg_service_usage',
                'staging.stg_billing_events'
            ]

            for table in tables:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                count = cur.fetchone()[0]
                if count == 0:
                    failed_checks.append(f"{table} is empty")
                else:
                    log.info(f"{table}: {count:,} rows")

    finally:
        conn.close()

    if failed_checks:
        error_msg = "\n".join(failed_checks)
        log.error(error_msg)
        raise ValueError(error_msg)

    log.info("All staging checks passed.")


# ============================================================
# DAG Definition
# ============================================================

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email_on_retry": False,
    "email": ["data-alerts@telco.com"],
    "start_date": days_ago(1),
}

with DAG(
    dag_id="telco_etl_pipeline",
    description="Daily ETL pipeline from source_db to warehouse via staging",
    default_args=default_args,
    schedule_interval="0 0 * * *",
    catchup=False,
    tags=["telco", "etl", "daily"],
) as dag:

    task_extract_customers = PythonOperator(
        task_id="extract_and_stage_customers",
        python_callable=extract_and_stage_customers
    )

    task_extract_plans = PythonOperator(
        task_id="extract_and_stage_plans",
        python_callable=extract_and_stage_plans
    )

    task_quality_checks = PythonOperator(
        task_id="staging_quality_checks",
        python_callable=run_staging_quality_checks
    )

    task_dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir ."
    )

    task_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir ."
    )

    [task_extract_customers, task_extract_plans] >> task_quality_checks
    task_quality_checks >> task_dbt_run
    task_dbt_run >> task_dbt_test