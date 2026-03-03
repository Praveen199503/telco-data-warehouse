-- ============================================================
-- TELCO DATA WAREHOUSE - Database Initialization Script
-- This script runs automatically when Docker starts PostgreSQL
-- ============================================================

-- ─────────────────────────────────────────────
-- source_db  = simulates the Telco's operational system
-- staging    = raw copy of source data (landing zone)
-- dwh        = the clean, transformed data warehouse
-- ─────────────────────────────────────────────

CREATE SCHEMA IF NOT EXISTS source_db;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dwh;


-- ============================================================
-- SOURCE SCHEMA (Operational / Transactional Tables)
-- These simulate what the Telco's backend system would have
-- ============================================================

-- ── 1. Service Plans ──
-- The products the Telco offers (Basic, Standard, Premium)
CREATE TABLE source_db.service_plans (
    plan_id             SERIAL PRIMARY KEY,
    plan_name           VARCHAR(100) NOT NULL,
    plan_type           VARCHAR(20) NOT NULL CHECK (plan_type IN ('prepaid', 'postpaid')),
    monthly_fee         NUMERIC(10,2) NOT NULL,
    data_limit_gb       NUMERIC(6,2),
    call_minutes_limit  INTEGER,
    sms_limit           INTEGER,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ── 2. Customers ──
-- The people who subscribe to the Telco's services
CREATE TABLE source_db.customers (
    customer_id         VARCHAR(20) PRIMARY KEY,   -- e.g. CUST-00001
    first_name          VARCHAR(100) NOT NULL,
    last_name           VARCHAR(100) NOT NULL,
    email               VARCHAR(200),
    phone_number        VARCHAR(20),
    date_of_birth       DATE,
    gender              VARCHAR(10) CHECK (gender IN ('Male', 'Female', 'Other')),
    address             TEXT,
    city                VARCHAR(100),
    country             VARCHAR(100) DEFAULT 'Sri Lanka',
    registration_date   DATE NOT NULL,
    customer_status     VARCHAR(20) DEFAULT 'active'
                        CHECK (customer_status IN ('active', 'churned', 'suspended')),
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ── 3. Customer Subscriptions ──
-- Which customer is on which plan (and history of plan changes)
CREATE TABLE source_db.customer_subscriptions (
    subscription_id     SERIAL PRIMARY KEY,
    customer_id         VARCHAR(20) NOT NULL REFERENCES source_db.customers(customer_id),
    plan_id             INTEGER NOT NULL REFERENCES source_db.service_plans(plan_id),
    start_date          DATE NOT NULL,
    end_date            DATE,                      -- NULL means still active
    subscription_status VARCHAR(20) DEFAULT 'active'
                        CHECK (subscription_status IN ('active', 'cancelled', 'upgraded', 'downgraded')),
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ── 4. Service Usage ──
-- Daily record of what each customer actually used
CREATE TABLE source_db.service_usage (
    usage_id            SERIAL PRIMARY KEY,
    customer_id         VARCHAR(20) NOT NULL REFERENCES source_db.customers(customer_id),
    subscription_id     INTEGER NOT NULL REFERENCES source_db.customer_subscriptions(subscription_id),
    usage_date          DATE NOT NULL,
    data_used_gb        NUMERIC(8,3) DEFAULT 0,
    call_minutes_used   INTEGER DEFAULT 0,
    sms_used            INTEGER DEFAULT 0,
    overage_charges     NUMERIC(10,2) DEFAULT 0,   -- Extra charges if over limit
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ── 5. Billing Events ──
-- Monthly bills sent to each customer
CREATE TABLE source_db.billing_events (
    billing_id              SERIAL PRIMARY KEY,
    customer_id             VARCHAR(20) NOT NULL REFERENCES source_db.customers(customer_id),
    subscription_id         INTEGER NOT NULL REFERENCES source_db.customer_subscriptions(subscription_id),
    billing_date            DATE NOT NULL,
    billing_period_start    DATE NOT NULL,
    billing_period_end      DATE NOT NULL,
    amount_due              NUMERIC(10,2) NOT NULL,
    amount_paid             NUMERIC(10,2) DEFAULT 0,
    payment_date            DATE,                  -- NULL if not yet paid
    payment_status          VARCHAR(20) DEFAULT 'pending'
                            CHECK (payment_status IN ('paid', 'overdue', 'pending')),
    payment_method          VARCHAR(50),           -- credit_card, bank_transfer, cash
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ── 6. Support Tickets ──
-- Customer complaints and support requests
CREATE TABLE source_db.support_tickets (
    ticket_id           SERIAL PRIMARY KEY,
    customer_id         VARCHAR(20) NOT NULL REFERENCES source_db.customers(customer_id),
    created_date        DATE NOT NULL,
    issue_type          VARCHAR(50)
                        CHECK (issue_type IN ('billing', 'technical', 'plan_change', 'other')),
    priority            VARCHAR(20) CHECK (priority IN ('low', 'medium', 'high')),
    status              VARCHAR(20) DEFAULT 'open'
                        CHECK (status IN ('open', 'resolved', 'escalated')),
    resolved_date       DATE,
    satisfaction_score  INTEGER CHECK (satisfaction_score BETWEEN 1 AND 5),
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- ============================================================
-- STAGING SCHEMA (Raw copies from source — Airflow loads here)
-- These mirror the source tables exactly, plus metadata columns
-- ============================================================

CREATE TABLE staging.stg_customers (
    customer_id         VARCHAR(20),
    first_name          VARCHAR(100),
    last_name           VARCHAR(100),
    email               VARCHAR(200),
    phone_number        VARCHAR(20),
    date_of_birth       DATE,
    gender              VARCHAR(10),
    city                VARCHAR(100),
    country             VARCHAR(100),
    registration_date   DATE,
    customer_status     VARCHAR(20),
    -- Metadata columns added by our pipeline:
    _loaded_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source             VARCHAR(50) DEFAULT 'source_db',
    _is_valid           BOOLEAN DEFAULT TRUE
);

CREATE TABLE staging.stg_service_plans (
    plan_id             INTEGER,
    plan_name           VARCHAR(100),
    plan_type           VARCHAR(20),
    monthly_fee         NUMERIC(10,2),
    data_limit_gb       NUMERIC(6,2),
    call_minutes_limit  INTEGER,
    sms_limit           INTEGER,
    _loaded_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source             VARCHAR(50) DEFAULT 'source_db'
);

CREATE TABLE staging.stg_service_usage (
    usage_id            INTEGER,
    customer_id         VARCHAR(20),
    subscription_id     INTEGER,
    usage_date          DATE,
    data_used_gb        NUMERIC(8,3),
    call_minutes_used   INTEGER,
    sms_used            INTEGER,
    overage_charges     NUMERIC(10,2),
    _loaded_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source             VARCHAR(50) DEFAULT 'source_db'
);

CREATE TABLE staging.stg_billing_events (
    billing_id          INTEGER,
    customer_id         VARCHAR(20),
    subscription_id     INTEGER,
    billing_date        DATE,
    billing_period_start DATE,
    billing_period_end  DATE,
    amount_due          NUMERIC(10,2),
    amount_paid         NUMERIC(10,2),
    payment_date        DATE,
    payment_status      VARCHAR(20),
    payment_method      VARCHAR(50),
    _loaded_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source             VARCHAR(50) DEFAULT 'source_db'
);

CREATE TABLE staging.stg_subscriptions (
    subscription_id     INTEGER,
    customer_id         VARCHAR(20),
    plan_id             INTEGER,
    start_date          DATE,
    end_date            DATE,
    subscription_status VARCHAR(20),
    _loaded_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source             VARCHAR(50) DEFAULT 'source_db'
);


-- ============================================================
-- DWH SCHEMA (Data Warehouse — DBT builds these)
-- We create empty tables here; DBT will populate them
-- ============================================================

-- ── Dimension: Date ──
CREATE TABLE dwh.dim_date (
    date_key            INTEGER PRIMARY KEY,       -- e.g. 20240115
    full_date           DATE NOT NULL,
    day_of_week         VARCHAR(10),
    day_number          INTEGER,
    month_number        INTEGER,
    month_name          VARCHAR(20),
    quarter             VARCHAR(5),
    year                INTEGER,
    is_weekend          BOOLEAN,
    is_holiday          BOOLEAN DEFAULT FALSE
);

-- ── Dimension: Service Plan ──
CREATE TABLE dwh.dim_service_plan (
    plan_key            SERIAL PRIMARY KEY,
    plan_id             INTEGER,
    plan_name           VARCHAR(100),
    plan_type           VARCHAR(20),
    monthly_fee         NUMERIC(10,2),
    data_limit_gb       NUMERIC(6,2),
    call_minutes_limit  INTEGER,
    sms_limit           INTEGER,
    plan_tier           VARCHAR(20)                -- Basic / Standard / Premium
);

-- ── Dimension: Customer (with SCD columns) ──
CREATE TABLE dwh.dim_customer (
    customer_key        SERIAL PRIMARY KEY,        -- Surrogate key (warehouse ID)
    customer_id         VARCHAR(20),               -- Source system ID
    first_name          VARCHAR(100),
    last_name           VARCHAR(100),
    email               VARCHAR(200),
    gender              VARCHAR(10),
    city                VARCHAR(100),
    country             VARCHAR(100),
    age_group           VARCHAR(20),               -- 18-25, 26-35, etc.
    registration_date   DATE,
    customer_status     VARCHAR(20),
    customer_segment    VARCHAR(50),               -- High Value / At Risk / etc.

    -- SCD Type 1: overwrite (e.g. email, city — just update, no history needed)
    -- (handled by UPDATE in DBT)

    -- SCD Type 2: track full history of plan changes
    current_plan_name   VARCHAR(100),
    plan_start_date     DATE,
    plan_end_date       DATE,
    is_current          BOOLEAN DEFAULT TRUE,      -- Is this the latest record?

    -- SCD Type 3: store one previous value
    previous_plan_name  VARCHAR(100),

    -- Metadata
    dw_created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dw_updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ── Fact: Service Usage ──
CREATE TABLE dwh.fact_service_usage (
    usage_key               SERIAL PRIMARY KEY,
    customer_key            INTEGER REFERENCES dwh.dim_customer(customer_key),
    plan_key                INTEGER REFERENCES dwh.dim_service_plan(plan_key),
    date_key                INTEGER REFERENCES dwh.dim_date(date_key),
    -- Measures:
    data_used_gb            NUMERIC(8,3),
    call_minutes_used       INTEGER,
    sms_used                INTEGER,
    overage_charges         NUMERIC(10,2),
    data_utilization_pct    NUMERIC(5,2)           -- data_used / data_limit * 100
);

-- ── Fact: Billing Events ──
CREATE TABLE dwh.fact_billing_events (
    billing_key             SERIAL PRIMARY KEY,
    customer_key            INTEGER REFERENCES dwh.dim_customer(customer_key),
    plan_key                INTEGER REFERENCES dwh.dim_service_plan(plan_key),
    date_key                INTEGER REFERENCES dwh.dim_date(date_key),
    -- Measures:
    amount_due              NUMERIC(10,2),
    amount_paid             NUMERIC(10,2),
    outstanding_balance     NUMERIC(10,2),         -- amount_due - amount_paid
    days_to_payment         INTEGER,               -- payment_date - billing_date
    is_overdue              BOOLEAN DEFAULT FALSE
);


-- ============================================================
-- USEFUL INDEXES (speeds up queries on large data)
-- ============================================================

CREATE INDEX idx_service_usage_customer ON source_db.service_usage(customer_id);
CREATE INDEX idx_service_usage_date ON source_db.service_usage(usage_date);
CREATE INDEX idx_billing_customer ON source_db.billing_events(customer_id);
CREATE INDEX idx_billing_status ON source_db.billing_events(payment_status);
CREATE INDEX idx_subscriptions_customer ON source_db.customer_subscriptions(customer_id);



SELECT 'Database initialized successfully! Schemas: source_db, staging, dwh' AS status;


