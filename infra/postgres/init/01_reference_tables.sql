-- Reference (dimension) tables for users and merchants.
-- Runs once on first container start via /docker-entrypoint-initdb.d.
-- Rows are populated by ingestion/transaction_generator/src/seed_reference.py.
--
-- These tables are the CDC source: Debezium captures inserts/updates from
-- the Postgres WAL and publishes them to Kafka. wal_level=logical is already
-- set in docker-compose.

CREATE TABLE IF NOT EXISTS public.users (
    user_id      TEXT PRIMARY KEY,
    email_hash   TEXT        NOT NULL,
    phone_hash   TEXT        NOT NULL,
    city         TEXT        NOT NULL,
    country      TEXT        NOT NULL,
    kyc_level    TEXT        NOT NULL,   -- VERIFIED | PENDING | REJECTED
    risk_score   NUMERIC(4,2) NOT NULL,  -- 0.00 - 1.00
    is_blocked   BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at   TIMESTAMPTZ NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.merchants (
    merchant_id     TEXT PRIMARY KEY,
    merchant_name   TEXT         NOT NULL,
    category        TEXT         NOT NULL,  -- see profiles.MERCHANT_CATEGORIES
    risk_tier       TEXT         NOT NULL,  -- LOW | MEDIUM | HIGH
    avg_ticket_size NUMERIC(12,2) NOT NULL,
    is_active       BOOLEAN      NOT NULL DEFAULT TRUE,
    onboarded_at    TIMESTAMPTZ  NOT NULL,
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- REPLICA IDENTITY FULL so Debezium update events carry the full before-image.
-- Without this, pgoutput only emits primary-key columns for the old row.
ALTER TABLE public.users     REPLICA IDENTITY FULL;
ALTER TABLE public.merchants REPLICA IDENTITY FULL;

-- Explicit publication for the Debezium connector (referenced by name in
-- infra/debezium/postgres-source.json). Created here so the connector does
-- not need CREATE privileges at runtime.
CREATE PUBLICATION fraud_cdc_pub FOR TABLE public.users, public.merchants;
