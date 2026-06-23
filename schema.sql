-- Car Rental System — Postgres schema
-- Run from repo root: psql -d carrental -f LLD/schema.sql

CREATE TABLE IF NOT EXISTS vehicles (
    id          TEXT PRIMARY KEY,
    owner_id    TEXT        NOT NULL,
    plate_no    TEXT        NOT NULL,
    model_year  INT         NOT NULL,
    status      TEXT        NOT NULL DEFAULT 'available'
);

CREATE TABLE IF NOT EXISTS bookings (
    id          SERIAL      PRIMARY KEY,
    booking_id  TEXT        NOT NULL,
    vehicle_id  TEXT        NOT NULL REFERENCES vehicles(id),
    date        INT         NOT NULL,
    status      TEXT        NOT NULL CHECK (status IN ('pending', 'booked')),
    UNIQUE (vehicle_id, date)          -- DB-level guard against double booking
);

CREATE INDEX IF NOT EXISTS idx_bookings_booking_id ON bookings(booking_id);
CREATE INDEX IF NOT EXISTS idx_bookings_vehicle_date ON bookings(vehicle_id, date);

CREATE TABLE IF NOT EXISTS tickets (
    booking_id  TEXT        PRIMARY KEY,
    user_id     TEXT        NOT NULL,
    vehicle_ids JSONB       NOT NULL,
    from_date   INT         NOT NULL,
    to_date     INT         NOT NULL,
    status      TEXT        NOT NULL DEFAULT 'confirmed' CHECK (status IN ('confirmed', 'failed')),
    reason      TEXT        NULL
);

CREATE TABLE IF NOT EXISTS outbox (
    id               BIGSERIAL   PRIMARY KEY,
    topic            TEXT        NOT NULL,
    payload          JSONB       NOT NULL,
    published        BOOLEAN     NOT NULL DEFAULT false,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Optional idempotency key. IngestionService sets this to booking_id so that
    -- a retried POST /confirm with the same booking_id hits ON CONFLICT DO NOTHING
    -- and never inserts a second outbox row. NULL rows (from other writers) are
    -- each treated as distinct by Postgres UNIQUE — NULLs never conflict.
    idempotency_key  TEXT        UNIQUE
);

-- Partial index so the relay only scans unpublished rows
CREATE INDEX IF NOT EXISTS idx_outbox_unpublished ON outbox(id) WHERE published = false;

CREATE TABLE IF NOT EXISTS payment_intents (
    booking_id          TEXT        PRIMARY KEY,
    user_id             TEXT        NOT NULL,
    amount              NUMERIC     NOT NULL,
    vehicle_ids         JSONB       NOT NULL,
    from_date           INT         NOT NULL,
    to_date             INT         NOT NULL,
    status              TEXT        NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'awaiting_payment', 'paid', 'failed')),
    stripe_session_id   TEXT        NULL,
    redirect_url        TEXT        NULL,
    failure_reason      TEXT        NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);