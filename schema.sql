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