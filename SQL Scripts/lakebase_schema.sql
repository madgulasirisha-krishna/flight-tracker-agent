-- Flight Tracker & Trip-Watch Agent — Lakebase Schema
-- Lakebase is Postgres-compatible, so this runs via any standard Postgres
-- client (psql, psycopg2, etc.) once you have the instance's connection string.

-- Enables gen_random_uuid() for primary keys
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ============================================================
-- users
-- ============================================================
CREATE TABLE IF NOT EXISTS users (
    user_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email       TEXT UNIQUE NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ============================================================
-- watched_flights
-- ============================================================
CREATE TABLE IF NOT EXISTS watched_flights (
    watch_id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id              UUID NOT NULL REFERENCES users(user_id),
    flight_number        TEXT NOT NULL,
    flight_date          DATE NOT NULL,
    origin_airport       TEXT,
    destination_airport  TEXT,
    status               TEXT NOT NULL DEFAULT 'active',  -- active | completed | cancelled
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_watched_flights_user_id ON watched_flights(user_id);
CREATE INDEX IF NOT EXISTS idx_watched_flights_status ON watched_flights(status);

-- ============================================================
-- trip_plans
-- ============================================================
CREATE TABLE IF NOT EXISTS trip_plans (
    trip_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id     UUID NOT NULL REFERENCES users(user_id),
    trip_name   TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_trip_plans_user_id ON trip_plans(user_id);

-- ============================================================
-- trip_flights (many-to-many: trips <-> watched flights)
-- ============================================================
CREATE TABLE IF NOT EXISTS trip_flights (
    trip_id   UUID NOT NULL REFERENCES trip_plans(trip_id),
    watch_id  UUID NOT NULL REFERENCES watched_flights(watch_id),
    PRIMARY KEY (trip_id, watch_id)
);

-- ============================================================
-- alerts
-- ============================================================
CREATE TABLE IF NOT EXISTS alerts (
    alert_id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    watch_id           UUID NOT NULL REFERENCES watched_flights(watch_id),
    alert_type         TEXT NOT NULL,  -- delay_threshold | gate_change | status_change | cancellation
    threshold_minutes  INTEGER,        -- nullable, used for delay_threshold
    status             TEXT NOT NULL DEFAULT 'active',  -- active | triggered | cancelled
    next_fire_at       TIMESTAMPTZ,
    triggered_at       TIMESTAMPTZ,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_alerts_watch_id ON alerts(watch_id);
CREATE INDEX IF NOT EXISTS idx_alerts_status ON alerts(status);

-- ============================================================
-- agent_action_log (append-only; source for the CDF analytics pipeline)
-- ============================================================
CREATE TABLE IF NOT EXISTS agent_action_log (
    action_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         UUID NOT NULL REFERENCES users(user_id),
    action_type     TEXT NOT NULL,   -- add_flight_watch | remove_flight_watch | create_trip |
                                     -- set_alert | cancel_alert | get_flight_status |
                                     -- get_historical_delay_stats | list_my_watched_flights |
                                     -- get_trip_summary | search_passenger_rights
    action_payload  JSONB,
    result_status   TEXT NOT NULL,   -- success | error
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_agent_action_log_user_id ON agent_action_log(user_id);
CREATE INDEX IF NOT EXISTS idx_agent_action_log_created_at ON agent_action_log(created_at);

-- ============================================================
-- Enable Change Data Feed equivalents (Lakebase/Postgres logical replication)
-- Uncomment and adjust once you're ready to wire up the analytics pipeline —
-- left commented for now since it needs a replication slot/publication set up
-- on the Lakebase side and isn't needed for basic CRUD testing yet.
-- ============================================================
-- CREATE PUBLICATION flight_tracker_cdf FOR TABLE watched_flights, alerts, agent_action_log;
