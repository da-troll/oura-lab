-- Migration 003: Multi-user support
-- Drops all existing single-user tables and recreates with user_id columns,
-- RLS policies, sessions, and OAuth state management.

BEGIN;

-- Preflight: prevent re-running
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables
             WHERE table_schema = 'public' AND table_name = 'users') THEN
    RAISE EXCEPTION 'Migration 003 already applied';
  END IF;
END $$;

-- Required for gen_random_uuid()
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ============================================
-- Drop old tables (FK order)
-- ============================================
DROP TABLE IF EXISTS oura_features_daily CASCADE;
DROP TABLE IF EXISTS oura_day_tags CASCADE;
DROP TABLE IF EXISTS oura_personal_info CASCADE;
DROP TABLE IF EXISTS oura_raw CASCADE;
DROP TABLE IF EXISTS oura_daily CASCADE;
DROP TABLE IF EXISTS oura_auth CASCADE;

-- ============================================
-- Users table
-- ============================================
CREATE TABLE users (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    email TEXT NOT NULL,
    email_normalized TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT email_normalized_check CHECK (email_normalized = lower(btrim(email)))
);

-- ============================================
-- Sessions table
-- ============================================
CREATE TABLE sessions (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token_hash TEXT UNIQUE NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ip INET,
    user_agent TEXT
);

CREATE INDEX idx_sessions_token_hash ON sessions(token_hash);
CREATE INDEX idx_sessions_user_id ON sessions(user_id);
CREATE INDEX idx_sessions_expires_at ON sessions(expires_at);

-- ============================================
-- OAuth states (single-use, for CSRF)
-- ============================================
CREATE TABLE oauth_states (
    state TEXT PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_oauth_states_expires ON oauth_states(expires_at);

-- ============================================
-- Oura auth (per-user)
-- ============================================
CREATE TABLE oura_auth (
    user_id UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    access_token TEXT NOT NULL,
    refresh_token TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    token_type TEXT NOT NULL DEFAULT 'Bearer',
    scope TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================
-- Raw Oura API payloads (per-user)
-- ============================================
CREATE TABLE oura_raw (
    id BIGSERIAL PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    source TEXT NOT NULL,
    day DATE,
    payload JSONB NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_oura_raw_user_source_day ON oura_raw(user_id, source, day);

-- ============================================
-- Daily metrics (per-user)
-- ============================================
CREATE TABLE oura_daily (
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    date DATE NOT NULL,

    -- Calendar context
    weekday SMALLINT NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    season TEXT,
    is_holiday BOOLEAN NOT NULL DEFAULT FALSE,
    holiday_name TEXT,

    -- Sleep metrics
    sleep_total_seconds INT,
    sleep_efficiency NUMERIC,
    sleep_rem_seconds INT,
    sleep_deep_seconds INT,
    sleep_light_seconds INT,
    sleep_latency_seconds INT,
    sleep_restlessness NUMERIC,
    sleep_bedtime_start TIMESTAMPTZ,
    sleep_bedtime_end TIMESTAMPTZ,
    sleep_regularity NUMERIC,
    sleep_score INT,

    -- Readiness metrics
    readiness_score INT,
    readiness_temperature_deviation NUMERIC,
    readiness_resting_heart_rate INT,
    readiness_hrv_balance INT,
    readiness_recovery_index INT,
    readiness_activity_balance INT,

    -- Activity metrics
    activity_score INT,
    steps INT,
    cal_total INT,
    cal_active INT,
    met_minutes INT,
    training_load NUMERIC,
    low_activity_minutes INT,
    medium_activity_minutes INT,
    high_activity_minutes INT,
    sedentary_minutes INT,

    -- Heart rate
    hr_lowest INT,
    hr_average NUMERIC,
    hrv_average NUMERIC,

    -- Stress metrics
    stress_high_minutes INT,
    recovery_high_minutes INT,
    stress_day_summary TEXT,

    -- SpO2 metrics
    spo2_average NUMERIC,
    breathing_disturbance_index NUMERIC,

    -- Cardiovascular age
    vascular_age INT,

    -- Sleep extras
    sleep_breath_average NUMERIC,

    -- Activity contributors
    activity_meet_daily_targets INT,
    activity_move_every_hour INT,
    activity_recovery_time INT,
    activity_training_frequency INT,
    activity_training_volume INT,
    non_wear_seconds INT,
    inactivity_alerts INT,

    -- Readiness extras
    readiness_sleep_balance INT,

    -- Workout aggregation
    workout_count INT,
    workout_total_minutes NUMERIC,
    workout_total_calories NUMERIC,

    -- Session aggregation
    session_count INT,
    session_total_minutes NUMERIC,

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (user_id, date)
);

CREATE INDEX idx_oura_daily_date ON oura_daily(date);

-- ============================================
-- Day tags (per-user)
-- ============================================
CREATE TABLE oura_day_tags (
    user_id UUID NOT NULL,
    date DATE NOT NULL,
    tag TEXT NOT NULL,
    PRIMARY KEY (user_id, date, tag),
    FOREIGN KEY (user_id, date) REFERENCES oura_daily(user_id, date) ON DELETE CASCADE
);

-- ============================================
-- Features daily (per-user)
-- ============================================
CREATE TABLE oura_features_daily (
    user_id UUID NOT NULL,
    date DATE NOT NULL,

    -- Rolling means for readiness
    rm_3_readiness_score NUMERIC,
    rm_7_readiness_score NUMERIC,
    rm_14_readiness_score NUMERIC,
    rm_28_readiness_score NUMERIC,

    -- Rolling means for sleep
    rm_3_sleep_total_seconds NUMERIC,
    rm_7_sleep_total_seconds NUMERIC,
    rm_14_sleep_total_seconds NUMERIC,
    rm_28_sleep_total_seconds NUMERIC,

    -- Rolling means for activity
    rm_7_steps NUMERIC,
    rm_14_steps NUMERIC,
    rm_28_steps NUMERIC,

    -- Deltas vs baseline
    delta_readiness_vs_rm7 NUMERIC,
    delta_sleep_vs_rm7 NUMERIC,
    delta_steps_vs_rm7 NUMERIC,

    -- Lag features for sleep
    lag_1_sleep_total_seconds INT,
    lag_2_sleep_total_seconds INT,
    lag_3_sleep_total_seconds INT,
    lag_4_sleep_total_seconds INT,
    lag_5_sleep_total_seconds INT,
    lag_6_sleep_total_seconds INT,
    lag_7_sleep_total_seconds INT,

    -- Lag features for readiness
    lag_1_readiness_score INT,
    lag_2_readiness_score INT,
    lag_3_readiness_score INT,

    -- Rolling standard deviation
    sd_7_sleep_total_seconds NUMERIC,
    sd_14_sleep_total_seconds NUMERIC,
    sd_7_readiness_score NUMERIC,
    sd_7_steps NUMERIC,

    -- Trend indicators
    trend_7_readiness_score NUMERIC,
    trend_7_sleep_total_seconds NUMERIC,

    -- HRV rolling features
    rm_7_hrv_average NUMERIC,
    rm_14_hrv_average NUMERIC,
    rm_28_hrv_average NUMERIC,
    delta_hrv_vs_rm7 NUMERIC,
    sd_7_hrv_average NUMERIC,
    trend_7_hrv_average NUMERIC,

    -- Stress rolling features
    rm_7_stress_high_minutes NUMERIC,
    rm_14_stress_high_minutes NUMERIC,

    -- SpO2 rolling features
    rm_7_spo2_average NUMERIC,

    -- Workout rolling features
    rm_7_workout_total_minutes NUMERIC,

    -- Timestamps
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (user_id, date),
    FOREIGN KEY (user_id, date) REFERENCES oura_daily(user_id, date) ON DELETE CASCADE
);

-- ============================================
-- Personal info (per-user)
-- ============================================
CREATE TABLE oura_personal_info (
    user_id UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    age INT,
    weight NUMERIC,
    height NUMERIC,
    biological_sex TEXT,
    email TEXT,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================
-- Row-Level Security
-- ============================================

-- Helper: safe user_id extraction (returns NULL if unset or empty)
-- NULLIF(..., '') ensures empty string also fails

-- oura_auth
ALTER TABLE oura_auth ENABLE ROW LEVEL SECURITY;
ALTER TABLE oura_auth FORCE ROW LEVEL SECURITY;

CREATE POLICY user_isolation_select ON oura_auth FOR SELECT
  USING (user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid);
CREATE POLICY user_isolation_insert ON oura_auth FOR INSERT
  WITH CHECK (user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid);
CREATE POLICY user_isolation_update ON oura_auth FOR UPDATE
  USING (user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid)
  WITH CHECK (user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid);
CREATE POLICY user_isolation_delete ON oura_auth FOR DELETE
  USING (user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid);

-- oura_raw
ALTER TABLE oura_raw ENABLE ROW LEVEL SECURITY;
ALTER TABLE oura_raw FORCE ROW LEVEL SECURITY;

CREATE POLICY user_isolation_select ON oura_raw FOR SELECT
  USING (user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid);
CREATE POLICY user_isolation_insert ON oura_raw FOR INSERT
  WITH CHECK (user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid);
CREATE POLICY user_isolation_update ON oura_raw FOR UPDATE
  USING (user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid)
  WITH CHECK (user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid);
CREATE POLICY user_isolation_delete ON oura_raw FOR DELETE
  USING (user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid);

-- oura_daily
ALTER TABLE oura_daily ENABLE ROW LEVEL SECURITY;
ALTER TABLE oura_daily FORCE ROW LEVEL SECURITY;

CREATE POLICY user_isolation_select ON oura_daily FOR SELECT
  USING (user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid);
CREATE POLICY user_isolation_insert ON oura_daily FOR INSERT
  WITH CHECK (user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid);
CREATE POLICY user_isolation_update ON oura_daily FOR UPDATE
  USING (user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid)
  WITH CHECK (user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid);
CREATE POLICY user_isolation_delete ON oura_daily FOR DELETE
  USING (user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid);

-- oura_day_tags
ALTER TABLE oura_day_tags ENABLE ROW LEVEL SECURITY;
ALTER TABLE oura_day_tags FORCE ROW LEVEL SECURITY;

CREATE POLICY user_isolation_select ON oura_day_tags FOR SELECT
  USING (user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid);
CREATE POLICY user_isolation_insert ON oura_day_tags FOR INSERT
  WITH CHECK (user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid);
CREATE POLICY user_isolation_update ON oura_day_tags FOR UPDATE
  USING (user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid)
  WITH CHECK (user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid);
CREATE POLICY user_isolation_delete ON oura_day_tags FOR DELETE
  USING (user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid);

-- oura_features_daily
ALTER TABLE oura_features_daily ENABLE ROW LEVEL SECURITY;
ALTER TABLE oura_features_daily FORCE ROW LEVEL SECURITY;

CREATE POLICY user_isolation_select ON oura_features_daily FOR SELECT
  USING (user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid);
CREATE POLICY user_isolation_insert ON oura_features_daily FOR INSERT
  WITH CHECK (user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid);
CREATE POLICY user_isolation_update ON oura_features_daily FOR UPDATE
  USING (user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid)
  WITH CHECK (user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid);
CREATE POLICY user_isolation_delete ON oura_features_daily FOR DELETE
  USING (user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid);

-- oura_personal_info
ALTER TABLE oura_personal_info ENABLE ROW LEVEL SECURITY;
ALTER TABLE oura_personal_info FORCE ROW LEVEL SECURITY;

CREATE POLICY user_isolation_select ON oura_personal_info FOR SELECT
  USING (user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid);
CREATE POLICY user_isolation_insert ON oura_personal_info FOR INSERT
  WITH CHECK (user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid);
CREATE POLICY user_isolation_update ON oura_personal_info FOR UPDATE
  USING (user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid)
  WITH CHECK (user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid);
CREATE POLICY user_isolation_delete ON oura_personal_info FOR DELETE
  USING (user_id = NULLIF(current_setting('app.current_user_id', true), '')::uuid);

-- No RLS on users and sessions (accessed by auth module before user context exists)

-- ============================================
-- Triggers for updated_at
-- ============================================
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_users_updated_at
    BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_oura_auth_updated_at
    BEFORE UPDATE ON oura_auth FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_oura_daily_updated_at
    BEFORE UPDATE ON oura_daily FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_oura_features_daily_updated_at
    BEFORE UPDATE ON oura_features_daily FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_oura_personal_info_updated_at
    BEFORE UPDATE ON oura_personal_info FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================
-- Grants for app_user role (if exists)
-- ============================================
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_user') THEN
    GRANT SELECT, INSERT, UPDATE, DELETE ON
      users, sessions, oauth_states, oura_auth,
      oura_raw, oura_daily, oura_day_tags, oura_features_daily, oura_personal_info
    TO app_user;
    GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO app_user;

    ALTER DEFAULT PRIVILEGES IN SCHEMA public
      GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO app_user;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public
      GRANT USAGE, SELECT ON SEQUENCES TO app_user;
  END IF;
END $$;

COMMIT;
