-- Add 60-day and 100-day rolling mean columns to oura_features_daily
ALTER TABLE oura_features_daily
    ADD COLUMN IF NOT EXISTS rm_60_readiness_score NUMERIC,
    ADD COLUMN IF NOT EXISTS rm_100_readiness_score NUMERIC,
    ADD COLUMN IF NOT EXISTS rm_60_sleep_total_seconds NUMERIC,
    ADD COLUMN IF NOT EXISTS rm_100_sleep_total_seconds NUMERIC,
    ADD COLUMN IF NOT EXISTS rm_60_steps NUMERIC,
    ADD COLUMN IF NOT EXISTS rm_100_steps NUMERIC,
    ADD COLUMN IF NOT EXISTS rm_60_hrv_average NUMERIC,
    ADD COLUMN IF NOT EXISTS rm_100_hrv_average NUMERIC;
