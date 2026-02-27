"""Feature engineering pipeline: daily -> features table (multi-user)."""

from datetime import date, timedelta
from decimal import Decimal
from typing import Any, Awaitable, Callable

import numpy as np
import pandas as pd

from app.db import get_db_for_user


async def load_daily_data(start_date: date, end_date: date, user_id: str) -> pd.DataFrame:
    """Load daily data from oura_daily table for a specific user."""
    async with get_db_for_user(user_id) as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT * FROM oura_daily
                WHERE date >= %(start)s AND date <= %(end)s
                AND user_id = %(uid)s
                ORDER BY date ASC
                """,
                {"start": start_date, "end": end_date, "uid": user_id},
            )
            rows = await cur.fetchall()

    if not rows:
        return pd.DataFrame()

    def convert_row(r: dict) -> dict:
        return {k: float(v) if isinstance(v, Decimal) else v for k, v in r.items()}

    df = pd.DataFrame([convert_row(dict(r)) for r in rows])
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date").sort_index()
    return df


def compute_rolling_features(
    df: pd.DataFrame,
    target_date: date,
    lookback_days: int = 28,
) -> dict[str, Any]:
    """Compute rolling features for a single day."""
    features: dict[str, Any] = {"date": target_date}

    target_dt = pd.Timestamp(target_date)
    if target_dt not in df.index:
        return features

    history = df.loc[:target_dt]

    if len(history) < 2:
        return features

    # Readiness rolling means
    readiness = history["readiness_score"].dropna()
    if len(readiness) >= 3:
        features["rm_3_readiness_score"] = float(readiness.iloc[:-1].tail(3).mean()) if len(readiness) > 1 else None
    if len(readiness) >= 7:
        features["rm_7_readiness_score"] = float(readiness.iloc[:-1].tail(7).mean()) if len(readiness) > 1 else None
    if len(readiness) >= 14:
        features["rm_14_readiness_score"] = float(readiness.iloc[:-1].tail(14).mean()) if len(readiness) > 1 else None
    if len(readiness) >= 28:
        features["rm_28_readiness_score"] = float(readiness.iloc[:-1].tail(28).mean()) if len(readiness) > 1 else None
    if len(readiness) >= 60:
        features["rm_60_readiness_score"] = float(readiness.iloc[:-1].tail(60).mean()) if len(readiness) > 1 else None
    if len(readiness) >= 100:
        features["rm_100_readiness_score"] = float(readiness.iloc[:-1].tail(100).mean()) if len(readiness) > 1 else None

    # Sleep total rolling means
    sleep = history["sleep_total_seconds"].dropna()
    if len(sleep) >= 3:
        features["rm_3_sleep_total_seconds"] = float(sleep.iloc[:-1].tail(3).mean()) if len(sleep) > 1 else None
    if len(sleep) >= 7:
        features["rm_7_sleep_total_seconds"] = float(sleep.iloc[:-1].tail(7).mean()) if len(sleep) > 1 else None
    if len(sleep) >= 14:
        features["rm_14_sleep_total_seconds"] = float(sleep.iloc[:-1].tail(14).mean()) if len(sleep) > 1 else None
    if len(sleep) >= 28:
        features["rm_28_sleep_total_seconds"] = float(sleep.iloc[:-1].tail(28).mean()) if len(sleep) > 1 else None
    if len(sleep) >= 60:
        features["rm_60_sleep_total_seconds"] = float(sleep.iloc[:-1].tail(60).mean()) if len(sleep) > 1 else None
    if len(sleep) >= 100:
        features["rm_100_sleep_total_seconds"] = float(sleep.iloc[:-1].tail(100).mean()) if len(sleep) > 1 else None

    # Steps rolling means
    steps = history["steps"].dropna()
    if len(steps) >= 7:
        features["rm_7_steps"] = float(steps.iloc[:-1].tail(7).mean()) if len(steps) > 1 else None
    if len(steps) >= 14:
        features["rm_14_steps"] = float(steps.iloc[:-1].tail(14).mean()) if len(steps) > 1 else None
    if len(steps) >= 28:
        features["rm_28_steps"] = float(steps.iloc[:-1].tail(28).mean()) if len(steps) > 1 else None
    if len(steps) >= 60:
        features["rm_60_steps"] = float(steps.iloc[:-1].tail(60).mean()) if len(steps) > 1 else None
    if len(steps) >= 100:
        features["rm_100_steps"] = float(steps.iloc[:-1].tail(100).mean()) if len(steps) > 1 else None

    # Deltas vs 7-day rolling mean
    if "rm_7_readiness_score" in features and features["rm_7_readiness_score"] is not None:
        current_readiness = history.loc[target_dt, "readiness_score"]
        if pd.notna(current_readiness):
            features["delta_readiness_vs_rm7"] = float(current_readiness - features["rm_7_readiness_score"])

    if "rm_7_sleep_total_seconds" in features and features["rm_7_sleep_total_seconds"] is not None:
        current_sleep = history.loc[target_dt, "sleep_total_seconds"]
        if pd.notna(current_sleep):
            features["delta_sleep_vs_rm7"] = float(current_sleep - features["rm_7_sleep_total_seconds"])

    if "rm_7_steps" in features and features["rm_7_steps"] is not None:
        current_steps = history.loc[target_dt, "steps"]
        if pd.notna(current_steps):
            features["delta_steps_vs_rm7"] = float(current_steps - features["rm_7_steps"])

    # Lag features
    for lag in range(1, 8):
        lag_date = target_dt - pd.Timedelta(days=lag)
        if lag_date in history.index:
            sleep_val = history.loc[lag_date, "sleep_total_seconds"]
            if pd.notna(sleep_val):
                features[f"lag_{lag}_sleep_total_seconds"] = int(sleep_val)

            if lag <= 3:
                readiness_val = history.loc[lag_date, "readiness_score"]
                if pd.notna(readiness_val):
                    features[f"lag_{lag}_readiness_score"] = int(readiness_val)

    # Rolling standard deviation
    if len(sleep) >= 8:
        features["sd_7_sleep_total_seconds"] = float(sleep.iloc[:-1].tail(7).std())
    if len(sleep) >= 15:
        features["sd_14_sleep_total_seconds"] = float(sleep.iloc[:-1].tail(14).std())

    if len(readiness) >= 8:
        features["sd_7_readiness_score"] = float(readiness.iloc[:-1].tail(7).std())

    if len(steps) >= 8:
        features["sd_7_steps"] = float(steps.iloc[:-1].tail(7).std())

    # Trend indicators
    if len(readiness) >= 8:
        recent_readiness = readiness.iloc[:-1].tail(7)
        if len(recent_readiness) == 7:
            x = np.arange(7)
            y = recent_readiness.values
            if not np.any(np.isnan(y)):
                slope, _ = np.polyfit(x, y, 1)
                features["trend_7_readiness_score"] = float(slope)

    if len(sleep) >= 8:
        recent_sleep = sleep.iloc[:-1].tail(7)
        if len(recent_sleep) == 7:
            x = np.arange(7)
            y = recent_sleep.values
            if not np.any(np.isnan(y)):
                slope, _ = np.polyfit(x, y, 1)
                features["trend_7_sleep_total_seconds"] = float(slope)

    # HRV rolling means
    if "hrv_average" in history.columns:
        hrv = history["hrv_average"].dropna()
        if len(hrv) >= 8:
            features["rm_7_hrv_average"] = float(hrv.iloc[:-1].tail(7).mean())
        if len(hrv) >= 15:
            features["rm_14_hrv_average"] = float(hrv.iloc[:-1].tail(14).mean())
        if len(hrv) >= 29:
            features["rm_28_hrv_average"] = float(hrv.iloc[:-1].tail(28).mean())
        if len(hrv) >= 61:
            features["rm_60_hrv_average"] = float(hrv.iloc[:-1].tail(60).mean())
        if len(hrv) >= 101:
            features["rm_100_hrv_average"] = float(hrv.iloc[:-1].tail(100).mean())

        if "rm_7_hrv_average" in features and features["rm_7_hrv_average"] is not None:
            current_hrv = history.loc[target_dt, "hrv_average"]
            if pd.notna(current_hrv):
                features["delta_hrv_vs_rm7"] = float(current_hrv - features["rm_7_hrv_average"])

        if len(hrv) >= 8:
            features["sd_7_hrv_average"] = float(hrv.iloc[:-1].tail(7).std())

        if len(hrv) >= 8:
            recent_hrv = hrv.iloc[:-1].tail(7)
            if len(recent_hrv) == 7:
                x = np.arange(7)
                y = recent_hrv.values
                if not np.any(np.isnan(y)):
                    slope, _ = np.polyfit(x, y, 1)
                    features["trend_7_hrv_average"] = float(slope)

    # Stress rolling means
    if "stress_high_minutes" in history.columns:
        stress = history["stress_high_minutes"].dropna()
        if len(stress) >= 8:
            features["rm_7_stress_high_minutes"] = float(stress.iloc[:-1].tail(7).mean())
        if len(stress) >= 15:
            features["rm_14_stress_high_minutes"] = float(stress.iloc[:-1].tail(14).mean())

    # SpO2 rolling mean
    if "spo2_average" in history.columns:
        spo2 = history["spo2_average"].dropna()
        if len(spo2) >= 8:
            features["rm_7_spo2_average"] = float(spo2.iloc[:-1].tail(7).mean())

    # Workout rolling mean
    if "workout_total_minutes" in history.columns:
        workouts = history["workout_total_minutes"].dropna()
        if len(workouts) >= 8:
            features["rm_7_workout_total_minutes"] = float(workouts.iloc[:-1].tail(7).mean())

    return features


async def recompute_features(
    start_date: date,
    end_date: date,
    user_id: str,
    progress_callback: Callable[[int, int], Awaitable[None]] | None = None,
) -> int:
    """Recompute features for a date range."""
    history_start = start_date - timedelta(days=100)
    df = await load_daily_data(history_start, end_date, user_id)

    if df.empty:
        if progress_callback is not None:
            await progress_callback(0, 0)
        return 0

    days_processed = 0
    current = start_date
    total_days = (end_date - start_date).days + 1

    async with get_db_for_user(user_id) as conn:
        while current <= end_date:
            features = compute_rolling_features(df, current)

            if len(features) > 1:
                # Add user_id to features
                features["user_id"] = user_id
                columns = list(features.keys())
                values = {k: v for k, v in features.items()}

                set_clause = ", ".join(
                    f"{col} = EXCLUDED.{col}"
                    for col in columns
                    if col not in ("date", "user_id")
                )

                await conn.execute(
                    f"""
                    INSERT INTO oura_features_daily ({', '.join(columns)}, computed_at)
                    VALUES ({', '.join(f'%({col})s' for col in columns)}, NOW())
                    ON CONFLICT (user_id, date) DO UPDATE SET
                        {set_clause},
                        updated_at = NOW()
                    """,
                    values,
                )
                days_processed += 1

            current += timedelta(days=1)
            if progress_callback is not None:
                processed = (current - start_date).days
                await progress_callback(processed, total_days)

    return days_processed
