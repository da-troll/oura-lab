"""Ingestion pipeline: Oura API -> raw -> daily tables (multi-user)."""

import asyncio
import json
from datetime import date, datetime, timedelta, timezone
from typing import Any, AsyncGenerator, Awaitable, Callable

from app.db import get_db_for_user
from app.oura.client import oura_client


def resolve_sleep_day(sleep_session: dict[str, Any]) -> date | None:
    """Map a sleep session to the date of waking up."""
    bedtime_end = sleep_session.get("bedtime_end")
    if not bedtime_end:
        day_str = sleep_session.get("day")
        if day_str:
            return date.fromisoformat(day_str)
        return None

    if isinstance(bedtime_end, str):
        try:
            dt = datetime.fromisoformat(bedtime_end.replace("Z", "+00:00"))
            return dt.date()
        except ValueError:
            return None

    return None


def select_primary_sleep_session(sessions: list[dict[str, Any]]) -> dict[str, Any]:
    """Pick the canonical nightly sleep session for a day.

    Preference order:
    1) long_sleep sessions (nightly sleep)
    2) if no explicit types are present, fall back to untyped records
    3) highest total_sleep_duration
    4) latest bedtime_end (stable tie-breaker)

    If records are present but none are nightly candidates (for example, naps),
    return {} so nightly metrics remain empty instead of recording tiny durations.
    """
    if not sessions:
        return {}

    def _session_type(session: dict[str, Any]) -> str:
        value = session.get("type")
        return str(value).strip().lower() if value is not None else ""

    def _to_seconds(value: Any) -> float:
        if value is None:
            return 0.0
        if isinstance(value, (int, float)):
            return float(value)
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    def _score(session: dict[str, Any]) -> tuple[int, float, str]:
        session_type = _session_type(session)
        is_long_sleep = 1 if session_type == "long_sleep" else 0
        total_sleep_seconds = _to_seconds(session.get("total_sleep_duration"))
        bedtime_end = str(session.get("bedtime_end") or "")
        return (is_long_sleep, total_sleep_seconds, bedtime_end)

    normalized = [session for session in sessions if isinstance(session, dict)]
    if not normalized:
        return {}

    long_sleep_sessions = [
        session for session in normalized if _session_type(session) == "long_sleep"
    ]
    if long_sleep_sessions:
        return max(long_sleep_sessions, key=_score)

    # Backward-compatibility for payloads that do not include "type".
    untyped_sessions = [session for session in normalized if _session_type(session) == ""]
    if untyped_sessions:
        return max(untyped_sessions, key=_score)

    # Typed but non-nightly sessions only (for example naps): do not
    # produce nightly sleep metrics from these.
    return {}


def resolve_raw_record_day(data_type: str, record: dict[str, Any]) -> Any:
    """Resolve the storage day key for a raw record."""
    day = record.get("day")
    if data_type == "sleep" and not day:
        # Keep Oura's canonical day when present; only derive from bedtime_end
        # as a fallback for malformed/missing day values.
        resolved_day = resolve_sleep_day(record)
        return str(resolved_day) if resolved_day else day
    return day


async def ingest_raw_data(
    start_date: date,
    end_date: date,
    user_id: str,
    data_types: list[str] | None = None,
    progress_callback: Callable[[str, int, int], Awaitable[None]] | None = None,
) -> dict[str, int]:
    """Fetch raw data from Oura API and store in oura_raw table."""
    if data_types is None:
        data_types = [
            "daily_sleep", "sleep", "daily_readiness", "daily_activity",
            "daily_stress", "daily_spo2", "daily_cardiovascular_age",
            "tag", "workout", "session",
        ]

    counts: dict[str, int] = {}

    fetch_map = {
        "daily_sleep": oura_client.fetch_daily_sleep,
        "sleep": oura_client.fetch_sleep_sessions,
        "daily_readiness": oura_client.fetch_daily_readiness,
        "daily_activity": oura_client.fetch_daily_activity,
        "daily_stress": oura_client.fetch_daily_stress,
        "daily_spo2": oura_client.fetch_daily_spo2,
        "daily_cardiovascular_age": oura_client.fetch_daily_cardiovascular_age,
        "tag": oura_client.fetch_tags,
        "workout": oura_client.fetch_workouts,
        "session": oura_client.fetch_sessions,
    }

    async with get_db_for_user(user_id) as conn:
        total_types = len(data_types)
        completed_types = 0
        for data_type in data_types:
            if data_type not in fetch_map:
                completed_types += 1
                if progress_callback is not None:
                    await progress_callback(data_type, completed_types, total_types)
                continue

            fetch_fn = fetch_map[data_type]
            try:
                records = await fetch_fn(start_date, end_date, user_id)
            except Exception:
                counts[data_type] = 0
                completed_types += 1
                if progress_callback is not None:
                    await progress_callback(data_type, completed_types, total_types)
                continue
            counts[data_type] = len(records)

            for record in records:
                day = resolve_raw_record_day(data_type, record)

                await conn.execute(
                    """
                    INSERT INTO oura_raw (user_id, source, day, payload, fetched_at)
                    VALUES (%(user_id)s, %(source)s, %(day)s, %(payload)s, %(fetched_at)s)
                    """,
                    {
                        "user_id": user_id,
                        "source": data_type,
                        "day": day,
                        "payload": json.dumps(record),
                        "fetched_at": datetime.now(timezone.utc),
                    },
                )

            completed_types += 1
            if progress_callback is not None:
                await progress_callback(data_type, completed_types, total_types)

    return counts


async def normalize_daily_data(
    start_date: date,
    end_date: date,
    user_id: str,
    progress_callback: Callable[[int, int], Awaitable[None]] | None = None,
) -> int:
    """Normalize raw data into oura_daily table."""
    days_processed = 0
    current = start_date
    total_days = (end_date - start_date).days + 1

    async with get_db_for_user(user_id) as conn:
        while current <= end_date:
            async with conn.cursor() as cur:
                # Get daily_sleep data
                await cur.execute(
                    "SELECT payload FROM oura_raw WHERE source = 'daily_sleep' AND day = %(day)s AND user_id = %(uid)s ORDER BY fetched_at DESC LIMIT 1",
                    {"day": str(current), "uid": user_id},
                )
                sleep_row = await cur.fetchone()
                daily_sleep_data = sleep_row["payload"] if sleep_row else {}

                # Get sleep session data
                await cur.execute(
                    "SELECT payload FROM oura_raw WHERE source = 'sleep' AND day = %(day)s AND user_id = %(uid)s",
                    {"day": str(current), "uid": user_id},
                )
                sleep_session_rows = await cur.fetchall()
                sleep_sessions = [
                    row["payload"]
                    for row in sleep_session_rows
                    if isinstance(row.get("payload"), dict)
                ]
                sleep_session_data = select_primary_sleep_session(sleep_sessions)

                # Get daily_readiness data
                await cur.execute(
                    "SELECT payload FROM oura_raw WHERE source = 'daily_readiness' AND day = %(day)s AND user_id = %(uid)s ORDER BY fetched_at DESC LIMIT 1",
                    {"day": str(current), "uid": user_id},
                )
                readiness_row = await cur.fetchone()
                readiness_data = readiness_row["payload"] if readiness_row else {}

                # Get daily_activity data
                await cur.execute(
                    "SELECT payload FROM oura_raw WHERE source = 'daily_activity' AND day = %(day)s AND user_id = %(uid)s ORDER BY fetched_at DESC LIMIT 1",
                    {"day": str(current), "uid": user_id},
                )
                activity_row = await cur.fetchone()
                activity_data = activity_row["payload"] if activity_row else {}

                # Get daily_stress data
                await cur.execute(
                    "SELECT payload FROM oura_raw WHERE source = 'daily_stress' AND day = %(day)s AND user_id = %(uid)s ORDER BY fetched_at DESC LIMIT 1",
                    {"day": str(current), "uid": user_id},
                )
                stress_row = await cur.fetchone()
                stress_data = stress_row["payload"] if stress_row else {}

                # Get daily_spo2 data
                await cur.execute(
                    "SELECT payload FROM oura_raw WHERE source = 'daily_spo2' AND day = %(day)s AND user_id = %(uid)s ORDER BY fetched_at DESC LIMIT 1",
                    {"day": str(current), "uid": user_id},
                )
                spo2_row = await cur.fetchone()
                spo2_data = spo2_row["payload"] if spo2_row else {}

                # Get daily_cardiovascular_age data
                await cur.execute(
                    "SELECT payload FROM oura_raw WHERE source = 'daily_cardiovascular_age' AND day = %(day)s AND user_id = %(uid)s ORDER BY fetched_at DESC LIMIT 1",
                    {"day": str(current), "uid": user_id},
                )
                cardio_age_row = await cur.fetchone()
                cardio_age_data = cardio_age_row["payload"] if cardio_age_row else {}

                # Get all workout records for this day
                await cur.execute(
                    "SELECT payload FROM oura_raw WHERE source = 'workout' AND day = %(day)s AND user_id = %(uid)s",
                    {"day": str(current), "uid": user_id},
                )
                workout_rows = await cur.fetchall()

                # Get all session records for this day
                await cur.execute(
                    "SELECT payload FROM oura_raw WHERE source = 'session' AND day = %(day)s AND user_id = %(uid)s",
                    {"day": str(current), "uid": user_id},
                )
                session_rows = await cur.fetchall()

            # Extract metrics
            weekday = current.weekday()
            is_weekend = weekday >= 5

            month = current.month
            if month in (3, 4, 5):
                season = "spring"
            elif month in (6, 7, 8):
                season = "summer"
            elif month in (9, 10, 11):
                season = "fall"
            else:
                season = "winter"

            sleep_total = sleep_session_data.get("total_sleep_duration")
            sleep_efficiency = sleep_session_data.get("efficiency")
            sleep_rem = sleep_session_data.get("rem_sleep_duration")
            sleep_deep = sleep_session_data.get("deep_sleep_duration")
            sleep_latency = sleep_session_data.get("latency")
            sleep_restfulness = sleep_session_data.get("restless_periods")
            sleep_score = daily_sleep_data.get("score")

            hrv_average = sleep_session_data.get("average_hrv")
            hr_lowest = sleep_session_data.get("lowest_heart_rate")
            hr_average = sleep_session_data.get("average_heart_rate")

            readiness_score = readiness_data.get("score")
            readiness_contributors = readiness_data.get("contributors", {})
            readiness_temp = readiness_contributors.get("body_temperature")
            readiness_rhr = readiness_contributors.get("resting_heart_rate")
            readiness_hrv = readiness_contributors.get("hrv_balance")
            readiness_recovery = readiness_contributors.get("recovery_index")
            readiness_activity = readiness_contributors.get("activity_balance")

            activity_score = activity_data.get("score")
            steps = activity_data.get("steps")
            cal_total = activity_data.get("total_calories")
            cal_active = activity_data.get("active_calories")
            met_minutes = activity_data.get("met", {}).get("minutes") if isinstance(activity_data.get("met"), dict) else activity_data.get("equivalent_walking_distance")
            low_activity = activity_data.get("low_activity_met_minutes")
            medium_activity = activity_data.get("medium_activity_met_minutes")
            high_activity = activity_data.get("high_activity_met_minutes")
            sedentary = activity_data.get("sedentary_met_minutes")

            stress_high_raw = stress_data.get("stress_high")
            recovery_high_raw = stress_data.get("recovery_high")
            stress_high = round(stress_high_raw / 60) if stress_high_raw else stress_high_raw
            recovery_high = round(recovery_high_raw / 60) if recovery_high_raw else recovery_high_raw
            stress_day_summary = stress_data.get("day_summary")

            spo2_percentage = spo2_data.get("spo2_percentage", {})
            spo2_average = spo2_percentage.get("average") if isinstance(spo2_percentage, dict) else None
            if spo2_average is not None and spo2_average == 0:
                spo2_average = None
            breathing_disturbance = spo2_data.get("breathing_disturbance_index")

            vascular_age = cardio_age_data.get("vascular_age")
            sleep_breath_average = sleep_session_data.get("average_breath")

            activity_contributors = activity_data.get("contributors", {})
            if not isinstance(activity_contributors, dict):
                activity_contributors = {}
            activity_meet_daily_targets = activity_contributors.get("meet_daily_targets")
            activity_move_every_hour = activity_contributors.get("move_every_hour")
            activity_recovery_time = activity_contributors.get("recovery_time")
            activity_training_frequency = activity_contributors.get("training_frequency")
            activity_training_volume = activity_contributors.get("training_volume")
            non_wear = activity_data.get("non_wear_minutes")
            inactivity_alerts_val = activity_data.get("inactivity_alerts")

            readiness_sleep_balance = readiness_contributors.get("sleep_balance")

            # Workout aggregation
            unique_workouts: dict[str, dict] = {}
            for wr in workout_rows:
                wid = wr["payload"].get("id", "")
                if wid not in unique_workouts:
                    unique_workouts[wid] = wr["payload"]
            workout_count = len(unique_workouts)
            workout_total_minutes = None
            workout_total_calories = None
            if workout_count > 0:
                total_minutes = 0.0
                total_cal = 0.0
                for wp in unique_workouts.values():
                    start_dt = wp.get("start_datetime")
                    end_dt = wp.get("end_datetime")
                    if start_dt and end_dt:
                        try:
                            s = datetime.fromisoformat(start_dt.replace("Z", "+00:00"))
                            e = datetime.fromisoformat(end_dt.replace("Z", "+00:00"))
                            total_minutes += (e - s).total_seconds() / 60
                        except (ValueError, TypeError):
                            pass
                    cal = wp.get("calories", 0) or 0
                    total_cal += cal
                workout_total_minutes = round(total_minutes, 1) if total_minutes else None
                workout_total_calories = round(total_cal, 1) if total_cal else None

            # Session aggregation
            unique_sessions: dict[str, dict] = {}
            for sr in session_rows:
                sid = sr["payload"].get("id", "")
                if sid not in unique_sessions:
                    unique_sessions[sid] = sr["payload"]
            session_count = len(unique_sessions)
            session_total_minutes = None
            if session_count > 0:
                total_minutes = 0.0
                for sp in unique_sessions.values():
                    start_dt = sp.get("start_datetime")
                    end_dt = sp.get("end_datetime")
                    if start_dt and end_dt:
                        try:
                            s = datetime.fromisoformat(start_dt.replace("Z", "+00:00"))
                            e = datetime.fromisoformat(end_dt.replace("Z", "+00:00"))
                            total_minutes += (e - s).total_seconds() / 60
                        except (ValueError, TypeError):
                            pass
                session_total_minutes = round(total_minutes, 1) if total_minutes else None

            has_data = (
                daily_sleep_data or sleep_session_data or readiness_data
                or activity_data or stress_data or spo2_data or cardio_age_data
            )
            if has_data:
                await conn.execute(
                    """
                    INSERT INTO oura_daily (
                        user_id, date, weekday, is_weekend, season, is_holiday,
                        sleep_total_seconds, sleep_efficiency, sleep_rem_seconds,
                        sleep_deep_seconds, sleep_latency_seconds, sleep_restlessness, sleep_score,
                        readiness_score, readiness_temperature_deviation, readiness_resting_heart_rate,
                        readiness_hrv_balance, readiness_recovery_index, readiness_activity_balance,
                        activity_score, steps, cal_total, cal_active, met_minutes,
                        low_activity_minutes, medium_activity_minutes, high_activity_minutes, sedentary_minutes,
                        hr_lowest, hr_average, hrv_average,
                        stress_high_minutes, recovery_high_minutes, stress_day_summary,
                        spo2_average, breathing_disturbance_index, vascular_age,
                        sleep_breath_average,
                        activity_meet_daily_targets, activity_move_every_hour,
                        activity_recovery_time, activity_training_frequency, activity_training_volume,
                        non_wear_seconds, inactivity_alerts,
                        readiness_sleep_balance,
                        workout_count, workout_total_minutes, workout_total_calories,
                        session_count, session_total_minutes
                    )
                    VALUES (
                        %(user_id)s, %(date)s, %(weekday)s, %(is_weekend)s, %(season)s, %(is_holiday)s,
                        %(sleep_total_seconds)s, %(sleep_efficiency)s, %(sleep_rem_seconds)s,
                        %(sleep_deep_seconds)s, %(sleep_latency_seconds)s, %(sleep_restlessness)s, %(sleep_score)s,
                        %(readiness_score)s, %(readiness_temperature_deviation)s, %(readiness_resting_heart_rate)s,
                        %(readiness_hrv_balance)s, %(readiness_recovery_index)s, %(readiness_activity_balance)s,
                        %(activity_score)s, %(steps)s, %(cal_total)s, %(cal_active)s, %(met_minutes)s,
                        %(low_activity_minutes)s, %(medium_activity_minutes)s, %(high_activity_minutes)s, %(sedentary_minutes)s,
                        %(hr_lowest)s, %(hr_average)s, %(hrv_average)s,
                        %(stress_high_minutes)s, %(recovery_high_minutes)s, %(stress_day_summary)s,
                        %(spo2_average)s, %(breathing_disturbance_index)s, %(vascular_age)s,
                        %(sleep_breath_average)s,
                        %(activity_meet_daily_targets)s, %(activity_move_every_hour)s,
                        %(activity_recovery_time)s, %(activity_training_frequency)s, %(activity_training_volume)s,
                        %(non_wear_seconds)s, %(inactivity_alerts)s,
                        %(readiness_sleep_balance)s,
                        %(workout_count)s, %(workout_total_minutes)s, %(workout_total_calories)s,
                        %(session_count)s, %(session_total_minutes)s
                    )
                    ON CONFLICT (user_id, date) DO UPDATE SET
                        weekday = EXCLUDED.weekday,
                        is_weekend = EXCLUDED.is_weekend,
                        season = EXCLUDED.season,
                        sleep_total_seconds = COALESCE(EXCLUDED.sleep_total_seconds, oura_daily.sleep_total_seconds),
                        sleep_efficiency = COALESCE(EXCLUDED.sleep_efficiency, oura_daily.sleep_efficiency),
                        sleep_rem_seconds = COALESCE(EXCLUDED.sleep_rem_seconds, oura_daily.sleep_rem_seconds),
                        sleep_deep_seconds = COALESCE(EXCLUDED.sleep_deep_seconds, oura_daily.sleep_deep_seconds),
                        sleep_latency_seconds = COALESCE(EXCLUDED.sleep_latency_seconds, oura_daily.sleep_latency_seconds),
                        sleep_restlessness = COALESCE(EXCLUDED.sleep_restlessness, oura_daily.sleep_restlessness),
                        sleep_score = COALESCE(EXCLUDED.sleep_score, oura_daily.sleep_score),
                        readiness_score = COALESCE(EXCLUDED.readiness_score, oura_daily.readiness_score),
                        readiness_temperature_deviation = COALESCE(EXCLUDED.readiness_temperature_deviation, oura_daily.readiness_temperature_deviation),
                        readiness_resting_heart_rate = COALESCE(EXCLUDED.readiness_resting_heart_rate, oura_daily.readiness_resting_heart_rate),
                        readiness_hrv_balance = COALESCE(EXCLUDED.readiness_hrv_balance, oura_daily.readiness_hrv_balance),
                        readiness_recovery_index = COALESCE(EXCLUDED.readiness_recovery_index, oura_daily.readiness_recovery_index),
                        readiness_activity_balance = COALESCE(EXCLUDED.readiness_activity_balance, oura_daily.readiness_activity_balance),
                        activity_score = COALESCE(EXCLUDED.activity_score, oura_daily.activity_score),
                        steps = COALESCE(EXCLUDED.steps, oura_daily.steps),
                        cal_total = COALESCE(EXCLUDED.cal_total, oura_daily.cal_total),
                        cal_active = COALESCE(EXCLUDED.cal_active, oura_daily.cal_active),
                        met_minutes = COALESCE(EXCLUDED.met_minutes, oura_daily.met_minutes),
                        low_activity_minutes = COALESCE(EXCLUDED.low_activity_minutes, oura_daily.low_activity_minutes),
                        medium_activity_minutes = COALESCE(EXCLUDED.medium_activity_minutes, oura_daily.medium_activity_minutes),
                        high_activity_minutes = COALESCE(EXCLUDED.high_activity_minutes, oura_daily.high_activity_minutes),
                        sedentary_minutes = COALESCE(EXCLUDED.sedentary_minutes, oura_daily.sedentary_minutes),
                        hr_lowest = COALESCE(EXCLUDED.hr_lowest, oura_daily.hr_lowest),
                        hr_average = COALESCE(EXCLUDED.hr_average, oura_daily.hr_average),
                        hrv_average = COALESCE(EXCLUDED.hrv_average, oura_daily.hrv_average),
                        stress_high_minutes = COALESCE(EXCLUDED.stress_high_minutes, oura_daily.stress_high_minutes),
                        recovery_high_minutes = COALESCE(EXCLUDED.recovery_high_minutes, oura_daily.recovery_high_minutes),
                        stress_day_summary = COALESCE(EXCLUDED.stress_day_summary, oura_daily.stress_day_summary),
                        spo2_average = COALESCE(EXCLUDED.spo2_average, oura_daily.spo2_average),
                        breathing_disturbance_index = COALESCE(EXCLUDED.breathing_disturbance_index, oura_daily.breathing_disturbance_index),
                        vascular_age = COALESCE(EXCLUDED.vascular_age, oura_daily.vascular_age),
                        sleep_breath_average = COALESCE(EXCLUDED.sleep_breath_average, oura_daily.sleep_breath_average),
                        activity_meet_daily_targets = COALESCE(EXCLUDED.activity_meet_daily_targets, oura_daily.activity_meet_daily_targets),
                        activity_move_every_hour = COALESCE(EXCLUDED.activity_move_every_hour, oura_daily.activity_move_every_hour),
                        activity_recovery_time = COALESCE(EXCLUDED.activity_recovery_time, oura_daily.activity_recovery_time),
                        activity_training_frequency = COALESCE(EXCLUDED.activity_training_frequency, oura_daily.activity_training_frequency),
                        activity_training_volume = COALESCE(EXCLUDED.activity_training_volume, oura_daily.activity_training_volume),
                        non_wear_seconds = COALESCE(EXCLUDED.non_wear_seconds, oura_daily.non_wear_seconds),
                        inactivity_alerts = COALESCE(EXCLUDED.inactivity_alerts, oura_daily.inactivity_alerts),
                        readiness_sleep_balance = COALESCE(EXCLUDED.readiness_sleep_balance, oura_daily.readiness_sleep_balance),
                        workout_count = COALESCE(EXCLUDED.workout_count, oura_daily.workout_count),
                        workout_total_minutes = COALESCE(EXCLUDED.workout_total_minutes, oura_daily.workout_total_minutes),
                        workout_total_calories = COALESCE(EXCLUDED.workout_total_calories, oura_daily.workout_total_calories),
                        session_count = COALESCE(EXCLUDED.session_count, oura_daily.session_count),
                        session_total_minutes = COALESCE(EXCLUDED.session_total_minutes, oura_daily.session_total_minutes),
                        updated_at = NOW()
                    """,
                    {
                        "user_id": user_id,
                        "date": current,
                        "weekday": weekday,
                        "is_weekend": is_weekend,
                        "season": season,
                        "is_holiday": False,
                        "sleep_total_seconds": sleep_total,
                        "sleep_efficiency": sleep_efficiency,
                        "sleep_rem_seconds": sleep_rem,
                        "sleep_deep_seconds": sleep_deep,
                        "sleep_latency_seconds": sleep_latency,
                        "sleep_restlessness": sleep_restfulness,
                        "sleep_score": sleep_score,
                        "readiness_score": readiness_score,
                        "readiness_temperature_deviation": readiness_temp,
                        "readiness_resting_heart_rate": readiness_rhr,
                        "readiness_hrv_balance": readiness_hrv,
                        "readiness_recovery_index": readiness_recovery,
                        "readiness_activity_balance": readiness_activity,
                        "activity_score": activity_score,
                        "steps": steps,
                        "cal_total": cal_total,
                        "cal_active": cal_active,
                        "met_minutes": met_minutes,
                        "low_activity_minutes": low_activity,
                        "medium_activity_minutes": medium_activity,
                        "high_activity_minutes": high_activity,
                        "sedentary_minutes": sedentary,
                        "hr_lowest": hr_lowest,
                        "hr_average": hr_average,
                        "hrv_average": hrv_average,
                        "stress_high_minutes": stress_high,
                        "recovery_high_minutes": recovery_high,
                        "stress_day_summary": stress_day_summary,
                        "spo2_average": spo2_average,
                        "breathing_disturbance_index": breathing_disturbance,
                        "vascular_age": vascular_age,
                        "sleep_breath_average": sleep_breath_average,
                        "activity_meet_daily_targets": activity_meet_daily_targets,
                        "activity_move_every_hour": activity_move_every_hour,
                        "activity_recovery_time": activity_recovery_time,
                        "activity_training_frequency": activity_training_frequency,
                        "activity_training_volume": activity_training_volume,
                        "non_wear_seconds": non_wear,
                        "inactivity_alerts": inactivity_alerts_val,
                        "readiness_sleep_balance": readiness_sleep_balance,
                        "workout_count": workout_count,
                        "workout_total_minutes": workout_total_minutes,
                        "workout_total_calories": workout_total_calories,
                        "session_count": session_count,
                        "session_total_minutes": session_total_minutes,
                    },
                )
                days_processed += 1

            current += timedelta(days=1)
            if progress_callback is not None:
                processed_days = (current - start_date).days
                await progress_callback(processed_days, total_days)

    return days_processed


async def ingest_tags(
    start_date: date,
    end_date: date,
    user_id: str,
    progress_callback: Callable[[int, int], Awaitable[None]] | None = None,
) -> int:
    """Normalize tags from raw data into oura_day_tags table."""
    tags_processed = 0

    async with get_db_for_user(user_id) as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT day, payload FROM oura_raw WHERE source = 'tag' AND day >= %(start)s AND day <= %(end)s AND user_id = %(uid)s",
                {"start": str(start_date), "end": str(end_date), "uid": user_id},
            )
            tag_rows = await cur.fetchall()
            total_rows = len(tag_rows)

        for idx, row in enumerate(tag_rows, start=1):
            day = row["day"]
            payload = row["payload"]

            if not day:
                continue

            tag_text = payload.get("tag_type_code") or payload.get("text")
            if not tag_text:
                continue

            # Ensure the day exists in oura_daily first
            await conn.execute(
                """
                INSERT INTO oura_daily (user_id, date, weekday, is_weekend, season, is_holiday)
                VALUES (
                    %(uid)s,
                    %(date)s,
                    EXTRACT(DOW FROM %(date)s::date)::int,
                    EXTRACT(DOW FROM %(date)s::date) IN (0, 6),
                    CASE
                        WHEN EXTRACT(MONTH FROM %(date)s::date) IN (3,4,5) THEN 'spring'
                        WHEN EXTRACT(MONTH FROM %(date)s::date) IN (6,7,8) THEN 'summer'
                        WHEN EXTRACT(MONTH FROM %(date)s::date) IN (9,10,11) THEN 'fall'
                        ELSE 'winter'
                    END,
                    FALSE
                )
                ON CONFLICT (user_id, date) DO NOTHING
                """,
                {"uid": user_id, "date": day},
            )

            await conn.execute(
                """
                INSERT INTO oura_day_tags (user_id, date, tag)
                VALUES (%(uid)s, %(date)s, %(tag)s)
                ON CONFLICT (user_id, date, tag) DO NOTHING
                """,
                {"uid": user_id, "date": day, "tag": tag_text},
            )
            tags_processed += 1

            if progress_callback is not None:
                await progress_callback(idx, total_rows)

    return tags_processed


async def ingest_personal_info(user_id: str) -> bool:
    """Fetch and store personal info from Oura API."""
    try:
        info = await oura_client.fetch_personal_info(user_id)
    except Exception:
        return False

    if not info:
        return False

    async with get_db_for_user(user_id) as conn:
        await conn.execute(
            """
            INSERT INTO oura_personal_info (user_id, age, weight, height, biological_sex, email, fetched_at)
            VALUES (%(uid)s, %(age)s, %(weight)s, %(height)s, %(biological_sex)s, %(email)s, NOW())
            ON CONFLICT (user_id) DO UPDATE SET
                age = COALESCE(EXCLUDED.age, oura_personal_info.age),
                weight = COALESCE(EXCLUDED.weight, oura_personal_info.weight),
                height = COALESCE(EXCLUDED.height, oura_personal_info.height),
                biological_sex = COALESCE(EXCLUDED.biological_sex, oura_personal_info.biological_sex),
                email = COALESCE(EXCLUDED.email, oura_personal_info.email),
                fetched_at = NOW()
            """,
            {
                "uid": user_id,
                "age": info.get("age"),
                "weight": info.get("weight"),
                "height": info.get("height"),
                "biological_sex": info.get("biological_sex"),
                "email": info.get("email"),
            },
        )

    return True


async def resolve_sync_window(user_id: str) -> tuple[date, date, str]:
    """Resolve sync range.

    First sync: discover oldest available Oura day and backfill to today.
    Subsequent syncs: fetch only missing days since latest stored date.
    """
    today = datetime.now(timezone.utc).date()

    latest_synced: date | None = None
    async with get_db_for_user(user_id) as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT MAX(date) AS latest_date FROM oura_daily WHERE user_id = %s",
                (user_id,),
            )
            row = await cur.fetchone()
            if row and row["latest_date"]:
                latest_synced = row["latest_date"]

    if latest_synced:
        return latest_synced + timedelta(days=1), today, "incremental"

    oldest = await oura_client.find_oldest_data_date(user_id)
    if oldest:
        return oldest, today, "initial_backfill"

    # No data yet on Oura account; do a no-op sync window for today.
    return today, today, "initial_backfill"


def _progress_percent(
    start_pct: int,
    end_pct: int,
    current: int,
    total: int,
) -> int:
    """Map a stage progress value into an overall percent range."""
    if total <= 0:
        return end_pct
    current_clamped = min(max(current, 0), total)
    span = end_pct - start_pct
    return start_pct + int((current_clamped / total) * span)


async def run_full_ingest_stream(
    start_date: date | None,
    end_date: date | None,
    user_id: str,
) -> AsyncGenerator[dict[str, Any], None]:
    """Run full sync and emit progress events for NDJSON streaming."""
    queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()

    async def emit(
        event_type: str,
        *,
        percent: int | None = None,
        phase: str | None = None,
        message: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        event: dict[str, Any] = {"type": event_type}
        if percent is not None:
            event["percent"] = max(0, min(100, int(percent)))
        if phase is not None:
            event["phase"] = phase
        if message is not None:
            event["message"] = message
        if extra:
            event.update(extra)
        await queue.put(event)

    async def worker() -> None:
        sync_mode = "manual"
        try:
            await emit(
                "progress",
                percent=1,
                phase="resolving_window",
                message="Resolving sync window",
            )

            resolved_start = start_date
            resolved_end = end_date
            if resolved_start is None or resolved_end is None:
                resolved_start, resolved_end, sync_mode = await resolve_sync_window(user_id)

            await emit(
                "progress",
                percent=5,
                phase="resolving_window",
                message=f"Sync window {resolved_start} to {resolved_end}",
            )

            if resolved_start > resolved_end:
                await emit(
                    "done",
                    percent=100,
                    message="Already up to date. No new days to sync.",
                    extra={
                        "status": "completed",
                        "days_processed": 0,
                        "tags_processed": 0,
                        "features_processed": 0,
                        "start_date": str(resolved_start),
                        "end_date": str(resolved_end),
                        "sync_mode": sync_mode,
                    },
                )
                return

            async def raw_progress(source: str, current: int, total: int) -> None:
                pct = _progress_percent(5, 50, current, total)
                await emit(
                    "progress",
                    percent=pct,
                    phase="fetch_raw",
                    message=f"Fetched {source} ({current}/{total})",
                )

            raw_counts = await ingest_raw_data(
                resolved_start,
                resolved_end,
                user_id,
                progress_callback=raw_progress,
            )

            async def normalize_progress(current: int, total: int) -> None:
                pct = _progress_percent(50, 85, current, total)
                await emit(
                    "progress",
                    percent=pct,
                    phase="normalize_daily",
                    message=f"Normalized day {current}/{total}",
                )

            days_processed = await normalize_daily_data(
                resolved_start,
                resolved_end,
                user_id,
                progress_callback=normalize_progress,
            )

            async def tags_progress(current: int, total: int) -> None:
                pct = _progress_percent(85, 92, current, total)
                await emit(
                    "progress",
                    percent=pct,
                    phase="ingest_tags",
                    message=f"Processed tags {current}/{total}",
                )

            tags_processed = await ingest_tags(
                resolved_start,
                resolved_end,
                user_id,
                progress_callback=tags_progress,
            )

            await emit(
                "progress",
                percent=92,
                phase="features",
                message="Computing derived features",
            )
            features_processed = 0
            if days_processed > 0:
                from app.pipelines import features

                async def features_progress(current: int, total: int) -> None:
                    pct = _progress_percent(92, 99, current, total)
                    await emit(
                        "progress",
                        percent=pct,
                        phase="features",
                        message=f"Computed features {current}/{total}",
                    )

                features_processed = await features.recompute_features(
                    resolved_start,
                    resolved_end,
                    user_id,
                    progress_callback=features_progress,
                )
            else:
                await emit(
                    "progress",
                    percent=99,
                    phase="features",
                    message="No new days for feature recompute",
                )

            personal_info_ok = await ingest_personal_info(user_id)

            if days_processed == 0:
                if sync_mode == "incremental":
                    done_message = "Already up to date. No new days to sync."
                else:
                    done_message = "No Oura data found to sync yet."
            else:
                done_message = (
                    f"Ingested {days_processed} days ({sync_mode}), "
                    f"{tags_processed} tags, {features_processed} feature days"
                )

            await emit(
                "done",
                percent=100,
                message=done_message,
                extra={
                    "status": "completed",
                    "days_processed": days_processed,
                    "tags_processed": tags_processed,
                    "features_processed": features_processed,
                    "personal_info": personal_info_ok,
                    "raw_counts": raw_counts,
                    "start_date": str(resolved_start),
                    "end_date": str(resolved_end),
                    "sync_mode": sync_mode,
                },
            )
        except Exception as e:
            await emit("error", message=str(e))
        finally:
            await queue.put({"type": "_end"})

    task = asyncio.create_task(worker())
    try:
        while True:
            event = await queue.get()
            if event.get("type") == "_end":
                break
            yield event
    finally:
        if not task.done():
            task.cancel()


async def run_full_ingest(
    start_date: date | None,
    end_date: date | None,
    user_id: str,
) -> dict[str, Any]:
    """Run the full ingestion pipeline."""
    sync_mode = "manual"
    if start_date is None or end_date is None:
        start_date, end_date, sync_mode = await resolve_sync_window(user_id)

    if start_date > end_date:
        return {
            "status": "completed",
            "raw_counts": {},
            "days_processed": 0,
            "tags_processed": 0,
            "personal_info": False,
            "start_date": start_date,
            "end_date": end_date,
            "sync_mode": sync_mode,
        }

    raw_counts = await ingest_raw_data(start_date, end_date, user_id)
    days_processed = await normalize_daily_data(start_date, end_date, user_id)
    tags_processed = await ingest_tags(start_date, end_date, user_id)

    personal_info_ok = False
    try:
        personal_info_ok = await ingest_personal_info(user_id)
    except Exception:
        pass

    return {
        "status": "completed",
        "raw_counts": raw_counts,
        "days_processed": days_processed,
        "tags_processed": tags_processed,
        "personal_info": personal_info_ok,
        "start_date": start_date,
        "end_date": end_date,
        "sync_mode": sync_mode,
    }
