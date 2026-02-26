"""Ingestion pipeline: Oura API -> raw -> daily tables (multi-user)."""

import json
from datetime import date, datetime, timedelta, timezone
from typing import Any

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


async def ingest_raw_data(
    start_date: date,
    end_date: date,
    user_id: str,
    data_types: list[str] | None = None,
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
        for data_type in data_types:
            if data_type not in fetch_map:
                continue

            fetch_fn = fetch_map[data_type]
            try:
                records = await fetch_fn(start_date, end_date, user_id)
            except Exception:
                counts[data_type] = 0
                continue
            counts[data_type] = len(records)

            for record in records:
                day = record.get("day")
                if data_type == "sleep":
                    resolved_day = resolve_sleep_day(record)
                    day = str(resolved_day) if resolved_day else day

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

    return counts


async def normalize_daily_data(start_date: date, end_date: date, user_id: str) -> int:
    """Normalize raw data into oura_daily table."""
    days_processed = 0
    current = start_date

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
                    "SELECT payload FROM oura_raw WHERE source = 'sleep' AND day = %(day)s AND user_id = %(uid)s ORDER BY fetched_at DESC LIMIT 1",
                    {"day": str(current), "uid": user_id},
                )
                sleep_session_row = await cur.fetchone()
                sleep_session_data = sleep_session_row["payload"] if sleep_session_row else {}

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

    return days_processed


async def ingest_tags(start_date: date, end_date: date, user_id: str) -> int:
    """Normalize tags from raw data into oura_day_tags table."""
    tags_processed = 0

    async with get_db_for_user(user_id) as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT day, payload FROM oura_raw WHERE source = 'tag' AND day >= %(start)s AND day <= %(end)s AND user_id = %(uid)s",
                {"start": str(start_date), "end": str(end_date), "uid": user_id},
            )
            tag_rows = await cur.fetchall()

        for row in tag_rows:
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


async def run_full_ingest(start_date: date, end_date: date, user_id: str) -> dict[str, Any]:
    """Run the full ingestion pipeline."""
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
    }
