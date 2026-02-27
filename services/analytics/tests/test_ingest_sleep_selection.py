"""Unit tests for sleep-session selection and day resolution."""

from app.pipelines.ingest import resolve_raw_record_day, select_primary_sleep_session


def test_select_primary_sleep_session_prefers_long_sleep():
    sessions = [
        {
            "type": "sleep",
            "total_sleep_duration": 28800,  # 8h nap-like/other record
            "bedtime_end": "2024-12-24T07:00:00+00:00",
        },
        {
            "type": "long_sleep",
            "total_sleep_duration": 25200,  # 7h main sleep
            "bedtime_end": "2024-12-24T06:00:00+00:00",
        },
    ]

    chosen = select_primary_sleep_session(sessions)
    assert chosen["type"] == "long_sleep"
    assert chosen["total_sleep_duration"] == 25200


def test_select_primary_sleep_session_falls_back_to_longest_duration():
    sessions = [
        {"type": "sleep", "total_sleep_duration": 360, "bedtime_end": "2024-12-24T05:00:00+00:00"},
        {"type": "sleep", "total_sleep_duration": 24600, "bedtime_end": "2024-12-24T07:00:00+00:00"},
    ]

    chosen = select_primary_sleep_session(sessions)
    assert chosen == {}


def test_select_primary_sleep_session_uses_untyped_records_as_legacy_fallback():
    sessions = [
        {"total_sleep_duration": 3600, "bedtime_end": "2024-12-24T04:00:00+00:00"},
        {"total_sleep_duration": 25200, "bedtime_end": "2024-12-24T07:00:00+00:00"},
    ]

    chosen = select_primary_sleep_session(sessions)
    assert chosen["total_sleep_duration"] == 25200


def test_resolve_raw_record_day_keeps_oura_day_for_sleep_when_present():
    record = {
        "day": "2024-12-24",
        "bedtime_end": "2024-12-23T23:30:00+00:00",
    }

    assert resolve_raw_record_day("sleep", record) == "2024-12-24"


def test_resolve_raw_record_day_uses_bedtime_end_fallback_for_sleep():
    record = {
        "bedtime_end": "2024-12-25T06:40:00+00:00",
    }

    assert resolve_raw_record_day("sleep", record) == "2024-12-25"
