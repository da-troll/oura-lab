"""Tests for streaming sync progress endpoint."""

import json

from tests.conftest import auth_headers, register_and_login


async def test_sync_stream_requires_auth(client):
    """Streaming sync should require authentication."""
    res = await client.post("/admin/ingest/stream")
    assert res.status_code == 401


async def test_sync_stream_requires_both_dates_or_neither(client):
    """Providing only one date should fail validation."""
    user = await register_and_login(client, "sync_stream_dates@example.com", "password123")
    res = await client.post(
        "/admin/ingest/stream?start=2025-01-01",
        headers=auth_headers(user["token"]),
    )
    assert res.status_code == 400
    assert "both start and end" in res.json()["detail"]


async def test_sync_stream_emits_ndjson_events(client, monkeypatch):
    """Streaming sync should emit progress and done NDJSON events."""
    user = await register_and_login(client, "sync_stream_events@example.com", "password123")

    async def fake_stream(start, end, user_id):
        assert user_id == user["user_id"]
        yield {
            "type": "progress",
            "percent": 10,
            "phase": "fetch_raw",
            "message": "Fetching daily_sleep",
        }
        yield {
            "type": "done",
            "status": "completed",
            "percent": 100,
            "days_processed": 2,
            "message": "Done",
            "sync_mode": "incremental",
            "start_date": "2025-01-01",
            "end_date": "2025-01-02",
        }

    monkeypatch.setattr("app.main.ingest.run_full_ingest_stream", fake_stream)

    res = await client.post("/admin/ingest/stream", headers=auth_headers(user["token"]))
    assert res.status_code == 200
    assert "application/x-ndjson" in res.headers.get("content-type", "")

    lines = [line for line in res.text.splitlines() if line.strip()]
    assert len(lines) == 2
    events = [json.loads(line) for line in lines]
    assert events[0]["type"] == "progress"
    assert events[1]["type"] == "done"
    assert events[1]["percent"] == 100
