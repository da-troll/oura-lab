"""Tests for OAuth state management and token security."""

import pytest

from app.oura.auth import consume_oauth_state, get_auth_url
from tests.conftest import auth_headers, register_and_login


async def test_oauth_state_consumed_once(client):
    """OAuth state should be single-use (replay attack prevention)."""
    user = await register_and_login(client, "oauth@example.com", "password123")

    # Generate OAuth URL (creates state in DB)
    _, state = await get_auth_url(user["user_id"])

    # First consume should succeed
    result = await consume_oauth_state(state, user["user_id"])
    assert result is True

    # Second consume should fail (already consumed)
    result = await consume_oauth_state(state, user["user_id"])
    assert result is False


async def test_oauth_state_wrong_user(client):
    """OAuth state should be bound to the user who created it."""
    user_a = await register_and_login(client, "oauth_a@example.com", "password123")
    user_b = await register_and_login(client, "oauth_b@example.com", "password123")

    # User A generates state
    _, state = await get_auth_url(user_a["user_id"])

    # User B tries to consume it
    result = await consume_oauth_state(state, user_b["user_id"])
    assert result is False

    # User A can still consume it
    result = await consume_oauth_state(state, user_a["user_id"])
    assert result is True


async def test_oauth_state_expired(client, db_conn):
    """Expired OAuth states should be rejected."""
    user = await register_and_login(client, "expired@example.com", "password123")

    _, state = await get_auth_url(user["user_id"])

    # Manually expire the state
    await db_conn.execute(
        "UPDATE oauth_states SET expires_at = NOW() - INTERVAL '1 hour' WHERE state = %s",
        (state,),
    )
    await db_conn.commit()

    # Should fail (expired)
    result = await consume_oauth_state(state, user["user_id"])
    assert result is False


async def test_oauth_status_requires_auth(client):
    """GET /auth/oura/status should require authentication."""
    res = await client.get("/auth/oura/status")
    assert res.status_code == 401


async def test_oauth_url_requires_auth(client):
    """GET /auth/oura/url should require authentication."""
    res = await client.get("/auth/oura/url")
    assert res.status_code == 401


async def test_oauth_revoke_requires_auth(client):
    """POST /auth/oura/revoke should require authentication."""
    res = await client.post("/auth/oura/revoke")
    assert res.status_code == 401
