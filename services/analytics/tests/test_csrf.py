"""Tests for backend auth validation (CSRF is enforced at BFF layer).

These tests verify that the backend correctly enforces Bearer token auth
on all protected endpoints.
"""

import pytest

from tests.conftest import auth_headers, register_and_login


async def test_bearer_token_required_on_mutations(client):
    """All mutating endpoints should require valid Bearer token."""
    mutation_endpoints = [
        ("/auth/logout", {}),
        ("/auth/oura/revoke", {}),
        ("/auth/oura/exchange", {"code": "fake_code"}),
    ]

    for url, body in mutation_endpoints:
        res = await client.post(url, json=body if body else None)
        assert res.status_code == 401, f"POST {url} should return 401 without token, got {res.status_code}"


async def test_valid_bearer_allows_access(client):
    """Valid Bearer token should grant access to protected endpoints."""
    user = await register_and_login(client, "csrf_test@example.com", "password123")
    headers = auth_headers(user["token"])

    res = await client.get("/auth/me", headers=headers)
    assert res.status_code == 200
    assert res.json()["email"] == "csrf_test@example.com"


async def test_expired_bearer_rejected(client, db_conn):
    """Expired session token should be rejected."""
    user = await register_and_login(client, "expired_session@example.com", "password123")
    headers = auth_headers(user["token"])

    # Verify access works first
    res = await client.get("/auth/me", headers=headers)
    assert res.status_code == 200

    # Manually expire all sessions for this user
    await db_conn.execute(
        "UPDATE sessions SET expires_at = NOW() - INTERVAL '1 hour' WHERE user_id = %s",
        (user["user_id"],),
    )
    await db_conn.commit()

    # Should now be rejected
    res = await client.get("/auth/me", headers=headers)
    assert res.status_code == 401
