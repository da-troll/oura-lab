"""Tests for chat agent endpoints."""

import pytest

from tests.conftest import auth_headers, register_and_login


async def test_chat_endpoints_require_auth(client):
    """Chat endpoints should require authentication."""
    res = await client.post("/chat", json={"message": "hello"})
    assert res.status_code == 401

    res = await client.get("/chat/conversations")
    assert res.status_code == 401

    res = await client.get("/chat/conversations/some-id")
    assert res.status_code == 401

    res = await client.delete("/chat/conversations/some-id")
    assert res.status_code == 401


async def test_chat_status_no_auth_required(client):
    """Chat status endpoint should work without auth."""
    res = await client.get("/chat/status")
    assert res.status_code == 200
    data = res.json()
    assert "enabled" in data


async def test_chat_feature_flag_disabled(client):
    """When chat_enabled is False, chat endpoints should return 403."""
    user = await register_and_login(client, "chat@example.com", "password123")
    headers = auth_headers(user["token"])

    # By default chat_enabled=False
    res = await client.post("/chat", json={"message": "test"}, headers=headers)
    assert res.status_code == 403

    res = await client.get("/chat/conversations", headers=headers)
    assert res.status_code == 403

    res = await client.delete("/chat/conversations/fake-id", headers=headers)
    assert res.status_code == 403


async def test_chat_status_returns_disabled(client):
    """Chat status should report disabled by default."""
    res = await client.get("/chat/status")
    assert res.status_code == 200
    assert res.json()["enabled"] is False
