"""Shared test fixtures."""

import asyncio
import os
from typing import AsyncGenerator

import psycopg
import pytest
import pytest_asyncio
from psycopg.rows import dict_row
from httpx import ASGITransport, AsyncClient

# Use test database
os.environ.setdefault(
    "DATABASE_URL",
    os.environ.get("DATABASE_URL_TEST", "postgresql://postgres:postgres@localhost:5433/oura_test"),
)
os.environ["ENABLE_AUTO_MIGRATE"] = "true"

from app.main import app
from app.settings import settings


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture
async def client() -> AsyncGenerator[AsyncClient, None]:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest_asyncio.fixture
async def db_conn() -> AsyncGenerator[psycopg.AsyncConnection, None]:
    async with await psycopg.AsyncConnection.connect(
        settings.database_url, row_factory=dict_row
    ) as conn:
        yield conn


async def register_and_login(client: AsyncClient, email: str = "test@example.com", password: str = "testpass123") -> dict:
    """Helper: register a user and return {user_id, email, token}."""
    res = await client.post("/auth/register", json={"email": email, "password": password})
    assert res.status_code == 200
    data = res.json()
    return {
        "user_id": data["user_id"],
        "email": data["email"],
        "token": data["session_token"],
    }


def auth_headers(token: str) -> dict:
    """Helper: return Authorization header dict."""
    return {"Authorization": f"Bearer {token}"}
