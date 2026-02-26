"""Database connection and utilities."""

from contextlib import asynccontextmanager
from typing import AsyncGenerator

import psycopg
from psycopg.rows import dict_row

from app.settings import settings


@asynccontextmanager
async def get_db_system():
    """Context manager for system-level database operations.

    Used for pre-auth operations: register, login, session validation,
    cleanup, migrations. No RLS context is set.
    """
    async with await psycopg.AsyncConnection.connect(
        settings.database_url,
        row_factory=dict_row,
    ) as conn:
        yield conn


@asynccontextmanager
async def get_db_for_user(user_id: str):
    """Context manager for user-scoped database operations.

    Sets SET LOCAL app.current_user_id for RLS enforcement.
    The connection stays in a single transaction for the entire request.

    Args:
        user_id: UUID string of the authenticated user
    """
    async with await psycopg.AsyncConnection.connect(
        settings.database_url,
        row_factory=dict_row,
        autocommit=False,
    ) as conn:
        async with conn.transaction():
            await conn.execute(
                "SET LOCAL app.current_user_id = %s", (str(user_id),)
            )
            yield conn


# Legacy alias — banned in request handlers (use get_db_for_user or get_db_system)
@asynccontextmanager
async def get_db():
    """Legacy context manager. Do not use in request handlers."""
    async with await psycopg.AsyncConnection.connect(
        settings.database_url,
        row_factory=dict_row,
    ) as conn:
        yield conn
