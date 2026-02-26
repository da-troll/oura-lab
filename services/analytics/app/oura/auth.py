"""Oura OAuth token management (multi-user)."""

import secrets
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

import httpx

from app.db import get_db_for_user, get_db_system
from app.settings import settings

# Optional Fernet encryption for tokens at rest
_fernet = None


def _get_fernet():
    global _fernet
    if _fernet is None and settings.token_encryption_key:
        from cryptography.fernet import Fernet
        _fernet = Fernet(settings.token_encryption_key.encode())
    return _fernet


def _encrypt(value: str) -> str:
    f = _get_fernet()
    if f:
        return f.encrypt(value.encode()).decode()
    return value


def _decrypt(value: str) -> str:
    f = _get_fernet()
    if f:
        return f.decrypt(value.encode()).decode()
    return value


class OAuthError(Exception):
    pass


class TokenExpiredError(OAuthError):
    pass


async def get_auth_url(user_id: str) -> tuple[str, str]:
    """Generate Oura OAuth authorization URL with single-use state bound to user."""
    state = secrets.token_urlsafe(32)

    # Store state in DB for CSRF verification
    expires_at = datetime.now(timezone.utc) + timedelta(minutes=10)
    async with get_db_system() as conn:
        await conn.execute(
            """
            INSERT INTO oauth_states (state, user_id, expires_at)
            VALUES (%s, %s, %s)
            """,
            (state, user_id, expires_at),
        )
        await conn.commit()

    params = {
        "response_type": "code",
        "client_id": settings.oura_client_id,
        "redirect_uri": settings.oura_redirect_uri,
        "scope": settings.oura_scopes,
        "state": state,
    }

    url = f"{settings.oura_auth_url}?{urlencode(params)}"
    return url, state


async def consume_oauth_state(state: str, user_id: str) -> bool:
    """Atomically consume an OAuth state (single-use + TTL + user binding).

    Returns True if state was valid and consumed, False otherwise.
    """
    async with get_db_system() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                DELETE FROM oauth_states
                WHERE state = %s AND user_id = %s AND expires_at > NOW()
                RETURNING user_id
                """,
                (state, user_id),
            )
            row = await cur.fetchone()
        await conn.commit()
    return row is not None


async def exchange_code(code: str) -> dict:
    """Exchange authorization code for tokens."""
    async with httpx.AsyncClient() as client:
        response = await client.post(
            settings.oura_token_url,
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": settings.oura_redirect_uri,
                "client_id": settings.oura_client_id,
                "client_secret": settings.oura_client_secret,
            },
        )

        if response.status_code != 200:
            raise OAuthError(f"Token exchange failed: {response.text}")

        return response.json()


async def refresh_access_token(refresh_token: str) -> dict:
    """Refresh the access token using a refresh token."""
    async with httpx.AsyncClient() as client:
        response = await client.post(
            settings.oura_token_url,
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": settings.oura_client_id,
                "client_secret": settings.oura_client_secret,
            },
        )

        if response.status_code != 200:
            raise OAuthError(f"Token refresh failed: {response.text}")

        return response.json()


async def store_tokens(tokens: dict, user_id: str) -> None:
    """Store OAuth tokens in the database (encrypted at rest)."""
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=tokens["expires_in"])

    async with get_db_for_user(user_id) as conn:
        await conn.execute(
            """
            INSERT INTO oura_auth (user_id, access_token, refresh_token, expires_at, token_type, scope)
            VALUES (%(user_id)s, %(access_token)s, %(refresh_token)s, %(expires_at)s, %(token_type)s, %(scope)s)
            ON CONFLICT (user_id) DO UPDATE SET
                access_token = EXCLUDED.access_token,
                refresh_token = EXCLUDED.refresh_token,
                expires_at = EXCLUDED.expires_at,
                token_type = EXCLUDED.token_type,
                scope = EXCLUDED.scope,
                updated_at = NOW()
            """,
            {
                "user_id": user_id,
                "access_token": _encrypt(tokens["access_token"]),
                "refresh_token": _encrypt(tokens["refresh_token"]),
                "expires_at": expires_at,
                "token_type": tokens.get("token_type", "Bearer"),
                "scope": tokens.get("scope"),
            },
        )


async def get_auth_record(user_id: str) -> dict | None:
    """Get the current auth record for a user."""
    async with get_db_for_user(user_id) as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT * FROM oura_auth WHERE user_id = %s", (user_id,)
            )
            row = await cur.fetchone()
            if not row:
                return None
            result = dict(row)
            result["access_token"] = _decrypt(result["access_token"])
            result["refresh_token"] = _decrypt(result["refresh_token"])
            return result


async def get_valid_access_token(user_id: str) -> str:
    """Get a valid access token for a user, refreshing if necessary."""
    auth = await get_auth_record(user_id)

    if not auth:
        raise OAuthError("Not connected to Oura. Please authorize first.")

    expires_at = auth["expires_at"]
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)

    buffer_time = datetime.now(timezone.utc) + timedelta(minutes=2)

    if expires_at <= buffer_time:
        try:
            new_tokens = await refresh_access_token(auth["refresh_token"])
            await store_tokens(new_tokens, user_id)
            return new_tokens["access_token"]
        except OAuthError as e:
            await clear_auth(user_id)
            raise TokenExpiredError(
                "Oura connection expired. Please reconnect."
            ) from e

    return auth["access_token"]


async def get_auth_status(user_id: str) -> dict:
    """Get current authentication status for a user."""
    auth = await get_auth_record(user_id)

    if not auth:
        return {"connected": False}

    expires_at = auth["expires_at"]
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)

    scopes = auth.get("scope", "").split() if auth.get("scope") else []

    return {
        "connected": True,
        "expires_at": expires_at.isoformat(),
        "scopes": scopes,
    }


async def clear_auth(user_id: str) -> None:
    """Clear the stored authentication for a user."""
    async with get_db_for_user(user_id) as conn:
        await conn.execute(
            "DELETE FROM oura_auth WHERE user_id = %s", (user_id,)
        )
