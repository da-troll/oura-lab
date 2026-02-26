"""User authentication: registration, login, session management."""

import hashlib
import secrets
from datetime import datetime, timedelta, timezone

from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError

from app.db import get_db_system
from app.settings import settings

ph = PasswordHasher()


def hash_password(password: str) -> str:
    return ph.hash(password)


def verify_password(password: str, password_hash: str) -> bool:
    try:
        return ph.verify(password_hash, password)
    except VerifyMismatchError:
        return False


def normalize_email(email: str) -> str:
    return email.strip().lower()


def validate_password(password: str) -> str | None:
    """Returns error message if invalid, None if ok."""
    if len(password) < 8:
        return "Password must be at least 8 characters"
    return None


async def create_user(email: str, password: str) -> dict:
    """Create a new user. Returns {id, email}."""
    email_norm = normalize_email(email)
    pw_hash = hash_password(password)

    async with get_db_system() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO users (email, email_normalized, password_hash)
                VALUES (%(email)s, %(email_normalized)s, %(password_hash)s)
                RETURNING id, email
                """,
                {
                    "email": email.strip(),
                    "email_normalized": email_norm,
                    "password_hash": pw_hash,
                },
            )
            row = await cur.fetchone()
        await conn.commit()

    return {"id": str(row["id"]), "email": row["email"]}


async def authenticate_user(email: str, password: str) -> dict | None:
    """Authenticate user by email and password. Returns {id, email} or None."""
    email_norm = normalize_email(email)

    async with get_db_system() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT id, email, password_hash FROM users WHERE email_normalized = %s",
                (email_norm,),
            )
            row = await cur.fetchone()

    if not row:
        # Perform dummy hash to prevent timing attacks
        ph.hash("dummy_password_for_timing")
        return None

    if not verify_password(password, row["password_hash"]):
        return None

    return {"id": str(row["id"]), "email": row["email"]}


async def create_session(
    user_id: str, ip: str | None = None, user_agent: str | None = None
) -> dict:
    """Create a new session. Returns {token, expires_at}."""
    token = secrets.token_urlsafe(32)
    token_hash = hashlib.sha256(token.encode()).hexdigest()
    expires_at = datetime.now(timezone.utc) + timedelta(
        hours=settings.session_max_age_hours
    )

    async with get_db_system() as conn:
        await conn.execute(
            """
            INSERT INTO sessions (user_id, token_hash, expires_at, ip, user_agent)
            VALUES (%(user_id)s, %(token_hash)s, %(expires_at)s, %(ip)s, %(user_agent)s)
            """,
            {
                "user_id": user_id,
                "token_hash": token_hash,
                "expires_at": expires_at,
                "ip": ip,
                "user_agent": user_agent,
            },
        )
        await conn.commit()

    return {"token": token, "expires_at": expires_at}


async def validate_session(token: str) -> dict | None:
    """Validate a session token. Returns {user_id, email} or None.

    Throttles last_seen_at updates to every 5 minutes.
    """
    token_hash = hashlib.sha256(token.encode()).hexdigest()

    async with get_db_system() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT s.id as session_id, s.user_id, s.last_seen_at, u.email
                FROM sessions s
                JOIN users u ON s.user_id = u.id
                WHERE s.token_hash = %s AND s.expires_at > NOW()
                """,
                (token_hash,),
            )
            row = await cur.fetchone()

        if not row:
            return None

        # Throttled last_seen_at update (every 5 min)
        last_seen = row["last_seen_at"]
        if last_seen.tzinfo is None:
            last_seen = last_seen.replace(tzinfo=timezone.utc)
        if datetime.now(timezone.utc) - last_seen > timedelta(minutes=5):
            await conn.execute(
                "UPDATE sessions SET last_seen_at = NOW() WHERE id = %s",
                (row["session_id"],),
            )
            await conn.commit()

    return {"user_id": str(row["user_id"]), "email": row["email"]}


async def delete_session(token: str) -> None:
    """Delete a session by raw token."""
    token_hash = hashlib.sha256(token.encode()).hexdigest()

    async with get_db_system() as conn:
        await conn.execute(
            "DELETE FROM sessions WHERE token_hash = %s", (token_hash,)
        )
        await conn.commit()


async def invalidate_user_sessions(user_id: str) -> None:
    """Delete all sessions for a user (e.g., on password change)."""
    async with get_db_system() as conn:
        await conn.execute(
            "DELETE FROM sessions WHERE user_id = %s", (user_id,)
        )
        await conn.commit()


async def cleanup_expired_sessions() -> int:
    """Delete expired sessions and oauth states. Returns count deleted."""
    async with get_db_system() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "DELETE FROM sessions WHERE expires_at < NOW()"
            )
            sessions_deleted = cur.rowcount or 0
            await cur.execute(
                "DELETE FROM oauth_states WHERE expires_at < NOW()"
            )
        await conn.commit()

    return sessions_deleted
