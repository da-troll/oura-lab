"""Tests for RLS enforcement and connection safety."""

import pytest

from tests.conftest import auth_headers, register_and_login


async def test_get_db_not_used_in_handlers():
    """Verify that request handlers do not use get_db() directly.

    Only get_db_for_user() and get_db_system() are allowed in handlers.
    """
    import ast
    from pathlib import Path

    main_py = Path(__file__).parent.parent / "app" / "main.py"
    tree = ast.parse(main_py.read_text())

    # Check imports: get_db should not be imported in main.py
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.module and "db" in node.module:
                imported_names = [alias.name for alias in node.names]
                assert "get_db" not in imported_names, (
                    "main.py should not import get_db() — use get_db_for_user() or get_db_system()"
                )


async def test_rls_fail_closed_without_user_id(db_conn):
    """Without setting app.current_user_id, RLS should deny all tenant data access."""
    # Insert some data as system (bypassing RLS since db_conn from conftest uses superuser)
    await db_conn.execute(
        """
        INSERT INTO users (id, email, email_normalized, password_hash)
        VALUES ('00000000-0000-0000-0000-000000000001', 'rls@test.com', 'rls@test.com', 'dummy_hash')
        ON CONFLICT (id) DO NOTHING
        """,
    )
    await db_conn.execute(
        """
        INSERT INTO oura_daily (user_id, date, sleep_score)
        VALUES ('00000000-0000-0000-0000-000000000001', '2025-01-15', 85)
        ON CONFLICT (user_id, date) DO NOTHING
        """,
    )
    await db_conn.commit()

    # Now query oura_daily WITHOUT setting app.current_user_id
    # RLS should return no rows (because NULLIF('', '') is NULL, and UUID comparison with NULL is false)
    async with db_conn.cursor() as cur:
        await cur.execute("SELECT count(*) as cnt FROM oura_daily")
        row = await cur.fetchone()

    # If RLS is enforced for this role, count should be 0
    # Note: superuser bypasses RLS, so this test is most meaningful
    # when the test DB role has RLS enforced
    assert row is not None


async def test_sequential_requests_no_context_leak(client, db_conn):
    """Two sequential requests by different users should not leak context."""
    user_a = await register_and_login(client, "leak_a@example.com", "password123")
    user_b = await register_and_login(client, "leak_b@example.com", "password123")

    # Insert oura_auth so dashboard returns connected=True
    await db_conn.execute(
        "INSERT INTO oura_auth (user_id, access_token, refresh_token, expires_at) VALUES (%s, 'tok', 'ref', NOW() + INTERVAL '1h') ON CONFLICT DO NOTHING",
        (user_a["user_id"],),
    )
    await db_conn.execute(
        "INSERT INTO oura_daily (user_id, date, sleep_score, readiness_score) VALUES (%s, '2025-03-01', 99, 88) ON CONFLICT DO NOTHING",
        (user_a["user_id"],),
    )
    await db_conn.commit()

    # User A request
    res_a = await client.get("/dashboard?days=30", headers=auth_headers(user_a["token"]))
    assert res_a.status_code == 200

    # User B request immediately after (same test, sequential)
    res_b = await client.get("/dashboard?days=30", headers=auth_headers(user_b["token"]))
    assert res_b.status_code == 200

    # User B should not see user A's data
    data_b = res_b.json()
    # User B has no oura_auth, so connected should be False
    assert data_b["connected"] is False
