"""FastAPI dependencies for authentication."""

from fastapi import HTTPException, Request

from app.auth import validate_session


async def get_current_user(request: Request) -> dict:
    """Extract and validate the session token from the Authorization header.

    Returns {user_id, email} or raises 401.
    """
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Not authenticated")

    token = auth_header[7:]
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")

    user = await validate_session(token)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid or expired session")

    return user
