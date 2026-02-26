"""Oura API client with retry logic (multi-user)."""

import asyncio
from datetime import date
from typing import Any

import httpx

from app.oura.auth import get_valid_access_token
from app.settings import settings


class OuraAPIError(Exception):
    """Error from the Oura API."""

    def __init__(self, status_code: int, message: str, response_text: str = ""):
        self.status_code = status_code
        self.message = message
        self.response_text = response_text
        super().__init__(f"Oura API error ({status_code}): {message}")


class OuraRateLimitError(OuraAPIError):
    """Rate limit exceeded."""

    def __init__(self, retry_after: int = 60):
        self.retry_after = retry_after
        super().__init__(429, f"Rate limit exceeded. Retry after {retry_after}s")


class OuraClient:
    """Client for interacting with the Oura API."""

    def __init__(self):
        self.base_url = settings.oura_api_base_url
        self.max_retries = 3
        self.base_delay = 2.0

    async def _request(
        self,
        method: str,
        endpoint: str,
        user_id: str,
        params: dict[str, Any] | None = None,
        retry_count: int = 0,
    ) -> dict[str, Any]:
        """Make an authenticated request to the Oura API."""
        token = await get_valid_access_token(user_id)

        async with httpx.AsyncClient() as client:
            try:
                response = await client.request(
                    method,
                    f"{self.base_url}{endpoint}",
                    params=params,
                    headers={"Authorization": f"Bearer {token}"},
                    timeout=30.0,
                )

                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", "60"))
                    if retry_count < self.max_retries:
                        delay = min(retry_after, self.base_delay * (2**retry_count))
                        await asyncio.sleep(delay)
                        return await self._request(
                            method, endpoint, user_id, params, retry_count + 1
                        )
                    raise OuraRateLimitError(retry_after)

                if response.status_code >= 500:
                    if retry_count < self.max_retries:
                        delay = self.base_delay * (2**retry_count)
                        await asyncio.sleep(delay)
                        return await self._request(
                            method, endpoint, user_id, params, retry_count + 1
                        )
                    raise OuraAPIError(
                        response.status_code, "Server error", response.text,
                    )

                if response.status_code == 401:
                    raise OuraAPIError(
                        401, "Unauthorized - token may be invalid", response.text,
                    )

                if response.status_code >= 400:
                    raise OuraAPIError(
                        response.status_code, f"API error: {response.text}", response.text,
                    )

                return response.json()

            except httpx.TimeoutException:
                if retry_count < self.max_retries:
                    delay = self.base_delay * (2**retry_count)
                    await asyncio.sleep(delay)
                    return await self._request(
                        method, endpoint, user_id, params, retry_count + 1
                    )
                raise OuraAPIError(0, "Request timeout")

            except httpx.RequestError as e:
                if retry_count < self.max_retries:
                    delay = self.base_delay * (2**retry_count)
                    await asyncio.sleep(delay)
                    return await self._request(
                        method, endpoint, user_id, params, retry_count + 1
                    )
                raise OuraAPIError(0, f"Request failed: {e}")

    async def fetch_daily_sleep(
        self, start_date: date, end_date: date, user_id: str
    ) -> list[dict[str, Any]]:
        response = await self._request(
            "GET", "/usercollection/daily_sleep", user_id,
            params={"start_date": str(start_date), "end_date": str(end_date)},
        )
        return response.get("data", [])

    async def fetch_sleep_sessions(
        self, start_date: date, end_date: date, user_id: str
    ) -> list[dict[str, Any]]:
        response = await self._request(
            "GET", "/usercollection/sleep", user_id,
            params={"start_date": str(start_date), "end_date": str(end_date)},
        )
        return response.get("data", [])

    async def fetch_daily_readiness(
        self, start_date: date, end_date: date, user_id: str
    ) -> list[dict[str, Any]]:
        response = await self._request(
            "GET", "/usercollection/daily_readiness", user_id,
            params={"start_date": str(start_date), "end_date": str(end_date)},
        )
        return response.get("data", [])

    async def fetch_daily_activity(
        self, start_date: date, end_date: date, user_id: str
    ) -> list[dict[str, Any]]:
        response = await self._request(
            "GET", "/usercollection/daily_activity", user_id,
            params={"start_date": str(start_date), "end_date": str(end_date)},
        )
        return response.get("data", [])

    async def fetch_heart_rate(
        self, start_date: date, end_date: date, user_id: str
    ) -> list[dict[str, Any]]:
        response = await self._request(
            "GET", "/usercollection/heartrate", user_id,
            params={
                "start_datetime": f"{start_date}T00:00:00+00:00",
                "end_datetime": f"{end_date}T23:59:59+00:00",
            },
        )
        return response.get("data", [])

    async def fetch_tags(
        self, start_date: date, end_date: date, user_id: str
    ) -> list[dict[str, Any]]:
        response = await self._request(
            "GET", "/usercollection/tag", user_id,
            params={"start_date": str(start_date), "end_date": str(end_date)},
        )
        return response.get("data", [])

    async def fetch_workouts(
        self, start_date: date, end_date: date, user_id: str
    ) -> list[dict[str, Any]]:
        response = await self._request(
            "GET", "/usercollection/workout", user_id,
            params={"start_date": str(start_date), "end_date": str(end_date)},
        )
        return response.get("data", [])

    async def fetch_sessions(
        self, start_date: date, end_date: date, user_id: str
    ) -> list[dict[str, Any]]:
        response = await self._request(
            "GET", "/usercollection/session", user_id,
            params={"start_date": str(start_date), "end_date": str(end_date)},
        )
        return response.get("data", [])

    async def fetch_daily_stress(
        self, start_date: date, end_date: date, user_id: str
    ) -> list[dict[str, Any]]:
        response = await self._request(
            "GET", "/usercollection/daily_stress", user_id,
            params={"start_date": str(start_date), "end_date": str(end_date)},
        )
        return response.get("data", [])

    async def fetch_daily_spo2(
        self, start_date: date, end_date: date, user_id: str
    ) -> list[dict[str, Any]]:
        response = await self._request(
            "GET", "/usercollection/daily_spo2", user_id,
            params={"start_date": str(start_date), "end_date": str(end_date)},
        )
        return response.get("data", [])

    async def fetch_daily_cardiovascular_age(
        self, start_date: date, end_date: date, user_id: str
    ) -> list[dict[str, Any]]:
        response = await self._request(
            "GET", "/usercollection/daily_cardiovascular_age", user_id,
            params={"start_date": str(start_date), "end_date": str(end_date)},
        )
        return response.get("data", [])

    async def fetch_personal_info(self, user_id: str) -> dict[str, Any]:
        return await self._request("GET", "/usercollection/personal_info", user_id)


# Singleton instance
oura_client = OuraClient()
