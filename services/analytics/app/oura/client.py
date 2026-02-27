"""Oura API client with retry logic (multi-user)."""

import asyncio
from datetime import date, datetime
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

    async def _request_all_pages(
        self,
        endpoint: str,
        user_id: str,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch all paginated pages for an endpoint using next_token."""
        all_items: list[dict[str, Any]] = []
        next_token: str | None = None
        seen_tokens: set[str] = set()
        page_count = 0

        while True:
            page_count += 1
            page_params = dict(params or {})
            if next_token:
                page_params["next_token"] = next_token

            response = await self._request(
                "GET",
                endpoint,
                user_id,
                params=page_params,
            )

            data = response.get("data", [])
            if isinstance(data, list):
                all_items.extend(data)

            token = response.get("next_token")
            if not token:
                break

            # Guard against malformed pagination responses.
            if token in seen_tokens or page_count >= 1000:
                break
            seen_tokens.add(token)
            next_token = token

        return all_items

    @staticmethod
    def _extract_record_date(record: dict[str, Any]) -> date | None:
        """Extract a date from common Oura payload fields."""
        day = record.get("day")
        if isinstance(day, str):
            try:
                return date.fromisoformat(day)
            except ValueError:
                pass

        for key in ("bedtime_end", "bedtime_start", "start_datetime", "end_datetime"):
            value = record.get(key)
            if not isinstance(value, str):
                continue
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00")).date()
            except ValueError:
                continue

        return None

    async def fetch_daily_sleep(
        self, start_date: date, end_date: date, user_id: str
    ) -> list[dict[str, Any]]:
        return await self._request_all_pages(
            "/usercollection/daily_sleep",
            user_id,
            params={"start_date": str(start_date), "end_date": str(end_date)},
        )

    async def fetch_sleep_sessions(
        self, start_date: date, end_date: date, user_id: str
    ) -> list[dict[str, Any]]:
        return await self._request_all_pages(
            "/usercollection/sleep",
            user_id,
            params={"start_date": str(start_date), "end_date": str(end_date)},
        )

    async def fetch_daily_readiness(
        self, start_date: date, end_date: date, user_id: str
    ) -> list[dict[str, Any]]:
        return await self._request_all_pages(
            "/usercollection/daily_readiness",
            user_id,
            params={"start_date": str(start_date), "end_date": str(end_date)},
        )

    async def fetch_daily_activity(
        self, start_date: date, end_date: date, user_id: str
    ) -> list[dict[str, Any]]:
        return await self._request_all_pages(
            "/usercollection/daily_activity",
            user_id,
            params={"start_date": str(start_date), "end_date": str(end_date)},
        )

    async def fetch_heart_rate(
        self, start_date: date, end_date: date, user_id: str
    ) -> list[dict[str, Any]]:
        return await self._request_all_pages(
            "/usercollection/heartrate",
            user_id,
            params={
                "start_datetime": f"{start_date}T00:00:00+00:00",
                "end_datetime": f"{end_date}T23:59:59+00:00",
            },
        )

    async def fetch_tags(
        self, start_date: date, end_date: date, user_id: str
    ) -> list[dict[str, Any]]:
        return await self._request_all_pages(
            "/usercollection/tag",
            user_id,
            params={"start_date": str(start_date), "end_date": str(end_date)},
        )

    async def fetch_workouts(
        self, start_date: date, end_date: date, user_id: str
    ) -> list[dict[str, Any]]:
        return await self._request_all_pages(
            "/usercollection/workout",
            user_id,
            params={"start_date": str(start_date), "end_date": str(end_date)},
        )

    async def fetch_sessions(
        self, start_date: date, end_date: date, user_id: str
    ) -> list[dict[str, Any]]:
        return await self._request_all_pages(
            "/usercollection/session",
            user_id,
            params={"start_date": str(start_date), "end_date": str(end_date)},
        )

    async def fetch_daily_stress(
        self, start_date: date, end_date: date, user_id: str
    ) -> list[dict[str, Any]]:
        return await self._request_all_pages(
            "/usercollection/daily_stress",
            user_id,
            params={"start_date": str(start_date), "end_date": str(end_date)},
        )

    async def fetch_daily_spo2(
        self, start_date: date, end_date: date, user_id: str
    ) -> list[dict[str, Any]]:
        return await self._request_all_pages(
            "/usercollection/daily_spo2",
            user_id,
            params={"start_date": str(start_date), "end_date": str(end_date)},
        )

    async def fetch_daily_cardiovascular_age(
        self, start_date: date, end_date: date, user_id: str
    ) -> list[dict[str, Any]]:
        return await self._request_all_pages(
            "/usercollection/daily_cardiovascular_age",
            user_id,
            params={"start_date": str(start_date), "end_date": str(end_date)},
        )

    async def find_oldest_data_date(self, user_id: str) -> date | None:
        """Find the oldest available day in the user's Oura history."""
        today = date.today()
        probe_start = date(2010, 1, 1)
        earliest: date | None = None
        window_days = 180

        probe_fetchers = (
            self.fetch_daily_sleep,
            self.fetch_daily_activity,
            self.fetch_daily_readiness,
        )

        for fetcher in probe_fetchers:
            window_end = today
            while window_end >= probe_start:
                window_start = max(
                    probe_start,
                    window_end - date.resolution * (window_days - 1),
                )
                records = await fetcher(window_start, window_end, user_id)
                for record in records:
                    record_date = self._extract_record_date(record)
                    if record_date is None:
                        continue
                    if earliest is None or record_date < earliest:
                        earliest = record_date
                window_end = window_start - date.resolution
            if earliest is not None:
                break

        return earliest

    async def fetch_personal_info(self, user_id: str) -> dict[str, Any]:
        return await self._request("GET", "/usercollection/personal_info", user_id)


# Singleton instance
oura_client = OuraClient()
