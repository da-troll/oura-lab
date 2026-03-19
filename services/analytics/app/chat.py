"""AI chat agent with typed tool functions for health data analysis."""

import asyncio
import hashlib
import json
import logging
import math
import re
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, AsyncGenerator

from openai import AsyncOpenAI

from app.db import get_db_for_user
from app.settings import settings

try:
    from redis import asyncio as redis_asyncio
except Exception:  # pragma: no cover - optional dependency
    redis_asyncio = None

logger = logging.getLogger(__name__)

SYSTEM_PROMPT_PATH = Path(__file__).parent / "prompts" / "system_prompt.md"
SYSTEM_PROMPT = ""


def initialize_system_prompt() -> None:
    """Load system prompt once at startup."""
    global SYSTEM_PROMPT

    if not SYSTEM_PROMPT_PATH.exists():
        raise RuntimeError(f"System prompt file not found: {SYSTEM_PROMPT_PATH}")

    prompt = SYSTEM_PROMPT_PATH.read_text(encoding="utf-8").strip()
    if not prompt:
        raise RuntimeError(f"System prompt file is empty: {SYSTEM_PROMPT_PATH}")

    SYSTEM_PROMPT = prompt

INTRO_SENTINEL = "__OURALIE_INTRO__"
DEFAULT_LOOKBACK_DAYS = 30
FOLLOW_UP_QUESTION = "Would you like a different time period, chart type, or another edit?"
FOLLOW_UP_QUESTION_ITALIC = f"*{FOLLOW_UP_QUESTION}*"

METRIC_LABELS = {
    "sleep_score": "Sleep Score",
    "readiness_score": "Readiness Score",
    "activity_score": "Activity Score",
    "steps": "Steps",
    "hrv_average": "HRV Average",
    "hr_lowest": "Resting Heart Rate",
    "sleep_total_seconds": "Sleep Duration",
    "sleep_deep_seconds": "Deep Sleep",
    "sleep_rem_seconds": "REM Sleep",
    "sleep_efficiency": "Sleep Efficiency",
    "cal_total": "Total Calories",
    "cal_active": "Active Calories",
    "stress_high_minutes": "Stress High Minutes",
    "recovery_high_minutes": "Recovery High Minutes",
    "spo2_average": "SpO2",
    "workout_total_minutes": "Workout Minutes",
}

CHART_COLORS = {
    "sleep_score": "#ec4899",
    "readiness_score": "#2563eb",
    "activity_score": "#16a34a",
    "steps": "#ea580c",
    "hrv_average": "#0891b2",
    "hr_lowest": "#dc2626",
    "sleep_total_seconds": "#8b5cf6",
    "sleep_deep_seconds": "#6366f1",
    "sleep_rem_seconds": "#a855f7",
    "sleep_efficiency": "#7c3aed",
    "cal_total": "#f97316",
    "cal_active": "#fb923c",
    "stress_high_minutes": "#f59e0b",
    "recovery_high_minutes": "#10b981",
    "spo2_average": "#6366f1",
    "workout_total_minutes": "#f97316",
}

SECONDS_METRICS = {"sleep_total_seconds", "sleep_deep_seconds", "sleep_rem_seconds"}
MINUTES_METRICS = {"stress_high_minutes", "recovery_high_minutes", "workout_total_minutes"}
ALLOWED_METRICS = set(METRIC_LABELS.keys())
METRIC_ALIASES = {
    "readiness": "readiness_score",
    "sleep": "sleep_score",
    "activity": "activity_score",
    "rhr": "hr_lowest",
    "resting_heart_rate": "hr_lowest",
    "resting_hr": "hr_lowest",
    "heart_rate": "hr_lowest",
    "hrv": "hrv_average",
    "sleep_hours": "sleep_total_seconds",
    "sleep_duration": "sleep_total_seconds",
    "sleep_total": "sleep_total_seconds",
    "deep_sleep": "sleep_deep_seconds",
    "rem_sleep": "sleep_rem_seconds",
    "stress_minutes": "stress_high_minutes",
    "recovery_minutes": "recovery_high_minutes",
    "workout_minutes": "workout_total_minutes",
    "spo2": "spo2_average",
}
ALLOWED_SERIES_CHART_TYPES = {"line", "bar", "area", "histogram"}
ALLOWED_SLEEP_ARCH_CHART_TYPES = {"stacked_bar", "stacked_area"}


def _resolve_series_chart_type(chart_type: str | None) -> str:
    normalized = (chart_type or "line").strip().lower()
    return normalized if normalized in ALLOWED_SERIES_CHART_TYPES else "line"


def _resolve_sleep_arch_chart_type(chart_type: str | None) -> str:
    normalized = (chart_type or "stacked_bar").strip().lower()
    return normalized if normalized in ALLOWED_SLEEP_ARCH_CHART_TYPES else "stacked_bar"


def _build_histogram_buckets(values: list[float], bins: int = 10) -> list[dict[str, Any]]:
    if not values:
        return []

    bins = max(5, min(int(bins), 40))
    min_value = min(values)
    max_value = max(values)

    if min_value == max_value:
        return [{
            "label": f"{min_value:.2f}",
            "bin_start": round(min_value, 4),
            "bin_end": round(max_value, 4),
            "count": len(values),
        }]

    bucket_size = (max_value - min_value) / bins
    counts = [0] * bins

    for value in values:
        idx = int((value - min_value) / bucket_size)
        if idx >= bins:
            idx = bins - 1
        counts[idx] += 1

    histogram = []
    for idx, count in enumerate(counts):
        start = min_value + idx * bucket_size
        end = start + bucket_size
        histogram.append({
            "label": f"{start:.2f}–{end:.2f}",
            "bin_start": round(start, 4),
            "bin_end": round(end, 4),
            "count": count,
        })

    return histogram


def _metric_label(metric: str) -> str:
    return METRIC_LABELS.get(metric, metric.replace("_", " ").title())


def _metric_unit(metric: str) -> str:
    if metric in SECONDS_METRICS:
        return "h"
    if metric in MINUTES_METRICS:
        return " min"
    if metric == "spo2_average":
        return "%"
    if metric == "hrv_average":
        return " ms"
    if metric == "hr_lowest":
        return " bpm"
    return ""


def _normalize_metric_value(metric: str, value: float | None) -> float | None:
    if value is None:
        return None
    numeric_value = float(value)
    if metric in SECONDS_METRICS:
        return round(numeric_value / 3600.0, 2)
    return round(numeric_value, 2)


def _normalize_metric_key(metric: str) -> str:
    normalized = metric.strip().lower().replace("-", "_").replace(" ", "_")
    while "__" in normalized:
        normalized = normalized.replace("__", "_")
    return normalized


def _canonicalize_metric(metric: str | None) -> str | None:
    if not metric:
        return None
    normalized = _normalize_metric_key(metric)
    if normalized in ALLOWED_METRICS:
        return normalized
    return METRIC_ALIASES.get(normalized)


def _resolve_date_window(
    start_date: str | None,
    end_date: str | None,
    lookback_days: int | None = None,
) -> tuple[date, date]:
    if (start_date is None) != (end_date is None):
        raise ValueError("Provide both start_date and end_date, or neither.")

    if start_date and end_date:
        try:
            start = date.fromisoformat(start_date)
            end = date.fromisoformat(end_date)
        except ValueError as exc:
            raise ValueError("Dates must be in YYYY-MM-DD format.") from exc
    else:
        try:
            days = int(lookback_days or DEFAULT_LOOKBACK_DAYS)
        except (TypeError, ValueError) as exc:
            raise ValueError("lookback_days must be an integer.") from exc
        if days < 1 or days > 3660:
            raise ValueError("lookback_days must be between 1 and 3660.")
        end = date.today()
        start = end - timedelta(days=days - 1)

    if start > end:
        raise ValueError("start_date cannot be after end_date.")

    return start, end


def _ensure_follow_up_question(content: str) -> str:
    stripped = content.strip()
    if not stripped:
        return stripped
    if FOLLOW_UP_QUESTION_ITALIC in stripped:
        return stripped
    follow_up_pattern = re.compile(re.escape(FOLLOW_UP_QUESTION), flags=re.IGNORECASE)
    if follow_up_pattern.search(stripped):
        return follow_up_pattern.sub(FOLLOW_UP_QUESTION_ITALIC, stripped, count=1)
    return f"{stripped}\n\n{FOLLOW_UP_QUESTION_ITALIC}"


def _sanitize_markdown_images(content: str) -> str:
    """Remove markdown/HTML image embeds from model output.

    Charts are rendered via artifact events; image markdown often produces broken icons.
    """
    if not content:
        return content

    # ![alt](url) -> alt
    content = re.sub(
        r"!\[([^\]]*)\]\(([^)]*)\)",
        lambda match: (match.group(1) or "Image").strip(),
        content,
    )
    # ![alt][ref] -> alt
    content = re.sub(
        r"!\[([^\]]*)\]\[[^\]]*\]",
        lambda match: (match.group(1) or "Image").strip(),
        content,
    )
    # Strip raw HTML img tags
    content = re.sub(r"<img\b[^>]*>", "", content, flags=re.IGNORECASE)
    return content


def _estimate_tokens_text(text: str) -> int:
    if not text:
        return 0
    # Lightweight approximation used for budgeting and telemetry.
    return max(1, math.ceil(len(text) / 4))


def _estimate_message_tokens(message: dict[str, Any]) -> int:
    base = 6
    content = message.get("content")
    if isinstance(content, str):
        base += _estimate_tokens_text(content)
    tool_calls = message.get("tool_calls")
    if tool_calls:
        base += _estimate_tokens_text(json.dumps(tool_calls, separators=(",", ":")))
    return base


def _estimate_messages_tokens(messages: list[dict[str, Any]]) -> int:
    return sum(_estimate_message_tokens(message) for message in messages)


def _parse_summary_state(raw: str | None) -> tuple[str, datetime | None]:
    if not raw:
        return "", None
    text = raw.strip()
    if not text:
        return "", None
    try:
        payload = json.loads(text)
        if isinstance(payload, dict):
            summary = str(payload.get("summary") or "").strip()
            up_to_raw = payload.get("up_to_created_at")
            if isinstance(up_to_raw, str) and up_to_raw:
                try:
                    up_to = datetime.fromisoformat(up_to_raw)
                except ValueError:
                    up_to = None
            else:
                up_to = None
            return summary, up_to
    except (TypeError, json.JSONDecodeError):
        pass
    # Backward compatibility for plain-text summaries.
    return text, None


def _serialize_summary_state(summary: str, up_to_created_at: datetime | None) -> str:
    payload: dict[str, Any] = {"summary": summary.strip()}
    if up_to_created_at:
        payload["up_to_created_at"] = up_to_created_at.isoformat()
    return json.dumps(payload, separators=(",", ":"))


def _truncate_text(value: str, max_chars: int) -> str:
    if len(value) <= max_chars:
        return value
    keep = max(0, max_chars - 13)
    return f"{value[:keep]}... [trimmed]"


def _json_hash(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:16]


def _compact_numeric_stats(rows: list[dict[str, Any]]) -> dict[str, dict[str, float]] | None:
    stats: dict[str, dict[str, float]] = {}
    if not rows:
        return None

    numeric_keys: dict[str, list[float]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        for key, raw in row.items():
            if isinstance(raw, bool):
                continue
            if isinstance(raw, (int, float)):
                numeric_keys.setdefault(str(key), []).append(float(raw))

    for key, values in numeric_keys.items():
        if not values:
            continue
        stats[key] = {
            "min": round(min(values), 4),
            "max": round(max(values), 4),
            "avg": round(sum(values) / len(values), 4),
        }

    return stats or None


def _compact_list_field(items: list[Any], max_sample: int = 2) -> dict[str, Any]:
    safe_items = [item for item in items if item is not None]
    if not safe_items:
        return {"count": 0}

    payload: dict[str, Any] = {"count": len(safe_items)}
    if isinstance(safe_items[0], dict):
        dict_rows = [item for item in safe_items if isinstance(item, dict)]
        payload["stats"] = _compact_numeric_stats(dict_rows)
        payload["sample_first"] = dict_rows[:max_sample]
        payload["sample_last"] = dict_rows[-max_sample:]

        # Optional trend direction for common numeric keys.
        for key in ("value", "rho", "x", "y", "magnitude", "count"):
            series = [float(row[key]) for row in dict_rows if isinstance(row.get(key), (int, float))]
            if len(series) >= 2:
                delta = series[-1] - series[0]
                payload["trend"] = {
                    "key": key,
                    "direction": "up" if delta > 0 else "down" if delta < 0 else "flat",
                    "delta": round(delta, 4),
                }
                break
    else:
        payload["sample_first"] = safe_items[:max_sample]
        payload["sample_last"] = safe_items[-max_sample:]
    return payload


def _compact_tool_result_for_context(tool_name: str, raw_result: str, max_chars: int) -> tuple[str, int]:
    compacted: str
    try:
        parsed = json.loads(raw_result)
    except (TypeError, json.JSONDecodeError):
        compacted = _truncate_text(raw_result, max_chars)
        saved = max(0, _estimate_tokens_text(raw_result) - _estimate_tokens_text(compacted))
        return compacted, saved

    payload: dict[str, Any] = {
        "tool": tool_name,
        "reference_id": _json_hash(parsed),
    }
    if isinstance(parsed, dict):
        scalar_keys = (
            "metric",
            "metrics",
            "target",
            "period",
            "count",
            "days_with_data",
            "chronotype",
            "social_jetlag_minutes",
            "error",
            "message",
        )
        for key in scalar_keys:
            if key in parsed:
                payload[key] = parsed[key]

        list_keys = (
            "data",
            "correlations",
            "anomalies",
            "change_points",
            "points",
            "bins",
            "weeks",
            "lags",
        )
        for key in list_keys:
            raw_items = parsed.get(key)
            if isinstance(raw_items, list):
                payload[key] = _compact_list_field(raw_items)

        if "data" not in payload and isinstance(parsed.get("data"), dict):
            payload["data"] = parsed["data"]
    elif isinstance(parsed, list):
        payload["items"] = _compact_list_field(parsed)
    else:
        payload["value"] = parsed

    compacted = json.dumps(payload, separators=(",", ":"), default=str)
    compacted = _truncate_text(compacted, max_chars)
    saved_tokens = max(0, _estimate_tokens_text(raw_result) - _estimate_tokens_text(compacted))
    return compacted, saved_tokens


def _build_context_from_history(
    *,
    base_messages: list[dict[str, Any]],
    history_messages: list[dict[str, Any]],
    budget_tokens: int,
    min_recent_messages: int,
) -> tuple[list[dict[str, Any]], int, int]:
    if not history_messages:
        base_tokens = _estimate_messages_tokens(base_messages)
        return list(base_messages), base_tokens, 0

    keep_recent = max(0, min(min_recent_messages, len(history_messages)))
    recent = history_messages[-keep_recent:] if keep_recent else []
    recent_tokens = _estimate_messages_tokens(recent)

    context = list(base_messages)
    total_tokens = _estimate_messages_tokens(context) + recent_tokens

    selected_older: list[dict[str, Any]] = []
    idx = len(history_messages) - keep_recent - 1
    while idx >= 0:
        candidate = history_messages[idx]
        candidate_tokens = _estimate_message_tokens(candidate)
        if total_tokens + candidate_tokens > budget_tokens:
            break
        selected_older.append(candidate)
        total_tokens += candidate_tokens
        idx -= 1

    selected_older.reverse()
    context.extend(selected_older)
    context.extend(recent)

    omitted_messages = max(0, idx + 1)
    return context, total_tokens, omitted_messages


def _render_messages_for_summary(rows: list[dict[str, Any]]) -> str:
    lines: list[str] = []
    for row in rows:
        role = str(row.get("role", "unknown"))
        content = str(row.get("content") or "").strip()
        if not content:
            continue
        lines.append(f"{role}: {content}")
    return "\n".join(lines)


def _normalize_memory_content(content: str) -> str:
    normalized = content.strip().lower()
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized


def _vector_literal(vector: list[float]) -> str:
    return "[" + ",".join(f"{value:.8f}" for value in vector) + "]"


def _cosine_similarity(vec_a: list[float], vec_b: list[float]) -> float:
    if not vec_a or not vec_b or len(vec_a) != len(vec_b):
        return 0.0
    dot = sum(a * b for a, b in zip(vec_a, vec_b))
    mag_a = math.sqrt(sum(a * a for a in vec_a))
    mag_b = math.sqrt(sum(b * b for b in vec_b))
    if mag_a == 0 or mag_b == 0:
        return 0.0
    return dot / (mag_a * mag_b)


def _build_memory_prompt_block(memories: list[dict[str, Any]], max_tokens: int) -> tuple[str, int]:
    if not memories:
        return "", 0

    lines = ["Relevant long-term memory (high confidence):"]
    for item in memories:
        memory_type = item.get("memory_type", "episodic")
        content = str(item.get("content") or "").strip()
        if not content:
            continue
        confidence = item.get("confidence")
        score = f" ({float(confidence):.2f})" if isinstance(confidence, (int, float)) else ""
        candidate_line = f"- [{memory_type}] {content}{score}"
        next_tokens = _estimate_tokens_text("\n".join(lines + [candidate_line]))
        if next_tokens > max_tokens:
            break
        lines.append(candidate_line)

    if len(lines) == 1:
        return "", 0
    block = "\n".join(lines)
    return block, _estimate_tokens_text(block)


class _ChatCache:
    def __init__(self):
        self._redis = None
        self._redis_init_done = False
        self._init_lock = asyncio.Lock()
        self._local: dict[str, tuple[float, Any]] = {}

    async def _get_redis(self):
        if not settings.chat_redis_cache_enabled or not settings.redis_url or redis_asyncio is None:
            return None
        if self._redis_init_done:
            return self._redis

        async with self._init_lock:
            if self._redis_init_done:
                return self._redis
            try:
                client = redis_asyncio.from_url(settings.redis_url, decode_responses=True)
                await client.ping()
                self._redis = client
            except Exception:
                logger.warning("Redis unavailable for chat cache; using local fallback", exc_info=True)
                self._redis = None
            finally:
                self._redis_init_done = True
        return self._redis

    async def get_json(self, key: str) -> Any | None:
        now = time.time()
        local_entry = self._local.get(key)
        if local_entry:
            expires_at, value = local_entry
            if expires_at >= now:
                return value
            self._local.pop(key, None)

        client = await self._get_redis()
        if not client:
            return None
        try:
            raw = await client.get(key)
            if raw is None:
                return None
            return json.loads(raw)
        except Exception:
            logger.warning("Redis get failed for key=%s", key, exc_info=True)
            return None

    async def set_json(self, key: str, value: Any, ttl_seconds: int) -> None:
        expires_at = time.time() + max(1, ttl_seconds)
        self._local[key] = (expires_at, value)

        client = await self._get_redis()
        if not client:
            return
        try:
            await client.set(key, json.dumps(value, separators=(",", ":"), default=str), ex=ttl_seconds)
        except Exception:
            logger.warning("Redis set failed for key=%s", key, exc_info=True)

    async def delete_prefix(self, prefix: str) -> None:
        for key in list(self._local.keys()):
            if key.startswith(prefix):
                self._local.pop(key, None)

        client = await self._get_redis()
        if not client:
            return
        try:
            cursor = 0
            pattern = f"{prefix}*"
            while True:
                cursor, keys = await client.scan(cursor=cursor, match=pattern, count=200)
                if keys:
                    await client.delete(*keys)
                if cursor == 0:
                    break
        except Exception:
            logger.warning("Redis delete_prefix failed for prefix=%s", prefix, exc_info=True)


_chat_cache = _ChatCache()


def _tool_cache_key(user_id: str, tool_name: str, args: dict[str, Any]) -> str:
    return f"chat:tool:{user_id}:{tool_name}:{_json_hash(args)}"


def _embedding_cache_key(user_id: str, text: str) -> str:
    return f"chat:embedding:{user_id}:{_json_hash(text)}"


def _session_context_key(user_id: str, conversation_id: str, history_hash: str) -> str:
    return f"chat:session-context:{user_id}:{conversation_id}:{history_hash}"


def _user_cache_prefix(user_id: str) -> str:
    return f"chat:{user_id}:"


def _build_chart_payload(tool_name: str, args: dict, raw_result: str) -> dict[str, Any] | None:
    """Build a UI-ready chart artifact from a tool result."""
    try:
        parsed = json.loads(raw_result)
    except (TypeError, json.JSONDecodeError):
        return None

    if not isinstance(parsed, dict) or parsed.get("error"):
        return None

    if tool_name == "get_summary":
        # Score-based metrics (already 0-100 scale)
        score_keys = [
            ("readiness_avg", "Readiness"),
            ("sleep_avg", "Sleep"),
            ("activity_avg", "Activity"),
        ]
        # Metrics with different scales — normalize to 0-100 for radar
        # (lo, hi, invert): invert=True means lower raw value = higher score
        scaled_keys = [
            ("steps_avg", "Steps", 0, 15000, False),
            ("hrv_avg", "HRV", 0, 100, False),
            ("recovery_avg", "Recovery", 0, 120, False),
        ]

        bar_data = []
        radar_data = []
        for key, label in score_keys:
            value = parsed.get(key)
            if value is None:
                continue
            v = round(float(value), 1)
            bar_data.append({"metric": label, "value": v})
            radar_data.append({"metric": label, "value": v})

        for key, label, lo, hi, invert in scaled_keys:
            value = parsed.get(key)
            if value is None:
                continue
            v = round(float(value), 1)
            bar_data.append({"metric": label, "value": v})
            normalized = round(min(max((float(value) - lo) / (hi - lo) * 100, 0), 100), 1)
            if invert:
                normalized = round(100 - normalized, 1)
            radar_data.append({"metric": label, "value": normalized})

        if not bar_data:
            return None

        chart_type = (args.get("chart_type") or "radar").strip().lower()
        if chart_type == "radar":
            return {
                "chartType": "radar",
                "title": "Health Summary Snapshot",
                "xKey": "metric",
                "series": [{"key": "value", "label": "Score (normalized)", "color": "#6366f1"}],
                "data": radar_data,
                "source": tool_name,
                "dateRange": parsed.get("period"),
            }

        return {
            "chartType": "grouped_bar",
            "title": "Health Summary Snapshot",
            "xKey": "metric",
            "series": [{"key": "value", "label": "Average", "color": "#6366f1"}],
            "data": bar_data,
            "source": tool_name,
            "dateRange": parsed.get("period"),
        }

    if tool_name == "get_metric_series":
        metric = parsed.get("metric")
        points = parsed.get("data")
        if not metric or not isinstance(points, list) or not points:
            return None

        chart_points = []
        for point in points:
            if not isinstance(point, dict):
                continue
            point_date = point.get("date")
            if not point_date:
                continue
            chart_points.append({
                "date": point_date,
                "value": _normalize_metric_value(metric, point.get("value")),
            })

        if not chart_points:
            return None

        requested_chart_type = _resolve_series_chart_type(args.get("chart_type"))
        if requested_chart_type == "histogram":
            values = [
                float(point["value"])
                for point in chart_points
                if point.get("value") is not None
            ]
            buckets = _build_histogram_buckets(values, int(args.get("bins", 10)))
            if not buckets:
                return None
            return {
                "chartType": "histogram",
                "title": f"{_metric_label(metric)} Distribution",
                "xKey": "label",
                "series": [
                    {
                        "key": "count",
                        "label": "Days",
                        "color": CHART_COLORS.get(metric, "#8b5cf6"),
                    }
                ],
                "data": buckets,
                "source": tool_name,
                "dateRange": parsed.get("period"),
            }

        return {
            "chartType": requested_chart_type,
            "title": f"{_metric_label(metric)} Trend",
            "xKey": "date",
            "series": [
                {
                    "key": "value",
                    "label": _metric_label(metric),
                    "color": CHART_COLORS.get(metric, "#8b5cf6"),
                }
            ],
            "data": chart_points,
            "unit": _metric_unit(metric),
            "source": tool_name,
            "dateRange": parsed.get("period"),
        }

    if tool_name == "get_correlations":
        correlations = parsed.get("correlations")
        target = parsed.get("target")
        if not isinstance(correlations, list) or not correlations:
            return None

        chart_points = []
        for item in correlations:
            if not isinstance(item, dict):
                continue
            metric_name = item.get("metric")
            rho = item.get("rho")
            if metric_name is None or rho is None:
                continue
            chart_points.append({
                "metric": _metric_label(str(metric_name)),
                "rho": round(float(rho), 3),
            })

        if not chart_points:
            return None

        chart_type = (args.get("chart_type") or "bar").strip().lower()
        if chart_type == "radar":
            return {
                "chartType": "radar",
                "title": f"Correlation with {_metric_label(str(target))}",
                "xKey": "metric",
                "series": [{"key": "rho", "label": "Spearman rho", "color": "#6366f1"}],
                "data": chart_points[:10],
                "source": tool_name,
                "dateRange": parsed.get("period"),
            }

        if len(chart_points) == 1:
            return {
                "chartType": "single_value",
                "title": f"Correlation with {_metric_label(str(target))}",
                "xKey": "metric",
                "series": [{"key": "rho", "label": "Spearman rho", "color": "#6366f1"}],
                "data": chart_points,
                "source": tool_name,
                "dateRange": parsed.get("period"),
                "yDomain": [-1, 1],
            }

        return {
            "chartType": "bar",
            "title": f"Correlation with {_metric_label(str(target))}",
            "xKey": "metric",
            "series": [{"key": "rho", "label": "Spearman rho", "color": "#6366f1"}],
            "data": chart_points[:10],
            "source": tool_name,
            "dateRange": parsed.get("period"),
            "yDomain": [-1, 1],
        }

    if tool_name == "get_anomalies":
        metric = parsed.get("metric")
        anomalies = parsed.get("anomalies")
        if not metric or not isinstance(anomalies, list) or not anomalies:
            return None

        chart_points = []
        for item in anomalies:
            if not isinstance(item, dict):
                continue
            point_date = item.get("date")
            value = item.get("value")
            if point_date is None or value is None:
                continue
            chart_points.append({
                "date": point_date,
                "value": _normalize_metric_value(metric, value),
                "zScore": round(float(item.get("z_score", 0)), 2),
                "direction": item.get("direction"),
            })

        if not chart_points:
            return None

        return {
            "chartType": "scatter",
            "title": f"{_metric_label(metric)} Anomalies",
            "xKey": "date",
            "series": [
                {
                    "key": "value",
                    "label": _metric_label(metric),
                    "color": CHART_COLORS.get(metric, "#f97316"),
                }
            ],
            "data": chart_points,
            "unit": _metric_unit(metric),
            "source": tool_name,
        }

    if tool_name == "get_trends":
        metric = parsed.get("metric")
        change_points = parsed.get("change_points")
        if not metric or not isinstance(change_points, list) or not change_points:
            return None

        chart_points = []
        for point in change_points:
            if not isinstance(point, dict):
                continue
            point_date = point.get("date")
            magnitude = point.get("magnitude")
            if point_date is None or magnitude is None:
                continue
            chart_points.append({
                "date": point_date,
                "magnitude": round(float(magnitude), 2),
                "direction": point.get("direction"),
            })

        if not chart_points:
            return None

        return {
            "chartType": "bar",
            "title": f"{_metric_label(metric)} Change Points",
            "xKey": "date",
            "series": [
                {
                    "key": "magnitude",
                    "label": "Change Magnitude",
                    "color": CHART_COLORS.get(metric, "#14b8a6"),
                }
            ],
            "data": chart_points,
            "source": tool_name,
            "dateRange": parsed.get("period"),
        }

    if tool_name == "get_scatter_data":
        metric_x = parsed.get("metric_x")
        metric_y = parsed.get("metric_y")
        points = parsed.get("points")
        if not metric_x or not metric_y or not isinstance(points, list) or not points:
            return None

        chart_points = []
        for point in points:
            if not isinstance(point, dict):
                continue
            x_value = point.get("x")
            y_value = point.get("y")
            if x_value is None or y_value is None:
                continue
            chart_points.append({
                "x": round(float(x_value), 2),
                "y": round(float(y_value), 2),
                "date": point.get("date"),
            })

        if not chart_points:
            return None

        return {
            "chartType": "scatter_xy",
            "title": f"{_metric_label(metric_x)} vs {_metric_label(metric_y)}",
            "xKey": "x",
            "yKey": "y",
            "xAxisLabel": _metric_label(metric_x),
            "yAxisLabel": _metric_label(metric_y),
            "series": [
                {
                    "key": "y",
                    "label": _metric_label(metric_y),
                    "color": CHART_COLORS.get(metric_y, "#22c55e"),
                }
            ],
            "data": chart_points,
            "source": tool_name,
            "dateRange": parsed.get("period"),
        }

    if tool_name == "get_multi_metric_series":
        metrics = parsed.get("metrics")
        points = parsed.get("data")
        if not isinstance(metrics, list) or not isinstance(points, list) or not points:
            return None

        valid_metrics = [metric for metric in metrics if metric in ALLOWED_METRICS]
        if not valid_metrics:
            return None

        chart_type = (args.get("chart_type") or "multi_line").strip().lower()
        if chart_type not in {"multi_line", "area", "stacked_area"}:
            chart_type = "multi_line"

        return {
            "chartType": chart_type,
            "title": "Multi-metric Trend",
            "xKey": "date",
            "series": [
                {
                    "key": metric,
                    "label": _metric_label(metric),
                    "color": CHART_COLORS.get(metric, "#8b5cf6"),
                }
                for metric in valid_metrics
            ],
            "data": points,
            "source": tool_name,
            "dateRange": parsed.get("period"),
        }

    if tool_name == "get_metric_distribution":
        bins = parsed.get("bins")
        metric = parsed.get("metric")
        if not metric or not isinstance(bins, list) or not bins:
            return None
        return {
            "chartType": "histogram",
            "title": f"{_metric_label(metric)} Distribution",
            "xKey": "label",
            "series": [{"key": "count", "label": "Days", "color": CHART_COLORS.get(metric, "#8b5cf6")}],
            "data": bins,
            "source": tool_name,
            "dateRange": parsed.get("period"),
        }

    if tool_name == "get_period_comparison":
        metric = parsed.get("metric")
        if not metric:
            return None
        current_avg = parsed.get("current_avg")
        previous_avg = parsed.get("previous_avg")
        if current_avg is None and previous_avg is None:
            return None
        return {
            "chartType": "grouped_bar",
            "title": f"{_metric_label(metric)}: Current vs Previous",
            "xKey": "period",
            "series": [{"key": "value", "label": _metric_label(metric), "color": CHART_COLORS.get(metric, "#8b5cf6")}],
            "data": [
                {"period": "Current", "value": current_avg},
                {"period": "Previous", "value": previous_avg},
            ],
            "unit": _metric_unit(metric),
            "source": tool_name,
            "dateRange": parsed.get("period"),
        }

    if tool_name == "get_sleep_architecture":
        rows = parsed.get("data")
        if not isinstance(rows, list) or not rows:
            return None

        return {
            "chartType": _resolve_sleep_arch_chart_type(args.get("chart_type")),
            "title": "Sleep Stage Breakdown",
            "xKey": "date",
            "series": [
                {"key": "deep_pct", "label": "Deep", "color": "#6366f1"},
                {"key": "rem_pct", "label": "REM", "color": "#a855f7"},
                {"key": "light_pct", "label": "Light", "color": "#8b5cf6"},
            ],
            "data": rows,
            "unit": "%",
            "source": tool_name,
            "dateRange": parsed.get("period"),
        }

    return None

# Tool definitions for OpenAI function calling
TOOL_DEFINITIONS = [
    {
        "type": "function",
        "function": {
            "name": "get_summary",
            "description": "Get summary metrics (averages) for the last N days. Use this for questions like 'how was my sleep this week?' or 'what are my average metrics?'",
            "parameters": {
                "type": "object",
                "properties": {
                    "days": {
                        "type": "integer",
                        "description": "Number of days to look back (default 30)",
                        "enum": [7, 14, 30, 60, 90, 120],
                        "default": DEFAULT_LOOKBACK_DAYS,
                    },
                    "chart_type": {
                        "type": "string",
                        "description": "Optional chart style for summary values",
                        "enum": ["radar", "bar"],
                        "default": "radar",
                    },
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_metric_series",
            "description": "Get daily values for a specific metric over a date range. If no dates are provided, default to the last 30 days.",
            "parameters": {
                "type": "object",
                "properties": {
                    "metric": {
                        "type": "string",
                        "description": "The metric to retrieve",
                        "enum": [
                            "sleep_score", "readiness_score", "activity_score",
                            "steps", "hrv_average", "hr_lowest",
                            "sleep_total_seconds", "sleep_deep_seconds", "sleep_rem_seconds",
                            "sleep_efficiency", "cal_total", "cal_active",
                            "stress_high_minutes", "recovery_high_minutes",
                            "spo2_average", "workout_total_minutes",
                        ],
                    },
                    "start_date": {
                        "type": "string",
                        "description": "Start date in YYYY-MM-DD format (optional when lookback_days is used)",
                    },
                    "end_date": {
                        "type": "string",
                        "description": "End date in YYYY-MM-DD format (optional when lookback_days is used)",
                    },
                    "lookback_days": {
                        "type": "integer",
                        "description": "How many days back from today to include if start/end are omitted (default 30)",
                        "default": DEFAULT_LOOKBACK_DAYS,
                    },
                    "chart_type": {
                        "type": "string",
                        "description": "Chart style for the series",
                        "enum": ["line", "bar", "area", "histogram"],
                        "default": "line",
                    },
                    "bins": {
                        "type": "integer",
                        "description": "Number of histogram bins when chart_type=histogram",
                        "default": 10,
                    },
                },
                "required": ["metric"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_multi_metric_series",
            "description": "Get aligned daily values for multiple metrics over the same period for multi-line or area charts.",
            "parameters": {
                "type": "object",
                "properties": {
                    "metrics": {
                        "type": "array",
                        "items": {"type": "string", "enum": sorted(list(ALLOWED_METRICS))},
                        "description": "List of metrics to trend together (2-4 recommended)",
                    },
                    "start_date": {
                        "type": "string",
                        "description": "Start date in YYYY-MM-DD format (optional when lookback_days is used)",
                    },
                    "end_date": {
                        "type": "string",
                        "description": "End date in YYYY-MM-DD format (optional when lookback_days is used)",
                    },
                    "lookback_days": {
                        "type": "integer",
                        "description": "How many days back from today to include if start/end are omitted (default 30)",
                        "default": DEFAULT_LOOKBACK_DAYS,
                    },
                    "chart_type": {
                        "type": "string",
                        "description": "Chart style for multi-metric view",
                        "enum": ["multi_line", "area", "stacked_area"],
                        "default": "multi_line",
                    },
                },
                "required": ["metrics"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_scatter_data",
            "description": "Get paired daily x/y metric points for scatter charts (e.g., readiness_score vs sleep_score). If no dates are provided, default to the last 30 days.",
            "parameters": {
                "type": "object",
                "properties": {
                    "metric_x": {
                        "type": "string",
                        "description": "Metric for x-axis",
                        "enum": sorted(list(ALLOWED_METRICS)),
                    },
                    "metric_y": {
                        "type": "string",
                        "description": "Metric for y-axis",
                        "enum": sorted(list(ALLOWED_METRICS)),
                    },
                    "start_date": {
                        "type": "string",
                        "description": "Start date in YYYY-MM-DD format (optional when lookback_days is used)",
                    },
                    "end_date": {
                        "type": "string",
                        "description": "End date in YYYY-MM-DD format (optional when lookback_days is used)",
                    },
                    "lookback_days": {
                        "type": "integer",
                        "description": "How many days back from today to include if start/end are omitted (default 30)",
                        "default": DEFAULT_LOOKBACK_DAYS,
                    },
                },
                "required": ["metric_x", "metric_y"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_correlations",
            "description": "Find correlations between a target metric and other metrics. Use for questions like 'what affects my sleep score?'",
            "parameters": {
                "type": "object",
                "properties": {
                    "target": {
                        "type": "string",
                        "description": "The target metric to find correlations for",
                        "enum": sorted(list(ALLOWED_METRICS)),
                    },
                    "candidates": {
                        "type": "array",
                        "items": {"type": "string", "enum": sorted(list(ALLOWED_METRICS))},
                        "description": "List of candidate metrics to check correlation with",
                    },
                    "start_date": {
                        "type": "string",
                        "description": "Start date in YYYY-MM-DD format (optional when lookback_days is used)",
                    },
                    "end_date": {
                        "type": "string",
                        "description": "End date in YYYY-MM-DD format (optional when lookback_days is used)",
                    },
                    "lookback_days": {
                        "type": "integer",
                        "description": "How many days back from today to include if start/end are omitted (default 30)",
                        "default": DEFAULT_LOOKBACK_DAYS,
                    },
                    "chart_type": {
                        "type": "string",
                        "description": "Chart style for correlation output",
                        "enum": ["bar", "radar"],
                        "default": "bar",
                    },
                },
                "required": ["target", "candidates"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_metric_distribution",
            "description": "Get a distribution histogram for one metric over a period.",
            "parameters": {
                "type": "object",
                "properties": {
                    "metric": {
                        "type": "string",
                        "description": "Metric to analyze",
                        "enum": sorted(list(ALLOWED_METRICS)),
                    },
                    "start_date": {
                        "type": "string",
                        "description": "Start date in YYYY-MM-DD format (optional when lookback_days is used)",
                    },
                    "end_date": {
                        "type": "string",
                        "description": "End date in YYYY-MM-DD format (optional when lookback_days is used)",
                    },
                    "lookback_days": {
                        "type": "integer",
                        "description": "How many days back from today to include if start/end are omitted (default 30)",
                        "default": DEFAULT_LOOKBACK_DAYS,
                    },
                    "bins": {
                        "type": "integer",
                        "description": "Number of buckets for histogram",
                        "default": 10,
                    },
                },
                "required": ["metric"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_period_comparison",
            "description": "Compare current period average vs previous period average for a metric.",
            "parameters": {
                "type": "object",
                "properties": {
                    "metric": {
                        "type": "string",
                        "description": "Metric to compare",
                        "enum": sorted(list(ALLOWED_METRICS)),
                    },
                    "period_days": {
                        "type": "integer",
                        "description": "Days per period (current and previous). Default 30.",
                        "default": DEFAULT_LOOKBACK_DAYS,
                    },
                },
                "required": ["metric"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_anomalies",
            "description": "Find anomalous (unusual) values in a metric. Use for questions like 'any unusual sleep patterns?'",
            "parameters": {
                "type": "object",
                "properties": {
                    "metric": {
                        "type": "string",
                        "description": "The metric to check for anomalies",
                    },
                    "threshold": {
                        "type": "number",
                        "description": "Z-score threshold (default 2.5, higher = more extreme only)",
                        "default": 2.5,
                    },
                    "start_date": {
                        "type": "string",
                        "description": "Start date in YYYY-MM-DD format (optional when lookback_days is used)",
                    },
                    "end_date": {
                        "type": "string",
                        "description": "End date in YYYY-MM-DD format (optional when lookback_days is used)",
                    },
                    "lookback_days": {
                        "type": "integer",
                        "description": "How many days back from today to include if start/end are omitted (default 30)",
                        "default": DEFAULT_LOOKBACK_DAYS,
                    },
                },
                "required": ["metric"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_trends",
            "description": "Detect significant changes (change points) in a metric over time. Use for 'has my sleep improved?'",
            "parameters": {
                "type": "object",
                "properties": {
                    "metric": {
                        "type": "string",
                        "description": "The metric to analyze for trends",
                    },
                    "start_date": {
                        "type": "string",
                        "description": "Start date in YYYY-MM-DD format (optional when lookback_days is used)",
                    },
                    "end_date": {
                        "type": "string",
                        "description": "End date in YYYY-MM-DD format (optional when lookback_days is used)",
                    },
                    "lookback_days": {
                        "type": "integer",
                        "description": "How many days back from today to include if start/end are omitted (default 30)",
                        "default": DEFAULT_LOOKBACK_DAYS,
                    },
                },
                "required": ["metric"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_sleep_architecture",
            "description": "Get sleep stage breakdown (deep, REM, light percentages). Use for questions about sleep quality or stages.",
            "parameters": {
                "type": "object",
                "properties": {
                    "days": {
                        "type": "integer",
                        "description": "Number of days to look back (default 30)",
                        "default": DEFAULT_LOOKBACK_DAYS,
                    },
                    "chart_type": {
                        "type": "string",
                        "description": "Chart style for sleep stage output",
                        "enum": ["stacked_bar", "stacked_area"],
                        "default": "stacked_bar",
                    },
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_chronotype",
            "description": "Analyze the user's chronotype (morning lark vs night owl) and social jetlag. Use for questions about sleep timing patterns.",
            "parameters": {
                "type": "object",
                "properties": {},
                "required": [],
            },
        },
    },
]


async def _execute_tool(
    tool_name: str, args: dict, user_id: str
) -> tuple[str, dict[str, Any] | None]:
    """Execute a tool function and return its text result + optional chart artifact."""
    try:
        if tool_name == "get_summary":
            result = await _tool_get_summary(user_id, args.get("days", DEFAULT_LOOKBACK_DAYS))
        elif tool_name == "get_metric_series":
            result = await _tool_get_metric_series(
                user_id,
                args.get("metric"),
                args.get("start_date"),
                args.get("end_date"),
                args.get("lookback_days", DEFAULT_LOOKBACK_DAYS),
            )
        elif tool_name == "get_multi_metric_series":
            result = await _tool_get_multi_metric_series(
                user_id,
                args.get("metrics"),
                args.get("start_date"),
                args.get("end_date"),
                args.get("lookback_days", DEFAULT_LOOKBACK_DAYS),
            )
        elif tool_name == "get_scatter_data":
            result = await _tool_get_scatter_data(
                user_id,
                args.get("metric_x"),
                args.get("metric_y"),
                args.get("start_date"),
                args.get("end_date"),
                args.get("lookback_days", DEFAULT_LOOKBACK_DAYS),
            )
        elif tool_name == "get_correlations":
            result = await _tool_get_correlations(
                user_id,
                args["target"],
                args["candidates"],
                args.get("start_date"),
                args.get("end_date"),
                args.get("lookback_days", DEFAULT_LOOKBACK_DAYS),
            )
        elif tool_name == "get_metric_distribution":
            result = await _tool_get_metric_distribution(
                user_id,
                args.get("metric"),
                args.get("start_date"),
                args.get("end_date"),
                args.get("lookback_days", DEFAULT_LOOKBACK_DAYS),
                args.get("bins", 10),
            )
        elif tool_name == "get_period_comparison":
            result = await _tool_get_period_comparison(
                user_id,
                args.get("metric"),
                args.get("period_days", DEFAULT_LOOKBACK_DAYS),
            )
        elif tool_name == "get_anomalies":
            result = await _tool_get_anomalies(
                user_id,
                args["metric"],
                args.get("threshold", 2.5),
                args.get("start_date"),
                args.get("end_date"),
                args.get("lookback_days", DEFAULT_LOOKBACK_DAYS),
            )
        elif tool_name == "get_trends":
            result = await _tool_get_trends(
                user_id,
                args["metric"],
                args.get("start_date"),
                args.get("end_date"),
                args.get("lookback_days", DEFAULT_LOOKBACK_DAYS),
            )
        elif tool_name == "get_sleep_architecture":
            result = await _tool_get_sleep_architecture(
                user_id, args.get("days", DEFAULT_LOOKBACK_DAYS)
            )
        elif tool_name == "get_chronotype":
            result = await _tool_get_chronotype(user_id)
        else:
            return json.dumps({"error": f"Unknown tool: {tool_name}"}), None

        return result, _build_chart_payload(tool_name, args, result)
    except Exception as e:
        logger.exception("Tool %s failed", tool_name)
        return json.dumps({"error": str(e)}), None


async def _tool_get_summary(user_id: str, days: int) -> str:
    try:
        days = int(days)
    except (TypeError, ValueError):
        return json.dumps({"error": "days must be an integer."})

    if days < 1 or days > 3660:
        return json.dumps({"error": "days must be between 1 and 3660."})

    async with get_db_for_user(user_id) as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT
                    AVG(readiness_score) as readiness_avg,
                    AVG(sleep_score) as sleep_avg,
                    AVG(activity_score) as activity_avg,
                    AVG(steps) as steps_avg,
                    AVG(hrv_average) as hrv_avg,
                    AVG(hr_lowest) as rhr_avg,
                    AVG(sleep_total_seconds / 3600.0) as sleep_hours_avg,
                    AVG(cal_total) as calories_avg,
                    AVG(stress_high_minutes) as stress_avg,
                    AVG(recovery_high_minutes) as recovery_avg,
                    COUNT(*) as days_with_data
                FROM oura_daily
                WHERE date >= CURRENT_DATE - %(days)s
                AND user_id = %(user_id)s
            """, {"days": days, "user_id": user_id})
            row = await cur.fetchone()

    result = {}
    for key in ["readiness_avg", "sleep_avg", "activity_avg", "steps_avg",
                 "hrv_avg", "rhr_avg", "sleep_hours_avg", "calories_avg",
                 "stress_avg", "recovery_avg", "days_with_data"]:
        val = row[key]
        result[key] = round(float(val), 1) if val is not None else None

    result["period"] = f"last {days} days"
    return json.dumps(result)


async def _tool_get_metric_series(
    user_id: str,
    metric: str | None,
    start_date: str | None = None,
    end_date: str | None = None,
    lookback_days: int | None = None,
) -> str:
    canonical_metric = _canonicalize_metric(metric)
    if not canonical_metric:
        return json.dumps({"error": f"Unknown metric: {metric}"})

    try:
        resolved_start, resolved_end = _resolve_date_window(start_date, end_date, lookback_days)
    except ValueError as exc:
        return json.dumps({"error": str(exc)})

    async with get_db_for_user(user_id) as conn:
        async with conn.cursor() as cur:
            await cur.execute(f"""
                SELECT date, {canonical_metric} as value
                FROM oura_daily
                WHERE date >= %s AND date <= %s AND user_id = %s
                ORDER BY date
            """, (resolved_start, resolved_end, user_id))
            rows = await cur.fetchall()

    points = [
        {"date": str(r["date"]), "value": float(r["value"]) if r["value"] is not None else None}
        for r in rows
    ]
    return json.dumps({
        "metric": canonical_metric,
        "period": f"{resolved_start.isoformat()} to {resolved_end.isoformat()}",
        "data": points,
        "count": len(points),
    })


async def _tool_get_multi_metric_series(
    user_id: str,
    metrics: list[str] | None,
    start_date: str | None = None,
    end_date: str | None = None,
    lookback_days: int | None = None,
) -> str:
    if not isinstance(metrics, list) or not metrics:
        return json.dumps({"error": "At least one metric is required."})

    canonical_metrics: list[str] = []
    for metric in metrics:
        canonical = _canonicalize_metric(metric)
        if canonical and canonical not in canonical_metrics:
            canonical_metrics.append(canonical)

    if not canonical_metrics:
        return json.dumps({"error": "No valid metrics provided."})

    if len(canonical_metrics) > 6:
        return json.dumps({"error": "Use at most 6 metrics in one chart."})

    try:
        resolved_start, resolved_end = _resolve_date_window(start_date, end_date, lookback_days)
    except ValueError as exc:
        return json.dumps({"error": str(exc)})

    select_columns = ", ".join(canonical_metrics)
    async with get_db_for_user(user_id) as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                SELECT date, {select_columns}
                FROM oura_daily
                WHERE date >= %s AND date <= %s AND user_id = %s
                ORDER BY date
                """,
                (resolved_start, resolved_end, user_id),
            )
            rows = await cur.fetchall()

    points = []
    for row in rows:
        point: dict[str, Any] = {"date": str(row["date"])}
        for metric in canonical_metrics:
            point[metric] = _normalize_metric_value(metric, row.get(metric))
        points.append(point)

    return json.dumps({
        "metrics": canonical_metrics,
        "period": f"{resolved_start.isoformat()} to {resolved_end.isoformat()}",
        "data": points,
        "count": len(points),
    })


async def _tool_get_metric_distribution(
    user_id: str,
    metric: str | None,
    start_date: str | None = None,
    end_date: str | None = None,
    lookback_days: int | None = None,
    bins: int = 10,
) -> str:
    canonical_metric = _canonicalize_metric(metric)
    if not canonical_metric:
        return json.dumps({"error": f"Unknown metric: {metric}"})

    try:
        resolved_start, resolved_end = _resolve_date_window(start_date, end_date, lookback_days)
        resolved_bins = int(bins)
    except (TypeError, ValueError) as exc:
        return json.dumps({"error": str(exc)})

    async with get_db_for_user(user_id) as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                SELECT {canonical_metric} as value
                FROM oura_daily
                WHERE date >= %s AND date <= %s AND user_id = %s
                AND {canonical_metric} IS NOT NULL
                ORDER BY date
                """,
                (resolved_start, resolved_end, user_id),
            )
            rows = await cur.fetchall()

    values = [
        _normalize_metric_value(canonical_metric, row["value"])
        for row in rows
        if row["value"] is not None
    ]
    normalized_values = [value for value in values if value is not None]
    histogram = _build_histogram_buckets(normalized_values, resolved_bins)

    return json.dumps({
        "metric": canonical_metric,
        "period": f"{resolved_start.isoformat()} to {resolved_end.isoformat()}",
        "bins": histogram,
        "count": len(normalized_values),
    })


async def _tool_get_period_comparison(
    user_id: str,
    metric: str | None,
    period_days: int = DEFAULT_LOOKBACK_DAYS,
) -> str:
    canonical_metric = _canonicalize_metric(metric)
    if not canonical_metric:
        return json.dumps({"error": f"Unknown metric: {metric}"})

    try:
        period_days = int(period_days)
    except (TypeError, ValueError):
        return json.dumps({"error": "period_days must be an integer."})

    if period_days < 1 or period_days > 3660:
        return json.dumps({"error": "period_days must be between 1 and 3660."})

    current_end = date.today()
    current_start = current_end - timedelta(days=period_days - 1)
    previous_end = current_start - timedelta(days=1)
    previous_start = previous_end - timedelta(days=period_days - 1)

    async with get_db_for_user(user_id) as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                SELECT AVG({canonical_metric}) as avg_value
                FROM oura_daily
                WHERE user_id = %s
                AND date >= %s AND date <= %s
                AND {canonical_metric} IS NOT NULL
                """,
                (user_id, current_start, current_end),
            )
            current_row = await cur.fetchone()

            await cur.execute(
                f"""
                SELECT AVG({canonical_metric}) as avg_value
                FROM oura_daily
                WHERE user_id = %s
                AND date >= %s AND date <= %s
                AND {canonical_metric} IS NOT NULL
                """,
                (user_id, previous_start, previous_end),
            )
            previous_row = await cur.fetchone()

    current_avg = _normalize_metric_value(canonical_metric, current_row["avg_value"])
    previous_avg = _normalize_metric_value(canonical_metric, previous_row["avg_value"])

    delta_pct: float | None = None
    if current_avg is not None and previous_avg is not None and previous_avg != 0:
        delta_pct = round(((current_avg - previous_avg) / previous_avg) * 100.0, 2)

    return json.dumps({
        "metric": canonical_metric,
        "period_days": period_days,
        "current_avg": current_avg,
        "previous_avg": previous_avg,
        "delta_pct": delta_pct,
        "current_period": f"{current_start.isoformat()} to {current_end.isoformat()}",
        "previous_period": f"{previous_start.isoformat()} to {previous_end.isoformat()}",
        "period": f"{previous_start.isoformat()} to {current_end.isoformat()}",
    })


async def _tool_get_scatter_data(
    user_id: str,
    metric_x: str | None,
    metric_y: str | None,
    start_date: str | None = None,
    end_date: str | None = None,
    lookback_days: int | None = None,
) -> str:
    from app.analysis.correlations import get_scatter_data

    canonical_metric_x = _canonicalize_metric(metric_x)
    canonical_metric_y = _canonicalize_metric(metric_y)
    if not canonical_metric_x or not canonical_metric_y:
        return json.dumps({"error": f"Unknown metrics: x={metric_x}, y={metric_y}"})

    try:
        resolved_start, resolved_end = _resolve_date_window(start_date, end_date, lookback_days)
    except ValueError as exc:
        return json.dumps({"error": str(exc)})

    result = await get_scatter_data(
        canonical_metric_x,
        canonical_metric_y,
        resolved_start,
        resolved_end,
        user_id,
    )
    result["metric_x"] = canonical_metric_x
    result["metric_y"] = canonical_metric_y
    result["period"] = f"{resolved_start.isoformat()} to {resolved_end.isoformat()}"
    return json.dumps(result, default=str)


async def _tool_get_correlations(
    user_id: str,
    target: str,
    candidates: list[str],
    start_date: str | None = None,
    end_date: str | None = None,
    lookback_days: int | None = None,
) -> str:
    from app.analysis.correlations import get_spearman_correlations
    canonical_target = _canonicalize_metric(target)
    if not canonical_target:
        return json.dumps({"error": f"Unknown target metric: {target}"})

    canonical_candidates = [
        candidate for candidate in (_canonicalize_metric(c) for c in candidates) if candidate
    ]
    if not canonical_candidates:
        return json.dumps({"error": "No valid candidate metrics provided."})

    try:
        resolved_start, resolved_end = _resolve_date_window(start_date, end_date, lookback_days)
    except ValueError as exc:
        return json.dumps({"error": str(exc)})

    result = await get_spearman_correlations(
        canonical_target,
        canonical_candidates,
        resolved_start,
        resolved_end,
        user_id,
    )
    result["period"] = f"{resolved_start.isoformat()} to {resolved_end.isoformat()}"
    return json.dumps(result, default=str)


async def _tool_get_anomalies(
    user_id: str,
    metric: str,
    threshold: float,
    start_date: str | None = None,
    end_date: str | None = None,
    lookback_days: int | None = None,
) -> str:
    from app.analysis.patterns import get_anomalies
    canonical_metric = _canonicalize_metric(metric)
    if not canonical_metric:
        return json.dumps({"error": f"Unknown metric: {metric}"})

    try:
        resolved_start, resolved_end = _resolve_date_window(start_date, end_date, lookback_days)
    except ValueError as exc:
        return json.dumps({"error": str(exc)})

    result = await get_anomalies(
        canonical_metric,
        resolved_start,
        resolved_end,
        threshold,
        user_id,
    )
    result["period"] = f"{resolved_start.isoformat()} to {resolved_end.isoformat()}"
    return json.dumps(result, default=str)


async def _tool_get_trends(
    user_id: str,
    metric: str,
    start_date: str | None = None,
    end_date: str | None = None,
    lookback_days: int | None = None,
) -> str:
    from app.analysis.patterns import get_change_points
    canonical_metric = _canonicalize_metric(metric)
    if not canonical_metric:
        return json.dumps({"error": f"Unknown metric: {metric}"})

    try:
        resolved_start, resolved_end = _resolve_date_window(start_date, end_date, lookback_days)
    except ValueError as exc:
        return json.dumps({"error": str(exc)})

    result = await get_change_points(
        canonical_metric,
        resolved_start,
        resolved_end,
        10.0,
        user_id,
    )
    result["period"] = f"{resolved_start.isoformat()} to {resolved_end.isoformat()}"
    return json.dumps(result, default=str)


async def _tool_get_sleep_architecture(user_id: str, days: int) -> str:
    try:
        days = int(days)
    except (TypeError, ValueError):
        return json.dumps({"error": "days must be an integer."})

    if days < 1 or days > 3660:
        return json.dumps({"error": "days must be between 1 and 3660."})

    async with get_db_for_user(user_id) as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT
                    date,
                    CASE WHEN sleep_total_seconds > 0
                        THEN sleep_deep_seconds * 100.0 / sleep_total_seconds END as deep_pct,
                    CASE WHEN sleep_total_seconds > 0
                        THEN sleep_rem_seconds * 100.0 / sleep_total_seconds END as rem_pct,
                    CASE WHEN sleep_total_seconds > 0
                        THEN (sleep_total_seconds - sleep_deep_seconds - sleep_rem_seconds) * 100.0 / sleep_total_seconds END as light_pct,
                    sleep_total_seconds / 3600.0 as total_hours
                FROM oura_daily
                WHERE date >= CURRENT_DATE - %(days)s
                AND user_id = %(user_id)s
                AND sleep_total_seconds IS NOT NULL
                AND sleep_total_seconds > 0
                ORDER BY date
            """, {"days": days, "user_id": user_id})
            rows = await cur.fetchall()

    if not rows:
        return json.dumps({
            "avg_deep_pct": None,
            "avg_rem_pct": None,
            "avg_light_pct": None,
            "avg_total_hours": None,
            "days_with_data": 0,
            "period": f"last {days} days",
            "data": [],
        })

    daily_rows = []
    deep_values: list[float] = []
    rem_values: list[float] = []
    light_values: list[float] = []
    total_values: list[float] = []

    for row in rows:
        deep_pct = float(row["deep_pct"]) if row["deep_pct"] is not None else None
        rem_pct = float(row["rem_pct"]) if row["rem_pct"] is not None else None
        light_pct = float(row["light_pct"]) if row["light_pct"] is not None else None
        total_hours = float(row["total_hours"]) if row["total_hours"] is not None else None

        if deep_pct is not None:
            deep_values.append(deep_pct)
        if rem_pct is not None:
            rem_values.append(rem_pct)
        if light_pct is not None:
            light_values.append(light_pct)
        if total_hours is not None:
            total_values.append(total_hours)

        daily_rows.append({
            "date": str(row["date"]),
            "deep_pct": round(deep_pct, 1) if deep_pct is not None else None,
            "rem_pct": round(rem_pct, 1) if rem_pct is not None else None,
            "light_pct": round(light_pct, 1) if light_pct is not None else None,
            "total_hours": round(total_hours, 2) if total_hours is not None else None,
        })

    return json.dumps({
        "avg_deep_pct": round(sum(deep_values) / len(deep_values), 1) if deep_values else None,
        "avg_rem_pct": round(sum(rem_values) / len(rem_values), 1) if rem_values else None,
        "avg_light_pct": round(sum(light_values) / len(light_values), 1) if light_values else None,
        "avg_total_hours": round(sum(total_values) / len(total_values), 1) if total_values else None,
        "days_with_data": len(daily_rows),
        "period": f"last {days} days",
        "data": daily_rows,
    })


async def _tool_get_chronotype(user_id: str) -> str:
    async with get_db_for_user(user_id) as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT
                    r.day as date,
                    r.payload->>'bedtime_start' as bedtime_start,
                    r.payload->>'bedtime_end' as bedtime_end,
                    d.is_weekend
                FROM oura_raw r
                JOIN oura_daily d ON r.day = d.date AND r.user_id = d.user_id
                WHERE r.source = 'sleep'
                AND r.payload->>'type' = 'long_sleep'
                AND r.user_id = %s
                ORDER BY r.day DESC
                LIMIT 90
            """, (user_id,))
            rows = await cur.fetchall()

    if not rows:
        return json.dumps({"chronotype": "unknown", "message": "Insufficient sleep data"})

    weekend_midpoints = []
    weekday_midpoints = []

    for r in rows:
        if r["bedtime_start"] and r["bedtime_end"]:
            try:
                start = datetime.fromisoformat(r["bedtime_start"].replace("Z", "+00:00"))
                end = datetime.fromisoformat(r["bedtime_end"].replace("Z", "+00:00"))
                midpoint = start + (end - start) / 2
                hours = midpoint.hour + midpoint.minute / 60
                if hours < 6:
                    hours += 24
                if r["is_weekend"]:
                    weekend_midpoints.append(hours)
                else:
                    weekday_midpoints.append(hours)
            except (ValueError, TypeError):
                continue

    if not weekend_midpoints or not weekday_midpoints:
        return json.dumps({"chronotype": "unknown", "message": "Need more data"})

    avg_weekend = sum(weekend_midpoints) / len(weekend_midpoints)
    avg_weekday = sum(weekday_midpoints) / len(weekday_midpoints)
    jetlag_minutes = int(abs(avg_weekend - avg_weekday) * 60)

    if avg_weekend < 27:
        chronotype = "Morning Lark"
    elif avg_weekend > 29:
        chronotype = "Night Owl"
    else:
        chronotype = "Intermediate"

    return json.dumps({
        "chronotype": chronotype,
        "social_jetlag_minutes": jetlag_minutes,
        "weekend_midpoint_hour": round(avg_weekend, 1),
        "weekday_midpoint_hour": round(avg_weekday, 1),
    })


async def _save_message(
    user_id: str,
    conversation_id: str,
    role: str,
    content: str,
    tool_calls: list | None = None,
    artifacts: list[dict[str, Any]] | None = None,
    model: str | None = None,
    tokens_in: int | None = None,
    tokens_out: int | None = None,
    latency_ms: int | None = None,
)-> str | None:
    """Persist a chat message to the database."""
    async with get_db_for_user(user_id) as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO chat_messages (
                    user_id,
                    conversation_id,
                    role,
                    content,
                    tool_calls,
                    artifacts,
                    model,
                    tokens_in,
                    tokens_out,
                    latency_ms
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    user_id,
                    conversation_id,
                    role,
                    content,
                    json.dumps(tool_calls) if tool_calls else None,
                    json.dumps(artifacts) if artifacts else None,
                    model,
                    tokens_in,
                    tokens_out,
                    latency_ms,
                ),
            )
            row = await cur.fetchone()
    return str(row["id"]) if row else None


async def _ensure_conversation(user_id: str, conversation_id: str | None, title: str | None = None) -> str:
    """Get or create a conversation. Returns conversation_id."""
    if conversation_id:
        # Verify ownership via RLS
        async with get_db_for_user(user_id) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT id FROM chat_conversations WHERE id = %s AND user_id = %s",
                    (conversation_id, user_id),
                )
                row = await cur.fetchone()
                if row:
                    return conversation_id

    # Create new conversation
    new_id = str(uuid.uuid4())
    async with get_db_for_user(user_id) as conn:
        await conn.execute(
            "INSERT INTO chat_conversations (id, user_id, title) VALUES (%s, %s, %s)",
            (new_id, user_id, title or "New conversation"),
        )
    return new_id


async def _load_conversation_state(user_id: str, conversation_id: str) -> tuple[str, list[dict[str, Any]]]:
    async with get_db_for_user(user_id) as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT memory_summary
                FROM chat_conversations
                WHERE id = %s AND user_id = %s
                """,
                (conversation_id, user_id),
            )
            summary_row = await cur.fetchone()
            memory_summary = summary_row["memory_summary"] if summary_row else ""

            await cur.execute(
                """
                SELECT id, role, content, created_at
                FROM chat_messages
                WHERE conversation_id = %s AND user_id = %s
                ORDER BY created_at ASC
                """,
                (conversation_id, user_id),
            )
            history_rows = await cur.fetchall()
    return memory_summary or "", history_rows


async def _save_conversation_summary_state(
    user_id: str,
    conversation_id: str,
    summary_text: str,
    up_to_created_at: datetime | None,
) -> None:
    payload = _serialize_summary_state(summary_text, up_to_created_at)
    async with get_db_for_user(user_id) as conn:
        await conn.execute(
            """
            UPDATE chat_conversations
            SET memory_summary = %s, updated_at = NOW()
            WHERE id = %s AND user_id = %s
            """,
            (payload, conversation_id, user_id),
        )


async def _maybe_refresh_conversation_summary(
    *,
    client: AsyncOpenAI,
    user_id: str,
    conversation_id: str,
    raw_memory_summary: str,
    history_rows: list[dict[str, Any]],
) -> tuple[str, datetime | None, bool]:
    summary_text, up_to_created_at = _parse_summary_state(raw_memory_summary)

    if not settings.chat_phase1_memory_enabled:
        return summary_text, up_to_created_at, False
    if not history_rows:
        return summary_text, up_to_created_at, False

    history_messages = [
        {"role": row["role"], "content": row["content"]}
        for row in history_rows
    ]
    history_tokens = _estimate_messages_tokens(history_messages)
    if history_tokens <= settings.chat_summary_trigger_tokens:
        return summary_text, up_to_created_at, False

    unsummarized_rows = [
        row for row in history_rows
        if up_to_created_at is None or row["created_at"] > up_to_created_at
    ]
    keep_recent = max(1, settings.chat_recent_turns_min)
    if len(unsummarized_rows) <= keep_recent:
        return summary_text, up_to_created_at, False

    rows_to_summarize = unsummarized_rows[:-keep_recent]
    transcript = _render_messages_for_summary(rows_to_summarize)
    if not transcript.strip():
        return summary_text, up_to_created_at, False

    try:
        response = await client.chat.completions.create(
            model="gpt-4o-mini",
            max_tokens=settings.chat_summary_max_tokens,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Summarize the conversation context for future turns. "
                        "Keep high-signal facts, user preferences, constraints, and open questions. "
                        "Be concise and do not invent facts."
                    ),
                },
                {
                    "role": "user",
                    "content": json.dumps(
                        {
                            "existing_summary": summary_text,
                            "new_messages": transcript,
                            "format": "Short bullet list, <= 12 bullets",
                        },
                        ensure_ascii=False,
                    ),
                },
            ],
        )
        updated_summary = (response.choices[0].message.content or "").strip()
    except Exception:
        logger.warning("Failed to refresh conversation summary", exc_info=True)
        return summary_text, up_to_created_at, False

    if not updated_summary:
        return summary_text, up_to_created_at, False

    new_up_to_created_at = rows_to_summarize[-1]["created_at"]
    await _save_conversation_summary_state(
        user_id=user_id,
        conversation_id=conversation_id,
        summary_text=updated_summary,
        up_to_created_at=new_up_to_created_at,
    )
    return updated_summary, new_up_to_created_at, True


async def _detect_chat_memories_embedding_kind(user_id: str) -> str | None:
    async with get_db_for_user(user_id) as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT udt_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'chat_memories'
                  AND column_name = 'embedding'
                """
            )
            row = await cur.fetchone()
            if not row:
                return None
            udt_name = row["udt_name"]
            if udt_name == "vector":
                return "vector"
            if udt_name == "_float8":
                return "array"
            return "array"


async def _get_embedding_for_text(
    *,
    client: AsyncOpenAI,
    user_id: str,
    text: str,
) -> list[float] | None:
    normalized = text.strip()
    if not normalized:
        return None

    cache_key = _embedding_cache_key(user_id, normalized)
    cached = await _chat_cache.get_json(cache_key)
    if isinstance(cached, list) and cached:
        return [float(value) for value in cached]

    try:
        response = await client.embeddings.create(
            model="text-embedding-3-large",
            input=normalized,
            dimensions=1024,
        )
    except Exception:
        logger.warning("Embedding generation failed", exc_info=True)
        return None

    vector = response.data[0].embedding if response.data else None
    if not vector:
        return None
    vector = [float(value) for value in vector]

    await _chat_cache.set_json(
        cache_key,
        vector,
        ttl_seconds=settings.chat_embedding_cache_ttl_seconds,
    )
    return vector


async def _retrieve_long_term_memory_block(
    *,
    client: AsyncOpenAI,
    user_id: str,
    query_text: str,
) -> tuple[str, int, int]:
    if not settings.chat_long_term_memory_enabled:
        return "", 0, 0

    embedding_kind = await _detect_chat_memories_embedding_kind(user_id)
    if embedding_kind is None:
        return "", 0, 0

    query_embedding = await _get_embedding_for_text(
        client=client,
        user_id=user_id,
        text=query_text,
    )
    if not query_embedding:
        return "", 0, 0

    top_k = max(1, settings.chat_memory_retrieval_top_k)
    keep_k = max(1, settings.chat_memory_retrieval_keep_k)
    threshold = settings.chat_memory_similarity_threshold
    now = datetime.now(timezone.utc)

    memories: list[dict[str, Any]] = []
    try:
        async with get_db_for_user(user_id) as conn:
            async with conn.cursor() as cur:
                if embedding_kind == "vector":
                    vector_literal = _vector_literal(query_embedding)
                    await cur.execute(
                        """
                        SELECT
                            id,
                            memory_type,
                            content,
                            confidence,
                            importance,
                            last_seen_at,
                            (1 - (embedding <=> %s::vector)) AS similarity
                        FROM chat_memories
                        WHERE user_id = %s
                          AND embedding IS NOT NULL
                          AND (expires_at IS NULL OR expires_at > NOW())
                        ORDER BY embedding <=> %s::vector
                        LIMIT %s
                        """,
                        (vector_literal, user_id, vector_literal, top_k),
                    )
                    memories = await cur.fetchall()
                else:
                    await cur.execute(
                        """
                        SELECT
                            id,
                            memory_type,
                            content,
                            confidence,
                            importance,
                            last_seen_at,
                            embedding
                        FROM chat_memories
                        WHERE user_id = %s
                          AND embedding IS NOT NULL
                          AND (expires_at IS NULL OR expires_at > NOW())
                        ORDER BY last_seen_at DESC
                        LIMIT %s
                        """,
                        (user_id, top_k * 2),
                    )
                    rows = await cur.fetchall()
                    for row in rows:
                        embedding = row.get("embedding")
                        if not isinstance(embedding, list):
                            continue
                        row["similarity"] = _cosine_similarity(
                            [float(v) for v in embedding],
                            query_embedding,
                        )
                        memories.append(row)
    except Exception:
        logger.warning("Long-term memory retrieval failed", exc_info=True)
        return "", 0, 0

    ranked: list[dict[str, Any]] = []
    for memory in memories:
        similarity = float(memory.get("similarity") or 0.0)
        if similarity < threshold:
            continue
        confidence = float(memory.get("confidence") or 0.5)
        importance = float(memory.get("importance") or 0.5)
        last_seen = memory.get("last_seen_at") or now
        age_days = max(0.0, (now - last_seen).total_seconds() / 86400.0)
        recency_score = math.exp(-age_days / 30.0)
        blended = 0.6 * similarity + 0.2 * confidence + 0.1 * importance + 0.1 * recency_score
        ranked.append({
            **memory,
            "score": blended,
        })

    ranked.sort(key=lambda item: item["score"], reverse=True)
    selected = ranked[:keep_k]
    prompt_block, memory_tokens = _build_memory_prompt_block(
        selected,
        max_tokens=settings.chat_memory_retrieval_max_tokens,
    )
    return prompt_block, memory_tokens, len(selected)


async def _extract_candidate_memories(
    *,
    client: AsyncOpenAI,
    user_message: str,
    assistant_message: str,
) -> list[dict[str, Any]]:
    try:
        response = await client.chat.completions.create(
            model="gpt-4o-mini",
            max_tokens=300,
            response_format={"type": "json_object"},
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Extract durable user memory candidates. "
                        "Return JSON object with key 'memories'. "
                        "Each memory: memory_type(profile|preference|goal|episodic), "
                        "content, confidence(0..1), importance(0..1), ttl_days(optional integer). "
                        "Only include high-signal facts worth reusing later."
                    ),
                },
                {
                    "role": "user",
                    "content": json.dumps(
                        {"user_message": user_message, "assistant_message": assistant_message},
                        ensure_ascii=False,
                    ),
                },
            ],
        )
        content = (response.choices[0].message.content or "").strip()
        parsed = json.loads(content)
    except Exception:
        logger.warning("Memory extraction failed", exc_info=True)
        return []

    raw_memories = parsed.get("memories")
    if not isinstance(raw_memories, list):
        return []

    candidates: list[dict[str, Any]] = []
    seen_norm: set[str] = set()
    for item in raw_memories:
        if not isinstance(item, dict):
            continue
        memory_type = str(item.get("memory_type") or "").strip().lower()
        if memory_type not in {"profile", "preference", "goal", "episodic"}:
            continue
        content = str(item.get("content") or "").strip()
        if not content:
            continue
        normalized = _normalize_memory_content(content)
        if normalized in seen_norm:
            continue
        seen_norm.add(normalized)

        confidence_raw = item.get("confidence", 0.7)
        importance_raw = item.get("importance", 0.5)
        try:
            confidence = min(1.0, max(0.0, float(confidence_raw)))
        except (TypeError, ValueError):
            confidence = 0.7
        try:
            importance = min(1.0, max(0.0, float(importance_raw)))
        except (TypeError, ValueError):
            importance = 0.5

        ttl_days_raw = item.get("ttl_days")
        ttl_days: int | None
        if ttl_days_raw is None:
            ttl_days = 90 if memory_type == "episodic" else None
        else:
            try:
                ttl_days = max(1, int(ttl_days_raw))
            except (TypeError, ValueError):
                ttl_days = 90 if memory_type == "episodic" else None

        candidates.append(
            {
                "memory_type": memory_type,
                "content": content,
                "content_norm": normalized,
                "confidence": confidence,
                "importance": importance,
                "ttl_days": ttl_days,
            }
        )
    return candidates


async def _upsert_memory_candidate(
    *,
    user_id: str,
    conversation_id: str,
    source_message_id: str | None,
    embedding_kind: str,
    candidate: dict[str, Any],
    embedding: list[float],
) -> bool:
    expires_at: datetime | None = None
    ttl_days = candidate.get("ttl_days")
    if isinstance(ttl_days, int):
        expires_at = datetime.now(timezone.utc) + timedelta(days=ttl_days)

    memory_type = candidate["memory_type"]
    content = candidate["content"]
    content_norm = candidate["content_norm"]
    confidence = float(candidate["confidence"])
    importance = float(candidate["importance"])

    try:
        async with get_db_for_user(user_id) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT id, confidence, importance
                    FROM chat_memories
                    WHERE user_id = %s
                      AND memory_type = %s
                      AND content_norm = %s
                    ORDER BY updated_at DESC
                    LIMIT 1
                    """,
                    (user_id, memory_type, content_norm),
                )
                existing = await cur.fetchone()
                if existing:
                    await cur.execute(
                        """
                        UPDATE chat_memories
                        SET
                            content = %s,
                            confidence = GREATEST(confidence, %s),
                            importance = GREATEST(importance, %s),
                            source_conversation_id = %s,
                            source_message_id = %s,
                            expires_at = COALESCE(%s, expires_at),
                            last_seen_at = NOW(),
                            updated_at = NOW()
                        WHERE id = %s
                        """,
                        (
                            content,
                            confidence,
                            importance,
                            conversation_id,
                            source_message_id,
                            expires_at,
                            existing["id"],
                        ),
                    )
                    return False

                if embedding_kind == "vector":
                    vector_literal = _vector_literal(embedding)
                    await cur.execute(
                        """
                        SELECT id, (1 - (embedding <=> %s::vector)) AS similarity
                        FROM chat_memories
                        WHERE user_id = %s
                          AND memory_type = %s
                          AND embedding IS NOT NULL
                        ORDER BY embedding <=> %s::vector
                        LIMIT 1
                        """,
                        (vector_literal, user_id, memory_type, vector_literal),
                    )
                    near = await cur.fetchone()
                    if near and float(near.get("similarity") or 0.0) >= 0.92:
                        await cur.execute(
                            """
                            UPDATE chat_memories
                            SET
                                content = %s,
                                content_norm = %s,
                                confidence = GREATEST(confidence, %s),
                                importance = GREATEST(importance, %s),
                                source_conversation_id = %s,
                                source_message_id = %s,
                                expires_at = COALESCE(%s, expires_at),
                                last_seen_at = NOW(),
                                updated_at = NOW()
                            WHERE id = %s
                            """,
                            (
                                content,
                                content_norm,
                                confidence,
                                importance,
                                conversation_id,
                                source_message_id,
                                expires_at,
                                near["id"],
                            ),
                        )
                        return False

                if embedding_kind == "vector":
                    embedding_value: Any = _vector_literal(embedding)
                    cast = "::vector"
                else:
                    embedding_value = embedding
                    cast = ""

                await cur.execute(
                    f"""
                    INSERT INTO chat_memories (
                        user_id,
                        memory_type,
                        content,
                        content_norm,
                        confidence,
                        importance,
                        source_conversation_id,
                        source_message_id,
                        expires_at,
                        last_seen_at,
                        embedding
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s{cast})
                    """,
                    (
                        user_id,
                        memory_type,
                        content,
                        content_norm,
                        confidence,
                        importance,
                        conversation_id,
                        source_message_id,
                        expires_at,
                        embedding_value,
                    ),
                )
                return True
    except Exception:
        logger.warning("Memory upsert failed", exc_info=True)
        return False


async def _extract_and_store_memories(
    *,
    client: AsyncOpenAI,
    user_id: str,
    conversation_id: str,
    user_message: str,
    assistant_message: str,
    source_message_id: str | None,
) -> tuple[int, int]:
    if not settings.chat_long_term_memory_enabled:
        return 0, 0
    if not user_message.strip() or not assistant_message.strip():
        return 0, 0

    embedding_kind = await _detect_chat_memories_embedding_kind(user_id)
    if embedding_kind is None:
        return 0, 0

    candidates = await _extract_candidate_memories(
        client=client,
        user_message=user_message,
        assistant_message=assistant_message,
    )
    if not candidates:
        return 0, 0

    contents = [candidate["content"] for candidate in candidates]
    vectors: list[list[float]] = []
    try:
        response = await client.embeddings.create(
            model="text-embedding-3-large",
            input=contents,
            dimensions=1024,
        )
        vectors = [[float(value) for value in item.embedding] for item in response.data]
    except Exception:
        logger.warning("Bulk memory embedding failed", exc_info=True)
        return 0, 0

    inserted = 0
    dedup_dropped = 0
    for candidate, vector in zip(candidates, vectors):
        created = await _upsert_memory_candidate(
            user_id=user_id,
            conversation_id=conversation_id,
            source_message_id=source_message_id,
            embedding_kind=embedding_kind,
            candidate=candidate,
            embedding=vector,
        )
        if created:
            inserted += 1
        else:
            dedup_dropped += 1
    return inserted, dedup_dropped


async def invalidate_user_chat_cache(user_id: str) -> None:
    await _chat_cache.delete_prefix(f"chat:tool:{user_id}:")
    await _chat_cache.delete_prefix(f"chat:embedding:{user_id}:")
    await _chat_cache.delete_prefix(f"chat:session-context:{user_id}:")


async def run_chat(
    user_id: str,
    message: str,
    conversation_id: str | None = None,
) -> AsyncGenerator[str, None]:
    """Run the chat agent with streaming NDJSON output.

    Yields newline-delimited JSON strings:
    - {"type": "conversation_id", "id": "..."}
    - {"type": "tool_call", "name": "...", "args": {...}}
    - {"type": "tool_result", "name": "...", "summary": "..."}
    - {"type": "chart", "chart": {...}}
    - {"type": "token", "content": "..."}
    - {"type": "done", "conversation_id": "..."}
    - {"type": "error", "message": "..."}
    """
    if not settings.openai_api_key:
        yield json.dumps({"type": "error", "message": "Chat is not configured (missing API key)"}) + "\n"
        return

    if not SYSTEM_PROMPT:
        raise RuntimeError(
            f"System prompt is not initialized. Ensure startup loaded {SYSTEM_PROMPT_PATH}."
        )

    is_intro = message == INTRO_SENTINEL

    # For intro, check if the user has synced Oura data
    has_synced_data = False
    if is_intro:
        async with get_db_for_user(user_id) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT COUNT(*) as cnt FROM oura_daily WHERE user_id = %s LIMIT 1",
                    (user_id,),
                )
                row = await cur.fetchone()
                has_synced_data = (row["cnt"] or 0) > 0

    # Ensure conversation exists
    conv_id = await _ensure_conversation(
        user_id, conversation_id, "New conversation" if is_intro else message[:50]
    )
    yield json.dumps({"type": "conversation_id", "id": conv_id}) + "\n"

    client = AsyncOpenAI(api_key=settings.openai_api_key)
    user_message_id: str | None = None

    # Save user message (skip for intro — it's not a real user message)
    if not is_intro:
        user_message_id = await _save_message(user_id, conv_id, "user", message)

    raw_memory_summary, history_rows = await _load_conversation_state(user_id, conv_id)
    summary_text, summary_up_to_created_at, _summary_refreshed = await _maybe_refresh_conversation_summary(
        client=client,
        user_id=user_id,
        conversation_id=conv_id,
        raw_memory_summary=raw_memory_summary,
        history_rows=history_rows,
    )

    unsummarized_history_rows = [
        row for row in history_rows
        if summary_up_to_created_at is None or row["created_at"] > summary_up_to_created_at
    ]
    history_messages = [
        {"role": row["role"], "content": row["content"]}
        for row in unsummarized_history_rows
    ]

    if is_intro and has_synced_data:
        intro_prompt = "Introduce yourself and give me a quick snapshot of how I've been doing over the last 30 days. Use the get_summary tool with chart_type set to 'radar'."
    elif is_intro:
        intro_prompt = (
            "Introduce yourself to a new user who hasn't synced their Oura data yet. "
            "Briefly explain what you can help with once they have data (sleep analysis, activity trends, readiness insights, correlations, etc.). "
            "Then encourage them to go to the Settings page, connect their Oura account, and hit the Sync button. "
            "Mention that the first sync backfills their full Oura history so it may take a little while with large amounts of data, "
            "but every sync after that will be super quick. Do NOT use any data tools since there is no data yet."
        )
    else:
        intro_prompt = None

    memory_query = (
        intro_prompt
        if is_intro else message
    )
    long_term_memory_block, memory_injected_tokens, memory_retrieved_count = await _retrieve_long_term_memory_block(
        client=client,
        user_id=user_id,
        query_text=memory_query,
    )

    base_messages: list[dict[str, Any]] = [{"role": "system", "content": SYSTEM_PROMPT}]
    summary_used = bool(summary_text.strip())
    if summary_used:
        base_messages.append({
            "role": "system",
            "content": f"Conversation summary:\n{summary_text.strip()}",
        })
    if long_term_memory_block:
        base_messages.append({
            "role": "system",
            "content": long_term_memory_block,
        })

    history_hash = _json_hash(
        [str(row["id"]) for row in unsummarized_history_rows]
        + [message]
        + [summary_text]
        + [long_term_memory_block]
    )
    session_key = _session_context_key(user_id, conv_id, history_hash)
    cached_context = await _chat_cache.get_json(session_key)
    context_tokens_est = 0
    omitted_messages = 0
    messages: list[dict[str, Any]]
    if isinstance(cached_context, dict) and isinstance(cached_context.get("messages"), list):
        messages = cached_context["messages"]
        context_tokens_est = int(cached_context.get("context_tokens_est") or 0)
        omitted_messages = int(cached_context.get("omitted_messages") or 0)
    else:
        messages, context_tokens_est, omitted_messages = _build_context_from_history(
            base_messages=base_messages,
            history_messages=history_messages,
            budget_tokens=settings.chat_context_budget_tokens,
            min_recent_messages=settings.chat_recent_turns_min,
        )
        await _chat_cache.set_json(
            session_key,
            {
                "messages": messages,
                "context_tokens_est": context_tokens_est,
                "omitted_messages": omitted_messages,
            },
            ttl_seconds=max(30, settings.chat_session_state_ttl_seconds),
        )

    if is_intro:
        intro_message = {
            "role": "user",
            "content": intro_prompt,
        }
        messages.append(intro_message)
        context_tokens_est += _estimate_message_tokens(intro_message)

    tool_call_count = 0
    start_time = time.monotonic()
    chart_artifacts: list[dict[str, Any]] = []
    total_prompt_tokens = 0
    total_completion_tokens = 0
    tool_tokens_saved_est = 0
    memory_extracted_count = 0
    memory_dedup_dropped_count = 0
    tool_cache_hits = 0

    try:
        while tool_call_count < settings.chat_max_tool_calls_per_turn:
            # Check timeout
            elapsed = time.monotonic() - start_time
            if elapsed > settings.chat_timeout_seconds:
                yield json.dumps({"type": "error", "message": "Chat timed out"}) + "\n"
                return

            # Call OpenAI
            context_tokens_est = _estimate_messages_tokens(messages)
            response = await asyncio.wait_for(
                client.chat.completions.create(
                    model="gpt-4o",
                    messages=messages,
                    tools=TOOL_DEFINITIONS,
                    max_tokens=settings.chat_max_tokens,
                ),
                timeout=settings.chat_timeout_seconds - elapsed,
            )
            if response.usage:
                total_prompt_tokens += response.usage.prompt_tokens or 0
                total_completion_tokens += response.usage.completion_tokens or 0

            choice = response.choices[0]
            assistant_message = choice.message

            # If there are tool calls, execute them
            if assistant_message.tool_calls:
                # Add assistant message with tool calls to history
                messages.append({
                    "role": "assistant",
                    "content": assistant_message.content or "",
                    "tool_calls": [
                        {
                            "id": tc.id,
                            "type": "function",
                            "function": {
                                "name": tc.function.name,
                                "arguments": tc.function.arguments,
                            },
                        }
                        for tc in assistant_message.tool_calls
                    ],
                })

                for tc in assistant_message.tool_calls:
                    tool_call_count += 1
                    if tool_call_count > settings.chat_max_tool_calls_per_turn:
                        yield json.dumps({"type": "error", "message": "Too many tool calls"}) + "\n"
                        break

                    name = tc.function.name
                    try:
                        args = json.loads(tc.function.arguments)
                    except json.JSONDecodeError:
                        args = {}

                    yield json.dumps({"type": "tool_call", "name": name, "args": args}) + "\n"

                    cache_key = _tool_cache_key(user_id, name, args)
                    cached_tool = await _chat_cache.get_json(cache_key)
                    if isinstance(cached_tool, dict) and isinstance(cached_tool.get("result"), str):
                        tool_cache_hits += 1
                        result = str(cached_tool["result"])
                        compacted_result = str(cached_tool.get("compacted_result") or result)
                        chart_payload = cached_tool.get("chart_payload")
                    else:
                        result, chart_payload = await _execute_tool(name, args, user_id)
                        compacted_result, saved_tokens = _compact_tool_result_for_context(
                            name,
                            result,
                            settings.chat_tool_result_max_chars,
                        )
                        tool_tokens_saved_est += saved_tokens
                        await _chat_cache.set_json(
                            cache_key,
                            {
                                "result": result,
                                "compacted_result": compacted_result,
                                "chart_payload": chart_payload,
                            },
                            ttl_seconds=max(60, settings.chat_cache_ttl_seconds),
                        )
                    if isinstance(cached_tool, dict):
                        tool_tokens_saved_est += max(
                            0,
                            _estimate_tokens_text(result) - _estimate_tokens_text(compacted_result),
                        )

                    yield json.dumps({"type": "tool_result", "name": name, "summary": result[:200]}) + "\n"

                    if chart_payload:
                        chart_artifacts.append(chart_payload)
                        yield json.dumps({"type": "chart", "chart": chart_payload}) + "\n"

                    messages.append({
                        "role": "tool",
                        "tool_call_id": tc.id,
                        "content": compacted_result,
                    })

                continue  # Loop back for another completion

            # No tool calls — stream the final text response
            content = assistant_message.content or ""
            content = _sanitize_markdown_images(content)
            if not content.strip() and chart_artifacts:
                content = FOLLOW_UP_QUESTION_ITALIC
            else:
                content = _ensure_follow_up_question(content)
            if content:
                yield json.dumps({"type": "token", "content": content}) + "\n"

            # Save assistant response
            latency_ms = int((time.monotonic() - start_time) * 1000)
            tokens_in = total_prompt_tokens if total_prompt_tokens > 0 else None
            tokens_out = total_completion_tokens if total_completion_tokens > 0 else None
            assistant_message_id = await _save_message(
                user_id,
                conv_id,
                "assistant",
                content,
                artifacts=chart_artifacts or None,
                model="gpt-4o",
                tokens_in=tokens_in,
                tokens_out=tokens_out,
                latency_ms=latency_ms,
            )

            if not is_intro and user_message_id:
                memory_extracted_count, memory_dedup_dropped_count = await _extract_and_store_memories(
                    client=client,
                    user_id=user_id,
                    conversation_id=conv_id,
                    user_message=message,
                    assistant_message=content,
                    source_message_id=assistant_message_id,
                )

            if tokens_in is not None or tokens_out is not None:
                yield json.dumps({
                    "type": "usage",
                    "tokens_in": tokens_in,
                    "tokens_out": tokens_out,
                    "tokens_total": (tokens_in or 0) + (tokens_out or 0),
                    "context_tokens_est": context_tokens_est,
                    "tool_tokens_saved_est": tool_tokens_saved_est,
                    "summary_used": summary_used,
                    "messages_omitted": omitted_messages,
                    "memory_retrieved_count": memory_retrieved_count,
                    "memory_injected_tokens": memory_injected_tokens,
                    "memory_extracted_count": memory_extracted_count,
                    "memory_dedup_dropped_count": memory_dedup_dropped_count,
                    "tool_cache_hits": tool_cache_hits,
                }) + "\n"

            # Update conversation title if it's a new conversation
            if (not conversation_id or is_intro) and content:
                title = content[:80].split("\n")[0]
                async with get_db_for_user(user_id) as conn:
                    await conn.execute(
                        "UPDATE chat_conversations SET title = %s, updated_at = NOW() WHERE id = %s AND user_id = %s",
                        (title, conv_id, user_id),
                    )

            logger.info(
                "chat_observability user_id=%s conv_id=%s context_tokens_est=%s tool_tokens_saved_est=%s summary_used=%s memory_retrieved=%s memory_extracted=%s memory_dedup_dropped=%s tool_cache_hits=%s",
                user_id,
                conv_id,
                context_tokens_est,
                tool_tokens_saved_est,
                summary_used,
                memory_retrieved_count,
                memory_extracted_count,
                memory_dedup_dropped_count,
                tool_cache_hits,
            )

            yield json.dumps({"type": "done", "conversation_id": conv_id}) + "\n"
            return

    except asyncio.TimeoutError:
        yield json.dumps({"type": "error", "message": "Chat request timed out"}) + "\n"
    except Exception as e:
        logger.exception("Chat error")
        yield json.dumps({"type": "error", "message": str(e)}) + "\n"


async def get_conversations(user_id: str) -> list[dict]:
    """List user's conversations ordered by most recent."""
    async with get_db_for_user(user_id) as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, title, created_at, updated_at
                FROM chat_conversations
                WHERE user_id = %s
                ORDER BY updated_at DESC
                LIMIT 50
                """,
                (user_id,),
            )
            rows = await cur.fetchall()
    return [
        {
            "id": str(r["id"]),
            "title": r["title"],
            "created_at": r["created_at"].isoformat(),
            "updated_at": r["updated_at"].isoformat(),
        }
        for r in rows
    ]


async def get_conversation_messages(
    user_id: str,
    conversation_id: str,
    *,
    limit: int | None = None,
    before: datetime | None = None,
) -> list[dict]:
    """Get conversation messages (ownership enforced via RLS).

    Supports optional pagination by loading newest messages first when `limit` is set.
    """
    max_limit = 500
    page_limit = max(1, min(int(limit), max_limit)) if limit is not None else None

    async with get_db_for_user(user_id) as conn:
        async with conn.cursor() as cur:
            if page_limit is None and before is None:
                await cur.execute(
                    """
                    SELECT role, content, tool_calls, artifacts, model, tokens_in, tokens_out, latency_ms, created_at
                    FROM chat_messages
                    WHERE conversation_id = %s AND user_id = %s
                    ORDER BY created_at
                    """,
                    (conversation_id, user_id),
                )
                rows = await cur.fetchall()
            else:
                effective_limit = page_limit or 200
                if before is None:
                    await cur.execute(
                        """
                        SELECT role, content, tool_calls, artifacts, model, tokens_in, tokens_out, latency_ms, created_at
                        FROM chat_messages
                        WHERE conversation_id = %s AND user_id = %s
                        ORDER BY created_at DESC
                        LIMIT %s
                        """,
                        (conversation_id, user_id, effective_limit),
                    )
                else:
                    await cur.execute(
                        """
                        SELECT role, content, tool_calls, artifacts, model, tokens_in, tokens_out, latency_ms, created_at
                        FROM chat_messages
                        WHERE conversation_id = %s AND user_id = %s AND created_at < %s
                        ORDER BY created_at DESC
                        LIMIT %s
                        """,
                        (conversation_id, user_id, before, effective_limit),
                    )
                rows = list(reversed(await cur.fetchall()))
    return [
        {
            "role": r["role"],
            "content": r["content"],
            "tool_calls": r["tool_calls"],
            "artifacts": r["artifacts"],
            "model": r["model"],
            "tokens_in": r["tokens_in"],
            "tokens_out": r["tokens_out"],
            "latency_ms": r["latency_ms"],
            "created_at": r["created_at"].isoformat(),
        }
        for r in rows
    ]


async def delete_conversation(user_id: str, conversation_id: str) -> bool:
    """Delete a conversation (ownership enforced via RLS). Returns True if deleted."""
    async with get_db_for_user(user_id) as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "DELETE FROM chat_conversations WHERE id = %s AND user_id = %s RETURNING id",
                (conversation_id, user_id),
            )
            row = await cur.fetchone()
    return row is not None
