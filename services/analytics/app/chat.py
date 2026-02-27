"""AI chat agent with typed tool functions for health data analysis."""

import asyncio
import json
import logging
import re
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any, AsyncGenerator

from openai import AsyncOpenAI

from app.db import get_db_for_user
from app.settings import settings

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are Ouralie, a personal health analytics assistant for Oura Ring data. Your name is Ouralie (pronounced "or-ah-lee"). You help users understand their sleep, activity, readiness, and other health metrics tracked by their Oura Ring.

Rules:
- ALWAYS use the available tools to look up data before answering questions about the user's health metrics. Never guess numbers.
- Cite the data sources and date ranges in your responses.
- Be concise but informative.
- If data is insufficient, say so honestly.
- Provide actionable insights when possible.
- Format numbers clearly (e.g., "7.5 hours" not "7.482 hours").
- Use standard markdown for formatting (headings, bullets, emphasis), and DO NOT use markdown tables or pipe-delimited ASCII tables.
- Do NOT include markdown images or HTML <img> tags in responses. Charts are rendered by the UI from tool results.
- If the user does not specify a time period, default to the last 10 days.
- When the user asks for a metric-vs-metric chart, use scatter data tools that return paired x/y points.
- After each response, ask this follow-up question exactly: "Would you like a different time period, chart type, or another edit?"
- Put an empty line before the follow-up question.
- When introducing yourself, briefly mention your name and what you can help with, then use your tools to share a quick snapshot of the user's recent health data to demonstrate your capabilities.
"""

INTRO_SENTINEL = "__OURALIE_INTRO__"
DEFAULT_LOOKBACK_DAYS = 10
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


def _build_chart_payload(tool_name: str, args: dict, raw_result: str) -> dict[str, Any] | None:
    """Build a UI-ready chart artifact from a tool result."""
    try:
        parsed = json.loads(raw_result)
    except (TypeError, json.JSONDecodeError):
        return None

    if not isinstance(parsed, dict) or parsed.get("error"):
        return None

    if tool_name == "get_summary":
        summary_keys = [
            ("readiness_avg", "Readiness"),
            ("sleep_avg", "Sleep"),
            ("activity_avg", "Activity"),
            ("steps_avg", "Steps"),
            ("hrv_avg", "HRV"),
            ("sleep_hours_avg", "Sleep Hours"),
        ]
        radar_data = []
        for key, label in summary_keys:
            value = parsed.get(key)
            if value is None:
                continue
            radar_data.append({"metric": label, "value": round(float(value), 2)})

        if not radar_data:
            return None

        chart_type = (args.get("chart_type") or "radar").strip().lower()
        if chart_type == "bar":
            return {
                "chartType": "grouped_bar",
                "title": "Health Summary Snapshot",
                "xKey": "metric",
                "series": [{"key": "value", "label": "Average", "color": "#6366f1"}],
                "data": radar_data,
                "source": tool_name,
                "dateRange": parsed.get("period"),
            }

        return {
            "chartType": "radar",
            "title": "Health Summary Snapshot",
            "xKey": "metric",
            "series": [{"key": "value", "label": "Average", "color": "#6366f1"}],
            "data": radar_data,
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
                "metric": str(metric_name),
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

        return {
            "chartType": "bar",
            "title": f"Correlation with {_metric_label(str(target))}",
            "xKey": "metric",
            "series": [{"key": "rho", "label": "Spearman rho", "color": "#6366f1"}],
            "data": chart_points[:10],
            "source": tool_name,
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
                        "description": "Number of days to look back (default 10)",
                        "enum": [7, 10, 14, 30, 60, 90],
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
            "description": "Get daily values for a specific metric over a date range. If no dates are provided, default to the last 10 days.",
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
                        "description": "How many days back from today to include if start/end are omitted (default 10)",
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
                        "description": "How many days back from today to include if start/end are omitted (default 10)",
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
            "description": "Get paired daily x/y metric points for scatter charts (e.g., readiness_score vs sleep_score). If no dates are provided, default to the last 10 days.",
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
                        "description": "How many days back from today to include if start/end are omitted (default 10)",
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
                        "description": "How many days back from today to include if start/end are omitted (default 10)",
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
                        "description": "How many days back from today to include if start/end are omitted (default 10)",
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
                        "description": "Days per period (current and previous). Default 10.",
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
                        "description": "How many days back from today to include if start/end are omitted (default 10)",
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
                        "description": "How many days back from today to include if start/end are omitted (default 10)",
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
                        "description": "Number of days to look back (default 10)",
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
):
    """Persist a chat message to the database."""
    async with get_db_for_user(user_id) as conn:
        await conn.execute(
            """
            INSERT INTO chat_messages (user_id, conversation_id, role, content, tool_calls, artifacts, model, tokens_in, tokens_out, latency_ms)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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

    is_intro = message == INTRO_SENTINEL

    # Ensure conversation exists
    conv_id = await _ensure_conversation(
        user_id, conversation_id, "New conversation" if is_intro else message[:50]
    )
    yield json.dumps({"type": "conversation_id", "id": conv_id}) + "\n"

    # Save user message (skip for intro — it's not a real user message)
    if not is_intro:
        await _save_message(user_id, conv_id, "user", message)

    # Load conversation history
    async with get_db_for_user(user_id) as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT role, content FROM chat_messages
                WHERE conversation_id = %s AND user_id = %s
                ORDER BY created_at
                LIMIT 50
                """,
                (conv_id, user_id),
            )
            history_rows = await cur.fetchall()

    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    for row in history_rows:
        messages.append({"role": row["role"], "content": row["content"]})

    if is_intro:
        messages.append({
            "role": "user",
            "content": "Introduce yourself and give me a quick snapshot of how I've been doing recently.",
        })

    client = AsyncOpenAI(api_key=settings.openai_api_key)
    tool_call_count = 0
    start_time = time.monotonic()
    chart_artifacts: list[dict[str, Any]] = []

    try:
        while tool_call_count < settings.chat_max_tool_calls_per_turn:
            # Check timeout
            elapsed = time.monotonic() - start_time
            if elapsed > settings.chat_timeout_seconds:
                yield json.dumps({"type": "error", "message": "Chat timed out"}) + "\n"
                return

            # Call OpenAI
            response = await asyncio.wait_for(
                client.chat.completions.create(
                    model="gpt-4o",
                    messages=messages,
                    tools=TOOL_DEFINITIONS,
                    max_tokens=settings.chat_max_tokens,
                ),
                timeout=settings.chat_timeout_seconds - elapsed,
            )

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

                    result, chart_payload = await _execute_tool(name, args, user_id)

                    yield json.dumps({"type": "tool_result", "name": name, "summary": result[:200]}) + "\n"

                    if chart_payload:
                        chart_artifacts.append(chart_payload)
                        yield json.dumps({"type": "chart", "chart": chart_payload}) + "\n"

                    messages.append({
                        "role": "tool",
                        "tool_call_id": tc.id,
                        "content": result,
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
            await _save_message(
                user_id,
                conv_id,
                "assistant",
                content,
                artifacts=chart_artifacts or None,
                model="gpt-4o",
                tokens_in=response.usage.prompt_tokens if response.usage else None,
                tokens_out=response.usage.completion_tokens if response.usage else None,
                latency_ms=latency_ms,
            )

            # Update conversation title if it's a new conversation
            if (not conversation_id or is_intro) and content:
                title = content[:80].split("\n")[0]
                async with get_db_for_user(user_id) as conn:
                    await conn.execute(
                        "UPDATE chat_conversations SET title = %s, updated_at = NOW() WHERE id = %s AND user_id = %s",
                        (title, conv_id, user_id),
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


async def get_conversation_messages(user_id: str, conversation_id: str) -> list[dict]:
    """Get messages for a conversation (ownership enforced via RLS)."""
    async with get_db_for_user(user_id) as conn:
        async with conn.cursor() as cur:
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
    return [
        {
            "role": r["role"],
            "content": r["content"],
            "tool_calls": r["tool_calls"],
            "artifacts": r["artifacts"],
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
