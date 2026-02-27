"""Unit tests for Phase 1 chat chart behavior."""

import pytest

from app.chat import (
    DEFAULT_LOOKBACK_DAYS,
    FOLLOW_UP_QUESTION,
    _build_chart_payload,
    _canonicalize_metric,
    _ensure_follow_up_question,
    _resolve_date_window,
    _sanitize_markdown_images,
)


def test_canonicalize_metric_aliases():
    assert _canonicalize_metric("readiness score") == "readiness_score"
    assert _canonicalize_metric("sleep-score") == "sleep_score"
    assert _canonicalize_metric("RHR") == "hr_lowest"
    assert _canonicalize_metric("hrv") == "hrv_average"


def test_canonicalize_metric_invalid():
    assert _canonicalize_metric("not_a_metric") is None
    assert _canonicalize_metric("") is None


def test_resolve_date_window_uses_default_lookback():
    start, end = _resolve_date_window(None, None, None)
    assert (end - start).days == DEFAULT_LOOKBACK_DAYS - 1


def test_resolve_date_window_requires_both_dates():
    with pytest.raises(ValueError, match="Provide both start_date and end_date"):
        _resolve_date_window("2026-01-01", None, None)


def test_follow_up_question_is_appended_once():
    content = "Here is your chart summary."
    with_follow_up = _ensure_follow_up_question(content)
    assert FOLLOW_UP_QUESTION in with_follow_up
    assert _ensure_follow_up_question(with_follow_up) == with_follow_up


def test_sanitize_markdown_images():
    content = "Chart below: ![Scatter Chart](https://example.com/scatter.png)\n<img src='x' />"
    sanitized = _sanitize_markdown_images(content)
    assert "![Scatter Chart]" not in sanitized
    assert "<img" not in sanitized
    assert "Scatter Chart" in sanitized


def test_histogram_chart_payload_for_metric_series():
    payload = _build_chart_payload(
        "get_metric_series",
        {"chart_type": "histogram", "bins": 8},
        """
        {
          "metric": "sleep_score",
          "period": "2026-01-01 to 2026-01-10",
          "data": [
            {"date": "2026-01-01", "value": 70},
            {"date": "2026-01-02", "value": 72},
            {"date": "2026-01-03", "value": 75}
          ]
        }
        """,
    )
    assert payload is not None
    assert payload["chartType"] == "histogram"
    assert payload["xKey"] == "label"
    assert isinstance(payload["data"], list)


def test_scatter_xy_chart_payload_shape():
    payload = _build_chart_payload(
        "get_scatter_data",
        {},
        """
        {
          "metric_x": "readiness_score",
          "metric_y": "sleep_score",
          "period": "2026-01-01 to 2026-01-10",
          "points": [
            {"x": 72.1, "y": 75.3, "date": "2026-01-01"},
            {"x": 74.0, "y": 77.2, "date": "2026-01-02"}
          ]
        }
        """,
    )
    assert payload is not None
    assert payload["chartType"] == "scatter_xy"
    assert payload["xKey"] == "x"
    assert payload["yKey"] == "y"
