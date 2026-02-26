"""AI chat agent with typed tool functions for health data analysis."""

import asyncio
import json
import logging
import time
import uuid
from datetime import date, datetime, timezone
from typing import AsyncGenerator

from openai import AsyncOpenAI

from app.db import get_db_for_user
from app.settings import settings

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are a health analytics assistant for Oura Ring data. You help users understand their sleep, activity, readiness, and other health metrics.

Rules:
- ALWAYS use the available tools to look up data before answering questions about the user's health metrics. Never guess numbers.
- Cite the data sources and date ranges in your responses.
- Be concise but informative.
- If data is insufficient, say so honestly.
- Provide actionable insights when possible.
- Format numbers clearly (e.g., "7.5 hours" not "7.482 hours").
"""

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
                        "description": "Number of days to look back (7, 14, or 30)",
                        "enum": [7, 14, 30],
                    }
                },
                "required": ["days"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_metric_series",
            "description": "Get daily values for a specific metric over a date range. Use for trend questions like 'show me my HRV over the last month'.",
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
                        "description": "Start date in YYYY-MM-DD format",
                    },
                    "end_date": {
                        "type": "string",
                        "description": "End date in YYYY-MM-DD format",
                    },
                },
                "required": ["metric", "start_date", "end_date"],
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
                    },
                    "candidates": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of candidate metrics to check correlation with",
                    },
                },
                "required": ["target", "candidates"],
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
                        "default": 30,
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


async def _execute_tool(tool_name: str, args: dict, user_id: str) -> str:
    """Execute a tool function and return the result as a string."""
    try:
        if tool_name == "get_summary":
            return await _tool_get_summary(user_id, args.get("days", 7))
        elif tool_name == "get_metric_series":
            return await _tool_get_metric_series(
                user_id, args["metric"], args["start_date"], args["end_date"]
            )
        elif tool_name == "get_correlations":
            return await _tool_get_correlations(
                user_id, args["target"], args["candidates"]
            )
        elif tool_name == "get_anomalies":
            return await _tool_get_anomalies(
                user_id, args["metric"], args.get("threshold", 2.5)
            )
        elif tool_name == "get_trends":
            return await _tool_get_trends(user_id, args["metric"])
        elif tool_name == "get_sleep_architecture":
            return await _tool_get_sleep_architecture(user_id, args.get("days", 30))
        elif tool_name == "get_chronotype":
            return await _tool_get_chronotype(user_id)
        else:
            return json.dumps({"error": f"Unknown tool: {tool_name}"})
    except Exception as e:
        logger.exception("Tool %s failed", tool_name)
        return json.dumps({"error": str(e)})


async def _tool_get_summary(user_id: str, days: int) -> str:
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


async def _tool_get_metric_series(user_id: str, metric: str, start_date: str, end_date: str) -> str:
    allowed_metrics = {
        "sleep_score", "readiness_score", "activity_score",
        "steps", "hrv_average", "hr_lowest",
        "sleep_total_seconds", "sleep_deep_seconds", "sleep_rem_seconds",
        "sleep_efficiency", "cal_total", "cal_active",
        "stress_high_minutes", "recovery_high_minutes",
        "spo2_average", "workout_total_minutes",
    }
    if metric not in allowed_metrics:
        return json.dumps({"error": f"Unknown metric: {metric}"})

    async with get_db_for_user(user_id) as conn:
        async with conn.cursor() as cur:
            await cur.execute(f"""
                SELECT date, {metric} as value
                FROM oura_daily
                WHERE date >= %s AND date <= %s AND user_id = %s
                ORDER BY date
            """, (start_date, end_date, user_id))
            rows = await cur.fetchall()

    points = [
        {"date": str(r["date"]), "value": float(r["value"]) if r["value"] is not None else None}
        for r in rows
    ]
    return json.dumps({"metric": metric, "period": f"{start_date} to {end_date}", "data": points, "count": len(points)})


async def _tool_get_correlations(user_id: str, target: str, candidates: list[str]) -> str:
    from app.analysis.correlations import get_spearman_correlations
    result = await get_spearman_correlations(target, candidates, None, None, user_id)
    return json.dumps(result, default=str)


async def _tool_get_anomalies(user_id: str, metric: str, threshold: float) -> str:
    from app.analysis.patterns import get_anomalies
    result = await get_anomalies(metric, None, None, threshold, user_id)
    return json.dumps(result, default=str)


async def _tool_get_trends(user_id: str, metric: str) -> str:
    from app.analysis.patterns import get_change_points
    result = await get_change_points(metric, None, None, 10.0, user_id)
    return json.dumps(result, default=str)


async def _tool_get_sleep_architecture(user_id: str, days: int) -> str:
    async with get_db_for_user(user_id) as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT
                    AVG(CASE WHEN sleep_total_seconds > 0
                        THEN sleep_deep_seconds * 100.0 / sleep_total_seconds END) as avg_deep_pct,
                    AVG(CASE WHEN sleep_total_seconds > 0
                        THEN sleep_rem_seconds * 100.0 / sleep_total_seconds END) as avg_rem_pct,
                    AVG(sleep_total_seconds / 3600.0) as avg_total_hours,
                    COUNT(*) as days_with_data
                FROM oura_daily
                WHERE date >= CURRENT_DATE - %(days)s
                AND user_id = %(user_id)s
                AND sleep_total_seconds IS NOT NULL
                AND sleep_total_seconds > 0
            """, {"days": days, "user_id": user_id})
            row = await cur.fetchone()

    return json.dumps({
        "avg_deep_pct": round(float(row["avg_deep_pct"]), 1) if row["avg_deep_pct"] else None,
        "avg_rem_pct": round(float(row["avg_rem_pct"]), 1) if row["avg_rem_pct"] else None,
        "avg_total_hours": round(float(row["avg_total_hours"]), 1) if row["avg_total_hours"] else None,
        "days_with_data": row["days_with_data"],
        "period": f"last {days} days",
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
    model: str | None = None,
    tokens_in: int | None = None,
    tokens_out: int | None = None,
    latency_ms: int | None = None,
):
    """Persist a chat message to the database."""
    async with get_db_for_user(user_id) as conn:
        await conn.execute(
            """
            INSERT INTO chat_messages (user_id, conversation_id, role, content, tool_calls, model, tokens_in, tokens_out, latency_ms)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                user_id,
                conversation_id,
                role,
                content,
                json.dumps(tool_calls) if tool_calls else None,
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
    - {"type": "token", "content": "..."}
    - {"type": "done", "conversation_id": "..."}
    - {"type": "error", "message": "..."}
    """
    if not settings.openai_api_key:
        yield json.dumps({"type": "error", "message": "Chat is not configured (missing API key)"}) + "\n"
        return

    # Ensure conversation exists
    conv_id = await _ensure_conversation(user_id, conversation_id, message[:50])
    yield json.dumps({"type": "conversation_id", "id": conv_id}) + "\n"

    # Save user message
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

    client = AsyncOpenAI(api_key=settings.openai_api_key)
    tool_call_count = 0
    start_time = time.monotonic()

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

                    result = await _execute_tool(name, args, user_id)

                    yield json.dumps({"type": "tool_result", "name": name, "summary": result[:200]}) + "\n"

                    messages.append({
                        "role": "tool",
                        "tool_call_id": tc.id,
                        "content": result,
                    })

                continue  # Loop back for another completion

            # No tool calls — stream the final text response
            content = assistant_message.content or ""
            if content:
                yield json.dumps({"type": "token", "content": content}) + "\n"

            # Save assistant response
            latency_ms = int((time.monotonic() - start_time) * 1000)
            await _save_message(
                user_id,
                conv_id,
                "assistant",
                content,
                model="gpt-4o",
                tokens_in=response.usage.prompt_tokens if response.usage else None,
                tokens_out=response.usage.completion_tokens if response.usage else None,
                latency_ms=latency_ms,
            )

            # Update conversation title if it's a new conversation
            if not conversation_id and content:
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
                SELECT role, content, tool_calls, model, tokens_in, tokens_out, latency_ms, created_at
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
