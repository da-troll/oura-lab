"""Main FastAPI application."""

import logging
from contextlib import asynccontextmanager
from datetime import date
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

logger = logging.getLogger(__name__)

from app.analysis import correlations, patterns
from app.oura import auth as oura_auth
from app.pipelines import features, ingest
from app.db import get_db
from app.settings import settings
from app.schemas import (
    AnomalyResponse,
    AuthStatusResponse,
    AuthUrlResponse,
    ChangePointResponse,
    ChronotypeResponse,
    ControlledCorrelationResponse,
    CorrelationMatrixResponse,
    DashboardResponse,
    DashboardSummary,
    ExchangeCodeRequest,
    ExchangeCodeResponse,
    HealthResponse,
    HeatmapPoint,
    HeatmapResponse,
    LaggedCorrelationResponse,
    PersonalInfoResponse,
    ScatterDataResponse,
    SleepArchitectureDay,
    SleepArchitectureResponse,
    SpearmanResponse,
    SyncResponse,
    TrendPoint,
    TrendSeries,
    WeeklyClusterResponse,
)


async def run_migrations():
    """Run SQL migrations on startup."""
    migrations_dir = Path(__file__).parent.parent / "migrations"
    if not migrations_dir.exists():
        logger.warning("Migrations directory not found: %s", migrations_dir)
        return

    migration_files = sorted(migrations_dir.glob("*.sql"))
    if not migration_files:
        return

    async with get_db() as conn:
        # Create migrations tracking table
        async with conn.cursor() as cur:
            await cur.execute("""
                CREATE TABLE IF NOT EXISTS _migrations (
                    filename TEXT PRIMARY KEY,
                    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            await conn.commit()

            # Check which migrations have been applied
            await cur.execute("SELECT filename FROM _migrations")
            applied = {row["filename"] for row in await cur.fetchall()}

        for migration_file in migration_files:
            if migration_file.name in applied:
                continue
            logger.info("Applying migration: %s", migration_file.name)
            sql = migration_file.read_text()
            async with conn.cursor() as cur:
                await cur.execute(sql)
                await cur.execute(
                    "INSERT INTO _migrations (filename) VALUES (%s)",
                    (migration_file.name,),
                )
            await conn.commit()
            logger.info("Applied migration: %s", migration_file.name)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan events."""
    await run_migrations()
    yield
    # Shutdown


app = FastAPI(
    title="Oura Analytics",
    description="Personal analytics service for Oura Ring data",
    version="0.1.0",
    lifespan=lifespan,
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in settings.cors_origins.split(",")],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """Health check endpoint."""
    return HealthResponse(ok=True)


# ============================================
# Dashboard Endpoints
# ============================================


@app.get("/dashboard", response_model=DashboardResponse)
async def get_dashboard(
    days: int = Query(default=7, description="Number of days for averages and trends"),
) -> DashboardResponse:
    """Get dashboard summary data."""
    if days not in (7, 10, 30):
        raise HTTPException(status_code=400, detail="days must be 7, 10, or 30")

    # Check auth status
    status = await oura_auth.get_auth_status()
    if not status["connected"]:
        return DashboardResponse(
            connected=False,
            summary=DashboardSummary(),
            trends=[],
        )

    async with get_db() as conn:
        async with conn.cursor() as cur:
            # Get averages for the selected period
            await cur.execute("""
                SELECT
                    AVG(readiness_score) as readiness_avg,
                    AVG(sleep_score) as sleep_score_avg,
                    AVG(activity_score) as activity_avg,
                    AVG(steps) as steps_avg,
                    AVG(hrv_average) as hrv_avg,
                    AVG(hr_lowest) as rhr_avg,
                    AVG(sleep_total_seconds / 3600.0) as sleep_hours_avg,
                    AVG(cal_total) as calories_avg,
                    AVG(stress_high_minutes) as stress_avg,
                    AVG(recovery_high_minutes) as recovery_avg,
                    AVG(spo2_average) as spo2_avg,
                    AVG(workout_total_minutes) as workout_minutes_avg,
                    COUNT(*) as days_with_data
                FROM oura_daily
                WHERE date >= CURRENT_DATE - %(days)s
                AND (readiness_score IS NOT NULL
                     OR sleep_score IS NOT NULL
                     OR activity_score IS NOT NULL
                     OR steps IS NOT NULL)
            """, {"days": days})
            summary_row = await cur.fetchone()

            # Get trend data for the selected period
            await cur.execute("""
                SELECT
                    date,
                    readiness_score,
                    sleep_score,
                    activity_score,
                    steps,
                    hrv_average,
                    hr_lowest as rhr,
                    sleep_total_seconds / 3600.0 as sleep_hours,
                    stress_high_minutes,
                    recovery_high_minutes,
                    spo2_average,
                    workout_total_minutes
                FROM oura_daily
                WHERE date >= CURRENT_DATE - %(days)s
                ORDER BY date
            """, {"days": days})
            trend_rows = await cur.fetchall()

    summary = DashboardSummary(
        readiness_avg=round(summary_row["readiness_avg"], 1) if summary_row["readiness_avg"] else None,
        sleep_score_avg=round(summary_row["sleep_score_avg"], 1) if summary_row["sleep_score_avg"] else None,
        activity_avg=round(summary_row["activity_avg"], 1) if summary_row["activity_avg"] else None,
        steps_avg=round(summary_row["steps_avg"]) if summary_row["steps_avg"] else None,
        hrv_avg=round(summary_row["hrv_avg"], 1) if summary_row["hrv_avg"] else None,
        rhr_avg=round(summary_row["rhr_avg"], 1) if summary_row["rhr_avg"] else None,
        sleep_hours_avg=round(summary_row["sleep_hours_avg"], 1) if summary_row["sleep_hours_avg"] else None,
        calories_avg=round(summary_row["calories_avg"]) if summary_row["calories_avg"] else None,
        stress_avg=round(summary_row["stress_avg"], 1) if summary_row["stress_avg"] else None,
        recovery_avg=round(summary_row["recovery_avg"], 1) if summary_row["recovery_avg"] else None,
        spo2_avg=round(summary_row["spo2_avg"], 1) if summary_row["spo2_avg"] else None,
        workout_minutes_avg=round(summary_row["workout_minutes_avg"], 1) if summary_row["workout_minutes_avg"] else None,
        days_with_data=summary_row["days_with_data"] or 0,
    )

    # Build trend series for each metric
    def build_trend(metric_key: str) -> list[TrendPoint]:
        return [
            TrendPoint(
                date=str(row["date"]),
                value=float(row[metric_key]) if row[metric_key] is not None else None,
            )
            for row in trend_rows
        ]

    trends = [
        TrendSeries(name="readiness", data=build_trend("readiness_score")),
        TrendSeries(name="sleep", data=build_trend("sleep_score")),
        TrendSeries(name="activity", data=build_trend("activity_score")),
        TrendSeries(name="steps", data=build_trend("steps")),
        TrendSeries(name="hrv", data=build_trend("hrv_average")),
        TrendSeries(name="rhr", data=build_trend("rhr")),
        TrendSeries(name="sleep_hours", data=build_trend("sleep_hours")),
        TrendSeries(name="stress", data=build_trend("stress_high_minutes")),
        TrendSeries(name="recovery", data=build_trend("recovery_high_minutes")),
        TrendSeries(name="spo2", data=build_trend("spo2_average")),
        TrendSeries(name="workout_minutes", data=build_trend("workout_total_minutes")),
    ]

    return DashboardResponse(
        connected=True,
        summary=summary,
        trends=trends,
    )


# ============================================
# OAuth Endpoints
# ============================================


@app.get("/auth/url", response_model=AuthUrlResponse)
async def get_auth_url() -> AuthUrlResponse:
    """Get Oura OAuth authorization URL."""
    url, state = await oura_auth.get_auth_url()
    return AuthUrlResponse(url=url, state=state)


@app.post("/auth/oura/exchange", response_model=ExchangeCodeResponse)
async def exchange_code(request: ExchangeCodeRequest) -> ExchangeCodeResponse:
    """Exchange OAuth authorization code for tokens.

    This endpoint is called by the Next.js callback handler.
    The client_secret is used here on the server side.
    """
    try:
        tokens = await oura_auth.exchange_code(request.code)
        await oura_auth.store_tokens(tokens)
        return ExchangeCodeResponse(success=True, message="Connected to Oura")
    except oura_auth.OAuthError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/auth/status", response_model=AuthStatusResponse)
async def get_auth_status() -> AuthStatusResponse:
    """Get current authentication status."""
    status = await oura_auth.get_auth_status()
    return AuthStatusResponse(
        connected=status["connected"],
        expires_at=status.get("expires_at"),
        scopes=status.get("scopes"),
    )


@app.post("/auth/revoke")
async def revoke_auth():
    """Disconnect from Oura (clear stored tokens)."""
    await oura_auth.clear_auth()
    return {"success": True, "message": "Disconnected from Oura"}


# ============================================
# Admin Endpoints
# ============================================


@app.post("/admin/ingest", response_model=SyncResponse)
async def admin_ingest(
    start: date = Query(..., description="Start date (YYYY-MM-DD)"),
    end: date = Query(..., description="End date (YYYY-MM-DD)"),
):
    """Run the full ingestion pipeline for a date range.

    This fetches data from Oura API, stores raw payloads,
    and normalizes into the daily tables.
    """
    try:
        result = await ingest.run_full_ingest(start, end)
        return SyncResponse(
            status="completed",
            days_processed=result["days_processed"],
            message=f"Ingested {result['days_processed']} days, {result['tags_processed']} tags",
        )
    except oura_auth.OAuthError as e:
        raise HTTPException(status_code=401, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/admin/features", response_model=SyncResponse)
async def admin_features(
    start: date = Query(..., description="Start date (YYYY-MM-DD)"),
    end: date = Query(..., description="End date (YYYY-MM-DD)"),
):
    """Compute derived features for a date range.

    This computes rolling means, lags, deltas, and variability features
    from the daily data.
    """
    try:
        days_processed = await features.recompute_features(start, end)
        return SyncResponse(
            status="completed",
            days_processed=days_processed,
            message=f"Computed features for {days_processed} days",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================
# Correlation Endpoints
# ============================================


@app.post("/analyze/correlations/spearman", response_model=SpearmanResponse)
async def analyze_spearman(
    target: str = Query(..., description="Target metric"),
    candidates: list[str] = Query(..., description="Candidate metrics"),
    start: date | None = Query(None, description="Start date (optional)"),
    end: date | None = Query(None, description="End date (optional)"),
):
    """Compute Spearman correlations between target and candidate metrics."""
    result = await correlations.get_spearman_correlations(
        target, candidates, start, end
    )
    return SpearmanResponse(
        target=result["target"],
        correlations=[
            {
                "metric": c["metric"],
                "rho": c["rho"],
                "p_value": c["p_value"],
                "n": c["n"],
            }
            for c in result["correlations"]
        ],
    )


@app.post("/analyze/correlations/matrix", response_model=CorrelationMatrixResponse)
async def analyze_correlation_matrix(
    metrics: list[str] = Query(..., description="Metrics to include in matrix"),
    start: date | None = Query(None, description="Start date (optional)"),
    end: date | None = Query(None, description="End date (optional)"),
):
    """Compute pairwise Spearman correlation matrix for selected metrics."""
    result = await correlations.get_correlation_matrix(metrics, start, end)
    return CorrelationMatrixResponse(
        metrics=result["metrics"],
        matrix=result["matrix"],
        p_values=result["p_values"],
        n_matrix=result["n_matrix"],
    )


@app.post("/analyze/correlations/scatter-data", response_model=ScatterDataResponse)
async def analyze_scatter_data(
    metric_x: str = Query(..., description="X-axis metric"),
    metric_y: str = Query(..., description="Y-axis metric"),
    start: date | None = Query(None, description="Start date (optional)"),
    end: date | None = Query(None, description="End date (optional)"),
):
    """Get scatter plot data for two metrics."""
    result = await correlations.get_scatter_data(metric_x, metric_y, start, end)
    return ScatterDataResponse(
        metric_x=result["metric_x"],
        metric_y=result["metric_y"],
        points=[
            {"x": p["x"], "y": p["y"], "date": p["date"]}
            for p in result["points"]
        ],
        n=result["n"],
    )


@app.post("/analyze/correlations/lagged", response_model=LaggedCorrelationResponse)
async def analyze_lagged(
    metric_x: str = Query(..., description="Predictor metric"),
    metric_y: str = Query(..., description="Target metric"),
    max_lag: int = Query(7, description="Maximum lag to test"),
    start: date | None = Query(None, description="Start date (optional)"),
    end: date | None = Query(None, description="End date (optional)"),
):
    """Compute lagged correlations to find if X predicts Y."""
    result = await correlations.get_lagged_correlations(
        metric_x, metric_y, max_lag, start, end
    )
    return LaggedCorrelationResponse(
        metric_x=result["metric_x"],
        metric_y=result["metric_y"],
        lags=[
            {
                "lag": l["lag"],
                "rho": l["rho"],
                "p_value": l["p_value"],
                "n": l["n"],
            }
            for l in result["lags"]
        ],
        best_lag=result["best_lag"],
    )


@app.post("/analyze/correlations/controlled", response_model=ControlledCorrelationResponse)
async def analyze_controlled(
    metric_x: str = Query(..., description="First metric"),
    metric_y: str = Query(..., description="Second metric"),
    control_vars: list[str] = Query(..., description="Variables to control for"),
    start: date | None = Query(None, description="Start date (optional)"),
    end: date | None = Query(None, description="End date (optional)"),
):
    """Compute partial correlation controlling for confounders."""
    result = await correlations.get_controlled_correlation(
        metric_x, metric_y, control_vars, start, end
    )
    return ControlledCorrelationResponse(
        metric_x=result["metric_x"],
        metric_y=result["metric_y"],
        rho=result["rho"],
        p_value=result["p_value"],
        n=result["n"],
        controlled_for=result["controlled_for"],
    )


# ============================================
# Pattern Endpoints
# ============================================


@app.post("/analyze/patterns/change-points", response_model=ChangePointResponse)
async def analyze_change_points(
    metric: str = Query(..., description="Metric to analyze"),
    start: date | None = Query(None, description="Start date (optional)"),
    end: date | None = Query(None, description="End date (optional)"),
    penalty: float = Query(10.0, description="PELT penalty parameter"),
):
    """Detect change points in a metric time series."""
    result = await patterns.get_change_points(metric, start, end, penalty)
    return ChangePointResponse(
        metric=result["metric"],
        change_points=[
            {
                "date": cp.get("date", ""),
                "index": cp["index"],
                "before_mean": cp["before_mean"],
                "after_mean": cp["after_mean"],
                "magnitude": cp["magnitude"],
                "direction": cp["direction"],
            }
            for cp in result["change_points"]
        ],
    )


@app.post("/analyze/patterns/anomalies", response_model=AnomalyResponse)
async def analyze_anomalies(
    metric: str = Query(..., description="Metric to analyze"),
    start: date | None = Query(None, description="Start date (optional)"),
    end: date | None = Query(None, description="End date (optional)"),
    threshold: float = Query(3.0, description="Z-score threshold"),
):
    """Detect anomalies in a metric time series."""
    result = await patterns.get_anomalies(metric, start, end, threshold)
    return AnomalyResponse(
        metric=result["metric"],
        anomalies=[
            {
                "date": a.get("date", ""),
                "value": a["value"],
                "z_score": a["z_score"],
                "direction": a["direction"],
            }
            for a in result["anomalies"]
        ],
    )


@app.post("/analyze/patterns/weekly-clusters", response_model=WeeklyClusterResponse)
async def analyze_weekly_clusters(
    features_list: list[str] = Query(..., alias="features", description="Features for clustering"),
    n_clusters: int = Query(4, description="Number of clusters"),
    start: date | None = Query(None, description="Start date (optional)"),
    end: date | None = Query(None, description="End date (optional)"),
):
    """Cluster weeks based on feature patterns."""
    result = await patterns.get_weekly_clusters(features_list, n_clusters, start, end)
    return WeeklyClusterResponse(
        weeks=[
            {
                "year": w["year"],
                "week": w["week"],
                "cluster": w["cluster"],
                "label": w.get("label"),
            }
            for w in result["weeks"]
        ],
        cluster_profiles=result["cluster_profiles"],
    )


# ============================================
# Insights Endpoints (Phase 1)
# ============================================


@app.get("/insights/heatmap", response_model=HeatmapResponse)
async def get_heatmap(
    metric: str = Query(..., description="Metric to display"),
    days: int = Query(365, description="Number of days to show"),
):
    """Get annual heatmap data for a metric."""
    async with get_db() as conn:
        async with conn.cursor() as cur:
            # Map friendly metric names to actual columns
            metric_map = {
                "readiness_score": "readiness_score",
                "sleep_score": "sleep_score",
                "activity_score": "activity_score",
                "steps": "steps",
                "hrv_average": "hrv_average",
                "hr_lowest": "hr_lowest",
                "sleep_total_seconds": "sleep_total_seconds / 3600.0",
                "sleep_efficiency": "sleep_efficiency",
                "sleep_deep_seconds": "sleep_deep_seconds / 3600.0",
                "sleep_rem_seconds": "sleep_rem_seconds / 3600.0",
                "cal_total": "cal_total",
                "cal_active": "cal_active",
                "stress_high_minutes": "stress_high_minutes",
                "recovery_high_minutes": "recovery_high_minutes",
                "spo2_average": "spo2_average",
                "vascular_age": "vascular_age",
                "workout_total_minutes": "workout_total_minutes",
                "workout_count": "workout_count",
                "sleep_breath_average": "sleep_breath_average",
            }

            column = metric_map.get(metric, metric)

            await cur.execute(f"""
                SELECT
                    date,
                    {column} as value
                FROM oura_daily
                WHERE date >= CURRENT_DATE - INTERVAL '{days} days'
                ORDER BY date
            """)
            rows = await cur.fetchall()

            # Calculate min/max for color scaling
            values = [r["value"] for r in rows if r["value"] is not None]
            min_val = min(values) if values else None
            max_val = max(values) if values else None

    return HeatmapResponse(
        metric=metric,
        data=[
            HeatmapPoint(date=str(r["date"]), value=float(r["value"]) if r["value"] else None)
            for r in rows
        ],
        min_value=min_val,
        max_value=max_val,
    )


@app.get("/insights/sleep-architecture", response_model=SleepArchitectureResponse)
async def get_sleep_architecture(
    days: int = Query(30, description="Number of days to show"),
):
    """Get sleep stage architecture data."""
    async with get_db() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT
                    date,
                    sleep_total_seconds,
                    sleep_deep_seconds,
                    sleep_rem_seconds
                FROM oura_daily
                WHERE date >= CURRENT_DATE - INTERVAL '%s days'
                AND sleep_total_seconds IS NOT NULL
                AND sleep_total_seconds > 0
                ORDER BY date
            """, (days,))
            rows = await cur.fetchall()

    data = []
    total_deep_pct = 0
    total_rem_pct = 0
    total_light_pct = 0
    valid_days = 0

    for r in rows:
        total = r["sleep_total_seconds"]
        deep = r["sleep_deep_seconds"] or 0
        rem = r["sleep_rem_seconds"] or 0
        light = total - deep - rem

        if total > 0:
            deep_pct = round((deep / total) * 100, 1)
            rem_pct = round((rem / total) * 100, 1)
            light_pct = round((light / total) * 100, 1)
            total_hours = round(total / 3600, 1)

            data.append(SleepArchitectureDay(
                date=str(r["date"]),
                deep_pct=deep_pct,
                rem_pct=rem_pct,
                light_pct=light_pct,
                total_hours=total_hours,
            ))

            total_deep_pct += deep_pct
            total_rem_pct += rem_pct
            total_light_pct += light_pct
            valid_days += 1

    return SleepArchitectureResponse(
        data=data,
        avg_deep_pct=round(total_deep_pct / valid_days, 1) if valid_days else None,
        avg_rem_pct=round(total_rem_pct / valid_days, 1) if valid_days else None,
        avg_light_pct=round(total_light_pct / valid_days, 1) if valid_days else None,
    )


@app.get("/insights/chronotype", response_model=ChronotypeResponse)
async def get_chronotype():
    """Analyze chronotype and social jetlag from sleep patterns."""
    async with get_db() as conn:
        async with conn.cursor() as cur:
            # Get sleep data with bedtime start info
            # We need to query the raw sleep data to get bedtime_start and bedtime_end
            await cur.execute("""
                SELECT
                    date,
                    is_weekend,
                    sleep_total_seconds
                FROM oura_daily
                WHERE sleep_total_seconds IS NOT NULL
                AND sleep_total_seconds > 0
                ORDER BY date DESC
                LIMIT 90
            """)
            daily_rows = await cur.fetchall()

            # Query raw sleep data for bedtime_start and bedtime_end
            await cur.execute("""
                SELECT
                    r.day as date,
                    r.payload->>'bedtime_start' as bedtime_start,
                    r.payload->>'bedtime_end' as bedtime_end,
                    d.is_weekend
                FROM oura_raw r
                JOIN oura_daily d ON r.day = d.date
                WHERE r.source = 'sleep'
                AND r.payload->>'type' = 'long_sleep'
                ORDER BY r.day DESC
                LIMIT 90
            """)
            raw_rows = await cur.fetchall()

    if not raw_rows:
        return ChronotypeResponse(
            chronotype="unknown",
            chronotype_label="Insufficient Data",
            weekend_midpoint=None,
            weekday_midpoint=None,
            social_jetlag_minutes=None,
            social_jetlag_label="N/A",
            recommendation="Need more sleep data to determine chronotype.",
        )

    from datetime import datetime, timedelta

    def parse_sleep_midpoint(bedtime_start: str, bedtime_end: str) -> float | None:
        """Calculate sleep midpoint as hours from midnight."""
        try:
            start = datetime.fromisoformat(bedtime_start.replace("Z", "+00:00"))
            end = datetime.fromisoformat(bedtime_end.replace("Z", "+00:00"))
            midpoint = start + (end - start) / 2

            # Convert to hours from midnight (handling overnight sleep)
            hours = midpoint.hour + midpoint.minute / 60
            # If midpoint is before 6am, add 24 to normalize (sleep went past midnight)
            if hours < 6:
                hours += 24
            return hours
        except (ValueError, TypeError):
            return None

    weekend_midpoints = []
    weekday_midpoints = []

    for r in raw_rows:
        if r["bedtime_start"] and r["bedtime_end"]:
            midpoint = parse_sleep_midpoint(r["bedtime_start"], r["bedtime_end"])
            if midpoint:
                if r["is_weekend"]:
                    weekend_midpoints.append(midpoint)
                else:
                    weekday_midpoints.append(midpoint)

    if not weekend_midpoints or not weekday_midpoints:
        return ChronotypeResponse(
            chronotype="unknown",
            chronotype_label="Insufficient Data",
            weekend_midpoint=None,
            weekday_midpoint=None,
            social_jetlag_minutes=None,
            social_jetlag_label="N/A",
            recommendation="Need more weekend and weekday sleep data.",
        )

    avg_weekend = sum(weekend_midpoints) / len(weekend_midpoints)
    avg_weekday = sum(weekday_midpoints) / len(weekday_midpoints)

    # Social jetlag is the difference
    jetlag_hours = abs(avg_weekend - avg_weekday)
    jetlag_minutes = int(jetlag_hours * 60)

    # Format midpoints as HH:MM
    def hours_to_time(h: float) -> str:
        h = h % 24  # Normalize back
        hours = int(h)
        minutes = int((h - hours) * 60)
        return f"{hours:02d}:{minutes:02d}"

    weekend_midpoint_str = hours_to_time(avg_weekend)
    weekday_midpoint_str = hours_to_time(avg_weekday)

    # Determine chronotype based on weekend midpoint
    # Before 3am = Morning Lark, After 5am = Night Owl, Between = Intermediate
    if avg_weekend < 27:  # Before 3am (27 = 3am in our 24+ system)
        chronotype = "morning_lark"
        chronotype_label = "Morning Lark 🌅"
    elif avg_weekend > 29:  # After 5am
        chronotype = "night_owl"
        chronotype_label = "Night Owl 🦉"
    else:
        chronotype = "intermediate"
        chronotype_label = "Intermediate ⚖️"

    # Format jetlag label
    jetlag_h = jetlag_minutes // 60
    jetlag_m = jetlag_minutes % 60
    if jetlag_h > 0:
        jetlag_label = f"{jetlag_h}h {jetlag_m}m"
    else:
        jetlag_label = f"{jetlag_m}m"

    # Generate recommendation
    if jetlag_minutes > 90:
        recommendation = "High social jetlag detected. Try to keep sleep times more consistent between weekdays and weekends to improve energy levels."
    elif jetlag_minutes > 60:
        recommendation = "Moderate social jetlag. Consider gradually aligning your weekday and weekend sleep schedules."
    else:
        recommendation = "Good sleep consistency! Your sleep schedule is well-aligned between weekdays and weekends."

    return ChronotypeResponse(
        chronotype=chronotype,
        chronotype_label=chronotype_label,
        weekend_midpoint=weekend_midpoint_str,
        weekday_midpoint=weekday_midpoint_str,
        social_jetlag_minutes=jetlag_minutes,
        social_jetlag_label=jetlag_label,
        recommendation=recommendation,
    )


@app.get("/personal-info", response_model=PersonalInfoResponse)
async def get_personal_info() -> PersonalInfoResponse:
    """Get stored personal info."""
    async with get_db() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT * FROM oura_personal_info WHERE id = 1")
            row = await cur.fetchone()

    if not row:
        return PersonalInfoResponse()

    return PersonalInfoResponse(
        age=row["age"],
        weight=float(row["weight"]) if row["weight"] else None,
        height=float(row["height"]) if row["height"] else None,
        biological_sex=row["biological_sex"],
        email=row["email"],
        fetched_at=row["fetched_at"],
    )
