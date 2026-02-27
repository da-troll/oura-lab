"""Main FastAPI application."""

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from datetime import date
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

logger = logging.getLogger(__name__)

from app.analysis import correlations, patterns
from app.auth import (
    LoginRateLimiter,
    authenticate_user,
    cleanup_expired_sessions,
    create_session,
    create_user,
    delete_session,
    validate_password,
)
from app.db import get_db_for_user, get_db_system
from app.dependencies import get_current_user
from app.oura import auth as oura_auth
from app.pipelines import features, ingest
from app.settings import settings
from app.schemas import (
    AnomalyResponse,
    AuthResponse,
    AuthStatusResponse,
    AuthUrlResponse,
    ChangePointResponse,
    ChatRequest,
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
    LoginRequest,
    MeResponse,
    PersonalInfoResponse,
    RegisterRequest,
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
    """Run SQL migrations on startup (dev-only, gated by ENABLE_AUTO_MIGRATE)."""
    if not settings.enable_auto_migrate:
        logger.info("Auto-migration disabled (set ENABLE_AUTO_MIGRATE=true to enable)")
        return

    migrations_dir = Path(__file__).parent.parent / "migrations"
    if not migrations_dir.exists():
        logger.warning("Migrations directory not found: %s", migrations_dir)
        return

    migration_files = sorted(migrations_dir.glob("*.sql"))
    if not migration_files:
        return

    async with get_db_system() as conn:
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


async def periodic_cleanup():
    """Periodically clean up expired sessions and oauth states."""
    while True:
        await asyncio.sleep(3600)  # Every hour
        try:
            count = await cleanup_expired_sessions()
            if count:
                logger.info("Cleaned up %d expired sessions/states", count)
        except Exception:
            logger.exception("Error in periodic cleanup")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan events."""
    # Validate TOKEN_ENCRYPTION_KEY is set and is a valid Fernet key
    if not settings.token_encryption_key:
        raise RuntimeError(
            "TOKEN_ENCRYPTION_KEY is not set. "
            "Generate one with: python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\""
        )
    try:
        from cryptography.fernet import Fernet
        Fernet(settings.token_encryption_key.encode())
    except Exception as e:
        raise RuntimeError(f"TOKEN_ENCRYPTION_KEY is not a valid Fernet key: {e}") from e

    await run_migrations()

    # Runtime DB-role guard
    if settings.expected_db_role:
        async with get_db_system() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT current_user")
                row = await cur.fetchone()
                actual_role = row["current_user"]
                if actual_role != settings.expected_db_role:
                    raise RuntimeError(
                        f"DB role mismatch: expected '{settings.expected_db_role}', "
                        f"got '{actual_role}'. Check DATABASE_URL credentials."
                    )
        logger.info("DB role guard passed: connected as '%s'", settings.expected_db_role)

    # Start periodic cleanup task
    cleanup_task = asyncio.create_task(periodic_cleanup())
    yield
    cleanup_task.cancel()


login_rate_limiter = LoginRateLimiter(
    max_attempts=settings.login_rate_limit_per_minute,
    window_seconds=60,
)

app = FastAPI(
    title="Oura Analytics",
    description="Personal analytics service for Oura Ring data",
    version="0.2.0",
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
# User Auth Endpoints
# ============================================


@app.post("/auth/register", response_model=AuthResponse)
async def register(body: RegisterRequest, request: Request):
    """Register a new user."""
    error = validate_password(body.password)
    if error:
        raise HTTPException(status_code=400, detail=error)

    try:
        user = await create_user(body.email, body.password)
    except Exception as e:
        if "unique" in str(e).lower() or "duplicate" in str(e).lower():
            raise HTTPException(status_code=409, detail="Email already registered")
        raise HTTPException(status_code=500, detail="Registration failed")

    session = await create_session(
        user["id"],
        ip=request.client.host if request.client else None,
        user_agent=request.headers.get("user-agent"),
    )

    return AuthResponse(
        user_id=user["id"],
        email=user["email"],
        session_token=session["token"],
        expires_at=session["expires_at"],
    )


@app.post("/auth/login", response_model=AuthResponse)
async def login(body: LoginRequest, request: Request):
    """Login with email and password."""
    # Resolve client IP for rate limiting/audit. Prefer forwarded chain (last hop),
    # otherwise fall back to direct client address.
    ip = request.client.host if request.client else None
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        hops = [hop.strip() for hop in forwarded.split(",") if hop.strip()]
        if hops:
            ip = hops[-1]
    if not ip:
        ip = "unknown"

    if not await login_rate_limiter.check(ip):
        raise HTTPException(
            status_code=429,
            detail="Too many login attempts. Please try again later.",
        )

    user = await authenticate_user(body.email, body.password)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid email or password")

    session = await create_session(
        user["id"],
        ip=ip,
        user_agent=request.headers.get("user-agent"),
    )

    return AuthResponse(
        user_id=user["id"],
        email=user["email"],
        session_token=session["token"],
        expires_at=session["expires_at"],
    )


@app.post("/auth/logout")
async def logout(request: Request, user: dict = Depends(get_current_user)):
    """Logout (delete session). Requires valid Bearer token."""
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        token = auth_header[7:]
        if token:
            await delete_session(token)
    return {"success": True}


@app.get("/auth/me", response_model=MeResponse)
async def get_me(user: dict = Depends(get_current_user)):
    """Get current user info."""
    return MeResponse(user_id=user["user_id"], email=user["email"])


# ============================================
# Dashboard Endpoints
# ============================================


@app.get("/dashboard", response_model=DashboardResponse)
async def get_dashboard(
    days: int = Query(default=7, description="Number of days for averages and trends"),
    user: dict = Depends(get_current_user),
) -> DashboardResponse:
    """Get dashboard summary data."""
    if days not in (7, 10, 30, 60, 100):
        raise HTTPException(status_code=400, detail="days must be 7, 10, 30, 60, or 100")

    user_id = user["user_id"]

    # Check auth status
    async with get_db_for_user(user_id) as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT user_id FROM oura_auth WHERE user_id = %s", (user_id,)
            )
            auth_row = await cur.fetchone()

    if not auth_row:
        return DashboardResponse(
            connected=False,
            summary=DashboardSummary(),
            trends=[],
        )

    async with get_db_for_user(user_id) as conn:
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
                AND user_id = %(user_id)s
                AND (readiness_score IS NOT NULL
                     OR sleep_score IS NOT NULL
                     OR activity_score IS NOT NULL
                     OR steps IS NOT NULL)
            """, {"days": days, "user_id": user_id})
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
                AND user_id = %(user_id)s
                ORDER BY date
            """, {"days": days, "user_id": user_id})
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


@app.get("/auth/oura/url", response_model=AuthUrlResponse)
async def get_auth_url(user: dict = Depends(get_current_user)) -> AuthUrlResponse:
    """Get Oura OAuth authorization URL."""
    url, state = await oura_auth.get_auth_url(user["user_id"])
    return AuthUrlResponse(url=url, state=state)


@app.post("/auth/oura/exchange", response_model=ExchangeCodeResponse)
async def exchange_code(
    request: ExchangeCodeRequest,
    user: dict = Depends(get_current_user),
) -> ExchangeCodeResponse:
    """Exchange OAuth authorization code for tokens."""
    # Validate and consume the OAuth state (single-use, user-bound)
    state_valid = await oura_auth.consume_oauth_state(request.state, user["user_id"])
    if not state_valid:
        raise HTTPException(
            status_code=400,
            detail="Invalid or expired OAuth state. Please restart the authorization flow.",
        )

    try:
        tokens = await oura_auth.exchange_code(request.code)
        await oura_auth.store_tokens(tokens, user["user_id"])
        return ExchangeCodeResponse(success=True, message="Connected to Oura")
    except oura_auth.OAuthError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/auth/oura/status", response_model=AuthStatusResponse)
async def get_auth_status(
    user: dict = Depends(get_current_user),
) -> AuthStatusResponse:
    """Get current Oura authentication status."""
    status = await oura_auth.get_auth_status(user["user_id"])
    return AuthStatusResponse(
        connected=status["connected"],
        expires_at=status.get("expires_at"),
        scopes=status.get("scopes"),
    )


@app.post("/auth/oura/revoke")
async def revoke_auth(user: dict = Depends(get_current_user)):
    """Disconnect from Oura (clear stored tokens)."""
    await oura_auth.clear_auth(user["user_id"])
    return {"success": True, "message": "Disconnected from Oura"}


# ============================================
# Admin Endpoints
# ============================================


@app.post("/admin/ingest", response_model=SyncResponse)
async def admin_ingest(
    start: date | None = Query(None, description="Start date (YYYY-MM-DD, optional)"),
    end: date | None = Query(None, description="End date (YYYY-MM-DD, optional)"),
    user: dict = Depends(get_current_user),
):
    """Run ingestion pipeline.

    If start/end are omitted, performs automatic sync:
    - First sync: backfill from oldest available Oura day.
    - Later syncs: fetch only missing days since latest stored date.
    """
    if (start is None) != (end is None):
        raise HTTPException(status_code=400, detail="Provide both start and end, or neither")

    try:
        result = await ingest.run_full_ingest(start, end, user["user_id"])

        if result["days_processed"] == 0:
            if result.get("sync_mode") == "incremental":
                msg = "Already up to date. No new days to sync."
            else:
                msg = "No Oura data found to sync yet."
        else:
            msg = (
                f"Ingested {result['days_processed']} days"
                f" ({result.get('sync_mode', 'manual')})"
            )

        return SyncResponse(
            status="completed",
            days_processed=result["days_processed"],
            message=msg,
            start_date=result.get("start_date"),
            end_date=result.get("end_date"),
            sync_mode=result.get("sync_mode"),
        )
    except oura_auth.OAuthError as e:
        raise HTTPException(status_code=401, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/admin/ingest/stream")
async def admin_ingest_stream(
    start: date | None = Query(None, description="Start date (YYYY-MM-DD, optional)"),
    end: date | None = Query(None, description="End date (YYYY-MM-DD, optional)"),
    user: dict = Depends(get_current_user),
):
    """Run ingestion pipeline and stream progress as NDJSON."""
    if (start is None) != (end is None):
        raise HTTPException(status_code=400, detail="Provide both start and end, or neither")

    async def stream():
        async for event in ingest.run_full_ingest_stream(start, end, user["user_id"]):
            yield json.dumps(event) + "\n"

    return StreamingResponse(
        stream(),
        media_type="application/x-ndjson",
        headers={"Cache-Control": "no-cache"},
    )


@app.post("/admin/features", response_model=SyncResponse)
async def admin_features(
    start: date = Query(..., description="Start date (YYYY-MM-DD)"),
    end: date = Query(..., description="End date (YYYY-MM-DD)"),
    user: dict = Depends(get_current_user),
):
    """Compute derived features for a date range."""
    try:
        days_processed = await features.recompute_features(start, end, user["user_id"])
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
    user: dict = Depends(get_current_user),
):
    """Compute Spearman correlations between target and candidate metrics."""
    result = await correlations.get_spearman_correlations(
        target, candidates, start, end, user["user_id"]
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
    user: dict = Depends(get_current_user),
):
    """Compute pairwise Spearman correlation matrix for selected metrics."""
    result = await correlations.get_correlation_matrix(metrics, start, end, user["user_id"])
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
    user: dict = Depends(get_current_user),
):
    """Get scatter plot data for two metrics."""
    result = await correlations.get_scatter_data(metric_x, metric_y, start, end, user["user_id"])
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
    user: dict = Depends(get_current_user),
):
    """Compute lagged correlations to find if X predicts Y."""
    result = await correlations.get_lagged_correlations(
        metric_x, metric_y, max_lag, start, end, user["user_id"]
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
    user: dict = Depends(get_current_user),
):
    """Compute partial correlation controlling for confounders."""
    result = await correlations.get_controlled_correlation(
        metric_x, metric_y, control_vars, start, end, user["user_id"]
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
    user: dict = Depends(get_current_user),
):
    """Detect change points in a metric time series."""
    result = await patterns.get_change_points(metric, start, end, penalty, user["user_id"])
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
    user: dict = Depends(get_current_user),
):
    """Detect anomalies in a metric time series."""
    result = await patterns.get_anomalies(metric, start, end, threshold, user["user_id"])
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
    user: dict = Depends(get_current_user),
):
    """Cluster weeks based on feature patterns."""
    result = await patterns.get_weekly_clusters(
        features_list, n_clusters, start, end, user["user_id"]
    )
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
# Insights Endpoints
# ============================================


@app.get("/insights/heatmap", response_model=HeatmapResponse)
async def get_heatmap(
    metric: str = Query(..., description="Metric to display"),
    days: int = Query(365, description="Number of days to show"),
    user: dict = Depends(get_current_user),
):
    """Get annual heatmap data for a metric."""
    user_id = user["user_id"]

    # Map friendly metric names to actual SQL column expressions
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

    if metric not in metric_map:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid metric '{metric}'. Must be one of: {', '.join(sorted(metric_map))}",
        )

    if not (1 <= days <= 3660):
        raise HTTPException(status_code=400, detail="days must be between 1 and 3660")

    column = metric_map[metric]

    async with get_db_for_user(user_id) as conn:
        async with conn.cursor() as cur:
            await cur.execute(f"""
                SELECT
                    date,
                    {column} as value
                FROM oura_daily
                WHERE date >= CURRENT_DATE - %(days)s * INTERVAL '1 day'
                AND user_id = %(user_id)s
                ORDER BY date
            """, {"user_id": user_id, "days": days})
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
    user: dict = Depends(get_current_user),
):
    """Get sleep stage architecture data."""
    user_id = user["user_id"]

    async with get_db_for_user(user_id) as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT
                    date,
                    sleep_total_seconds,
                    sleep_deep_seconds,
                    sleep_rem_seconds
                FROM oura_daily
                WHERE date >= CURRENT_DATE - %(days)s * INTERVAL '1 day'
                AND user_id = %(user_id)s
                AND sleep_total_seconds IS NOT NULL
                AND sleep_total_seconds > 0
                ORDER BY date
            """, {"days": days, "user_id": user_id})
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
async def get_chronotype(user: dict = Depends(get_current_user)):
    """Analyze chronotype and social jetlag from sleep patterns."""
    user_id = user["user_id"]

    async with get_db_for_user(user_id) as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT
                    date,
                    is_weekend,
                    sleep_total_seconds
                FROM oura_daily
                WHERE sleep_total_seconds IS NOT NULL
                AND sleep_total_seconds > 0
                AND user_id = %s
                ORDER BY date DESC
                LIMIT 90
            """, (user_id,))
            daily_rows = await cur.fetchall()

            # Query raw sleep data for bedtime_start and bedtime_end
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

    from datetime import datetime

    def parse_sleep_midpoint(bedtime_start: str, bedtime_end: str) -> float | None:
        """Calculate sleep midpoint as hours from midnight."""
        try:
            start = datetime.fromisoformat(bedtime_start.replace("Z", "+00:00"))
            end = datetime.fromisoformat(bedtime_end.replace("Z", "+00:00"))
            midpoint = start + (end - start) / 2
            hours = midpoint.hour + midpoint.minute / 60
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
    jetlag_hours = abs(avg_weekend - avg_weekday)
    jetlag_minutes = int(jetlag_hours * 60)

    def hours_to_time(h: float) -> str:
        h = h % 24
        hours = int(h)
        minutes = int((h - hours) * 60)
        return f"{hours:02d}:{minutes:02d}"

    weekend_midpoint_str = hours_to_time(avg_weekend)
    weekday_midpoint_str = hours_to_time(avg_weekday)

    if avg_weekend < 27:
        chronotype = "morning_lark"
        chronotype_label = "Morning Lark"
    elif avg_weekend > 29:
        chronotype = "night_owl"
        chronotype_label = "Night Owl"
    else:
        chronotype = "intermediate"
        chronotype_label = "Intermediate"

    jetlag_h = jetlag_minutes // 60
    jetlag_m = jetlag_minutes % 60
    if jetlag_h > 0:
        jetlag_label = f"{jetlag_h}h {jetlag_m}m"
    else:
        jetlag_label = f"{jetlag_m}m"

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
async def get_personal_info(
    user: dict = Depends(get_current_user),
) -> PersonalInfoResponse:
    """Get stored personal info."""
    user_id = user["user_id"]

    async with get_db_for_user(user_id) as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT * FROM oura_personal_info WHERE user_id = %s", (user_id,)
            )
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


# ============================================
# Chat Endpoints (behind feature flag)
# ============================================


@app.get("/chat/status")
async def chat_status():
    """Check if chat feature is enabled."""
    return {"enabled": settings.chat_enabled}


@app.post("/chat")
async def chat_endpoint(
    body: ChatRequest,
    user: dict = Depends(get_current_user),
):
    """Stream a chat response as NDJSON."""
    if not settings.chat_enabled:
        raise HTTPException(status_code=403, detail="Chat feature is not enabled")

    from app.chat import run_chat
    from fastapi.responses import StreamingResponse

    return StreamingResponse(
        run_chat(user["user_id"], body.message, body.conversation_id),
        media_type="application/x-ndjson",
    )


@app.get("/chat/conversations")
async def list_conversations(user: dict = Depends(get_current_user)):
    """List user's chat conversations."""
    if not settings.chat_enabled:
        raise HTTPException(status_code=403, detail="Chat feature is not enabled")

    from app.chat import get_conversations
    return await get_conversations(user["user_id"])


@app.get("/chat/conversations/{conversation_id}")
async def get_conversation(
    conversation_id: str,
    user: dict = Depends(get_current_user),
):
    """Get messages for a specific conversation."""
    if not settings.chat_enabled:
        raise HTTPException(status_code=403, detail="Chat feature is not enabled")

    from app.chat import get_conversation_messages
    return await get_conversation_messages(user["user_id"], conversation_id)


@app.delete("/chat/conversations/{conversation_id}")
async def delete_conversation_endpoint(
    conversation_id: str,
    user: dict = Depends(get_current_user),
):
    """Delete a conversation."""
    if not settings.chat_enabled:
        raise HTTPException(status_code=403, detail="Chat feature is not enabled")

    from app.chat import delete_conversation
    deleted = await delete_conversation(user["user_id"], conversation_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Conversation not found")
    return {"success": True}
