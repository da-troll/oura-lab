"""Pydantic schemas mirroring the shared Zod schemas."""

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, Field


# ============================================
# Health Check
# ============================================


class HealthResponse(BaseModel):
    """Health check response."""

    ok: bool


# ============================================
# User Auth (registration / login / session)
# ============================================


class RegisterRequest(BaseModel):
    """Request to register a new user."""

    email: str
    password: str = Field(min_length=8)


class LoginRequest(BaseModel):
    """Request to login."""

    email: str
    password: str


class AuthResponse(BaseModel):
    """Response from register or login."""

    user_id: str
    email: str
    session_token: str
    expires_at: datetime


class MeResponse(BaseModel):
    """Current user info."""

    user_id: str
    email: str


# ============================================
# Oura OAuth
# ============================================


class AuthStatusResponse(BaseModel):
    """Oura authentication status response."""

    connected: bool
    expires_at: datetime | None = None
    scopes: list[str] | None = None


class AuthUrlResponse(BaseModel):
    """OAuth authorization URL response."""

    url: str
    state: str


class ExchangeCodeRequest(BaseModel):
    """Request to exchange OAuth code for tokens."""

    code: str
    state: str | None = None


class ExchangeCodeResponse(BaseModel):
    """Response from token exchange."""

    success: bool
    message: str | None = None


# ============================================
# Time Series
# ============================================


class SeriesMetricRequest(BaseModel):
    """Request for time series data."""

    metric: str
    start: date
    end: date
    filters: dict[str, str] | None = None


class SeriesPoint(BaseModel):
    """Single point in a time series."""

    x: str  # YYYY-MM-DD
    y: float | None


class SeriesMetricResponse(BaseModel):
    """Response with time series data."""

    metric: str
    points: list[SeriesPoint]


# ============================================
# Correlations
# ============================================


class SpearmanCorrelation(BaseModel):
    """Single Spearman correlation result."""

    metric: str
    rho: float
    p_value: float
    n: int


class SpearmanRequest(BaseModel):
    """Request for Spearman correlation analysis."""

    target: str
    candidates: list[str]
    start: date | None = None
    end: date | None = None


class SpearmanResponse(BaseModel):
    """Response with Spearman correlation results."""

    target: str
    correlations: list[SpearmanCorrelation]


class CorrelationMatrixResponse(BaseModel):
    """Pairwise correlation matrix response."""

    metrics: list[str]
    matrix: list[list[float]]
    p_values: list[list[float]]
    n_matrix: list[list[int]]


class ScatterPoint(BaseModel):
    """Single point in a scatter plot."""

    x: float
    y: float
    date: str


class ScatterDataResponse(BaseModel):
    """Scatter data response for two metrics."""

    metric_x: str
    metric_y: str
    points: list[ScatterPoint]
    n: int


class LaggedCorrelationPoint(BaseModel):
    """Correlation at a specific lag."""

    lag: int
    rho: float
    p_value: float
    n: int


class LaggedCorrelationRequest(BaseModel):
    """Request for lagged correlation analysis."""

    metric_x: str
    metric_y: str
    max_lag: int = 7
    start: date | None = None
    end: date | None = None


class LaggedCorrelationResponse(BaseModel):
    """Response with lagged correlation results."""

    metric_x: str
    metric_y: str
    lags: list[LaggedCorrelationPoint]
    best_lag: int


class ControlledCorrelationRequest(BaseModel):
    """Request for partial correlation controlling for variables."""

    metric_x: str
    metric_y: str
    control_vars: list[str]
    start: date | None = None
    end: date | None = None


class ControlledCorrelationResponse(BaseModel):
    """Response with controlled correlation results."""

    metric_x: str
    metric_y: str
    rho: float
    p_value: float
    n: int
    controlled_for: list[str]


# ============================================
# Patterns
# ============================================


class ChangePoint(BaseModel):
    """Detected change point in a time series."""

    date: date
    index: int
    before_mean: float
    after_mean: float
    magnitude: float
    direction: Literal["increase", "decrease"]


class ChangePointRequest(BaseModel):
    """Request for change point detection."""

    metric: str
    start: date | None = None
    end: date | None = None
    penalty: float | None = None


class ChangePointResponse(BaseModel):
    """Response with detected change points."""

    metric: str
    change_points: list[ChangePoint]


class Anomaly(BaseModel):
    """Detected anomaly."""

    date: date
    value: float
    z_score: float
    direction: Literal["high", "low"]


class AnomalyRequest(BaseModel):
    """Request for anomaly detection."""

    metric: str
    start: date | None = None
    end: date | None = None
    threshold: float = 3.0


class AnomalyResponse(BaseModel):
    """Response with detected anomalies."""

    metric: str
    anomalies: list[Anomaly]


class WeeklyCluster(BaseModel):
    """Weekly cluster assignment."""

    year: int
    week: int
    cluster: int
    label: str | None = None


class WeeklyClusterRequest(BaseModel):
    """Request for weekly clustering."""

    features: list[str]
    n_clusters: int = 4
    start: date | None = None
    end: date | None = None


class WeeklyClusterResponse(BaseModel):
    """Response with weekly cluster assignments."""

    weeks: list[WeeklyCluster]
    cluster_profiles: dict[str, dict[str, float]]


# ============================================
# Admin / Sync
# ============================================


class SyncRequest(BaseModel):
    """Request to sync data from Oura."""

    start: date
    end: date


class SyncResponse(BaseModel):
    """Response from sync operation."""

    status: Literal["completed", "failed", "in_progress"]
    days_processed: int | None = None
    message: str | None = None


# ============================================
# Dashboard
# ============================================


class DashboardSummary(BaseModel):
    """Summary metrics for dashboard."""

    readiness_avg: float | None = None
    sleep_score_avg: float | None = None
    activity_avg: float | None = None
    steps_avg: float | None = None
    hrv_avg: float | None = None
    rhr_avg: float | None = None
    sleep_hours_avg: float | None = None
    calories_avg: float | None = None
    stress_avg: float | None = None
    recovery_avg: float | None = None
    spo2_avg: float | None = None
    workout_minutes_avg: float | None = None
    days_with_data: int = 0


class TrendPoint(BaseModel):
    """Point in trend data."""

    date: str
    value: float | None
    baseline: float | None = None


class TrendSeries(BaseModel):
    """A named trend series."""

    name: str
    data: list[TrendPoint]


class DashboardResponse(BaseModel):
    """Dashboard data response."""

    connected: bool
    summary: DashboardSummary
    trends: list[TrendSeries] = []


# ============================================
# Insights (Phase 1)
# ============================================


class HeatmapPoint(BaseModel):
    """Single day in a heatmap."""

    date: str
    value: float | None


class HeatmapResponse(BaseModel):
    """Annual heatmap data response."""

    metric: str
    data: list[HeatmapPoint]
    min_value: float | None = None
    max_value: float | None = None


class SleepArchitectureDay(BaseModel):
    """Sleep stage percentages for a single night."""

    date: str
    deep_pct: float | None
    rem_pct: float | None
    light_pct: float | None
    total_hours: float | None


class SleepArchitectureResponse(BaseModel):
    """Sleep architecture data response."""

    data: list[SleepArchitectureDay]
    avg_deep_pct: float | None = None
    avg_rem_pct: float | None = None
    avg_light_pct: float | None = None


class ChronotypeResponse(BaseModel):
    """Chronotype and social jetlag analysis."""

    chronotype: str  # "morning_lark", "night_owl", or "intermediate"
    chronotype_label: str  # Human-readable label
    weekend_midpoint: str | None  # HH:MM format
    weekday_midpoint: str | None  # HH:MM format
    social_jetlag_minutes: int | None
    social_jetlag_label: str  # Human-readable (e.g., "1h 15m")
    recommendation: str | None


# ============================================
# Personal Info
# ============================================


class PersonalInfoResponse(BaseModel):
    """Personal info response."""

    age: int | None = None
    weight: float | None = None
    height: float | None = None
    biological_sex: str | None = None
    email: str | None = None
    fetched_at: datetime | None = None


# ============================================
# Error Responses
# ============================================


class ErrorResponse(BaseModel):
    """Error response."""

    error: str
    message: str
    details: dict | None = None


# ============================================
# Chat
# ============================================


class ChatMessage(BaseModel):
    """A single chat message."""

    role: Literal["user", "assistant", "system"]
    content: str


class ChatRequest(BaseModel):
    """Request to send a chat message."""

    message: str
    conversation_id: str | None = None


class ConversationSummary(BaseModel):
    """Summary of a conversation."""

    id: str
    title: str | None
    created_at: datetime
    updated_at: datetime
