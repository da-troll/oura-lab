"""Correlation analysis module (multi-user)."""

from datetime import date
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats

from app.db import get_db_for_user


async def load_analysis_data(
    user_id: str,
    start_date: date | None = None,
    end_date: date | None = None,
) -> pd.DataFrame:
    """Load daily and feature data for analysis, scoped to a user."""
    async with get_db_for_user(user_id) as conn:
        async with conn.cursor() as cur:
            query = """
                SELECT d.*, f.*
                FROM oura_daily d
                LEFT JOIN oura_features_daily f ON d.date = f.date AND d.user_id = f.user_id
                WHERE d.user_id = %(uid)s
            """
            params: dict[str, Any] = {"uid": user_id}

            if start_date:
                query += " AND d.date >= %(start)s"
                params["start"] = start_date
            if end_date:
                query += " AND d.date <= %(end)s"
                params["end"] = end_date

            query += " ORDER BY d.date ASC"

            await cur.execute(query, params)
            rows = await cur.fetchall()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame([dict(r) for r in rows])
    if "date" in df.columns:
        df = df.loc[:, ~df.columns.duplicated()]
    return df


def compute_spearman_correlations(
    df: pd.DataFrame,
    target: str,
    candidates: list[str],
) -> list[dict[str, Any]]:
    """Compute Spearman correlations between target and candidate metrics."""
    if target not in df.columns:
        return []

    results = []
    target_series = df[target].dropna()

    for candidate in candidates:
        if candidate not in df.columns or candidate == target:
            continue

        candidate_series = df[candidate].dropna()
        common_idx = target_series.index.intersection(candidate_series.index)
        if len(common_idx) < 10:
            continue

        x = target_series.loc[common_idx]
        y = candidate_series.loc[common_idx]
        rho, p_value = stats.spearmanr(x, y)

        if not np.isnan(rho):
            results.append({
                "metric": candidate,
                "rho": float(rho),
                "p_value": float(p_value),
                "n": len(common_idx),
            })

    results.sort(key=lambda r: abs(r["rho"]), reverse=True)
    return results


def compute_lagged_correlations(
    df: pd.DataFrame,
    metric_x: str,
    metric_y: str,
    max_lag: int = 7,
) -> dict[str, Any]:
    """Compute correlations at various lags."""
    if metric_x not in df.columns or metric_y not in df.columns:
        return {"metric_x": metric_x, "metric_y": metric_y, "lags": [], "best_lag": 0}

    results = []
    best_rho = 0
    best_lag = 0

    x = df[metric_x].dropna()
    y = df[metric_y].dropna()

    for lag in range(max_lag + 1):
        if lag == 0:
            x_lagged = x
            y_aligned = y
        else:
            x_lagged = x.iloc[:-lag] if lag > 0 else x
            y_aligned = y.iloc[lag:] if lag > 0 else y

        common_idx = x_lagged.index.intersection(y_aligned.index)
        if len(common_idx) < 10:
            continue

        x_vals = x_lagged.loc[common_idx]
        y_vals = y_aligned.loc[common_idx]
        rho, p_value = stats.spearmanr(x_vals, y_vals)

        if not np.isnan(rho):
            results.append({
                "lag": lag,
                "rho": float(rho),
                "p_value": float(p_value),
                "n": len(common_idx),
            })
            if abs(rho) > abs(best_rho):
                best_rho = rho
                best_lag = lag

    return {
        "metric_x": metric_x,
        "metric_y": metric_y,
        "lags": results,
        "best_lag": best_lag,
    }


def compute_controlled_correlation(
    df: pd.DataFrame,
    metric_x: str,
    metric_y: str,
    control_vars: list[str],
) -> dict[str, Any]:
    """Compute partial correlation controlling for confounders."""
    required_cols = [metric_x, metric_y] + control_vars
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        return {
            "metric_x": metric_x, "metric_y": metric_y,
            "rho": 0, "p_value": 1, "n": 0, "controlled_for": control_vars,
        }

    clean_df = df[required_cols].dropna()
    if len(clean_df) < 10:
        return {
            "metric_x": metric_x, "metric_y": metric_y,
            "rho": 0, "p_value": 1, "n": len(clean_df), "controlled_for": control_vars,
        }

    from sklearn.linear_model import LinearRegression

    X_controls = clean_df[control_vars].values.astype(float)
    x = clean_df[metric_x].values.astype(float)
    y = clean_df[metric_y].values.astype(float)

    reg_x = LinearRegression().fit(X_controls, x)
    residuals_x = x - reg_x.predict(X_controls)
    reg_y = LinearRegression().fit(X_controls, y)
    residuals_y = y - reg_y.predict(X_controls)
    rho, p_value = stats.spearmanr(residuals_x, residuals_y)

    return {
        "metric_x": metric_x,
        "metric_y": metric_y,
        "rho": float(rho) if not np.isnan(rho) else 0,
        "p_value": float(p_value) if not np.isnan(p_value) else 1,
        "n": len(clean_df),
        "controlled_for": control_vars,
    }


def compute_correlation_matrix(
    df: pd.DataFrame,
    metrics: list[str],
) -> dict[str, Any]:
    """Compute pairwise Spearman correlation matrix."""
    available = [m for m in metrics if m in df.columns]
    n = len(available)
    matrix = [[0.0] * n for _ in range(n)]
    p_values = [[0.0] * n for _ in range(n)]
    n_matrix = [[0] * n for _ in range(n)]

    for i in range(n):
        for j in range(n):
            if i == j:
                matrix[i][j] = 1.0
                p_values[i][j] = 0.0
                n_matrix[i][j] = int(df[available[i]].dropna().shape[0])
            elif j > i:
                x = df[available[i]]
                y = df[available[j]]
                mask = x.notna() & y.notna()
                n_obs = int(mask.sum())
                if n_obs >= 10:
                    rho, p = stats.spearmanr(x[mask], y[mask])
                    if not np.isnan(rho):
                        matrix[i][j] = float(rho)
                        matrix[j][i] = float(rho)
                        p_values[i][j] = float(p)
                        p_values[j][i] = float(p)
                n_matrix[i][j] = n_obs
                n_matrix[j][i] = n_obs

    return {
        "metrics": available,
        "matrix": matrix,
        "p_values": p_values,
        "n_matrix": n_matrix,
    }


def get_metric_pair_data(
    df: pd.DataFrame,
    metric_x: str,
    metric_y: str,
) -> dict[str, Any]:
    """Extract aligned daily values for two metrics."""
    if metric_x not in df.columns or metric_y not in df.columns:
        return {"points": [], "n": 0}

    mask = df[metric_x].notna() & df[metric_y].notna()
    subset = df[mask]

    points = []
    for _, row in subset.iterrows():
        points.append({
            "x": float(row[metric_x]),
            "y": float(row[metric_y]),
            "date": str(row["date"]),
        })

    return {"points": points, "n": len(points)}


# Public API functions — all take user_id

async def get_correlation_matrix(
    metrics: list[str],
    start_date: date | None = None,
    end_date: date | None = None,
    user_id: str = "",
) -> dict[str, Any]:
    df = await load_analysis_data(user_id, start_date, end_date)
    if df.empty:
        return {"metrics": [], "matrix": [], "p_values": [], "n_matrix": []}
    return compute_correlation_matrix(df, metrics)


async def get_scatter_data(
    metric_x: str,
    metric_y: str,
    start_date: date | None = None,
    end_date: date | None = None,
    user_id: str = "",
) -> dict[str, Any]:
    df = await load_analysis_data(user_id, start_date, end_date)
    if df.empty:
        return {"metric_x": metric_x, "metric_y": metric_y, "points": [], "n": 0}
    result = get_metric_pair_data(df, metric_x, metric_y)
    return {"metric_x": metric_x, "metric_y": metric_y, **result}


async def get_spearman_correlations(
    target: str,
    candidates: list[str],
    start_date: date | None = None,
    end_date: date | None = None,
    user_id: str = "",
) -> dict[str, Any]:
    df = await load_analysis_data(user_id, start_date, end_date)
    if df.empty:
        return {"target": target, "correlations": []}
    correlations = compute_spearman_correlations(df, target, candidates)
    return {"target": target, "correlations": correlations}


async def get_lagged_correlations(
    metric_x: str,
    metric_y: str,
    max_lag: int = 7,
    start_date: date | None = None,
    end_date: date | None = None,
    user_id: str = "",
) -> dict[str, Any]:
    df = await load_analysis_data(user_id, start_date, end_date)
    if df.empty:
        return {"metric_x": metric_x, "metric_y": metric_y, "lags": [], "best_lag": 0}
    return compute_lagged_correlations(df, metric_x, metric_y, max_lag)


async def get_controlled_correlation(
    metric_x: str,
    metric_y: str,
    control_vars: list[str],
    start_date: date | None = None,
    end_date: date | None = None,
    user_id: str = "",
) -> dict[str, Any]:
    df = await load_analysis_data(user_id, start_date, end_date)
    if df.empty:
        return {
            "metric_x": metric_x, "metric_y": metric_y,
            "rho": 0, "p_value": 1, "n": 0, "controlled_for": control_vars,
        }
    return compute_controlled_correlation(df, metric_x, metric_y, control_vars)
