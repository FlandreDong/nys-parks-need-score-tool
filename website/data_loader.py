"""
Data loading utilities for the website module.

Reads exports under ``data/outputs``. After changing demand/supply inputs, run
``py run_pipeline.py`` from the project root so ``need_score_map.geojson`` and
``need_score_by_region.csv`` are refreshed (``export.export_results`` writes
these stable names on each run).
"""

from __future__ import annotations

from pathlib import Path
import geopandas as gpd
import pandas as pd

from config.settings import paths

from website.demand_loader import (
    load_activity_demand_wide,
    load_d8_activity_aggregates,
    load_demand_activity_wide,
)

OUTPUT_DIR = paths.data_outputs
PROJECT_DIR = paths.base_dir

# Re-export for ``from website import data_loader; data_loader.load_activity_demand_wide``
__all__ = [
    "load_need_score_by_region",
    "load_priority_ranking",
    "load_need_score_map",
    "load_activity_demand_wide",
    "load_d8_activity_aggregates",
    "load_demand_activity_wide",
    "compute_summary",
]


def _find_latest(pattern: str) -> Path:
    """
    Find the most recent file in data/outputs matching a glob pattern.
    """
    candidates = sorted(OUTPUT_DIR.glob(pattern), key=lambda p: p.stat().st_mtime)
    if not candidates:
        raise FileNotFoundError(f"No files matching pattern {pattern!r} in {OUTPUT_DIR}")
    return candidates[-1]


def load_need_score_by_region() -> pd.DataFrame:
    """
    Load the main need score table.

    Prefers a canonical name if present, otherwise falls back to the
    timestamped CSV written by the existing export logic.
    """
    # Prefer conceptual filename if user creates it in future
    preferred = OUTPUT_DIR / "need_score_by_region.csv"
    if preferred.exists():
        return pd.read_csv(preferred)

    # Fallback to our current export naming: need_table_*.csv
    path = _find_latest("need_table_*.csv")
    return pd.read_csv(path)


def load_priority_ranking() -> pd.DataFrame:
    """
    Load the priority ranking table.

    Prefers 'priority_ranking.csv' if present, else uses need_improvement_*.csv.
    """
    preferred = OUTPUT_DIR / "priority_ranking.csv"
    if preferred.exists():
        return pd.read_csv(preferred)

    path = _find_latest("need_improvement_*.csv")
    return pd.read_csv(path)


def load_need_score_map() -> gpd.GeoDataFrame:
    """
    Load the GeoJSON map of need scores.

    Prefers 'need_score_map.geojson' if present, else uses need_spatial_*.geojson.
    """
    preferred = OUTPUT_DIR / "need_score_map.geojson"
    if preferred.exists():
        return gpd.read_file(preferred)

    path = _find_latest("need_spatial_*.geojson")
    return gpd.read_file(path)


def compute_summary(df: pd.DataFrame) -> dict:
    """
    Compute high-level summary statistics for the overview panel.

    Expects columns:
    - region_id
    - need_score
    - need_class
    """
    summary: dict = {}
    if df.empty:
        return {
            "total_regions": 0,
            "needs_improvement_count": 0,
            "avg_need_score": None,
            "max_need_score": None,
            "highest_need_region": None,
        }

    summary["total_regions"] = int(df["region_id"].nunique())
    summary["needs_improvement_count"] = int(
        df["need_class"].fillna("").str.contains("Needs Improvement").sum()
    )
    summary["avg_need_score"] = float(df["need_score"].mean())

    idx_max = df["need_score"].idxmax()
    summary["max_need_score"] = float(df.loc[idx_max, "need_score"])
    summary["highest_need_region"] = str(df.loc[idx_max, "region_id"])

    return summary

