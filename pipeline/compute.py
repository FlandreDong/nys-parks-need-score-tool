"""
Compute module for the park planning pipeline (Need Score).

This module implements a simple, interpretable need assessment:

    NeedScore = (DemandValue / SupplyValue) * NEED_SCORE_SCALE

    Default ``NEED_SCORE_SCALE`` is 100 (demand/supply ratio × 100). Classification bands follow the
    same decision boundaries as raw ratio 0.005 / 0.01 (here: 0.5 / 1.0).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple

import geopandas as gpd
import numpy as np
import pandas as pd


@dataclass
class DemandSupplyInputs:
    """
    Container for cleaned demand / supply / boundaries.

    Attributes
    ----------
    survey_df : pd.DataFrame
        Cleaned survey / demand table with columns at least:
        - region_id
        - population (optional, if using participants / population)
        - participants
    facilities_gdf : gpd.GeoDataFrame
        Cleaned facility supply with columns at least:
        - region_id
        - facility_id
        - capacity (optional)
    boundaries_gdf : gpd.GeoDataFrame
        Spatial boundaries (region_id + geometry) for optional map output.
    """

    survey_df: pd.DataFrame
    facilities_gdf: gpd.GeoDataFrame
    boundaries_gdf: Optional[gpd.GeoDataFrame] = None


# ---------------------------------------------------------------------------
# A. Demand calculation
# ---------------------------------------------------------------------------

def compute_demand_from_survey(
    survey_df: pd.DataFrame,
    use_rate: bool = True,
) -> pd.DataFrame:
    """
    Compute demand by region from survey-style data.

    Parameters
    ----------
    survey_df : pd.DataFrame
        Must contain columns: region_id, participants; and optionally population.
    use_rate : bool
        If True and population is available, demand_value = participants / population.
        If False or population missing, demand_value = participants.

    Returns
    -------
    pd.DataFrame
        Columns: region_id, demand_value.
    """
    df = survey_df.copy()
    if "region_id" not in df.columns or "participants" not in df.columns:
        raise ValueError("survey_df must have columns: region_id, participants")

    if use_rate and "population" in df.columns:
        pop = df["population"].replace({0: np.nan})
        demand_value = df["participants"] / pop
    else:
        demand_value = df["participants"].astype(float)

    out = pd.DataFrame(
        {
            "region_id": df["region_id"].astype(str),
            "demand_value": demand_value.replace([np.inf, -np.inf], np.nan).fillna(0.0),
        }
    )
    return out


def compute_demand_from_table(
    demand_df: pd.DataFrame,
    value_column: str = "demand_value",
) -> pd.DataFrame:
    """
    Use an existing demand table, just standardize column names.

    This is useful if demand has already been pre-computed (e.g. from
    D8/M4-style demand-at-origin or another pre-aggregated demand table).
    """
    if "region_id" not in demand_df.columns:
        raise ValueError("demand_df must have a region_id column.")
    if value_column not in demand_df.columns:
        raise ValueError(f"demand_df must have column {value_column!r}.")
    out = demand_df[["region_id", value_column]].copy()
    out["region_id"] = out["region_id"].astype(str)
    out = out.rename(columns={value_column: "demand_value"})
    out["demand_value"] = pd.to_numeric(out["demand_value"], errors="coerce").fillna(0.0)
    return out


# ---------------------------------------------------------------------------
# B. Supply calculation
# ---------------------------------------------------------------------------

def compute_supply_from_facilities(
    facilities_gdf: gpd.GeoDataFrame,
    use_capacity: bool = True,
) -> pd.DataFrame:
    """
    Compute supply by region from facility data.

    Parameters
    ----------
    facilities_gdf : GeoDataFrame
        Cleaned facilities with at least region_id and facility_id.
        Optional column capacity can be used when use_capacity=True.
    use_capacity : bool
        If True and capacity column exists, supply_value = sum(capacity) per region.
        Otherwise supply_value = facility count per region.

    Returns
    -------
    pd.DataFrame
        Columns: region_id, supply_value.
    """
    gdf = facilities_gdf.copy()
    if "region_id" not in gdf.columns:
        raise ValueError("facilities_gdf must have a region_id column.")

    group = gdf.groupby("region_id")

    if use_capacity and "capacity" in gdf.columns:
        agg = group.agg(
            supply_value=("capacity", "sum"),
            facility_count=("facility_id", "count"),
        ).reset_index()
        agg["supply_value"] = agg["supply_value"].fillna(agg["facility_count"])
    else:
        agg = group.agg(facility_count=("facility_id", "count")).reset_index()
        agg["supply_value"] = agg["facility_count"].astype(float)

    agg["region_id"] = agg["region_id"].astype(str)
    # Avoid zero supply; leave as NaN so need_ratio can be handled explicitly.
    agg["supply_value"] = pd.to_numeric(agg["supply_value"], errors="coerce")
    agg.loc[agg["supply_value"] <= 0, "supply_value"] = np.nan
    return agg[["region_id", "supply_value"]]


def compute_supply_constant(region_ids: pd.Series, value: float = 1.0) -> pd.DataFrame:
    """
    Convenience helper: assume the same supply value for all regions.
    """
    return pd.DataFrame(
        {"region_id": region_ids.astype(str), "supply_value": float(value)}
    )


# ---------------------------------------------------------------------------
# C. Need Score calculation
# ---------------------------------------------------------------------------

NEED_SCORE_SCALE = 100.0

# Classification on scaled score (×100 vs raw ratio); equivalent to raw 0.005 / 0.01
NEED_CLASS_SCORE_LOW = 0.5
NEED_CLASS_SCORE_HIGH = 1.0


def compute_need_score(
    demand_df: pd.DataFrame,
    supply_df: pd.DataFrame,
    boundaries_gdf: Optional[gpd.GeoDataFrame] = None,
) -> Tuple[Optional[gpd.GeoDataFrame], pd.DataFrame]:
    """
    Compute NeedScore per region and return both spatial and tabular outputs.

    NeedScore = (demand_value / supply_value) * NEED_SCORE_SCALE
    Zero or missing supply yields NaN (no crash).

    Parameters
    ----------
    demand_df : pd.DataFrame
        Columns: region_id, demand_value.
    supply_df : pd.DataFrame
        Columns: region_id, supply_value.
    boundaries_gdf : GeoDataFrame, optional
        Columns: region_id, geometry. If None, returns only tabular output.

    Returns
    -------
    need_gdf : GeoDataFrame or None
        Spatial layer if boundaries provided; otherwise None.
    need_table : pd.DataFrame
        Columns: region_id, demand_value, supply_value, need_score, need_class, priority_rank.
    """
    if "region_id" not in demand_df.columns or "demand_value" not in demand_df.columns:
        raise ValueError("demand_df must have columns: region_id, demand_value")
    if "region_id" not in supply_df.columns or "supply_value" not in supply_df.columns:
        raise ValueError("supply_df must have columns: region_id, supply_value")

    merged = demand_df.merge(supply_df, on="region_id", how="outer")

    merged["supply_value"] = pd.to_numeric(merged["supply_value"], errors="coerce")
    merged["demand_value"] = pd.to_numeric(merged["demand_value"], errors="coerce").fillna(0.0)

    with np.errstate(divide="ignore", invalid="ignore"):
        merged["need_score"] = (
            merged["demand_value"] / merged["supply_value"]
        ) * NEED_SCORE_SCALE

    merged["need_score"] = merged["need_score"].replace([np.inf, -np.inf], np.nan)

    if boundaries_gdf is not None:
        merged = merged.merge(
            boundaries_gdf[["region_id", "geometry"]],
            on="region_id",
            how="left",
        )

    merged = classify_need_score(merged)
    merged = rank_regions_by_need_score(merged)

    if boundaries_gdf is not None and "geometry" in merged.columns:
        need_gdf = gpd.GeoDataFrame(
            merged, geometry="geometry", crs=boundaries_gdf.crs
        )
        need_table = merged.drop(columns="geometry").copy()
    else:
        need_gdf = None
        need_table = merged.copy()

    return need_gdf, need_table


# ---------------------------------------------------------------------------
# D. Need classification & E. Ranking
# ---------------------------------------------------------------------------

def classify_need_score(df: pd.DataFrame) -> pd.DataFrame:
    """
    Assign need_class from NeedScore for planner interpretation.

    Rules
    -----
    - NeedScore < 0.5         -> "Supply Sufficient"  (raw ratio < 0.005)
    - 0.5 <= NeedScore <= 1.0  -> "Balanced"
    - NeedScore > 1.0         -> "Needs Improvement"
    - supply missing (NaN)  -> "Needs Improvement (No Supply)"
    """
    out = df.copy()
    score = out["need_score"]

    need_class = pd.Series(index=out.index, dtype="object")

    no_supply_mask = out["supply_value"].isna()
    need_class[no_supply_mask] = "Needs Improvement (No Supply)"

    valid = score.notna() & ~no_supply_mask
    low, high = NEED_CLASS_SCORE_LOW, NEED_CLASS_SCORE_HIGH
    need_class.loc[valid & (score < low)] = "Supply Sufficient"
    need_class.loc[valid & (score >= low) & (score <= high)] = "Balanced"
    need_class.loc[valid & (score > high)] = "Needs Improvement"

    out["need_class"] = need_class
    return out


def rank_regions_by_need_score(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add priority_rank by NeedScore (descending). Highest score = rank 1.
    """
    out = df.copy()
    rank_base = out["need_score"].fillna(-np.inf)
    out["priority_rank"] = (
        (-rank_base).rank(method="min").where(rank_base > -np.inf)
    )
    return out


def regions_needing_improvement(
    need_table: pd.DataFrame, threshold: float = NEED_CLASS_SCORE_HIGH
) -> pd.DataFrame:
    """
    Return regions where need_score exceeds the given threshold (default 1.0, i.e. raw ratio > 0.01 at ×100).
    """
    df = need_table.copy()
    mask = (df["need_score"] > threshold) | (
        df["need_class"].str.contains("Needs Improvement", na=False)
    )
    high_need = df[mask].copy()
    return high_need.sort_values("need_score", ascending=False)


