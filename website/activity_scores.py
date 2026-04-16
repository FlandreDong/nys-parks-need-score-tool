"""
Activity-specific ActivityNeedScore for the Streamlit UI (sidebar + map).

Uses the same rules as the sidebar: D8 aggregate demand × county supply from the discovered supply file.
Merged onto county boundaries by ``region_id`` / normalized key — not legacy pipeline ``need_score``.
"""

from __future__ import annotations

from typing import Any

import geopandas as gpd
import pandas as pd

from pipeline.compute import NEED_SCORE_SCALE
from pipeline.region_keys import normalize_merge_key
from pipeline.summary_footer import is_d8_summary_footer_row
from website.activity_supply_loader import load_activity_supply_for_ranking


def compute_activity_scores_table(
    activity: str,
    demand_aggregates: pd.Series,
) -> tuple[pd.DataFrame, dict]:
    """
    One row per supply county: Region, activity_demand, ActivityNeedScore.

    Returns
    -------
    table
        Columns ``Region``, ``activity_demand``, ``ActivityNeedScore``.
    stats
        ``supply_rows``, etc.
    """
    empty_cols = ["Region", "activity_demand", "ActivityNeedScore"]
    stats: dict[str, Any] = {"supply_rows": 0, "matched_rows": 0}
    try:
        demand_scalar = float(demand_aggregates[activity])
    except (KeyError, TypeError, ValueError):
        return pd.DataFrame(columns=empty_cols), stats

    supply_df = load_activity_supply_for_ranking(activity)
    if supply_df is None or supply_df.empty:
        return pd.DataFrame(columns=empty_cols), stats

    ms = supply_df[["_key", "supply_value"]].copy()
    ms["_key"] = ms["_key"].map(normalize_merge_key)
    ms["supply_value"] = pd.to_numeric(ms["supply_value"], errors="coerce")
    ms = ms[ms["_key"] != ""]
    ms = ms.drop_duplicates(subset=["_key"], keep="last")
    stats["supply_rows"] = len(ms)
    stats["matched_rows"] = len(ms)

    out = ms.rename(columns={"_key": "Region"}).copy()
    out["activity_demand"] = demand_scalar
    out["ActivityNeedScore"] = pd.NA
    sv = out["supply_value"]
    valid = sv.notna() & (sv != 0)
    out.loc[valid, "ActivityNeedScore"] = (
        demand_scalar / sv[valid]
    ) * float(NEED_SCORE_SCALE)
    out = out.drop(columns=["supply_value"], errors="ignore")
    out = out.loc[~out["Region"].map(is_d8_summary_footer_row)]
    return out[["Region", "activity_demand", "ActivityNeedScore"]], stats


def merge_activity_scores_to_map_gdf(
    base_gdf: gpd.GeoDataFrame,
    scores_df: pd.DataFrame,
) -> gpd.GeoDataFrame:
    """
    Join ActivityNeedScore onto county polygons; output column ``activity_need_score`` for rendering.

    Drops legacy pipeline columns used only for the old NeedScore choropleth.
    """
    gdf = base_gdf.copy()
    if scores_df is None or scores_df.empty:
        for legacy in ("need_score", "need_class", "priority_rank", "demand_value", "supply_value"):
            if legacy in gdf.columns:
                gdf = gdf.drop(columns=[legacy])
        gdf["activity_need_score"] = float("nan")
        return gdf
    if gdf.crs and str(gdf.crs) != "EPSG:4326":
        gdf = gdf.to_crs("EPSG:4326")

    gdf["_join_key"] = gdf["region_id"].map(normalize_merge_key)
    sc = scores_df.copy()
    sc["_join_key"] = sc["Region"].map(normalize_merge_key)
    sc = sc[["_join_key", "ActivityNeedScore"]].drop_duplicates(subset=["_join_key"], keep="last")

    merged = gdf.merge(sc, on="_join_key", how="left")
    merged = merged.drop(columns=["_join_key"])

    for legacy in ("need_score", "need_class", "priority_rank", "demand_value", "supply_value"):
        if legacy in merged.columns:
            merged = merged.drop(columns=[legacy])

    merged["activity_need_score"] = pd.to_numeric(merged["ActivityNeedScore"], errors="coerce")
    merged = merged.drop(columns=["ActivityNeedScore"], errors="ignore")

    return merged
