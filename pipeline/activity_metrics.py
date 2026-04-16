"""
County × activity metrics: D8 demand is **aggregated** across its rows (model inputs), then
combined with **county-level supply** only. There is **no** county-key join between the D8
workbook and the supply table.

For each activity ``a`` and each county ``c`` in the supply file:

- ``demand_value`` = aggregate D8 measure for activity ``a`` (same for all counties).
- ``supply_value`` = supply for county ``c`` (and ``a`` if an activity column exists, else total supply).

``need_score`` = (demand_value / supply_value) × NEED_SCORE_SCALE (default 100). Rows are only counties present in supply.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd

from pipeline.compute import NEED_SCORE_SCALE
from pipeline.demand_aggregate import aggregate_d8_per_activity
from pipeline.ingest_demand_supply import ACTIVITY_COLUMNS
from pipeline.supply_for_activity import load_supply_keyed_for_activity


def build_county_activity_metrics_long(
    m4_wide: pd.DataFrame,
    supply_path: Path | None,
    *,
    how: Literal["sum", "mean"] = "sum",
) -> pd.DataFrame:
    """
    Long table: one row per (county from supply, activity) with aggregated D8 demand × county supply.
    """
    if m4_wide is None or m4_wide.empty:
        return pd.DataFrame(
            columns=[
                "region_id",
                "activity",
                "demand_value",
                "supply_value",
                "gap",
                "supply_demand_ratio",
                "need_score",
            ]
        )

    agg = aggregate_d8_per_activity(m4_wide, how=how)
    if agg.empty:
        return pd.DataFrame(
            columns=[
                "region_id",
                "activity",
                "demand_value",
                "supply_value",
                "gap",
                "supply_demand_ratio",
                "need_score",
            ]
        )

    spath = Path(supply_path) if supply_path is not None else None
    rows: list[pd.DataFrame] = []

    for activity in ACTIVITY_COLUMNS:
        if activity not in agg.index:
            continue
        d_val = float(agg[activity])
        if spath is None:
            continue
        sk = load_supply_keyed_for_activity(spath, activity)
        if sk is None or sk.empty:
            continue

        out = sk.rename(columns={"_key": "region_id"}).copy()
        out["activity"] = activity
        out["demand_value"] = d_val
        out["supply_value"] = pd.to_numeric(out["supply_value"], errors="coerce")

        out["gap"] = out["demand_value"] - out["supply_value"]
        with np.errstate(divide="ignore", invalid="ignore"):
            out["supply_demand_ratio"] = np.where(
                out["demand_value"] > 0,
                out["supply_value"] / out["demand_value"],
                np.nan,
            )
            out["need_score"] = np.where(
                (out["supply_value"].notna()) & (out["supply_value"] != 0),
                (out["demand_value"] / out["supply_value"]) * NEED_SCORE_SCALE,
                np.nan,
            )
        out["supply_demand_ratio"] = out["supply_demand_ratio"].replace(
            [np.inf, -np.inf], np.nan
        )
        out["need_score"] = out["need_score"].replace([np.inf, -np.inf], np.nan)

        rows.append(
            out[
                [
                    "region_id",
                    "activity",
                    "demand_value",
                    "supply_value",
                    "gap",
                    "supply_demand_ratio",
                    "need_score",
                ]
            ]
        )

    if not rows:
        return pd.DataFrame(
            columns=[
                "region_id",
                "activity",
                "demand_value",
                "supply_value",
                "gap",
                "supply_demand_ratio",
                "need_score",
            ]
        )

    return pd.concat(rows, ignore_index=True)
