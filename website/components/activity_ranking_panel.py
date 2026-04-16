"""
Activity Ranking Panel (sidebar).

Table values match the map: **ActivityNeedScore** for the selected activity (D8 aggregate × county supply).
Legacy pipeline **NeedScore** is not shown here.
"""

from __future__ import annotations

from typing import List, Optional

import pandas as pd
import streamlit as st

from pipeline.compute import NEED_SCORE_SCALE

ACTIVITIES: List[str] = [
    "Park",
    "Swimming",
    "Biking",
    "Golfing",
    "Court Games",
    "Field Games",
    "Walking/Jogging",
    "Camping",
    "Fishing",
    "Boating",
    "Local Winter",
    "Downhill Skiing",
    "Snowmobiling",
]

SORT_OPTIONS = (
    "Descending (High → Low)",
    "Ascending (Low → High)",
)


def _apply_sort_and_limit(
    frame: pd.DataFrame,
    descending: bool,
    limit: int,
    sort_column: str,
) -> pd.DataFrame:
    if frame.empty:
        return frame
    if sort_column not in frame.columns:
        return frame
    out = frame.sort_values(
        sort_column,
        ascending=not descending,
        na_position="last",
    ).reset_index(drop=True)
    return out.head(limit)


def _format_activity_need_score(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "-"
    try:
        x = float(value)
    except (TypeError, ValueError):
        return str(value)
    return f"{x:.3f}"


def _render_compact_list(
    frame: pd.DataFrame,
    *,
    merge_stats: Optional[dict] = None,
) -> None:
    if frame.empty:
        return
    col_name = "ActivityNeedScore"
    label = "ActivityNeedScore"
    has_any_score = bool(frame["ActivityNeedScore"].notna().any())

    header = f"{'Region':<20} {label:>14}"
    lines = [header, "-" * len(header)]
    for _, row in frame.iterrows():
        region = str(row["Region"])[:20]
        score_s = _format_activity_need_score(row[col_name])
        lines.append(f"{region:<20} {score_s:>14}")

    with st.container(height=360):
        st.code("\n".join(lines), language="text")

    if not has_any_score:
        ms = merge_stats or {}
        if ms.get("supply_rows", 0) == 0:
            st.caption(
                "ActivityNeedScore: supply was not loaded (filename contains supply/S12), or the denominator is all zeros."
            )
        else:
            if NEED_SCORE_SCALE == 1.0:
                cap = (
                    "ActivityNeedScore: D8 aggregated demand ÷ county supply (ratio; no scaling factor). "
                    "If all values are '-', check the supply input."
                )
            else:
                cap = (
                    f"ActivityNeedScore: D8 aggregated demand ÷ county supply × {NEED_SCORE_SCALE}. "
                    "If all values are '-', check the supply input."
                )
            st.caption(cap)


def render_activity_ranking_sidebar(
    scores_df: pd.DataFrame,
    merge_stats: dict,
    *,
    descending: bool,
) -> None:
    """Render the text ranking for the current activity (same rows/values as the map)."""
    st.subheader("Activity Ranking")

    display_df = _apply_sort_and_limit(
        scores_df,
        descending=descending,
        limit=len(scores_df) if not scores_df.empty else 0,
        sort_column="ActivityNeedScore",
    )
    if scores_df.empty:
        st.caption("No supply data is available for the selected activity, or ActivityNeedScore cannot be computed.")
        return
    _render_compact_list(display_df, merge_stats=merge_stats)
