"""
Layout: sidebar activity ranking and main map both use **ActivityNeedScore** for the selected activity.
"""

from __future__ import annotations

import pandas as pd
import streamlit as st

from website import data_loader
from website.activity_scores import compute_activity_scores_table, merge_activity_scores_to_map_gdf
from website.components.activity_ranking_panel import (
    ACTIVITIES,
    SORT_OPTIONS,
    render_activity_ranking_sidebar,
)
from website.components.download_panel import render_download_panel
from website.components.map_view import render_map_view
from website.demand_loader import load_d8_activity_aggregates


def render_app() -> None:
    try:
        base_gdf = data_loader.load_need_score_map()
    except FileNotFoundError as exc:
        base_gdf = None
        st.warning(
            f"County GeoJSON not found ({exc}). Run ``py run_pipeline.py --boundaries`` "
            "so ``data/outputs/need_score_map.geojson`` exists."
        )

    try:
        demand_aggregates = load_d8_activity_aggregates()
        demand_error = None
    except (FileNotFoundError, ValueError) as exc:
        demand_aggregates = None
        demand_error = str(exc)

    scores_df = pd.DataFrame()
    merge_stats: dict = {}
    activity: str | None = None

    with st.sidebar:
        if demand_error:
            st.caption(f"D8 aggregates unavailable: {demand_error}")
        if demand_aggregates is None:
            st.subheader("Activity Ranking")
            st.caption("Add a D8/demand Excel wide file (see pipeline filename rules).")
        else:
            available = [a for a in ACTIVITIES if a in demand_aggregates.index]
            st.subheader("Activity Ranking")
            if not available:
                st.caption("No standard activity columns found in D8.")
            else:
                activity = st.selectbox(
                    "Select Activity",
                    available,
                    index=0,
                    key="main_activity_select",
                )
                sort_order = st.selectbox(
                    "Sort Order",
                    SORT_OPTIONS,
                    index=0,
                    key="activity_ranking_sort",
                )
                descending = sort_order == SORT_OPTIONS[0]
                scores_df, merge_stats = compute_activity_scores_table(
                    activity, demand_aggregates
                )
                render_activity_ranking_sidebar(
                    scores_df, merge_stats, descending=descending
                )

    if base_gdf is not None and activity is not None and demand_aggregates is not None:
        map_gdf = merge_activity_scores_to_map_gdf(base_gdf, scores_df)
        render_map_view(map_gdf, activity_label=activity)
    elif base_gdf is not None:
        if demand_aggregates is None:
            st.info("Load D8 aggregate demand (Excel wide file) to compute ActivityNeedScore for the map.")
        else:
            st.info(
                "Select an activity in the sidebar to color counties by **ActivityNeedScore** "
                "(same values as the ranking list)."
            )
    else:
        st.header("Spatial distribution")
        st.caption("Export county GeoJSON from the pipeline to enable the map.")

    render_download_panel(horizontal=True)
