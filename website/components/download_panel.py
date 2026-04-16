"""
Download panel: CSV and GeoJSON exports.
"""

from __future__ import annotations

from pathlib import Path

import streamlit as st

from config.settings import paths


def render_download_panel(*, horizontal: bool = True) -> None:
    """Download panel. Use horizontal=True for a single row below the map."""
    st.subheader("Download")

    output_dir = paths.data_outputs

    candidates = {
        "need_score_by_region.csv": [
            output_dir / "need_score_by_region.csv",
            *_sorted(output_dir.glob("need_table_*.csv")),
        ],
        "priority_ranking.csv": [
            output_dir / "priority_ranking.csv",
            *_sorted(output_dir.glob("need_improvement_*.csv")),
        ],
        "need_score_map.geojson": [
            output_dir / "need_score_map.geojson",
            *_sorted(output_dir.glob("need_spatial_*.geojson")),
        ],
    }

    items = list(candidates.items())
    if horizontal:
        cols = st.columns(len(items))
    else:
        cols = [st.container() for _ in items]

    for (filename, paths_list), col in zip(items, cols):
        with col:
            available = [p for p in paths_list if p.exists()]
            if not available:
                st.caption(f"{filename} (not found)")
                continue
            path = available[-1]
            with open(path, "rb") as f:
                data = f.read()
            st.download_button(
                label=filename,
                data=data,
                file_name=filename,
                mime="application/octet-stream",
                use_container_width=True,
            )


def _sorted(paths_iter):
    return sorted(paths_iter, key=lambda p: p.stat().st_mtime)

