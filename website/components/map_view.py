"""
Map view: choropleth by **ActivityNeedScore** only (synced with the sidebar ranking).
"""

from __future__ import annotations

import numpy as np
import geopandas as gpd
import pandas as pd
import streamlit as st


def render_map_view(
    gdf: gpd.GeoDataFrame,
    *,
    activity_label: str,
) -> None:
    """Interactive choropleth using ``activity_need_score``; no legacy ``need_score``."""
    title = (
        f"Spatial distribution — ActivityNeedScore ({activity_label})"
        if activity_label
        else "Spatial distribution — ActivityNeedScore"
    )
    st.header(title)

    if gdf is None or gdf.empty:
        st.info("No spatial data to display.")
        return

    try:
        import folium
    except ImportError:
        st.warning("Folium is required for the map. Install with: pip install folium")
        return

    if gdf.crs and str(gdf.crs) != "EPSG:4326":
        gdf = gdf.to_crs("EPSG:4326")

    gdf = gdf[gdf.geometry.notna()].copy()
    if gdf.empty:
        st.info("No valid geometries to display.")
        return

    if "activity_need_score" not in gdf.columns:
        st.warning("Missing activity_need_score column; cannot render ActivityNeedScore map.")
        return

    plot = gdf.copy()
    # Keep NaN for counties with no score — do NOT use a sentinel like -1 or the legend
    # scale spans [-1, max] and washes out the real value range (e.g. Park ~0.08–0.2).
    plot["activity_need_score"] = pd.to_numeric(plot["activity_need_score"], errors="coerce")

    valid = plot["activity_need_score"].dropna()
    if valid.empty:
        st.warning("No ActivityNeedScore values are available; the map cannot be colored.")
        return

    lo, hi = float(valid.min()), float(valid.max())
    if hi <= lo:
        hi = lo + 1e-12
    # Explicit bins from actual data so the legend matches the sidebar (YlOrRd: pale → dark red)
    threshold_scale = np.linspace(lo, hi, 6).tolist()

    m = folium.Map(location=[43.0, -75.5], zoom_start=7, tiles="CartoDB positron")
    legend_caption = (
        f"ActivityNeedScore ({activity_label})"
        if activity_label
        else "ActivityNeedScore"
    )
    folium.Choropleth(
        geo_data=plot,
        data=plot,
        columns=["region_id", "activity_need_score"],
        key_on="feature.properties.region_id",
        fill_color="YlOrRd",
        fill_opacity=0.7,
        line_opacity=0.3,
        legend_name=legend_caption,
        threshold_scale=threshold_scale,
        nan_fill_color="#bbbbbb",
        nan_fill_opacity=0.35,
    ).add_to(m)

    plot["_activity_tip"] = plot["activity_need_score"].apply(
        lambda x: f"{float(x):.6f}" if pd.notna(x) else "—"
    )
    tip_fields = ["region_id", "_activity_tip"]
    tip_aliases = ["Region", "ActivityNeedScore"]
    folium.GeoJson(
        plot,
        tooltip=folium.features.GeoJsonTooltip(
            fields=tip_fields,
            aliases=tip_aliases,
        ),
    ).add_to(m)

    st.components.v1.html(m._repr_html_(), height=500)
