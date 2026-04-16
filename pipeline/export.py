"""
Export module for the park planning pipeline (Need Score).

Exports: need_score, need_class, priority_rank. Optionally writes all run
outputs (web map, PDF, Excel) into a single timestamped folder for easy sharing.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional, Tuple

import geopandas as gpd
import pandas as pd

from config.settings import paths, version
from pipeline.compute import regions_needing_improvement


def _timestamped_name(base: str, ext: str) -> Path:
    return paths.data_outputs / f"{base}_{version.vintage}_{version.run_timestamp}.{ext}"


def _export_run_folder() -> Path:
    """Create and return a timestamped folder for this run (exports/run_<timestamp>).

    Note: the `exports/` directory may be absent in a clean workspace; it will be created on demand.
    """
    folder = paths.base_dir / "exports" / f"run_{version.run_timestamp}"
    folder.mkdir(parents=True, exist_ok=True)
    return folder


def export_results(
    need_table: pd.DataFrame,
    need_gdf: Optional[gpd.GeoDataFrame] = None,
    threshold: float = 1.0,
    export_to_run_folder: bool = True,
    activity_metrics_long: Optional[pd.DataFrame] = None,
) -> Tuple[Optional[Path], Path, Path, Optional[Path]]:
    """
    Export Need Score results. Optionally copy table, improvement list, and
    spatial layer into a timestamped run folder (for Excel + GeoJSON there).

    Parameters
    ----------
    need_table : pd.DataFrame
        region_id, demand_value, supply_value, need_score, need_class, priority_rank.
    need_gdf : gpd.GeoDataFrame, optional
        Spatial layer with same fields + geometry.
    activity_metrics_long : pd.DataFrame, optional
        County × activity metrics (gap, supply_demand_ratio, need_score); written when non-empty.
    threshold : float
        Need score threshold for "needs improvement" list (default 1.0 with NEED_SCORE_SCALE=100).
    export_to_run_folder : bool
        If True, create exports/run_<timestamp> and write Excel + GeoJSON there.

    Returns
    -------
    spatial_path : Path or None
    table_path : Path (CSV in data/outputs)
    improvement_path : Path (CSV in data/outputs)
    run_folder : Path or None (exports/run_<timestamp> if export_to_run_folder)
    """
    table_path = _timestamped_name("need_table", "csv")
    improvement_path = _timestamped_name("need_improvement", "csv")

    need_table.to_csv(table_path, index=False)

    improvement_df = regions_needing_improvement(need_table, threshold=threshold)
    improvement_df.to_csv(improvement_path, index=False)

    spatial_path: Optional[Path] = None
    if need_gdf is not None:
        spatial_path = _timestamped_name("need_spatial", "geojson")
        need_gdf.to_file(spatial_path, driver="GeoJSON")

    # Stable names for Streamlit / website (overwritten each run)
    out_dir = paths.data_outputs
    out_dir.mkdir(parents=True, exist_ok=True)
    need_table.to_csv(out_dir / "need_score_by_region.csv", index=False)
    improvement_df.to_csv(out_dir / "priority_ranking.csv", index=False)
    if need_gdf is not None:
        need_gdf.to_file(out_dir / "need_score_map.geojson", driver="GeoJSON")

    if activity_metrics_long is not None and not activity_metrics_long.empty:
        am_path = _timestamped_name("activity_metrics_by_county", "csv")
        activity_metrics_long.to_csv(am_path, index=False)
        activity_metrics_long.to_csv(out_dir / "activity_metrics_by_county.csv", index=False)

    run_folder: Optional[Path] = None
    if export_to_run_folder:
        run_folder = _export_run_folder()
        need_table.to_excel(run_folder / "need_table.xlsx", index=False)
        improvement_df.to_excel(run_folder / "need_improvement.xlsx", index=False)
        if need_gdf is not None:
            need_gdf.to_file(run_folder / "need_spatial.geojson", driver="GeoJSON")
        if activity_metrics_long is not None and not activity_metrics_long.empty:
            activity_metrics_long.to_excel(run_folder / "activity_metrics_by_county.xlsx", index=False)

    return spatial_path, table_path, improvement_path, run_folder


def export_map(
    need_gdf: gpd.GeoDataFrame,
    output_path: Optional[Path] = None,
    title: str = "Need Score (Demand/Supply × 100)",
    column: str = "need_score",
    cmap: str = "YlOrRd",
    figsize: Tuple[float, float] = (10, 8),
    formats: Tuple[str, ...] = ("png", "pdf"),
) -> Optional[Path]:
    """
    Export choropleth map of need_score (static PNG/PDF).
    """
    if need_gdf is None or need_gdf.empty or column not in need_gdf.columns:
        return None
    gdf = need_gdf.dropna(subset=["geometry"]).copy()
    if gdf.empty:
        return None

    import matplotlib.pyplot as plt

    if output_path is None:
        output_path = paths.base_dir / f"need_map_{version.vintage}_{version.run_timestamp}"
    else:
        output_path = Path(output_path)
    if output_path.suffix:
        output_path = output_path.with_suffix("")

    fig, ax = plt.subplots(1, 1, figsize=figsize)
    gdf.plot(column=column, ax=ax, legend=True, cmap=cmap, edgecolor="gray", linewidth=0.4)
    ax.set_title(title)
    ax.set_axis_off()
    plt.tight_layout()

    first_path: Optional[Path] = None
    for fmt in formats:
        if fmt.lower() == "png":
            p = Path(f"{output_path}.png")
            fig.savefig(p, dpi=150, bbox_inches="tight")
            if first_path is None:
                first_path = p
        elif fmt.lower() == "pdf":
            p = Path(f"{output_path}.pdf")
            fig.savefig(p, bbox_inches="tight")
            if first_path is None:
                first_path = p
    plt.close(fig)
    return first_path


def export_map_html(
    need_gdf: gpd.GeoDataFrame,
    output_path: Optional[Path] = None,
    title: str = "Need Score (Demand/Supply × 100)",
    column: str = "need_score",
    tooltip_columns: Optional[Tuple[str, ...]] = None,
) -> Optional[Path]:
    """
    Export interactive map website (single HTML) with need_score choropleth and tooltips.
    """
    if need_gdf is None or need_gdf.empty or column not in need_gdf.columns:
        return None
    gdf = need_gdf.dropna(subset=["geometry"]).copy()
    if gdf.empty:
        return None

    try:
        import folium
    except ImportError:
        return None

    if output_path is None:
        output_path = paths.base_dir / f"need_map_{version.vintage}_{version.run_timestamp}.html"
    else:
        output_path = Path(output_path)

    if gdf.crs and str(gdf.crs) != "EPSG:4326":
        gdf = gdf.to_crs("EPSG:4326")

    gdf = gdf.copy()
    gdf[column] = gdf[column].fillna(-1)

    m = folium.Map(location=[43.0, -75.5], zoom_start=7, tiles="CartoDB positron")
    folium.Choropleth(
        geo_data=gdf,
        data=gdf,
        columns=["region_id", column],
        key_on="feature.properties.region_id",
        fill_color="YlOrRd",
        fill_opacity=0.7,
        line_opacity=0.3,
        legend_name=title,
        nan_fill_color="gray",
        nan_fill_opacity=0.3,
    ).add_to(m)

    if tooltip_columns is None:
        tooltip_columns = ("region_id", "need_score", "need_class", "priority_rank")
    available = [c for c in tooltip_columns if c in gdf.columns]
    if available:
        folium.GeoJson(
            gdf,
            name="Tooltips",
            tooltip=folium.features.GeoJsonTooltip(
                fields=available,
                aliases=[c.replace("_", " ").title() for c in available],
                localize=True,
            ),
        ).add_to(m)

    m.get_root().html.add_child(
        folium.Element(
            "<div style='position:absolute;top:10px;left:50px;z-index:9999;"
            "font-family:sans-serif;font-size:16px;'><strong>Park Planning: "
            "Need Score by County</strong><br/>NeedScore = (Demand/Supply)×100 | "
            "&lt;0.5 Sufficient | 0.5–1.0 Balanced | &gt;1.0 Needs Improvement</div>"
        )
    )

    m.save(str(output_path))
    return Path(output_path)
