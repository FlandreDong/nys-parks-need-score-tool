"""
Load NY State administrative boundaries from project NY/ folder.

Shapefile: new-york Administrative Areas_USA_2.shp
- NAME_2 = county name (Title Case). Normalized to region_id to match legacy workbooks (uppercase,
  Saint Lawrence -> St. Lawrence).
"""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd

from config.settings import geo

# Default path relative to project root
DEFAULT_NY_SHP = Path("NY") / "new-york Administrative Areas_USA_2.shp"


def _name2_to_region_id(name: str) -> str:
    """Normalize NAME_2 to region_id (match legacy workbook county names)."""
    s = str(name).strip().upper()
    if s == "SAINT LAWRENCE":
        return "ST. LAWRENCE"
    return s


def load_ny_boundaries(
    path: Path | str | None = None,
    region_col: str = "NAME_2",
    target_crs: str | None = None,
) -> gpd.GeoDataFrame:
    """
    Load NY county boundaries and set region_id for pipeline join.

    Parameters
    ----------
    path : Path | str | None
        Path to .shp. If None, use DEFAULT_NY_SHP (NY/new-york Administrative Areas_USA_2.shp).
    region_col : str
        Attribute column for area name (default NAME_2 = county).
    target_crs : str | None
        If set, reproject to this CRS (e.g. config geo.crs). Default None = keep WGS84.

    Returns
    -------
    gpd.GeoDataFrame
        Columns: region_id, geometry (+ original cols). Dropped: Lake Ontario if present.
    """
    path = path or DEFAULT_NY_SHP
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"NY boundaries not found: {path}")

    gdf = gpd.read_file(path)
    if region_col not in gdf.columns:
        raise ValueError(f"Column {region_col} not in {list(gdf.columns)}")

    gdf["region_id"] = gdf[region_col].apply(_name2_to_region_id)
    # Drop non-county rows that are not in M4 (e.g. Lake Ontario)
    gdf = gdf[~gdf["region_id"].isin(("", "LAKE ONTARIO", "NAN"))].copy()

    if target_crs and str(gdf.crs) != target_crs:
        gdf = gdf.to_crs(target_crs)

    return gdf[["region_id", "geometry"]].copy() if "region_id" in gdf.columns else gdf


def load_ny_boundaries_for_m4(
    path: Path | str | None = None,
) -> gpd.GeoDataFrame:
    """
    Load NY boundaries and reproject to pipeline CRS, ready for merge with M4 demand.
    """
    return load_ny_boundaries(path=path, target_crs=geo.crs)
