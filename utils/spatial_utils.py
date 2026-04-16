"""
Spatial utility functions for the RIN pipeline.

This module centralizes common spatial operations used across
ingest / clean / compute stages, such as:
- CRS normalization
- Geometry validation and fixing
- Spatial joins for aligning to a target geography (county / tract)
"""

from __future__ import annotations

from typing import Literal

import geopandas as gpd
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union

from config.settings import geo


def to_pipeline_crs(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Reproject a GeoDataFrame to the pipeline-wide CRS.

    Parameters
    ----------
    gdf : GeoDataFrame
        Input GeoDataFrame with a defined CRS.
    """
    if gdf.crs is None:
        raise ValueError("Input GeoDataFrame must have a CRS defined.")
    if str(gdf.crs) == geo.crs:
        return gdf
    return gdf.to_crs(geo.crs)


def ensure_valid_geometries(
    gdf: gpd.GeoDataFrame, repair: bool = True
) -> gpd.GeoDataFrame:
    """
    Ensure geometries are valid; optionally attempt to repair them.

    Uses a simple buffer(0) fix which is generally safe for polygon data.
    """
    if not repair:
        return gdf[gdf.geometry.notna()].copy()

    gdf = gdf[gdf.geometry.notna()].copy()
    gdf["geometry"] = gdf["geometry"].buffer(0)
    return gdf


def dissolve_to_geography(
    gdf: gpd.GeoDataFrame, geo_id_col: str
) -> gpd.GeoDataFrame:
    """
    Dissolve geometries and attributes to a given geographic ID column.

    Typically used to transform tract-level inputs to county-level
    or aggregate facility points into polygons.
    """
    if geo_id_col not in gdf.columns:
        raise KeyError(f"Column '{geo_id_col}' not found in GeoDataFrame.")

    return gdf.dissolve(by=geo_id_col)


def spatial_join_to_target(
    source_gdf: gpd.GeoDataFrame,
    target_gdf: gpd.GeoDataFrame,
    how: Literal["left", "right", "inner"] = "left",
    predicate: str = "intersects",
) -> gpd.GeoDataFrame:
    """
    Join source geometries to a target geography.

    This is used to align survey / facility data to county or tract
    boundaries by spatially intersecting with a canonical boundary layer.
    """
    source = to_pipeline_crs(source_gdf)
    target = to_pipeline_crs(target_gdf)

    return gpd.sjoin(source, target, how=how, predicate=predicate)


def total_area(geometries: list[BaseGeometry]) -> float:
    """
    Compute total area of a collection of geometries in the pipeline CRS.

    This can be useful if supply / demand need to be normalized by area.
    """
    union = unary_union(geometries)
    return float(union.area)

