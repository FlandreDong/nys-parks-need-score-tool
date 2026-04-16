"""
Clean module for the RIN pipeline.

Responsibilities
----------------
- Standardize column names
- Validate schemas via `pydantic` models
- Align all datasets to a common geographic unit (county or tract)
- Handle missing values
- Ensure geometry validity
"""

from __future__ import annotations

from typing import Literal, Tuple

import geopandas as gpd
import pandas as pd

from config.settings import geo, paths
from utils.spatial_utils import ensure_valid_geometries, spatial_join_to_target
from utils.validation_utils import (
    BoundaryRecord,
    CensusRecord,
    FacilityRecord,
    M4DemandRecord,
    SurveyRecord,
    validate_dataframe,
)


GeographicUnit = Literal["county", "tract"]


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize column names to lowercase snake_case."""
    df = df.copy()
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )
    return df


def clean_survey(df: pd.DataFrame, region_col: str) -> pd.DataFrame:
    """
    Clean and validate survey participation data.
    """
    df = standardize_columns(df)
    rename_map = {region_col: "region_id"}
    df = df.rename(columns=rename_map)
    df = df[["region_id", "population", "participants"]]
    df = validate_dataframe(df, SurveyRecord)
    df = df.fillna({"participants": 0})
    return df


def clean_census(df: pd.DataFrame, region_col: str) -> pd.DataFrame:
    """
    Clean and validate ACS demographic data.
    """
    df = standardize_columns(df)
    rename_map = {region_col: "region_id"}
    df = df.rename(columns=rename_map)
    keep_cols = [
        "region_id",
        "total_population",
        "median_income",
        "poverty_rate",
    ]
    df = df[keep_cols]
    df = validate_dataframe(df, CensusRecord)
    return df


def clean_facilities(
    gdf: gpd.GeoDataFrame, region_col: str | None = None
) -> gpd.GeoDataFrame:
    """
    Clean and validate facility supply data.

    If facilities are not pre-joined to a region, `region_col` may be None
    and will be filled later via spatial join to boundaries.
    """
    gdf = gdf.copy()
    gdf = ensure_valid_geometries(gdf)
    gdf = gdf.to_crs(geo.crs)

    gdf.columns = [c.strip().lower().replace(" ", "_").replace("-", "_") for c in gdf.columns]

    # Basic required columns
    if "facility_id" not in gdf.columns:
        gdf["facility_id"] = gdf.index.astype(str)
    if "facility_type" not in gdf.columns:
        gdf["facility_type"] = "unknown"

    if region_col and region_col in gdf.columns:
        gdf = gdf.rename(columns={region_col: "region_id"})

    df = pd.DataFrame(gdf.drop(columns="geometry"))
    df_valid = validate_dataframe(df, FacilityRecord)
    gdf = gdf.merge(df_valid[["facility_id"]], on="facility_id")
    return gdf


def clean_boundaries(
    gdf: gpd.GeoDataFrame, region_col: str, unit: GeographicUnit
) -> gpd.GeoDataFrame:
    """
    Clean geography boundaries and enforce target geographic unit.
    """
    gdf = gdf.copy()
    gdf = ensure_valid_geometries(gdf)
    gdf = gdf.to_crs(geo.crs)

    gdf.columns = [c.strip().lower().replace(" ", "_").replace("-", "_") for c in gdf.columns]
    gdf = gdf.rename(columns={region_col: "region_id"})

    df = pd.DataFrame(gdf.drop(columns="geometry"))
    df_valid = validate_dataframe(df, BoundaryRecord)
    gdf = gdf.merge(df_valid[["region_id"]], on="region_id")

    # Future: if inputs are in tracts and unit="county", dissolve here.
    # For now, simply assume provided boundaries match the configured unit.
    return gdf


def clean_m4_demand_long(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean activity-level demand in long form (region_id, activity, demand).

    Used for D8 / legacy M4-style long tables from ``ingest_demand_supply``.

    - Standardize column names and region_id (strip, upper)
    - Validate with M4DemandRecord; drop invalid rows
    - Fill missing demand with 0
    """
    df = standardize_columns(df.copy())
    if "region_id" not in df.columns or "activity" not in df.columns or "demand" not in df.columns:
        raise ValueError("Demand long table must have columns: region_id, activity, demand")
    df["region_id"] = df["region_id"].astype(str).str.strip().str.upper()
    df["activity"] = df["activity"].astype(str).str.strip()
    df["demand"] = pd.to_numeric(df["demand"], errors="coerce").fillna(0)
    df = validate_dataframe(df, M4DemandRecord)
    return df.reset_index(drop=True)


def clean_m4_demand_wide(
    df: pd.DataFrame,
    activity_columns: list[str] | None = None,
) -> pd.DataFrame:
    """
    Clean activity-level demand in wide form (one row per region, one column per activity).

    Used for D8 / legacy M4-style Excel from ``ingest_demand_supply``.

    - Standardize column names and region_id (strip, upper)
    - Ensure activity columns are numeric, demand >= 0, fillna(0)
    - If activity_columns is None, treat all columns except region_id as activity columns
    """
    df = standardize_columns(df.copy())
    if "region_id" not in df.columns:
        raise ValueError("Demand wide table must have column region_id")
    df["region_id"] = df["region_id"].astype(str).str.strip().str.upper()

    if activity_columns is None:
        activity_columns = [c for c in df.columns if c != "region_id"]
    for c in activity_columns:
        if c not in df.columns:
            continue
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).clip(lower=0)
    return df


def clean_m5_rin(
    df: pd.DataFrame,
    rin_min: float = 1.0,
    rin_max: float = 10.0,
    rin_columns: list[str] | None = None,
) -> pd.DataFrame:
    """
    Clean legacy RIN sheet (region_id + activity RIN scores).

    - Standardize column names and region_id (strip)
    - Ensure RIN columns are numeric and in [rin_min, rin_max]; clip invalid values
    - Fill missing RIN with NaN (or drop rows) so downstream can decide
    """
    df = standardize_columns(df.copy())
    if "region_id" not in df.columns:
        # First column is typically region/county name
        df = df.rename(columns={df.columns[0]: "region_id"})
    df["region_id"] = df["region_id"].astype(str).str.strip()

    if rin_columns is None:
        rin_columns = [c for c in df.columns if c != "region_id" and df[c].dtype in ("float64", "int64")]
        if not rin_columns:
            rin_columns = [c for c in df.columns if c != "region_id"]
    for c in rin_columns:
        if c not in df.columns:
            continue
        df[c] = pd.to_numeric(df[c], errors="coerce")
        df[c] = df[c].clip(lower=rin_min, upper=rin_max)
    return df


def align_to_geography(
    survey_df: pd.DataFrame,
    census_df: pd.DataFrame,
    facilities_gdf: gpd.GeoDataFrame,
    boundaries_gdf: gpd.GeoDataFrame,
    unit: GeographicUnit,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, gpd.GeoDataFrame]:
    """
    Align all datasets to the common geographic unit (county or tract).

    For this template, we assume:
    - survey_df and census_df already have `region_id` aligned to boundaries
    - facilities are spatially joined to boundaries if `region_id` is missing
    """
    bnds = boundaries_gdf[["region_id", "geometry"]].copy()

    if "region_id" not in facilities_gdf.columns:
        joined = spatial_join_to_target(facilities_gdf, bnds, how="left")
        facilities_gdf = joined

    # Ensure only facilities with a matched region_id proceed
    facilities_gdf = facilities_gdf[facilities_gdf["region_id"].notna()].copy()

    # Persist processed versions
    survey_df.to_csv(paths.data_processed / f"survey_{unit}.csv", index=False)
    census_df.to_csv(paths.data_processed / f"census_{unit}.csv", index=False)
    facilities_gdf.to_file(
        paths.data_processed / f"facilities_{unit}.geojson",
        driver="GeoJSON",
    )
    bnds.to_file(
        paths.data_processed / f"boundaries_{unit}.geojson",
        driver="GeoJSON",
    )

    return survey_df, census_df, facilities_gdf, bnds

