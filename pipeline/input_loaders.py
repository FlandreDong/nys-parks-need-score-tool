"""
Load demand/supply inputs from discovered paths into pipeline-ready DataFrames.
"""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pandas as pd

from pipeline import compute


def load_demand_from_path(path: Path) -> tuple[pd.DataFrame | None, pd.DataFrame]:
    """
    Load demand as (m4_wide_optional, demand_df).

    - Excel (.xlsx/.xls): demand wide table via ``ingest_and_clean_demand_workbook``; demand_df
      must be built by caller from wide + activity options.
    - CSV: if ``region_id`` and ``demand_value`` are present, returns (None, demand_df).
      Otherwise raises with a clear message.
    """
    from pipeline.ingest_demand_supply import ingest_and_clean_demand_workbook

    suf = path.suffix.lower()
    if suf in (".xlsx", ".xls"):
        m4_wide = ingest_and_clean_demand_workbook(path, form="wide")
        return m4_wide, None

    if suf == ".csv":
        df = pd.read_csv(path)
        cols = {str(c).strip().lower(): c for c in df.columns}
        if "region_id" in cols and "demand_value" in cols:
            sub = df[[cols["region_id"], cols["demand_value"]]].copy()
            sub.columns = ["region_id", "demand_value"]
            sub["region_id"] = sub["region_id"].astype(str).str.strip()
            sub["demand_value"] = pd.to_numeric(sub["demand_value"], errors="coerce").fillna(0.0)
            demand_df = compute.compute_demand_from_table(sub, value_column="demand_value")
            return None, demand_df
        raise ValueError(
            "CSV demand file must contain columns region_id and demand_value, "
            "or use an Excel demand workbook (D8-style wide)."
        )

    raise ValueError(f"Unsupported demand file type: {path.suffix}")


def load_supply_from_path(path: Path) -> pd.DataFrame:
    """
    Build ``region_id``, ``supply_value`` table from a supply file.

    Supports:
    - GeoJSON / GeoPackage: facility geometries; aggregated via ``compute_supply_from_facilities``.
    - CSV / Excel: tabular; detects region and supply columns by common names.
    """
    suf = path.suffix.lower()
    if suf in (".geojson", ".json", ".gpkg"):
        gdf = gpd.read_file(path)
        if "facility_id" not in gdf.columns:
            gdf = gdf.copy()
            gdf["facility_id"] = [f"fac_{i}" for i in range(len(gdf))]
        if "region_id" not in gdf.columns:
            raise ValueError(
                "Facilities GeoJSON must include region_id (or spatial-join upstream). "
                "Missing region_id in supply file."
            )
        return compute.compute_supply_from_facilities(gdf)

    if suf == ".csv":
        df = pd.read_csv(path)
    elif suf in (".xlsx", ".xls"):
        engine = "xlrd" if suf == ".xls" else "openpyxl"
        df = pd.read_excel(path, engine=engine)
    else:
        raise ValueError(f"Unsupported supply file type: {path.suffix}")

    return load_supply_from_tabular_dataframe(df)


def load_supply_from_tabular_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Detect region + supply columns (same rules as tabular ``load_supply_from_path``).
    Used when scanning multiple Excel sheets for a valid supply table.
    """
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    lower = {str(c).strip().lower(): c for c in df.columns}
    rid_col = None
    for key in ("region_id", "region", "county", "name_2", "origin", "county_name"):
        if key in lower:
            rid_col = lower[key]
            break
    if rid_col is None:
        rid_col = df.columns[0]

    sv_col = None
    for key in ("supply_value", "supply", "capacity", "total_supply", "finalcap"):
        if key in lower:
            sv_col = lower[key]
            break
    if sv_col is None:
        num_cols = df.select_dtypes(include=["number"]).columns.tolist()
        if not num_cols:
            raise ValueError("Supply table has no numeric column for supply_value.")
        sv_col = num_cols[0]

    out = df[[rid_col, sv_col]].copy()
    out.columns = ["region_id", "supply_value"]
    out["region_id"] = out["region_id"].astype(str).str.strip()
    out["supply_value"] = pd.to_numeric(out["supply_value"], errors="coerce")
    return out
