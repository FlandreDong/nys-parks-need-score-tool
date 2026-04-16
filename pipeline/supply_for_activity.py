"""
Resolve per-county supply for one recreation **activity** from a tabular or vector supply file.

Used by the website sidebar and by :func:`pipeline.activity_metrics.build_county_activity_metrics_long`.
Matches the semi-automated rule: activity-named column if present, else a single ``supply_value`` column
(see :func:`pipeline.input_loaders.load_supply_from_path`).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from pipeline.data_discovery import discover_latest_supply_path
from pipeline.input_loaders import load_supply_from_path, load_supply_from_tabular_dataframe
from pipeline.region_keys import normalize_merge_key
from pipeline.summary_footer import is_d8_summary_footer_row


def _column_key(name: str) -> str:
    return "".join(ch.lower() for ch in str(name).strip() if ch not in " \t/_\\-")


def _region_column(df: pd.DataFrame) -> str:
    lower = {str(c).strip().lower(): c for c in df.columns}
    for key in ("region_id", "region", "county", "name_2", "origin", "county_name"):
        if key in lower:
            return lower[key]
    return str(df.columns[0])


def _activity_supply_column(df: pd.DataFrame, activity: str) -> str | None:
    act_l = activity.strip().lower()
    act_k = _column_key(activity)
    for c in df.columns:
        s = str(c).strip()
        if s.lower() == act_l or _column_key(s) == act_k:
            return s
    return None


def _to_keyed_supply(sub: pd.DataFrame) -> pd.DataFrame | None:
    out = sub.copy()
    out["supply_value"] = pd.to_numeric(out["supply_value"], errors="coerce")
    out = out.loc[~out["region_id"].map(is_d8_summary_footer_row)]
    if out.empty:
        return None
    out["_key"] = out["region_id"].map(normalize_merge_key)
    out = out[out["_key"] != ""]
    if out.empty:
        return None
    agg = out.groupby("_key", as_index=False)["supply_value"].sum()
    if agg.empty or agg["supply_value"].notna().sum() == 0:
        return None
    return agg


def load_supply_keyed_for_activity(path: Path, activity: str) -> pd.DataFrame | None:
    """
    Return ``_key`` (region id, uppercased) and ``supply_value`` for ``activity`` from ``path``.
    """
    path = Path(path)
    suf = path.suffix.lower()
    try:
        if suf in (".geojson", ".json", ".gpkg"):
            sub = load_supply_from_path(path)
            return _to_keyed_supply(sub)

        if suf == ".csv":
            df = pd.read_csv(path)
            df.columns = [str(c).strip() for c in df.columns]
            rid = _region_column(df)
            act_col = _activity_supply_column(df, activity)
            if act_col is not None and act_col != rid:
                sub = df[[rid, act_col]].copy()
                sub.columns = ["region_id", "supply_value"]
                return _to_keyed_supply(sub)
            sub = load_supply_from_tabular_dataframe(df)
            return _to_keyed_supply(sub)

        if suf in (".xlsx", ".xls"):
            engine = "xlrd" if suf == ".xls" else "openpyxl"
            xl = pd.ExcelFile(path, engine=engine)
            for sheet in xl.sheet_names:
                try:
                    df = pd.read_excel(path, sheet_name=sheet, engine=engine)
                except Exception:
                    continue
                df.columns = [str(c).strip() for c in df.columns]
                if len(df.columns) < 2:
                    continue
                rid = _region_column(df)
                act_col = _activity_supply_column(df, activity)
                if act_col is not None and act_col != rid:
                    sub = df[[rid, act_col]].copy()
                    sub.columns = ["region_id", "supply_value"]
                    got = _to_keyed_supply(sub)
                    if got is not None:
                        return got
                try:
                    sub = load_supply_from_tabular_dataframe(df)
                    got = _to_keyed_supply(sub)
                    if got is not None:
                        return got
                except Exception:
                    continue
            return None
        return None
    except Exception:
        return None


def load_supply_keyed_for_discovered_activity(activity: str) -> pd.DataFrame | None:
    """Same as :func:`load_supply_keyed_for_activity` using ``discover_latest_supply_path()``."""
    p = discover_latest_supply_path()
    if p is None:
        return None
    return load_supply_keyed_for_activity(p, activity)
