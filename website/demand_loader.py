"""
Load wide-format activity demand for the Streamlit sidebar (D8 / same layout as pipeline).

Separate from ``data_loader`` so imports resolve reliably when Streamlit caches modules.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd

from config.settings import paths
from pipeline.data_discovery import discover_latest_demand_path
from pipeline.demand_excel_reader import read_wide_demand_workbook
from pipeline.demand_aggregate import aggregate_d8_per_activity
from pipeline.ingest_demand_supply import ACTIVITY_COLUMNS
from pipeline.region_keys import normalize_merge_key

PROJECT_DIR = paths.base_dir


def _detect_demand_path() -> Path:
    found = discover_latest_demand_path()
    if found is not None:
        return found
    for ext in (".xlsx", ".xls", ".csv"):
        p = PROJECT_DIR / f"M4_2019_Demand_at_Origin_Simple{ext}"
        if p.exists():
            return p
    raise FileNotFoundError(
        f"No demand dataset found. Use a filename containing D8 or demand (legacy: M4 prefix), "
        f"xlsx/xls/csv, under {paths.data_raw} or {PROJECT_DIR}, "
        f"or place M4_2019_Demand_at_Origin_Simple.* at project root."
    )


def _guess_region_column(columns: list[str]) -> Optional[str]:
    candidates = [
        "region_id",
        "region",
        "county",
        "origin",
        "origin_name",
        "county_name",
    ]
    norm = {str(c).strip().lower(): c for c in columns}
    for key in candidates:
        if key in norm:
            return norm[key]
    for c in columns:
        if c in ACTIVITY_COLUMNS:
            continue
        if str(c).strip().lower().startswith("unnamed"):
            continue
        return c
    return None


def load_activity_demand_wide() -> pd.DataFrame:
    """
    Latest demand file by mtime (D8/demand filename rules; legacy: M4 prefix), wide activity columns.
    """
    path = _detect_demand_path()
    suffix = path.suffix.lower()

    if suffix in {".xlsx", ".xls"}:
        wide = read_wide_demand_workbook(path)
        df = wide.rename(columns={"region_id": "Region"})
        region_col = "Region"
    elif suffix == ".csv":
        df = pd.read_csv(path)
        region_col = _guess_region_column([str(c) for c in df.columns])
        if region_col is None:
            raise ValueError("Could not detect a region/county column in the demand CSV.")
    else:
        raise ValueError(f"Unsupported demand file type: {suffix}")

    present = [c for c in ACTIVITY_COLUMNS if c in df.columns]
    if not present:
        raise ValueError(
            "Demand table has none of the standard activity columns "
            f"({', '.join(ACTIVITY_COLUMNS[:3])}, …)."
        )

    out = df[[region_col] + present].copy()
    out = out.rename(columns={region_col: "Region"})
    out["Region"] = out["Region"].map(normalize_merge_key)
    out = out[out["Region"] != ""]
    for c in ACTIVITY_COLUMNS:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    out = out.dropna(subset=["Region"])
    return out


def load_demand_activity_wide() -> pd.DataFrame:
    """Alias for :func:`load_activity_demand_wide`."""
    return load_activity_demand_wide()


# Backwards-compatible alias
def load_m4_activity_demand_wide() -> pd.DataFrame:
    return load_demand_activity_wide()


def load_d8_activity_aggregates() -> pd.Series:
    """
    Per-activity demand scalars from D8: **sum across all rows** in the workbook (model inputs).

    Used by the sidebar ranking with county-level supply; D8 rows are **not** matched to supply by county.
    """
    path = _detect_demand_path()
    suf = path.suffix.lower()
    if suf not in (".xlsx", ".xls"):
        raise ValueError(
            "Aggregated D8 demand is only available for Excel wide files. "
            "CSV demand (region_id + demand_value) uses a different pipeline path."
        )
    wide = read_wide_demand_workbook(path)
    s = aggregate_d8_per_activity(wide, how="sum")
    if s.empty:
        raise ValueError("Could not aggregate any standard activity columns from the D8 workbook.")
    return s
