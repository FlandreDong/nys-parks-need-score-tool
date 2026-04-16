"""
Load **demand** from Excel in the D8-style wide layout (legacy workbook compatible).

Used by ``input_loaders``, ``run_pipeline``, and (for the same activity columns) the website.
**Supply** files are loaded via ``input_loaders.load_supply_from_path`` (S12 / tabular / geo).
NY boundaries: ``ingest_ny_boundaries``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import pandas as pd

# Standard activity columns in the demand workbook (cols 2–14 after index + county).
ACTIVITY_COLUMNS = [
    "Park",
    "Swimming",
    "Biking",
    "Golfing",
    "Court Games",
    "Field Games",
    "Walking/Jogging",
    "Camping",
    "Fishing",
    "Boating",
    "Local Winter",
    "Downhill Skiing",
    "Snowmobiling",
]

# Backwards-compatible alias (older code referred to these as M4 activities).
M4_ACTIVITIES = ACTIVITY_COLUMNS


def load_demand_long(
    path: Path,
    sheet_name: str = "Sheet1",
    header_rows: int = 3,
) -> pd.DataFrame:
    """Long form: region_id, activity, demand."""
    df = load_demand_wide(path, sheet_name, header_rows)
    out = df.melt(
        id_vars=["region_id"],
        value_vars=ACTIVITY_COLUMNS,
        var_name="activity",
        value_name="demand",
    )
    out["demand"] = pd.to_numeric(out["demand"], errors="coerce").fillna(0)
    return out


def load_demand_wide(
    path: Path,
    sheet_name: str = "Sheet1",
    header_rows: int = 3,
) -> pd.DataFrame:
    """Wide: region_id + one column per activity (auto-detects sheet / header layout)."""
    _ = (sheet_name, header_rows)  # API compat; flexible reader scans sheets
    from pipeline.demand_excel_reader import read_wide_demand_workbook

    return read_wide_demand_workbook(path)


# Backwards-compatible function names
def load_m4_demand(path: Path, sheet_name: str = "Sheet1", header_rows: int = 3) -> pd.DataFrame:
    return load_demand_long(path, sheet_name=sheet_name, header_rows=header_rows)


def load_m4_demand_wide(path: Path, sheet_name: str = "Sheet1", header_rows: int = 3) -> pd.DataFrame:
    return load_demand_wide(path, sheet_name=sheet_name, header_rows=header_rows)


def ingest_and_clean_demand_workbook(
    path: Path | str,
    form: Literal["long", "wide"] = "wide",
    sheet_name: str = "Sheet1",
    header_rows: int = 3,
) -> pd.DataFrame:
    """Load demand Excel and run ``clean`` helpers."""
    from pipeline import clean

    path = Path(path)
    if form == "long":
        df = load_demand_long(path, sheet_name=sheet_name, header_rows=header_rows)
        return clean.clean_m4_demand_long(df)
    df = load_demand_wide(path, sheet_name=sheet_name, header_rows=header_rows)
    return clean.clean_m4_demand_wide(df, activity_columns=ACTIVITY_COLUMNS)


# Backwards-compatible alias
def ingest_and_clean_m4(
    path: Path | str,
    form: Literal["long", "wide"] = "wide",
    sheet_name: str = "Sheet1",
    header_rows: int = 3,
) -> pd.DataFrame:
    return ingest_and_clean_demand_workbook(
        path, form=form, sheet_name=sheet_name, header_rows=header_rows
    )
