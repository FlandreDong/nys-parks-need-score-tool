"""
Read D8-style **wide** demand workbooks with multiple possible layouts:

1. **Legacy fixed layout**: a sheet where row 0 is not the header; data starts after 3 title rows,
   columns are index + region + 13 activity columns (same order as ``ACTIVITY_COLUMNS``).
2. **Header row**: any sheet where some row has headers including a region/county column
   and all standard activity names (case- and spacing-insensitive).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from pipeline.ingest_demand_supply import ACTIVITY_COLUMNS


def _excel_engine(path: Path) -> str:
    return "xlrd" if path.suffix.lower() == ".xls" else "openpyxl"


def _column_key(name: str) -> str:
    return "".join(ch.lower() for ch in str(name).strip() if ch not in " \t/_\\-")


def _sheet_order(names: list[str]) -> list[str]:
    preferred: list[str] = []
    for key in ("sheet1", "demand", "d8", "m4", "data"):
        for s in names:
            if str(s).strip().lower() == key:
                preferred.append(s)
                break
    rest = [s for s in names if s not in preferred]
    return preferred + rest


def _try_m4_fixed_layout(path: Path, engine: str, sheet: str) -> pd.DataFrame | None:
    try:
        raw = pd.read_excel(path, sheet_name=sheet, header=None, engine=engine)
    except Exception:
        return None
    nrows, ncols = raw.shape
    need = 2 + len(ACTIVITY_COLUMNS)
    if ncols < need or nrows < 5:
        return None
    data = raw.iloc[3:, :need].copy()
    data.columns = ["_idx", "region_id"] + list(ACTIVITY_COLUMNS)
    data = data.drop(columns=["_idx"], errors="ignore")
    rid = data["region_id"]
    if rid.isna().all():
        return None
    return data


def _match_region_column(cols_stripped: list[str]) -> str | None:
    lm = {c.lower(): c for c in cols_stripped}
    candidates = (
        "region_id",
        "region",
        "county",
        "county_name",
        "origin",
        "origin_name",
        "name_2",
        "name2",
        "geography",
        "geo_name",
    )
    for key in candidates:
        if key in lm:
            return lm[key]
    act_norms = {_column_key(a) for a in ACTIVITY_COLUMNS}
    for c in cols_stripped:
        if _column_key(c) in act_norms:
            continue
        if c.lower().startswith("unnamed"):
            continue
        return c
    return None


def _try_header_row_layout(path: Path, engine: str, sheet: str, header_row: int) -> pd.DataFrame | None:
    try:
        df = pd.read_excel(path, sheet_name=sheet, header=header_row, engine=engine)
    except Exception:
        return None
    col_list = list(df.columns)
    cols_stripped = [
        str(c).strip() if c is not None and str(c).strip() != "" else f"unnamed_{i}"
        for i, c in enumerate(col_list)
    ]
    df = df.copy()
    df.columns = cols_stripped

    lm = {}
    for c in cols_stripped:
        k = c.lower()
        if k not in lm:
            lm[k] = c

    norm_to_col: dict[str, str] = {}
    for c in cols_stripped:
        nk = _column_key(c)
        if nk not in norm_to_col:
            norm_to_col[nk] = c

    region_col = _match_region_column(cols_stripped)
    if region_col is None:
        return None

    activity_src_cols: list[str] = []
    for act in ACTIVITY_COLUMNS:
        act_k = act.lower()
        cand = None
        if act_k in lm:
            cand = lm[act_k]
        else:
            nk = _column_key(act)
            if nk in norm_to_col:
                cand = norm_to_col[nk]
        if cand is None:
            return None
        activity_src_cols.append(cand)

    if region_col in activity_src_cols:
        return None

    sub = df[[region_col] + activity_src_cols].copy()
    sub.columns = ["region_id"] + list(ACTIVITY_COLUMNS)
    sub["region_id"] = sub["region_id"].astype(str).str.strip()
    if sub["region_id"].replace("", pd.NA).isna().all():
        return None
    return sub


def read_wide_demand_workbook(path: Path) -> pd.DataFrame:
    """
    Return wide demand: ``region_id`` (uppercase) and one numeric column per entry in
    ``ACTIVITY_COLUMNS``.
    """
    path = Path(path)
    if path.name.lower().startswith("~$"):
        raise ValueError(
            f"Cannot open Excel lock file {path.name!r}. Close the file in Excel and use the real "
            f"workbook (not the ~$ temporary copy)."
        )
    engine = _excel_engine(path)
    xl = pd.ExcelFile(path, engine=engine)
    sheets = _sheet_order(xl.sheet_names)

    out: pd.DataFrame | None = None
    for sheet in sheets:
        got = _try_m4_fixed_layout(path, engine, sheet)
        if got is not None:
            out = got
            break

    if out is None:
        for sheet in sheets:
            for h in range(0, 15):
                got = _try_header_row_layout(path, engine, sheet, h)
                if got is not None:
                    out = got
                    break
            if out is not None:
                break

    if out is None:
        raise ValueError(
            "Could not parse demand workbook: need either (1) a sheet with 3 leading title rows "
            "then columns index + region + the 13 standard activity columns, or (2) a header row "
            "with Region/County and those activity names."
        )

    out["region_id"] = out["region_id"].astype(str).str.strip().str.upper()
    for c in ACTIVITY_COLUMNS:
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0)
    return out
