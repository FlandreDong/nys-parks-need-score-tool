"""
D8 (wide) demand as **model inputs**, not a table to row-match against supply by county.

Aggregate each activity column across all rows in the D8 workbook (survey-weighted county rows
are combined into a single demand measure per activity). Those scalars are then paired with
**county-level supply** from S12: for each county ``c``, scores use the same demand input(s) and
``supply[c]``.

County alignment for mapping is **result table ↔ NY boundaries**, not D8 regions ↔ supply regions.
"""

from __future__ import annotations

from typing import Literal

import pandas as pd

from pipeline.ingest_demand_supply import ACTIVITY_COLUMNS
from pipeline.summary_footer import is_d8_summary_footer_row


def drop_d8_summary_footer_rows(m4_wide: pd.DataFrame) -> pd.DataFrame:
    """Remove summary/total rows before aggregating D8 activity columns."""
    if m4_wide is None or m4_wide.empty or "region_id" not in m4_wide.columns:
        return m4_wide
    mask = ~m4_wide["region_id"].map(is_d8_summary_footer_row)
    return m4_wide.loc[mask].copy()


def _column_key(name: str) -> str:
    return "".join(ch.lower() for ch in str(name).strip() if ch not in " \t/_\\-")


def _resolve_activity_column(df: pd.DataFrame, activity_label: str) -> str | None:
    """Map canonical activity name to actual column (handles ``park`` vs ``Park`` after clean)."""
    want = _column_key(activity_label)
    for c in df.columns:
        if str(c).lower() == "region_id":
            continue
        if _column_key(str(c)) == want:
            return str(c)
    return None


def aggregate_d8_per_activity(
    m4_wide: pd.DataFrame,
    *,
    how: Literal["sum", "mean"] = "sum",
) -> pd.Series:
    """
    One value per activity: aggregate across **all rows** of the D8 wide table.

    Parameters
    ----------
    how
        ``sum`` — total demand index mass in the file (default). ``mean`` — average across county rows.
    """
    m4_wide = drop_d8_summary_footer_rows(m4_wide)
    data: dict[str, pd.Series] = {}
    for act in ACTIVITY_COLUMNS:
        col = _resolve_activity_column(m4_wide, act)
        if col is None:
            continue
        data[act] = pd.to_numeric(m4_wide[col], errors="coerce").fillna(0.0)
    if not data:
        return pd.Series(dtype=float)
    sub = pd.DataFrame(data)
    if how == "sum":
        return sub.sum(axis=0)
    return sub.mean(axis=0)


def scalar_demand_for_pipeline(
    m4_wide: pd.DataFrame,
    *,
    activity: str | None = None,
    how: Literal["sum", "mean"] = "sum",
) -> float:
    """
    Single demand scalar for the main NeedScore run (all activities or one activity column).

    Used to build a constant ``demand_value`` for every county in the supply table.
    """
    s = aggregate_d8_per_activity(m4_wide, how=how)
    if s.empty:
        return 0.0
    if activity:
        ac = activity.strip()
        if ac in s.index:
            return float(s[ac])
        ac_l = ac.lower()
        for k in s.index:
            if str(k).lower() == ac_l:
                return float(s[k])
        raise ValueError(f"Activity {activity!r} not found in D8 aggregates: {list(s.index)}")
    return float(s.sum())
