"""
Per-activity supply denominators for the website (sidebar ActivityNeedScore).

Delegates to :mod:`pipeline.supply_for_activity` for the latest name-classified supply file,
no legacy workbook fallback.
"""

from __future__ import annotations

import pandas as pd

from pipeline.supply_for_activity import load_supply_keyed_for_discovered_activity


def load_discovered_supply_for_activity(activity: str) -> pd.DataFrame | None:
    return load_supply_keyed_for_discovered_activity(activity)


def load_activity_supply_for_ranking(activity: str) -> pd.DataFrame | None:
    got = load_discovered_supply_for_activity(activity)
    if got is None or got.empty:
        return None
    return got
