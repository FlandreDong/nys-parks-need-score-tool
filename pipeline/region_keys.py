"""
Normalize region/county labels so demand and supply tables merge reliably.

Matching is **case-insensitive** (labels are uppercased for comparison).

Excel often reads FIPS as float (e.g. 36001.0) which becomes the string ``"36001.0"`` and fails
to match ``"36001"`` on the supply side unless normalized.
"""

from __future__ import annotations

import math
import re


def normalize_merge_key(value: object) -> str:
    """
    Single key for merging demand ``Region`` with supply ``region_id`` / ``_key``.

    Case-insensitive: ``albany`` and ``ALBANY`` become the same key.

    - Strips whitespace, uppercases (ASCII)
    - Converts numeric cells to integers without a trailing ``.0`` (e.g. 36001.0 → ``"36001"``)
    - Collapses internal whitespace
    """
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    if isinstance(value, bool):
        return str(value).strip().upper()
    if isinstance(value, (int,)):
        return str(int(value)).strip().upper()
    if isinstance(value, float):
        if value.is_integer():
            return str(int(round(value))).strip().upper()
        return str(value).strip().upper()
    s = str(value).strip()
    if not s or s.lower() == "nan":
        return ""
    # "36001.0" from mixed csv/excel
    if re.fullmatch(r"\d+\.0", s):
        s = s[:-2]
    s = " ".join(s.split())
    return s.upper()
