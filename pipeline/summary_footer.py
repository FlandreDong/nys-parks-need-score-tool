"""
Workbook summary/footer rows (e.g. GRAND TOTAL, SUBTOTAL) — not county-level data.

Kept in a tiny standalone module so imports never depend on demand_aggregate or region_keys order.
"""

from __future__ import annotations


def is_d8_summary_footer_row(region_label: object) -> bool:
    """
    True for footer rows like ``GRAND TOTAL`` / ``SUBTOTAL`` — exclude from merges and rankings.
    """
    t = str(region_label).strip().lower()
    if not t or t == "nan":
        return False
    if "grand total" in t:
        return True
    if "subtotal" in t:
        return True
    return False
