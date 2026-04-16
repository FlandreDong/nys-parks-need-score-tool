"""
Semi-automated discovery of demand and supply inputs.

1. **Role** is inferred from the **file name only** (case-insensitive markers).
2. **Which file** is used is the **newest by modification time** within each role:

       latest = max(candidates, key=lambda p: p.stat().st_mtime)

Scans ``data/raw`` (recursive, depth-limited) and top-level project root.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, Literal, Optional

from config.settings import paths

# Demand inputs: spreadsheets / CSV only
DEMAND_EXTENSIONS = {".xlsx", ".xls", ".csv"}

# Supply: tabular or vector facilities
SUPPLY_EXTENSIONS = {".xlsx", ".xls", ".csv", ".geojson", ".json", ".gpkg", ".shp"}

InputRole = Literal["demand", "supply"]


def _skip_for_discovery(filename: str) -> bool:
    """
    Skip paths that are not real user datasets (Excel lock files, etc.).

    When a workbook is open, Excel creates ``~$BookName.xlsx``; it is not readable and
    must not be chosen as ``latest`` by mtime.
    """
    fn = filename.strip().lower()
    if fn.startswith("~$"):
        return True
    return False


def classify_input_filename(filename: str) -> Optional[InputRole]:
    """
    Decide demand vs supply from the **filename** only.

    Markers (case-insensitive):

    - **demand**: ``D8``, substring ``demand`` (legacy: name starting with ``M4``)
    - **supply**: ``S12``, substring ``supply``, or substring ``facilit`` (facilities)

    Returns ``None`` if neither matches, or if **both** match (ambiguous).

    Lock files such as ``~$Workbook.xlsx`` (Excel) always return ``None``.
    """
    if _skip_for_discovery(filename):
        return None
    n = filename.lower()
    demand_hit = "d8" in n or "demand" in n or n.startswith("m4")
    supply_hit = "s12" in n or "supply" in n or "facilit" in n
    if demand_hit and supply_hit:
        return None
    if demand_hit:
        return "demand"
    if supply_hit:
        return "supply"
    return None


def _iter_files_in_raw(max_depth: int = 4) -> Iterable[Path]:
    root = paths.data_raw
    if not root.is_dir():
        return
    for p in root.rglob("*"):
        if p.is_file() and len(p.relative_to(root).parts) <= max_depth:
            yield p


def _iter_top_level_base() -> Iterable[Path]:
    base = paths.base_dir
    if not base.is_dir():
        return
    for pattern in ("*.xlsx", "*.xls", "*.csv", "*.geojson", "*.gpkg", "*.shp"):
        yield from base.glob(pattern)


def _iter_unique_input_paths() -> Iterable[Path]:
    seen: set[Path] = set()
    for p in _iter_files_in_raw():
        try:
            rp = p.resolve()
        except OSError:
            continue
        if rp not in seen:
            seen.add(rp)
            yield p
    for p in _iter_top_level_base():
        try:
            rp = p.resolve()
        except OSError:
            continue
        if rp not in seen:
            seen.add(rp)
            yield p


def scan_demand_and_supply_paths() -> tuple[list[Path], list[Path]]:
    """
    Walk inputs once; split paths into demand vs supply lists using ``classify_input_filename``
    and allowed extensions per role.
    """
    demand_files: list[Path] = []
    supply_files: list[Path] = []
    for p in _iter_unique_input_paths():
        role = classify_input_filename(p.name)
        if role is None:
            continue
        ext = p.suffix.lower()
        if role == "demand" and ext in DEMAND_EXTENSIONS:
            demand_files.append(p)
        elif role == "supply" and ext in SUPPLY_EXTENSIONS:
            supply_files.append(p)
    return demand_files, supply_files


def latest_by_mtime(files: list[Path]) -> Optional[Path]:
    if not files:
        return None
    return max(files, key=lambda x: x.stat().st_mtime)


def discover_latest_input_paths() -> tuple[Optional[Path], Optional[Path]]:
    """
    Single directory scan: return ``(latest_demand, latest_supply)`` by mtime each.

    Use this when both roles may be auto-selected to avoid scanning twice.
    """
    demand_paths, supply_paths = scan_demand_and_supply_paths()
    return latest_by_mtime(demand_paths), latest_by_mtime(supply_paths)


def discover_latest_demand_path() -> Optional[Path]:
    """Newest **demand** file (by mtime) among name-classified candidates."""
    d, _ = discover_latest_input_paths()
    return d


def discover_latest_supply_path() -> Optional[Path]:
    """Newest **supply** file (by mtime) among name-classified candidates."""
    _, s = discover_latest_input_paths()
    return s
