"""
Global configuration and versioning settings for the RIN pipeline.

This module centralizes:
- Data source configuration
- Geographic unit configuration
- Version tags for reproducible outputs
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]


@dataclass
class PathConfig:
    base_dir: Path = BASE_DIR
    data_raw: Path = BASE_DIR / "data" / "raw"
    data_processed: Path = BASE_DIR / "data" / "processed"
    data_outputs: Path = BASE_DIR / "data" / "outputs"


@dataclass
class GeoConfig:
    """
    Geographic configuration for the pipeline.

    Attributes
    ----------
    unit : str
        Geographic unit to use, e.g. "county" or "tract".
    crs : str
        Coordinate reference system for spatial processing.
    """

    unit: str = "county"  # or "tract"
    crs: str = "EPSG:3857"


@dataclass
class VersionConfig:
    """
    Version and run metadata configuration.

    Attributes
    ----------
    vintage : str
        Data vintage / reference year (e.g. "2025").
    run_timestamp : str
        ISO-like timestamp used in filenames to distinguish runs.
    """

    vintage: str = "2025"
    run_timestamp: str = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")


paths = PathConfig()
geo = GeoConfig()
version = VersionConfig()


def ensure_directories() -> None:
    """Ensure that required data directories exist."""
    for d in (paths.data_raw, paths.data_processed, paths.data_outputs):
        d.mkdir(parents=True, exist_ok=True)

