"""
Run the Need Score pipeline (NeedScore = Demand / Supply × 100).

This script is the canonical pipeline entry point.

Typical usage (from project root):
    py run_pipeline.py
    py run_pipeline.py --boundaries                 # export county GeoJSON for the website map
    py run_pipeline.py --demand path/to/demand.xlsx # Excel wide (D8-style) or CSV demand table
    py run_pipeline.py --supply path/to/supply.csv  # optional; defaults to constant supply if missing
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from config.settings import ensure_directories, paths
from pipeline import compute, export
from pipeline.activity_metrics import build_county_activity_metrics_long
from pipeline.data_discovery import discover_latest_input_paths
from pipeline.demand_aggregate import drop_d8_summary_footer_rows, scalar_demand_for_pipeline
from pipeline.input_loaders import load_demand_from_path, load_supply_from_path
from pipeline.region_keys import normalize_merge_key


def _demand_df_from_d8_aggregate(
    demand_wide: pd.DataFrame,
    supply_df: pd.DataFrame,
    activity: str | None,
    normalize_demand: bool,
) -> pd.DataFrame:
    """
    One Excel-derived demand scalar (aggregated across rows), repeated for each region in **supply**.

    The Excel rows are not matched to supply by region; the scalar is a model input and the
    supply table defines which regions are included.
    """
    scalar = scalar_demand_for_pipeline(demand_wide, activity=activity, how="sum")
    if normalize_demand:
        total = scalar_demand_for_pipeline(demand_wide, activity=None, how="sum")
        if total > 0:
            scalar = scalar / total
    out = pd.DataFrame(
        {
            "region_id": supply_df["region_id"].astype(str),
            "demand_value": float(scalar),
        }
    )
    return compute.compute_demand_from_table(out, value_column="demand_value")


def _resolve_demand_path(
    demand_path: Path | str | None,
    *,
    auto_discover: bool,
    cached_latest: Path | None = None,
) -> Path:
    if demand_path is not None:
        p = Path(demand_path)
        if not p.exists():
            raise FileNotFoundError(f"Demand file not found: {p}")
        return p
    if auto_discover and cached_latest is not None:
        print(f"Using latest demand file (by mtime, name-classified): {cached_latest}", flush=True)
        return cached_latest

    # Back-compat: allow the legacy workbook name if it exists.
    legacy = paths.base_dir / "M4_2019_Demand_at_Origin_Simple.xlsx"
    if legacy.exists():
        print(f"Using legacy default demand file: {legacy}", flush=True)
        return legacy

    raise FileNotFoundError(
        "No demand file found. Put a demand Excel/CSV under data/raw (or project root) with a "
        "name containing D8 or demand, or pass --demand explicitly."
    )


def _resolve_supply_path(
    supply_path: Path | str | None,
    *,
    auto_discover: bool,
    cached_latest: Path | None = None,
) -> Path | None:
    if supply_path is not None:
        p = Path(supply_path)
        if not p.exists():
            raise FileNotFoundError(f"Supply file not found: {p}")
        return p
    if auto_discover and cached_latest is not None:
        print(f"Using latest supply file (by mtime, name-classified): {cached_latest}", flush=True)
        return cached_latest
    return None


def run_pipeline(
    demand_path: Path | str | None = None,
    supply_path: Path | str | None = None,
    boundaries_path: Path | str | None = None,
    activity: str | None = None,
    normalize_demand: bool = True,
    need_threshold: float = 1.0,
    auto_discover: bool = True,
) -> tuple[Path | None, Path, Path, Path | None, Path | None, Path | None]:
    """
    Run Need Score pipeline: demand from latest or explicit path, supply from latest or
    explicit path, else constant supply per region. Exports table + optional map outputs.
    """
    ensure_directories()

    cached_demand: Path | None = None
    cached_supply: Path | None = None
    if auto_discover:
        cached_demand, cached_supply = discover_latest_input_paths()

    demand_path = _resolve_demand_path(
        demand_path, auto_discover=auto_discover, cached_latest=cached_demand
    )
    demand_wide, demand_df_direct = load_demand_from_path(demand_path)

    resolved_supply = _resolve_supply_path(
        supply_path, auto_discover=auto_discover, cached_latest=cached_supply
    )

    if demand_df_direct is not None:
        demand_df = demand_df_direct
        if resolved_supply is None:
            print(
                "No supply file found; using constant supply_value=1.0 for all regions.",
                flush=True,
            )
            supply_df = compute.compute_supply_constant(demand_df["region_id"])
        else:
            supply_df = load_supply_from_path(resolved_supply)
        activity_metrics_long = None
    else:
        if resolved_supply is None:
            print(
                "No supply file found; using constant supply=1.0 per region from demand region rows.",
                flush=True,
            )
            mw = drop_d8_summary_footer_rows(demand_wide)
            r = mw["region_id"].map(normalize_merge_key)
            r = r[r != ""].drop_duplicates()
            supply_df = compute.compute_supply_constant(r)
        else:
            supply_df = load_supply_from_path(resolved_supply)
        demand_df = _demand_df_from_d8_aggregate(
            demand_wide, supply_df, activity=activity, normalize_demand=normalize_demand
        )
        activity_metrics_long = build_county_activity_metrics_long(
            demand_wide,
            resolved_supply,
            how="sum",
        )

    boundaries_gdf = None
    if boundaries_path:
        from pipeline.ingest_ny_boundaries import load_ny_boundaries_for_m4

        boundaries_gdf = load_ny_boundaries_for_m4(path=Path(boundaries_path))

    need_gdf, need_table = compute.compute_need_score(
        demand_df=demand_df,
        supply_df=supply_df,
        boundaries_gdf=boundaries_gdf,
    )

    spatial_path, table_path, improvement_path, run_folder = export.export_results(
        need_table,
        need_gdf=need_gdf,
        threshold=need_threshold,
        activity_metrics_long=activity_metrics_long,
    )

    map_path = None
    map_html_path = None
    if need_gdf is not None:
        if run_folder is not None:
            map_path = export.export_map(
                need_gdf,
                output_path=run_folder / "need_map",
                column="need_score",
                title="Need Score (Demand/Supply × 100)",
                formats=("png", "pdf"),
            )
            map_html_path = export.export_map_html(
                need_gdf,
                output_path=run_folder / "need_map.html",
                column="need_score",
                title="Need Score (Demand/Supply × 100)",
            )
        else:
            map_path = export.export_map(
                need_gdf,
                column="need_score",
                title="Need Score (Demand/Supply × 100)",
                formats=("png", "pdf"),
            )
            map_html_path = export.export_map_html(
                need_gdf, column="need_score", title="Need Score (Demand/Supply × 100)"
            )

    if activity_metrics_long is not None and not activity_metrics_long.empty:
        print(
            f"  - {paths.data_outputs / 'activity_metrics_by_county.csv'} "
            "(per-county × per-activity: gap, supply/demand ratio, need_score)",
            flush=True,
        )

    return spatial_path, table_path, improvement_path, map_path, map_html_path, run_folder


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run Need Score pipeline (NeedScore = Demand / Supply × 100)"
    )
    parser.add_argument(
        "--demand",
        type=Path,
        default=None,
        help="Demand file (Excel wide demand layout or CSV with region_id,demand_value). "
        "Default: newest matching file under data/raw / project root by mtime.",
    )
    # Back-compat alias: keep old flag name so existing notes still work.
    parser.add_argument(
        "--m4",
        type=Path,
        default=None,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--supply",
        type=Path,
        default=None,
        help="Supply file (CSV/Excel region_id+supply_value, or GeoJSON facilities with region_id). "
        "Default: newest matching file by mtime; if none, constant supply.",
    )
    parser.add_argument(
        "--no-auto-discover",
        action="store_true",
        help="Require explicit --demand (no mtime scan and no legacy fallback).",
    )
    parser.add_argument(
        "--activity",
        type=str,
        default=None,
        help="Use a single activity column (e.g. Park). Default: sum all activities",
    )
    parser.add_argument(
        "--no-normalize",
        action="store_true",
        help="Do not normalize demand to [0,1]; use raw demand",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=1.0,
        help="Need score threshold for 'needs improvement' (default 1.0; scores use ×100)",
    )
    parser.add_argument(
        "--boundaries",
        type=Path,
        nargs="?",
        const=Path("NY/new-york Administrative Areas_USA_2.shp"),
        default=None,
        metavar="PATH",
        help="Use NY boundaries and output GeoJSON for the website map",
    )
    args = parser.parse_args()

    demand = args.demand if args.demand is not None else args.m4
    auto_discover = not args.no_auto_discover
    if args.no_auto_discover and demand is None:
        parser.error("--no-auto-discover requires --demand")

    spatial_path, table_path, improvement_path, map_path, map_html_path, run_folder = run_pipeline(
        demand_path=demand,
        supply_path=args.supply,
        boundaries_path=args.boundaries,
        activity=args.activity,
        normalize_demand=not args.no_normalize,
        need_threshold=args.threshold,
        auto_discover=auto_discover,
    )
    print("Pipeline completed (Need Score = Demand/Supply × 100). Outputs:", flush=True)
    if run_folder:
        print(f"  Run folder (maps + Excel): {run_folder}", flush=True)
    if spatial_path:
        print(f"  - {spatial_path}", flush=True)
    print(f"  - {table_path}", flush=True)
    print(f"  - {improvement_path}", flush=True)
    if map_path:
        print(f"  - {map_path}", flush=True)
    if map_html_path:
        print(f"  - {map_html_path} (open in browser)", flush=True)


if __name__ == "__main__":
    main()

