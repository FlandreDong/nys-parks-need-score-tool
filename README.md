# nys-parks-need-score-tool
A local data pipeline and visualization tool for analyzing activity supply-demand balance and Need Score for New York State Parks.
## Need Score / RIN Data Pipeline (New York State)

This repository contains a reproducible Python data pipeline for updating New York State parks planning outputs related to **Need Score / RIN (Recreation Indicator of Need)**. The intent is that you can re-run the pipeline in future cycles by swapping input files while keeping the code unchanged.

### Project layout

High-level flow:

- **Data Sources → Ingest → Clean → Compute → Export**

Directory overview:

- `data/raw/`: archived, immutable copies of raw inputs
- `data/processed/`: cleaned, aligned intermediate datasets
- `data/outputs/`: final CSV/GeoJSON outputs with timestamps
- `pipeline/ingest_demand_supply.py`: read demand/supply inputs (e.g., D8-style wide tables)
- `pipeline/ingest_ny_boundaries.py`: NY county boundaries aligned to `region_id`
- `pipeline/clean.py`: standardize, validate, align to county/tract
- `pipeline/compute.py`: calculate demand, supply, and RIN
- `pipeline/export.py`: write CSV/GeoJSON and ranked tables
- `config/settings.py`: directories, geographic unit, versioning
- `utils/spatial_utils.py`: CRS and geometry helpers
- `utils/validation_utils.py`: pydantic schemas and dataframe validation
- `run_pipeline.py`: **pipeline entrypoint** (recommended)
- `main.py`: legacy entrypoint (delegates to `run_pipeline.py`)
- `website/app.py`: **web app entrypoint** (Streamlit)
- `inspect_legacy_excels.py`: optional helper to inspect legacy Excel formats (not required to run)
- `inspect_excel.py`: legacy shim that delegates to `inspect_legacy_excels.py`

### Input conventions (auto-discovery)

Place input files in `data/raw/` (recommended) or the project root. The pipeline identifies file roles by **filename only** (it does not parse file content for discovery):

- **demand**: filename contains `D8` or `demand` (legacy compatibility: files starting with `M4` are also treated as demand)
- **supply**: filename contains `S12`, `supply`, or `facilit` (facilities)

If you do not want to rely on auto-discovery, you can pass explicit paths via `--demand` / `--supply`.

### Install

```bash
python -m venv .venv
.venv\Scripts\activate  # on Windows
pip install -r requirements.txt
```

#### Run the pipeline (generate `data/outputs`)

Then run:

```bash
py run_pipeline.py
```

To export county GeoJSON for the web map (requires NY boundaries):

```bash
py run_pipeline.py --boundaries
```

Outputs are written to `data/outputs/` (timestamped), and the pipeline also attempts to write stable filenames when applicable (e.g., `need_score_map.geojson`).

#### Run the web app (Streamlit, port 8051)

Option A: double-click `run_website.bat`

Option B:

```bash
py -m streamlit run website/app.py --server.port 8051
```

Open in a browser: `http://localhost:8051`

### Compatibility notes (legacy names)

- **Legacy entrypoint**: `py main.py` is still supported (delegates to `run_pipeline.py`)
- **Legacy flag**: `run_pipeline.py` accepts `--m4` (equivalent to `--demand`)
- **Legacy module naming**: some `M4_*` aliases remain (e.g., `M4_ACTIVITIES`), but new code should prefer `ACTIVITY_COLUMNS` and `--demand`

### FAQ

- **Port already in use**: change `--server.port 8051` in `run_website.bat` to another port
- **Auto-discovery did not find files**: verify filenames include `D8/demand` or `S12/supply/facilit`, or pass `--demand` / `--supply` explicitly

