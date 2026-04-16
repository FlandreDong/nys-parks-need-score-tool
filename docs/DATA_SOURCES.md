# Sample data notes: demand (D8/M4), supply (S12), and NY boundaries

## File locations

- **M4_2019_Demand_at_Origin_Simple.xlsx** — demand (Demand at Origin), in the project root
- **NY/** — New York State administrative boundaries (Shapefile), used for spatial outputs and county name alignment

### NY boundaries (New York State)

The `NY/` directory contains the **new-york Administrative Areas_USA_2** Shapefile (county level):

- **new-york Administrative Areas_USA_2.shp** (and .dbf, .shx, .prj, etc.): 63 county polygons
- Attribute **NAME_2** = county name (e.g., Albany, Bronx, Saint Lawrence)
- CRS: WGS84 (EPSG:4326)

The pipeline reads boundaries via `pipeline.ingest_ny_boundaries.load_ny_boundaries_for_m4()` and normalizes NAME_2 into `region_id` (uppercase; Saint Lawrence → ST. LAWRENCE) to match historical workbook county naming. To run with spatial outputs:

```bash
py run_pipeline.py --boundaries
```

This produces `data/outputs/rin_spatial_*.geojson`.

---

## M4_2019_Demand_at_Origin_Simple.xlsx

**Purpose**: county-by-activity aggregated recreation demand (demand at origin).

| Item | Notes |
|------|------|
| Valid worksheets | **Sheet1** only (Sheet2/Sheet3 are empty) |
| Shape | ~67 rows × 15 columns |
| Header | Rows 0–2 are header/blank; data starts at row 3 |

**Column layout** (when using `header=None`):

- **Column 0**: index (1, 2, 3, ...)
- **Column 1**: county name (e.g., ALBANY, ALLEGANY, BRONX, BROOME, ...)
- **Columns 2–14**: 13 activity demand values (floats), mapped as:

| Column index | Activity |
|--------------|----------|
| 2 | Park |
| 3 | Swimming |
| 4 | Biking |
| 5 | Golfing |
| 6 | Court Games |
| 7 | Field Games |
| 8 | Walking/Jogging |
| 9 | Camping |
| 10 | Fishing |
| 11 | Boating |
| 12 | Local Winter |
| 13 | Downhill Skiing |
| 14 | Snowmobiling |

**How it fits the pipeline**: M4 can be used directly as a demand input; after county aggregation it can feed the Demand portion of RIN (or serve as a substitute for survey-style demand).

---

## M5_2019_ActyDistribution.xls

**Status**: legacy reference file (the current pipeline and web app do not depend on it).

| Item | Notes |
|------|------|
| Worksheets | Many; most relevant: **RINs**, **FINALRIN**, **Metadata**, and per-activity sheets (Park, Swim, Bike, ...) |
| Format | legacy Excel `.xls` (requires the `xlrd` engine) |

### Key worksheets

1. **Metadata**  
   - Title, version, publisher (John.Davis@parks.ny.gov), dates, etc.

2. **RINs** (~63×18)  
   - Column 0: `Index of Needs` (county name)  
   - Columns 1–17: per-activity RIN scores (1–10), columns like Park, Swim, Bike, Golf, Court, Field, Walk, Camp, Fish, Boat, LocWint, Ski, SnMb, etc.

3. **FINALRIN** (~63×15)  
   - Column 0: `IndexofNeeds` (county name)  
   - Columns 1+: final RIN scores (1–10), one column per activity  
   - Useful as a reference output to compare against the pipeline-computed RIN for sanity checks

4. **Park / Swim / Bike / ...**  
   - Example (**Park**): 134×75 “destination × origin” matrix (Origins ► × Destinations▼); cell values represent activity distribution  
   - Can be used for supply/allocation-side analysis together with M4 demand-side inputs

---

## How to use with the current RIN pipeline

- **Demand**: use **M4** Sheet1; aggregate by county + activity and map into the pipeline’s survey/demand inputs (map column 1 to `region_id`; aggregate columns 2–14 or select a single activity as the demand indicator).
- **Supply**: prefer S12/supply files (auto-discovered by filename). If none are provided, the pipeline falls back to a constant supply (1.0).
- **Validation**: compare **M5 FINALRIN** against the pipeline `compute_rin` output to check scale and whether high-score counties are broadly consistent.

For the exact read/mapping logic, see `pipeline/ingest_demand_supply.py` (demand tables) and `pipeline/supply_for_activity.py` (web-side supply lookup by activity).
