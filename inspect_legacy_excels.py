"""Quick script to inspect legacy Excel workbooks (optional).

This is **not required** for running the pipeline or the website. It is only a helper for
understanding the structure of older, reference workbooks.
"""

from __future__ import annotations

import sys

import pandas as pd


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")

    demand_path = "M4_2019_Demand_at_Origin_Simple.xlsx"  # legacy filename (still supported)
    xl4 = pd.ExcelFile(demand_path)
    print(f"=== Demand workbook (legacy): {demand_path} ===")
    print("Sheets:", xl4.sheet_names)
    df4_raw = pd.read_excel(demand_path, sheet_name="Sheet1", header=None)
    print("\n--- Sheet1 (first 5 rows, first 6 cols) ---")
    print("Shape:", df4_raw.shape)
    print(df4_raw.iloc[:5, :6].to_string())
    print("\nRow 0 (activity names):", df4_raw.iloc[0, 2:].tolist())

    m5_path = "M5_2019_ActyDistribution.xls"  # legacy reference workbook (not required)
    try:
        xl5 = pd.ExcelFile(m5_path)
    except Exception:
        xl5 = None

    if xl5 is not None:
        print(f"\n=== Legacy reference workbook: {m5_path} ===")
        print("Sheets:", xl5.sheet_names)
        meta = pd.read_excel(m5_path, sheet_name="Metadata", header=None)
        print("\n--- Metadata (first 5 rows) ---")
        print(meta.iloc[:5].to_string())

        park = pd.read_excel(m5_path, sheet_name="Park", header=None)
        print("\n--- Park sheet ---")
        print("Shape:", park.shape)
        print("First 3 rows, first 8 cols:")
        print(park.iloc[:3, :8].to_string())

        for name in ["RINs", "FINALRIN"]:
            df = pd.read_excel(m5_path, sheet_name=name, header=None)
            print(f"\n--- {name} ---")
            print("Shape:", df.shape)
            print("First 5 rows, first 6 cols:")
            print(df.iloc[:5, :6].to_string())


if __name__ == "__main__":
    main()

