"""
Streamlit entry point for the park planning website.

Usage (from project root):
    streamlit run website/app.py
    or double-click: run_website.bat

Reads county GeoJSON from data/outputs for basemap geometry. **ActivityNeedScore** for the map and
sidebar is computed live from D8 aggregates and the discovered supply file (same logic everywhere).
"""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure project root is on sys.path (works when cwd is not the project folder)
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import streamlit as st

from website.layout import render_app


def main() -> None:
    st.set_page_config(page_title="Activity Need Score", layout="wide")
    render_app()


if __name__ == "__main__":
    main()

