"""
Project entry point.

Use the Need Score pipeline entry point instead:

    py run_pipeline.py
    py run_pipeline.py --boundaries

Website (Streamlit):

    py -m streamlit run website/app.py --server.port 8051
"""

from __future__ import annotations


def run_pipeline() -> None:
    from run_pipeline import main

    main()


if __name__ == "__main__":
    run_pipeline()
