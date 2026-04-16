"""Compatibility shim.

This file used to contain the legacy Excel inspection helper.
It now forwards to `inspect_legacy_excels.py` so existing references keep working.
"""

from __future__ import annotations

from inspect_legacy_excels import main


if __name__ == "__main__":
    main()
