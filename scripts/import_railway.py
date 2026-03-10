#!/usr/bin/env python
"""Import railway data from ODPT JSON files.

Usage:
    python scripts/import_railway.py
    python scripts/import_railway.py --config configs/default.yaml
    python scripts/import_railway.py --data-dir /path/to/data
"""

import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.settings import load_config, get_config
from src.import_data.railway import import_all_railway


def main():
    """Parses CLI args and imports railway data from ODPT JSON."""
    p = argparse.ArgumentParser(description="Import railway data from ODPT JSON")
    p.add_argument("--config", default=None, help="YAML config file path")
    p.add_argument("--data-dir", default=None, help="Root data directory")
    args = p.parse_args()

    cfg = load_config(args.config)
    data_dir = args.data_dir or cfg.get("import", {}).get("data_dir", "data")
    if not os.path.isabs(data_dir):
        data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), data_dir)

    import_all_railway(data_dir)


if __name__ == "__main__":
    main()
