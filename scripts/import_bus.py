#!/usr/bin/env python
"""Import bus data from ODPT JSON files.

Usage:
    python scripts/import_bus.py
    python scripts/import_bus.py --config configs/default.yaml
    python scripts/import_bus.py --data-dir /path/to/data
"""

import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.settings import load_config, get_config
from src.import_data.bus import import_all_bus


def main():
    """Parses CLI args and imports bus data from ODPT JSON."""
    p = argparse.ArgumentParser(description="Import bus data from ODPT JSON")
    p.add_argument("--config", default=None, help="YAML config file path")
    p.add_argument("--data-dir", default=None, help="Root data directory")
    args = p.parse_args()

    cfg = load_config(args.config)
    data_dir = args.data_dir or cfg.get("import", {}).get("data_dir", "data")
    if not os.path.isabs(data_dir):
        data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), data_dir)

    import_all_bus(data_dir)


if __name__ == "__main__":
    main()
