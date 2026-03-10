#!/usr/bin/env python
"""Import Tokyo Metro GTFS data.

Usage:
    python scripts/import_metro.py
    python scripts/import_metro.py --config configs/default.yaml
    python scripts/import_metro.py --gtfs-dir /path/to/TokyoMetro-Train-GTFS
"""

import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.settings import load_config, get_config
from src.import_data.metro import import_metro_gtfs


def main():
    """Parses CLI args and imports Tokyo Metro GTFS data."""
    p = argparse.ArgumentParser(description="Import Tokyo Metro GTFS data")
    p.add_argument("--config", default=None, help="YAML config file path")
    p.add_argument("--gtfs-dir", default=None, help="Path to GTFS directory")
    args = p.parse_args()

    cfg = load_config(args.config)
    gtfs_dir = args.gtfs_dir or cfg.get("import", {}).get("metro_gtfs_dir")
    if gtfs_dir and not os.path.isabs(gtfs_dir):
        gtfs_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), gtfs_dir)

    import_metro_gtfs(gtfs_dir)


if __name__ == "__main__":
    main()
