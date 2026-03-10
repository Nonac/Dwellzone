#!/usr/bin/env python
"""Full DB bootstrap: create schema + import all data sources.

Usage:
    python scripts/import_all.py
    python scripts/import_all.py --config configs/default.yaml
    python scripts/import_all.py --data-dir /path/to/data

Import order:
    1. Create all tables (bus + railway)
    2. Import bus data (ODPT JSON)
    3. Import railway data (ODPT JSON)
    4. Import Tokyo Metro (GTFS)
    5. Import mini-tokyo-3d (41 other operators)
"""

import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.settings import load_config
from src.import_data.schema import create_all_tables
from src.import_data.bus import import_all_bus
from src.import_data.railway import import_all_railway
from src.import_data.metro import import_metro_gtfs
from src.import_data.minitokyo3d import import_minitokyo3d


def main():
    """Parses CLI args and runs full DB bootstrap with all data imports."""
    p = argparse.ArgumentParser(description="Full DB bootstrap: schema + all imports")
    p.add_argument("--config", default=None, help="YAML config file path")
    p.add_argument("--data-dir", default=None, help="Root data directory")
    args = p.parse_args()

    cfg = load_config(args.config)
    project_root = os.path.dirname(os.path.dirname(__file__))
    import_cfg = cfg.get("import", {})

    data_dir = args.data_dir or import_cfg.get("data_dir", "data")
    if not os.path.isabs(data_dir):
        data_dir = os.path.join(project_root, data_dir)

    metro_dir = import_cfg.get("metro_gtfs_dir")
    if metro_dir and not os.path.isabs(metro_dir):
        metro_dir = os.path.join(project_root, metro_dir)

    minitokyo_dir = import_cfg.get("minitokyo3d_dir")
    if minitokyo_dir and not os.path.isabs(minitokyo_dir):
        minitokyo_dir = os.path.join(project_root, minitokyo_dir)

    print("=" * 60)
    print("  Full database bootstrap")
    print("=" * 60)

    # 1. Schema
    print("\n[1/5] Creating tables...")
    create_all_tables()

    # 2. Bus
    print("\n[2/5] Importing bus data...")
    import_all_bus(data_dir)

    # 3. Railway (ODPT)
    print("\n[3/5] Importing railway data (ODPT)...")
    import_all_railway(data_dir)

    # 4. Tokyo Metro (GTFS)
    print("\n[4/5] Importing Tokyo Metro (GTFS)...")
    import_metro_gtfs(metro_dir)

    # 5. mini-tokyo-3d
    print("\n[5/5] Importing mini-tokyo-3d...")
    import_minitokyo3d(minitokyo_dir)

    print("=" * 60)
    print("  All imports complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
