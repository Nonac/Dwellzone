#!/usr/bin/env python
"""Import mini-tokyo-3d railway data (41 operators).

Usage:
    python scripts/import_minitokyo3d.py
    python scripts/import_minitokyo3d.py --config configs/default.yaml
    python scripts/import_minitokyo3d.py --data-dir /path/to/mini-tokyo-3d/data
"""

import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.settings import load_config, get_config
from src.import_data.minitokyo3d import import_minitokyo3d


def main():
    """Parses CLI args and imports mini-tokyo-3d railway data."""
    p = argparse.ArgumentParser(description="Import mini-tokyo-3d railway data")
    p.add_argument("--config", default=None, help="YAML config file path")
    p.add_argument("--data-dir", default=None, help="Path to mini-tokyo-3d/data/ directory")
    args = p.parse_args()

    cfg = load_config(args.config)
    data_dir = args.data_dir or cfg.get("import", {}).get("minitokyo3d_dir")
    if data_dir and not os.path.isabs(data_dir):
        data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), data_dir)

    import_minitokyo3d(data_dir)


if __name__ == "__main__":
    main()
