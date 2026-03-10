#!/usr/bin/env python
"""Initialize transit database schema (all tables + indexes).

Usage:
    python scripts/init_db.py
    python scripts/init_db.py --config configs/default.yaml
"""

import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.settings import load_config
from src.import_data.schema import create_all_tables


def main():
    """Parses CLI args and initializes the transit database schema."""
    p = argparse.ArgumentParser(description="Initialize transit DB schema")
    p.add_argument("--config", default=None, help="YAML config file path")
    args = p.parse_args()

    load_config(args.config)
    print("=== Initializing database schema ===")
    create_all_tables()
    print("=== Schema initialization complete ===")


if __name__ == "__main__":
    main()
