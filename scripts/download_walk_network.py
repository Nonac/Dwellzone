#!/usr/bin/env python
"""Downloads Kanto OSM data and builds the Tokyo walking network.

Usage:
    python scripts/download_walk_network.py
    python scripts/download_walk_network.py --config configs/default.yaml
"""

import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.settings import load_config
from src.walking.downloader import download_and_build


def main():
    """Parses CLI args and downloads the Tokyo walking network."""
    p = argparse.ArgumentParser(description="Download walk network")
    p.add_argument("--config", default=None, help="YAML config file path")
    args = p.parse_args()
    load_config(args.config)
    download_and_build()


if __name__ == "__main__":
    main()
