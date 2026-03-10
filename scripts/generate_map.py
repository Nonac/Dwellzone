#!/usr/bin/env python
"""Computes isochrones and generates map HTML + GeoJSON.

Usage:
    python scripts/generate_map.py
    python scripts/generate_map.py --config configs/default.yaml
    python scripts/generate_map.py --lat 35.66 --lon 139.70 --duration 45
    python scripts/generate_map.py --output my_run

Output goes to outputs/ by default (configurable in YAML).
Auto-generates filenames from parameters if --output is not given:
    outputs/contour_35.7115_139.7857_0830_60min_Weekday.html
    outputs/contour_35.7115_139.7857_0830_60min_Weekday.geojson
"""

import argparse
import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.settings import load_config
from src.isochrone import compute_isochrone, compute_isochrone_bands
from src.rendering.folium_map import render_single_band, render_contour, save_geojson
from src.output import resolve_output_path, should_save_geojson


def main():
    """Parses CLI args, computes isochrones, and generates map output."""
    p = argparse.ArgumentParser(description="Isochrone map generator")
    p.add_argument("--config", default=None, help="YAML config file path")
    p.add_argument("--lat", type=float, default=None)
    p.add_argument("--lon", type=float, default=None)
    p.add_argument("--time", default=None, help="Departure time HH:MM")
    p.add_argument("--date", default=None, help="Date YYYY-MM-DD (default: today)")
    p.add_argument("--duration", type=int, default=None, help="Time range in minutes")
    p.add_argument("--calendar", default=None, choices=["Weekday", "SaturdayHoliday"])
    p.add_argument("--output", default=None, help="Output filename stem (without ext)")
    p.add_argument("--contour", action="store_true", default=None, help="Contour mode")
    p.add_argument("--interval", type=int, default=None, help="Contour interval (min)")
    p.add_argument("--no-geojson", action="store_true", help="Skip GeoJSON export")
    args = p.parse_args()

    cfg = load_config(args.config)
    iso = cfg.get("isochrone", {})

    lat = args.lat or iso.get("lat", 35.7114817)
    lon = args.lon or iso.get("lon", 139.7856803)
    time_str = args.time or iso.get("time", "08:30")
    duration = args.duration or iso.get("duration", 60)
    interval = args.interval or iso.get("interval", 10)
    calendar = args.calendar or iso.get("calendar")
    contour = args.contour if args.contour is not None else iso.get("contour", False)

    h, m = map(int, time_str.split(":"))
    if args.date:
        y, mo, d = map(int, args.date.split("-"))
    else:
        today = datetime.now()
        y, mo, d = today.year, today.month, today.day

    dep_time = datetime(y, mo, d, h, m, 0)

    # Resolve output paths
    html_path = resolve_output_path(
        lat, lon, dep_time, duration, contour=contour,
        calendar=calendar, ext=".html", cli_output=args.output,
    )
    geojson_path = resolve_output_path(
        lat, lon, dep_time, duration, contour=contour,
        calendar=calendar, ext=".geojson", cli_output=args.output,
    )
    do_geojson = should_save_geojson() and not args.no_geojson

    if contour:
        features = compute_isochrone_bands(
            lat=lat, lon=lon,
            departure_time=dep_time,
            duration_minutes=duration,
            band_interval=interval,
            calendar=calendar,
        )
        render_contour(features, lat, lon, dep_time, interval, html_path)
        if do_geojson:
            save_geojson(features, geojson_path)
    else:
        feature = compute_isochrone(
            lat=lat, lon=lon,
            departure_time=dep_time,
            duration_minutes=duration,
            calendar=calendar,
        )
        render_single_band(feature, lat, lon, dep_time, duration, html_path)
        if do_geojson:
            save_geojson(feature, geojson_path)


if __name__ == "__main__":
    main()
