"""Output path generation and management."""

import os
from datetime import datetime

from src.settings import get_config

_PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))


def get_output_dir():
    """Returns the absolute output directory path, creating it if needed.

    Returns:
        Absolute path to the output directory.
    """
    cfg = get_config().get("output", {})
    out_dir = cfg.get("dir", "outputs")
    if not os.path.isabs(out_dir):
        out_dir = os.path.join(_PROJECT_ROOT, out_dir)
    os.makedirs(out_dir, exist_ok=True)
    return out_dir


def should_save_geojson():
    """Returns True if GeoJSON export is enabled in config.

    Returns:
        Boolean indicating whether GeoJSON should be saved.
    """
    return get_config().get("output", {}).get("save_geojson", True)


def build_output_stem(lat, lon, departure_time, duration, contour=False, calendar=None):
    """Builds a descriptive filename stem from run parameters.

    Args:
        lat: Origin latitude.
        lon: Origin longitude.
        departure_time: datetime object.
        duration: Duration in minutes.
        contour: Whether this is a contour (multi-band) run.
        calendar: "Weekday" or "SaturdayHoliday" (None = auto).

    Returns:
        A string like "contour_35.7115_139.7857_0830_60min_Weekday".
    """
    mode = "contour" if contour else "single"
    time_str = departure_time.strftime("%H%M")

    if calendar is None:
        calendar = "Weekday" if departure_time.weekday() < 5 else "SaturdayHoliday"

    return f"{mode}_{lat:.4f}_{lon:.4f}_{time_str}_{duration}min_{calendar}"


def resolve_output_path(lat, lon, departure_time, duration, contour=False,
                        calendar=None, ext=".html", cli_output=None):
    """Resolves the full output file path.

    Priority:
        1. CLI --output (used as-is if absolute, otherwise relative to output dir)
        2. config output.filename (same logic)
        3. Auto-generated from parameters

    Args:
        lat: Origin latitude.
        lon: Origin longitude.
        departure_time: datetime object.
        duration: Duration in minutes.
        contour: Whether this is a contour run.
        calendar: Calendar type string.
        ext: File extension (e.g. ".html", ".geojson").
        cli_output: Value from --output CLI arg (or None).

    Returns:
        Absolute path to the output file.
    """
    out_dir = get_output_dir()
    cfg_filename = get_config().get("output", {}).get("filename")

    explicit = cli_output or cfg_filename
    if explicit:
        stem, _ = os.path.splitext(explicit)
        path = stem + ext
        if not os.path.isabs(path):
            path = os.path.join(out_dir, path)
        return path

    stem = build_output_stem(lat, lon, departure_time, duration, contour, calendar)
    return os.path.join(out_dir, stem + ext)
