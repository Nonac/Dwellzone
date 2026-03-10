"""Top-level isochrone computation entry points."""

import time
from datetime import datetime

from src.config import DEFAULT_WALK_SPEED_KMH, DEFAULT_MAX_WALK_M
from src.timer import reset_timer, elapsed
from src.transit.loader import load_all
from src.transit.graph import compute_reachable
from src.geometry.geojson import (
    build_isochrone_geojson,
    build_isochrone_bands_geojson,
    save_geojson,
)


def _timer(name):
    """Cumulative timing context manager.

    Args:
        name: Label to print with the elapsed time.

    Returns:
        A context manager that prints timing on exit.
    """
    class Timer:
        def __enter__(self):
            return self
        def __exit__(self, *args):
            print(f"[@{elapsed():.1f}s] {name}")
    return Timer()


# Data cache: (calendar, max_walk_m) -> data dict
_cache = {}


def _infer_calendar(dt):
    """Infers the calendar type from a datetime.

    Args:
        dt: A datetime object.

    Returns:
        "Weekday" for Mon-Fri, "SaturdayHoliday" for Sat-Sun.
    """
    if dt.weekday() < 5:
        return "Weekday"
    return "SaturdayHoliday"


def compute_isochrone(
    lat,
    lon,
    departure_time,
    duration_minutes,
    calendar=None,
    walk_speed_kmh=DEFAULT_WALK_SPEED_KMH,
    max_walk_m=DEFAULT_MAX_WALK_M,
    output_file=None,
):
    """Computes a single-band isochrone.

    Args:
        lat: Origin latitude.
        lon: Origin longitude.
        departure_time: A datetime object for the departure.
        duration_minutes: Time budget in minutes.
        calendar: "Weekday" or "SaturdayHoliday" (None to auto-infer).
        walk_speed_kmh: Walking speed in km/h.
        max_walk_m: Maximum walking distance in meters.
        output_file: Path to write GeoJSON output (None to skip).

    Returns:
        A GeoJSON Feature dict.
    """
    if calendar is None:
        calendar = _infer_calendar(departure_time)

    dep_seconds = (
        departure_time.hour * 3600
        + departure_time.minute * 60
        + departure_time.second
    )
    dur_seconds = duration_minutes * 60

    print(
        f"[isochrone] ({lat:.4f}, {lon:.4f}) "
        f"departure {departure_time.strftime('%H:%M')} "
        f"range {duration_minutes} min "
        f"calendar {calendar}"
    )

    cache_key = (calendar, max_walk_m)
    if cache_key not in _cache:
        _cache[cache_key] = load_all(calendar=calendar, max_walk_m=max_walk_m)
    data = _cache[cache_key]

    reachable, origin_node = compute_reachable(
        lat, lon, dep_seconds, dur_seconds, data,
        walk_speed_kmh=walk_speed_kmh,
        max_walk_m=max_walk_m,
    )
    print(f"[isochrone] Reachable stops: {len(reachable)}")

    feature = build_isochrone_geojson(
        reachable, data["stops"], dep_seconds, dur_seconds,
        origin_lat=lat, origin_lon=lon,
        walk_speed_kmh=walk_speed_kmh,
        walk_graph=data.get("walk_graph"),
        snapped=data.get("snapped"),
        origin_node=origin_node,
    )

    if output_file:
        save_geojson(feature, output_file)

    return feature


def compute_isochrone_bands(
    lat,
    lon,
    departure_time,
    duration_minutes,
    band_interval=10,
    calendar=None,
    walk_speed_kmh=DEFAULT_WALK_SPEED_KMH,
    max_walk_m=DEFAULT_MAX_WALK_M,
):
    """Computes multi-band isochrones (contour style).

    Runs Dijkstra once with the maximum duration, then generates a GeoJSON
    Feature for each time band.

    Args:
        lat: Origin latitude.
        lon: Origin longitude.
        departure_time: A datetime object for the departure.
        duration_minutes: Maximum time budget in minutes.
        band_interval: Interval between bands in minutes (default 10).
        calendar: "Weekday" or "SaturdayHoliday" (None to auto-infer).
        walk_speed_kmh: Walking speed in km/h.
        max_walk_m: Maximum walking distance in meters.

    Returns:
        A list of GeoJSON Feature dicts, each with a duration_minutes property.
    """
    reset_timer()

    if calendar is None:
        calendar = _infer_calendar(departure_time)

    dep_seconds = (
        departure_time.hour * 3600
        + departure_time.minute * 60
        + departure_time.second
    )
    max_dur_seconds = duration_minutes * 60

    print(
        f"[isochrone] ({lat:.4f}, {lon:.4f}) "
        f"departure {departure_time.strftime('%H:%M')} "
        f"contour {band_interval}min intervals max {duration_minutes}min "
        f"calendar {calendar}"
    )

    cache_key = (calendar, max_walk_m)
    if cache_key not in _cache:
        with _timer("Data loaded"):
            _cache[cache_key] = load_all(calendar=calendar, max_walk_m=max_walk_m)
    data = _cache[cache_key]

    with _timer("Transit reachability computed"):
        reachable, origin_node = compute_reachable(
            lat, lon, dep_seconds, max_dur_seconds, data,
            walk_speed_kmh=walk_speed_kmh,
            max_walk_m=max_walk_m,
        )
    print(f"[isochrone] Reachable stops: {len(reachable)}")

    bands_minutes = list(range(band_interval, duration_minutes + 1, band_interval))
    bands_seconds = [b * 60 for b in bands_minutes]

    with _timer("Polygon generation"):
        features = build_isochrone_bands_geojson(
            reachable, data["stops"], dep_seconds, bands_seconds,
            origin_lat=lat, origin_lon=lon,
            walk_speed_kmh=walk_speed_kmh,
            walk_graph=data.get("walk_graph"),
            snapped=data.get("snapped"),
            origin_node=origin_node,
        )

    for feature, band_min in zip(features, bands_minutes):
        feature["properties"]["duration_minutes"] = band_min
        stops_in_band = feature["properties"]["reachable_stops"]
        print(f"[isochrone]   {band_min}min: {stops_in_band} stops")

    print(f"[@{elapsed():.1f}s] Isochrone computation complete")
    return features
