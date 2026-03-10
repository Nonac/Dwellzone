"""Constants and utility functions, loaded from YAML config."""

import math
import os

from src.settings import get_config

_cfg = get_config()
_walk_cfg = _cfg.get("walking", {})
_parallel_cfg = _cfg.get("parallel", {})

DEFAULT_WALK_SPEED_KMH = _walk_cfg.get("speed_kmh", 5.0)
DEFAULT_MAX_WALK_M = _walk_cfg.get("max_distance_m", 800)

# Greater Tokyo walking network bbox: (south, north, west, east)
_bbox = _walk_cfg.get("bbox", [35.50, 35.85, 139.40, 139.95])
TOKYO_BBOX = tuple(_bbox)

# Parallel processing
NUM_WORKERS = _parallel_cfg.get("num_workers", 30)
CHUNK_SIZE = _parallel_cfg.get("chunk_size", 5000)

# Walking network cache directory
_cache_dir = _walk_cfg.get("cache_dir", "data/walk_cache")
if os.path.isabs(_cache_dir):
    WALK_CACHE_DIR = _cache_dir
else:
    WALK_CACHE_DIR = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), _cache_dir
    )

# Meters per degree at Tokyo latitude (~35.68 N)
_TOKYO_LAT_RAD = math.radians(35.68)
_DEG_LAT_M = 111_320.0  # 1 degree latitude ~ 111.32 km
_DEG_LON_M = 111_320.0 * math.cos(_TOKYO_LAT_RAD)  # 1 degree longitude ~ 90.4 km


def haversine_m(lat1, lon1, lat2, lon2):
    """Equirectangular approximation of distance in meters.

    Accurate to <0.1% at Tokyo latitude.

    Args:
        lat1: Latitude of point 1 in decimal degrees.
        lon1: Longitude of point 1 in decimal degrees.
        lat2: Latitude of point 2 in decimal degrees.
        lon2: Longitude of point 2 in decimal degrees.

    Returns:
        Distance in meters.
    """
    dlat = (lat2 - lat1) * _DEG_LAT_M
    dlon = (lon2 - lon1) * _DEG_LON_M
    return math.sqrt(dlat * dlat + dlon * dlon)


def walk_seconds(distance_m, speed_kmh=DEFAULT_WALK_SPEED_KMH):
    """Returns the time in seconds to walk the given distance.

    Args:
        distance_m: Distance in meters.
        speed_kmh: Walking speed in km/h.

    Returns:
        Walking time in seconds.
    """
    return distance_m / (speed_kmh * 1000.0 / 3600.0)


def meters_to_degrees(meters):
    """Converts meters to degrees (for Shapely buffer radius).

    Uses the average of latitude and longitude scale factors at Tokyo.

    Args:
        meters: Distance in meters.

    Returns:
        Approximate equivalent in decimal degrees.
    """
    avg_deg_per_m = 2.0 / (_DEG_LAT_M + _DEG_LON_M)
    return meters * avg_deg_per_m
