"""Address geocoding via GSI (国土地理院) API."""

import time

import requests


_GSI_URL = "https://msearch.gsi.go.jp/address-search/AddressSearch"


def geocode_address(address):
    """Geocodes a Japanese address using the GSI API.

    Free, no API key required, no rate limit (but be polite).

    Args:
        address: Japanese address string.

    Returns:
        (latitude, longitude) tuple, or (None, None) on failure.
    """
    try:
        resp = requests.get(_GSI_URL, params={"q": address}, timeout=10)
        resp.raise_for_status()
        results = resp.json()
        if results:
            # GSI returns [lon, lat] in geometry.coordinates
            coords = results[0]["geometry"]["coordinates"]
            return coords[1], coords[0]  # lat, lon
    except Exception as e:
        print(f"[geocoder] Failed for '{address}': {e}")

    return None, None


def geocode_from_station(station_name, walk_graph=None, snapped=None):
    """Estimates coordinates from the nearest station name.

    Falls back to geocoding the station name itself.

    Args:
        station_name: Station name (e.g. '池袋駅').
        walk_graph: OSM walk graph (unused for now).
        snapped: Snapped stops dict (unused for now).

    Returns:
        (latitude, longitude) tuple, or (None, None).
    """
    return geocode_address(station_name)
