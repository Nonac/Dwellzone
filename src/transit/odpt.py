"""ODPT (Open Data Platform for Public Transportation) ID cleaning and API calls."""

import json
import os
import requests


# -- Basic cleaning --------------------------------------------------------


def strip_odpt_prefix(raw_id):
    """Strips the 'odpt.Type:' prefix, returning the part after the colon.

    Args:
        raw_id: Raw ODPT identifier string.

    Returns:
        The ID portion after the colon, or the original string if no colon.

    Examples:
        'odpt.Station:Toei.Mita.Nishitakashimadaira' -> 'Toei.Mita.Nishitakashimadaira'
        'odpt.Operator:Toei'                          -> 'Toei'
        'Toei.Mita'                                   -> 'Toei.Mita'  (no prefix, returned as-is)
    """
    if raw_id and ":" in raw_id:
        return raw_id.split(":")[-1]
    return raw_id


# -- Operator --------------------------------------------------------------


def clean_operator(operator):
    """Cleans the operator field (handles both string and list inputs).

    Args:
        operator: Raw operator value (string or list).

    Returns:
        Cleaned operator string with ODPT prefix stripped.
    """
    if isinstance(operator, list) and operator:
        operator = operator[0]
    if isinstance(operator, str):
        return strip_odpt_prefix(operator)
    return operator


# -- Bus -------------------------------------------------------------------


def clean_bus_stop_id(raw_id):
    """Keeps the last two segments of a busstop_id (stop_id.direction).

    Args:
        raw_id: Raw ODPT busstop pole identifier.

    Returns:
        Cleaned bus stop ID with last two dot-separated segments.

    Example:
        'odpt.BusstopPole:Toei.Zoshiki.848.2' -> '848.2'
    """
    s = strip_odpt_prefix(raw_id) if raw_id else raw_id
    if s:
        parts = s.split(".")
        if len(parts) >= 2:
            return ".".join(parts[-2:])
    return raw_id


def clean_bus_route_id(raw_id):
    """Keeps the last two segments of a bus route ID.

    Args:
        raw_id: Raw ODPT bus route pattern identifier.

    Returns:
        Cleaned bus route ID with last two dot-separated segments.

    Example:
        'odpt.BusroutePattern:Toei.Ou57.30902.2' -> '30902.2'
    """
    s = strip_odpt_prefix(raw_id) if raw_id else raw_id
    if s:
        parts = s.split(".")
        if len(parts) >= 2:
            return ".".join(parts[-2:])
    return raw_id


def clean_busroute_pattern(busroute_pattern):
    """Cleans each element in a busroute_pattern list (keeps last two segments).

    Args:
        busroute_pattern: List of raw ODPT bus route pattern identifiers.

    Returns:
        List of cleaned bus route IDs.
    """
    return [clean_bus_route_id(each) for each in busroute_pattern if each]


# -- Railway ---------------------------------------------------------------


def clean_railway_station_id(raw_id):
    """Keeps the full station ID (Operator.Line.Station) after stripping prefix.

    Args:
        raw_id: Raw ODPT station identifier.

    Returns:
        Cleaned station ID string.

    Example:
        'odpt.Station:Toei.Mita.Nishitakashimadaira' -> 'Toei.Mita.Nishitakashimadaira'
    """
    return strip_odpt_prefix(raw_id) if raw_id else raw_id


def clean_railway_id(raw_id):
    """Strips the prefix from a railway line ID.

    Args:
        raw_id: Raw ODPT railway identifier.

    Returns:
        Cleaned railway ID string.

    Example:
        'odpt.Railway:Toei.Mita' -> 'Toei.Mita'
    """
    return strip_odpt_prefix(raw_id) if raw_id else raw_id


def clean_calendar(raw_id):
    """Strips the prefix from a calendar ID.

    Args:
        raw_id: Raw ODPT calendar identifier.

    Returns:
        Cleaned calendar string (e.g. 'Weekday').

    Example:
        'odpt.Calendar:Weekday' -> 'Weekday'
    """
    return strip_odpt_prefix(raw_id) if raw_id else raw_id


def clean_rail_direction(raw_id):
    """Strips the prefix from a rail direction ID.

    Args:
        raw_id: Raw ODPT rail direction identifier.

    Returns:
        Cleaned direction string (e.g. 'Inbound').

    Example:
        'odpt.RailDirection:Inbound' -> 'Inbound'
    """
    return strip_odpt_prefix(raw_id) if raw_id else raw_id


def clean_train_type(raw_id):
    """Strips the prefix from a train type ID.

    Args:
        raw_id: Raw ODPT train type identifier.

    Returns:
        Cleaned train type string (e.g. 'Toei.Local').

    Example:
        'odpt.TrainType:Toei.Local' -> 'Toei.Local'
    """
    return strip_odpt_prefix(raw_id) if raw_id else raw_id


# -- API calls -------------------------------------------------------------


def fetch_odpt(url, api_key):
    """Calls an ODPT API endpoint and returns the JSON response list.

    Args:
        url: ODPT API endpoint URL.
        api_key: ODPT API consumer key.

    Returns:
        Parsed JSON response, or None on request error.
    """
    params = {"acl:consumerKey": api_key}
    try:
        resp = requests.get(url, params=params)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.RequestException as e:
        print(f"ODPT API request error: {e}")
        return None


def load_or_fetch(json_path, url=None, api_key=None):
    """Loads data from a local JSON cache, or fetches from the API and saves.

    Args:
        json_path: Path to the local JSON file.
        url: ODPT API endpoint URL (optional).
        api_key: ODPT API consumer key (optional).

    Returns:
        The parsed JSON data.

    Raises:
        FileNotFoundError: If the local file does not exist and no API
            parameters are provided.
    """
    if os.path.isfile(json_path):
        with open(json_path, "r", encoding="utf-8") as f:
            return json.load(f)

    if url and api_key:
        data = fetch_odpt(url, api_key)
        if data is not None:
            os.makedirs(os.path.dirname(json_path), exist_ok=True)
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print(f"Saved to {json_path}")
        return data

    raise FileNotFoundError(f"Local file not found and no API parameters provided: {json_path}")
