"""Time-dependent Dijkstra algorithm for public transit reachability.

Given a GPS origin, departure time, and duration, computes all reachable
stops and their earliest arrival times.
"""

import heapq
from bisect import bisect_left

from src.config import (
    DEFAULT_WALK_SPEED_KMH,
    DEFAULT_MAX_WALK_M,
    haversine_m,
    walk_seconds as calc_walk_seconds,
)


def _find_initial_stops(
    lat, lon, stops, max_walk_m, walk_speed_kmh,
    walk_graph=None, snapped=None,
):
    """Finds all transit stops reachable by walking from a GPS origin.

    Uses road network Dijkstra when available, otherwise falls back to haversine.

    Args:
        lat: Origin latitude.
        lon: Origin longitude.
        stops: Dict of {stop_id: (lat, lon, type)}.
        max_walk_m: Maximum walking distance in meters.
        walk_speed_kmh: Walking speed in km/h.
        walk_graph: OSMnx walking network graph (None for fallback).
        snapped: Dict of {stop_id: osm_node_id} (None for fallback).

    Returns:
        A tuple of (initial_list, origin_node):
            initial_list: [(stop_id, walk_seconds), ...]
            origin_node: OSM node ID the origin snapped to, or None.
    """
    if walk_graph is not None and snapped is not None:
        from src.walking.neighbors import find_initial_stops_road
        return find_initial_stops_road(
            walk_graph, lat, lon, snapped, max_walk_m
        )

    # Fallback to haversine
    initial = []
    for sid, (slat, slon, _typ) in stops.items():
        dist = haversine_m(lat, lon, slat, slon)
        if dist <= max_walk_m:
            ws = calc_walk_seconds(dist, walk_speed_kmh)
            initial.append((sid, ws))
    return initial, None


def _ride_trip(trip_stops, board_stop, board_time):
    """Propagates arrival times along a trip from the boarding stop.

    Args:
        trip_stops: [(stop_id, arr_s, dep_s), ...] in order.
        board_stop: The stop_id where the passenger boards.
        board_time: Boarding time in seconds.

    Yields:
        (stop_id, arrival_seconds) for each stop after the boarding stop.
    """
    found = False
    prev_time = board_time
    for sid, arr_s, dep_s in trip_stops:
        if not found:
            if sid == board_stop:
                found = True
            continue
        # Time must be >= boarding time and monotonically increasing
        t = arr_s if arr_s is not None else dep_s
        if t is not None and t >= prev_time:
            prev_time = t
            yield (sid, t)


def compute_reachable(
    lat,
    lon,
    dep_seconds,
    dur_seconds,
    data,
    walk_speed_kmh=DEFAULT_WALK_SPEED_KMH,
    max_walk_m=DEFAULT_MAX_WALK_M,
):
    """Runs time-dependent Dijkstra to find all reachable stops.

    Args:
        lat: Origin latitude.
        lon: Origin longitude.
        dep_seconds: Departure time (seconds since midnight, e.g. 8:30 = 30600).
        dur_seconds: Time budget in seconds (e.g. 30 min = 1800).
        data: Dict returned by loader.load_all().
        walk_speed_kmh: Walking speed.
        max_walk_m: Maximum walking distance.

    Returns:
        A tuple of (best, origin_node):
            best: {stop_id: earliest_arrival_seconds}
            origin_node: OSM node ID the origin snapped to, or None.
    """
    stops = data["stops"]
    departures = data["departures"]
    trip_stops = data["trip_stops"]
    walk_neighbors = data["walk_neighbors"]
    walk_graph = data.get("walk_graph")
    snapped = data.get("snapped")

    deadline = dep_seconds + dur_seconds
    best = {}  # stop_id -> earliest arrival time

    # Priority queue: (arrival_time, stop_id)
    pq = []

    # Initialize: walk from GPS origin to nearby stops
    initial, origin_node = _find_initial_stops(
        lat, lon, stops, max_walk_m, walk_speed_kmh,
        walk_graph=walk_graph, snapped=snapped,
    )
    for sid, ws in initial:
        arr = dep_seconds + ws
        if arr <= deadline:
            heapq.heappush(pq, (arr, sid))

    while pq:
        arr_time, sid = heapq.heappop(pq)

        if sid in best and best[sid] <= arr_time:
            continue

        if arr_time > deadline:
            continue

        best[sid] = arr_time

        # Walking edges
        for nid, ws in walk_neighbors.get(sid, []):
            n_arr = arr_time + ws
            if n_arr <= deadline and (nid not in best or n_arr < best[nid]):
                heapq.heappush(pq, (n_arr, nid))

        # Transit edges
        deps = departures.get(sid, [])
        if not deps:
            continue

        # Binary search for first departure >= arr_time
        idx = bisect_left(deps, (arr_time,))

        seen_trips = set()

        for i in range(idx, len(deps)):
            dep_time, trip_id = deps[i]

            if dep_time > deadline:
                break

            if trip_id in seen_trips:
                continue
            seen_trips.add(trip_id)

            ts = trip_stops.get(trip_id)
            if ts is None:
                continue

            for next_sid, next_arr in _ride_trip(ts, sid, dep_time):
                if next_arr is not None and next_arr <= deadline:
                    if next_sid not in best or next_arr < best[next_sid]:
                        heapq.heappush(pq, (next_arr, next_sid))

    return best, origin_node
