"""Loads all public transit data from PostgreSQL into memory.

Returns a dict:
{
    "stops":          {stop_id: (lat, lon, "rail"|"bus")},
    "departures":     {stop_id: [(dep_seconds, trip_id), ...] sorted},
    "trip_stops":     {trip_id: [(stop_id, arr_s, dep_s), ...] ordered},
    "walk_neighbors": {stop_id: [(neighbor_id, walk_seconds), ...]},
    "walk_graph":     networkx.MultiDiGraph (OSMnx walk network) or None,
    "snapped":        {stop_id: osm_node_id} or None,
}
"""

import time
from collections import defaultdict

from src.db import get_cursor
from src.config import (
    DEFAULT_MAX_WALK_M,
    haversine_m,
    walk_seconds as calc_walk_seconds,
)


def _time_to_seconds(t):
    """Converts datetime.time or timedelta to seconds since midnight.

    Args:
        t: A datetime.time, timedelta, or None.

    Returns:
        Seconds since midnight as int, or None if input is None.
    """
    if t is None:
        return None
    # psycopg2 returns TIME as timedelta
    if hasattr(t, "total_seconds"):
        return int(t.total_seconds())
    # datetime.time
    return t.hour * 3600 + t.minute * 60 + t.second


# -- Railway ---------------------------------------------------------------


def _load_railway_stops(cur):
    """Loads railway stations from the database.

    Args:
        cur: A psycopg2 cursor.

    Returns:
        A dict of {station_id: (lat, lon, 'rail')}.
    """
    cur.execute(
        "SELECT station_id, latitude, longitude FROM railway_stations "
        "WHERE latitude IS NOT NULL AND longitude IS NOT NULL"
    )
    stops = {}
    for sid, lat, lon in cur.fetchall():
        stops[sid] = (float(lat), float(lon), "rail")
    return stops


def _load_railway_trips(cur, calendar):
    """Loads railway trips from the database.

    Two phases:
    1. Lines with train_timetable -> use train_timetable_stops directly
    2. Lines without train_timetable -> synthesize from station_timetable

    Args:
        cur: A psycopg2 cursor.
        calendar: Calendar type string ('Weekday' or 'SaturdayHoliday').

    Returns:
        A tuple of (departures, trip_stops) where departures is
        {stop_id: [(dep_seconds, trip_id)]} and trip_stops is
        {trip_id: [(stop_id, arr_s, dep_s)]}.
    """
    departures = defaultdict(list)  # stop_id -> [(dep_s, trip_id)]
    trip_stops = {}  # trip_id -> [(stop_id, arr_s, dep_s)]

    # Phase 1: lines with train_timetable
    cur.execute(
        "SELECT timetable_id, railway_id FROM railway_train_timetable "
        "WHERE calendar = %s",
        (calendar,),
    )
    tt_rows = cur.fetchall()
    timetable_ids = [r[0] for r in tt_rows]
    railways_with_tt = {r[1] for r in tt_rows}

    if timetable_ids:
        cur.execute(
            "SELECT timetable_id, station_id, arrival_time, departure_time, stop_order "
            "FROM railway_train_timetable_stops "
            "WHERE timetable_id = ANY(%s) "
            "ORDER BY timetable_id, stop_order",
            (timetable_ids,),
        )
        current_tid = None
        current_stops = []
        for tid, sid, arr, dep, _order in cur.fetchall():
            arr_s = _time_to_seconds(arr)
            dep_s = _time_to_seconds(dep)
            if tid != current_tid:
                if current_tid and current_stops:
                    trip_stops[current_tid] = current_stops
                current_tid = tid
                current_stops = []
            current_stops.append((sid, arr_s, dep_s))
        if current_tid and current_stops:
            trip_stops[current_tid] = current_stops

        for tid, stops_list in trip_stops.items():
            for sid, _arr_s, dep_s in stops_list:
                if dep_s is not None:
                    departures[sid].append((dep_s, tid))

    # Phase 2: synthesize missing lines from station_timetable
    cur.execute("SELECT DISTINCT railway_id FROM railway_stations")
    all_railways = {r[0] for r in cur.fetchall()}
    missing_railways = all_railways - railways_with_tt

    if missing_railways:
        _synthesize_trips(cur, calendar, missing_railways, departures, trip_stops)

    return departures, trip_stops


def _synthesize_trips(cur, calendar, railway_ids, departures, trip_stops):
    """Synthesizes train trips from station_timetable records.

    Groups by (railway_id, rail_direction, train_number) and chains
    station departure times into a single trip. Modifies departures
    and trip_stops dicts in place.

    Args:
        cur: A psycopg2 cursor.
        calendar: Calendar type string.
        railway_ids: Set of railway IDs missing train_timetable data.
        departures: Dict to append departure entries to (mutated).
        trip_stops: Dict to append trip stop entries to (mutated).
    """
    # Get station order for each railway
    station_order = {}  # (railway_id, direction) -> {station_id: order}
    for rid in railway_ids:
        cur.execute(
            "SELECT station_id FROM railway_stations WHERE railway_id = %s ORDER BY id",
            (rid,),
        )
        stations = [r[0] for r in cur.fetchall()]
        station_order[(rid, "Outbound")] = {s: i for i, s in enumerate(stations)}
        station_order[(rid, "Inbound")] = {
            s: i for i, s in enumerate(reversed(stations))
        }

    # Load station_timetable records
    placeholders = ",".join(["%s"] * len(railway_ids))
    cur.execute(
        f"SELECT st.station_id, st.rail_direction, st.train_number, "
        f"       st.departure_time, rs.railway_id "
        f"FROM railway_station_timetable st "
        f"JOIN railway_stations rs ON st.station_id = rs.station_id "
        f"WHERE rs.railway_id IN ({placeholders}) AND st.calendar = %s "
        f"ORDER BY rs.railway_id, st.rail_direction, st.train_number, st.departure_time",
        (*railway_ids, calendar),
    )

    # Group by (railway_id, direction, train_number)
    trains = defaultdict(list)  # key -> [(station_id, dep_seconds)]
    for sid, direction, train_num, dep_time, rid in cur.fetchall():
        dep_s = _time_to_seconds(dep_time)
        if dep_s is not None:
            key = (rid, direction, train_num)
            trains[key].append((sid, dep_s))

    # Generate a trip for each group
    for (rid, direction, train_num), stop_list in trains.items():
        if len(stop_list) < 2:
            continue

        # Sort by departure time
        stop_list.sort(key=lambda x: x[1])

        trip_id = f"synth_{rid}_{direction}_{train_num}"
        stops_ordered = [(sid, dep_s, dep_s) for sid, dep_s in stop_list]
        trip_stops[trip_id] = stops_ordered

        for sid, dep_s in stop_list:
            departures[sid].append((dep_s, trip_id))


# -- Bus -------------------------------------------------------------------


def _load_bus_stops(cur):
    """Loads bus stops from the database.

    Args:
        cur: A psycopg2 cursor.

    Returns:
        A dict of {busstop_id: (lat, lon, 'bus')}.
    """
    cur.execute(
        "SELECT busstop_id, latitude, longitude FROM bus_stops "
        "WHERE latitude IS NOT NULL AND longitude IS NOT NULL"
    )
    stops = {}
    for sid, lat, lon in cur.fetchall():
        stops[sid] = (float(lat), float(lon), "bus")
    return stops


def _load_bus_trips(cur):
    """Loads bus trips (ignores calendar, treats all as valid).

    Groups bus_timetable by timetable_id; each group is one trip.

    Args:
        cur: A psycopg2 cursor.

    Returns:
        A tuple of (departures, trip_stops).
    """
    departures = defaultdict(list)
    trip_stops = {}

    cur.execute(
        "SELECT timetable_id, busstop_id, arrival_time, departure_time, stop_order "
        "FROM bus_timetable "
        "ORDER BY timetable_id, stop_order"
    )

    current_tid = None
    current_stops = []
    for tid, sid, arr, dep, _order in cur.fetchall():
        arr_s = _time_to_seconds(arr)
        dep_s = _time_to_seconds(dep)
        if tid != current_tid:
            if current_tid and current_stops:
                trip_stops[current_tid] = current_stops
            current_tid = tid
            current_stops = []
        current_stops.append((sid, arr_s, dep_s))
    if current_tid and current_stops:
        trip_stops[current_tid] = current_stops

    for tid, stops_list in trip_stops.items():
        for sid, _arr_s, dep_s in stops_list:
            if dep_s is not None:
                departures[sid].append((dep_s, tid))

    return departures, trip_stops


# -- Walk index ------------------------------------------------------------


def _build_walk_index_road(stops, max_walk_m=DEFAULT_MAX_WALK_M):
    """Builds walk neighbor index using the OSM road network.

    Args:
        stops: Dict of {stop_id: (lat, lon, type)}.
        max_walk_m: Maximum walking distance in meters.

    Returns:
        A tuple of (walk_neighbors, walk_graph, snapped).
    """
    from src.walking.network import get_walk_graph
    from src.walking.snap import snap_all_stops
    from src.walking.neighbors import build_walk_neighbors

    G = get_walk_graph()
    snapped = snap_all_stops(G, stops)
    walk_neighbors = build_walk_neighbors(G, stops, snapped, max_walk_m)
    return walk_neighbors, G, snapped


def _build_walk_index(stops, max_walk_m=DEFAULT_MAX_WALK_M):
    """Builds walk neighbor index using grid-based haversine (fallback).

    Partitions space into ~500m grid cells and checks same + 8 adjacent cells.

    Args:
        stops: Dict of {stop_id: (lat, lon, type)}.
        max_walk_m: Maximum walking distance in meters.

    Returns:
        A dict of {stop_id: [(neighbor_id, walk_seconds)]}.
    """
    grid_size = 0.005  # ~500m
    grid = defaultdict(list)

    for sid, (lat, lon, _typ) in stops.items():
        gx = int(lon / grid_size)
        gy = int(lat / grid_size)
        grid[(gx, gy)].append(sid)

    walk_neighbors = defaultdict(list)

    for sid, (lat, lon, _typ) in stops.items():
        gx = int(lon / grid_size)
        gy = int(lat / grid_size)

        for dx in (-1, 0, 1):
            for dy in (-1, 0, 1):
                for nid in grid.get((gx + dx, gy + dy), []):
                    if nid == sid:
                        continue
                    nlat, nlon, _ = stops[nid]
                    dist = haversine_m(lat, lon, nlat, nlon)
                    if dist <= max_walk_m:
                        ws = calc_walk_seconds(dist)
                        walk_neighbors[sid].append((nid, ws))

    return dict(walk_neighbors)


# -- Main loader -----------------------------------------------------------


def load_all(calendar="Weekday", max_walk_m=DEFAULT_MAX_WALK_M):
    """Loads all data from the database and builds in-memory indexes.

    Args:
        calendar: "Weekday" or "SaturdayHoliday".
        max_walk_m: Maximum walking distance in meters.

    Returns:
        A dict with keys: stops, departures, trip_stops, walk_neighbors,
        walk_graph, snapped.
    """
    print(f"[loader] Loading data (calendar={calendar}, max_walk={max_walk_m}m)...")

    t0 = time.time()
    with get_cursor(commit=False) as cur:
        rail_stops = _load_railway_stops(cur)
        bus_stops = _load_bus_stops(cur)
        stops = {**rail_stops, **bus_stops}
        print(f"  Stops: rail {len(rail_stops)}, bus {len(bus_stops)}, total {len(stops)}")

        rail_deps, rail_trips = _load_railway_trips(cur, calendar)
        bus_deps, bus_trips = _load_bus_trips(cur)

        departures = defaultdict(list)
        for sid, deps in rail_deps.items():
            departures[sid].extend(deps)
        for sid, deps in bus_deps.items():
            departures[sid].extend(deps)

        trip_stops = {**rail_trips, **bus_trips}
        print(
            f"  Trips: rail {len(rail_trips)}, bus {len(bus_trips)}, total {len(trip_stops)}"
        )
    print(f"  [timer] DB load: {time.time()-t0:.1f}s")

    # Sort departures
    for sid in departures:
        departures[sid].sort()
    departures = dict(departures)
    print(f"  Stops with departures: {len(departures)}")

    # Walk index: prefer road network, fall back to haversine
    walk_graph = None
    snapped = None
    t1 = time.time()
    try:
        walk_neighbors, walk_graph, snapped = _build_walk_index_road(
            stops, max_walk_m
        )
        print(f"  Stops with walk neighbors (road): {len(walk_neighbors)}")
    except Exception as e:
        print(f"  [warn] Road walk index failed ({e}), falling back to haversine")
        walk_neighbors = _build_walk_index(stops, max_walk_m)
        print(f"  Stops with walk neighbors (haversine): {len(walk_neighbors)}")
    print(f"  [timer] Walk index build: {time.time()-t1:.1f}s")

    print("[loader] Loading complete")
    return {
        "stops": stops,
        "departures": departures,
        "trip_stops": trip_stops,
        "walk_neighbors": walk_neighbors,
        "walk_graph": walk_graph,
        "snapped": snapped,
    }
