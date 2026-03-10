"""Tokyo Metro GTFS data import (appends to existing transit data)."""

import csv
import os

from src.db import connect_db, table_row_count


# -- Constants -----------------------------------------------------------------

OPERATOR = "TokyoMetro"

# route_id -> English line name
ROUTE_NAME_MAP = {
    "1": "Ginza",
    "2": "Marunouchi",
    "3": "Hibiya",
    "4": "Tozai",
    "5": "Chiyoda",
    "6": "Yurakucho",
    "7": "Hanzomon",
    "8": "Namboku",
    "9": "Fukutoshin",
}

# stop_code first letter -> English line name
STOP_PREFIX_MAP = {
    "G": "Ginza",
    "M": "Marunouchi",
    "H": "Hibiya",
    "T": "Tozai",
    "C": "Chiyoda",
    "Y": "Yurakucho",
    "Z": "Hanzomon",
    "N": "Namboku",
    "F": "Fukutoshin",
}

# service_id -> calendar
CALENDAR_MAP = {"0": "Weekday", "1": "SaturdayHoliday"}

# direction_id -> direction
DIRECTION_MAP = {"0": "Outbound", "1": "Inbound"}


# -- CSV reading ---------------------------------------------------------------

def _read_csv(filepath):
    """Reads a GTFS CSV file and returns a list of dicts.

    Args:
        filepath: Path to the CSV file.

    Returns:
        A list of dicts (one per row).
    """
    with open(filepath, "r", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


# -- Mapping builders ----------------------------------------------------------

def _build_route_map(routes):
    """Builds route_id -> railway_id mapping (e.g. 'TokyoMetro.Ginza').

    Args:
        routes: List of GTFS route dicts.

    Returns:
        A dict of {route_id: railway_id}.
    """
    result = {}
    for r in routes:
        route_id = r["route_id"]
        name = ROUTE_NAME_MAP.get(route_id)
        if name:
            result[route_id] = f"{OPERATOR}.{name}"
    return result


def _build_stop_map(stops):
    """Builds stop_id -> (station_id, railway_id) mapping.

    station_id format: TokyoMetro.{Line}.{stop_code}
    railway_id inferred from stop_code first letter.

    Args:
        stops: List of GTFS stop dicts.

    Returns:
        A dict of {stop_id: (station_id, railway_id)}.
    """
    result = {}
    for s in stops:
        stop_id = s["stop_id"]
        stop_code = s["stop_code"]
        if not stop_code:
            continue
        prefix = stop_code[0].upper()
        line_name = STOP_PREFIX_MAP.get(prefix)
        if not line_name:
            continue
        railway_id = f"{OPERATOR}.{line_name}"
        station_id = f"{OPERATOR}.{line_name}.{stop_code}"
        result[stop_id] = (station_id, railway_id)
    return result


def _build_translation_map(gtfs_dir):
    """Builds (stop_name, 'en') -> english_name mapping from translations.txt.

    Args:
        gtfs_dir: Path to the GTFS directory.

    Returns:
        A dict of {japanese_name: english_name}.
    """
    path = os.path.join(gtfs_dir, "translations.txt")
    if not os.path.exists(path):
        return {}
    rows = _read_csv(path)
    result = {}
    for r in rows:
        if r.get("table_name") == "stops" and r.get("language") == "en":
            result[r["field_value"]] = r["translation"]
    return result


# -- Time normalization --------------------------------------------------------

def _normalize_time(time_str):
    """Converts GTFS time to PostgreSQL TIME format.

    Handles hour >= 24 by taking modulo (e.g. '24:08:00' -> '00:08:00').

    Args:
        time_str: Time string in HH:MM:SS format, or None.

    Returns:
        Normalized time string, or None if input is None.
    """
    if not time_str:
        return None
    parts = time_str.split(":")
    h = int(parts[0]) % 24
    return f"{h:02d}:{parts[1]}:{parts[2]}"


# -- Import helpers ------------------------------------------------------------

def _import_lines(cur, route_map):
    """Inserts railway_lines (9 rows), ON CONFLICT DO NOTHING.

    Args:
        cur: A psycopg2 cursor.
        route_map: Dict of {route_id: railway_id}.
    """
    for railway_id in route_map.values():
        cur.execute("""
            INSERT INTO railway_lines (railway_id, operator)
            VALUES (%s, %s)
            ON CONFLICT (railway_id) DO NOTHING;
        """, (railway_id, OPERATOR))


def _import_stations(cur, stops, stop_map, en_names):
    """Inserts railway_stations (185 rows).

    Args:
        cur: A psycopg2 cursor.
        stops: List of GTFS stop dicts.
        stop_map: Dict of {stop_id: (station_id, railway_id)}.
        en_names: Dict of {japanese_name: english_name}.
    """
    for s in stops:
        stop_id = s["stop_id"]
        if stop_id not in stop_map:
            continue
        station_id, railway_id = stop_map[stop_id]
        stop_code = s["stop_code"]
        name_ja = s["stop_name"]
        name_en = en_names.get(name_ja, "")
        lat = float(s["stop_lat"])
        lon = float(s["stop_lon"])

        cur.execute("""
            INSERT INTO railway_stations
                (station_id, station_code, name_ja, name_en,
                 latitude, longitude, geom, railway_id)
            VALUES (%s, %s, %s, %s, %s, %s,
                    ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s)
            ON CONFLICT (station_id) DO NOTHING;
        """, (station_id, stop_code, name_ja, name_en,
              lat, lon, lon, lat, railway_id))


def _import_timetables(cur, trips, stop_times, route_map, stop_map):
    """Inserts railway_train_timetable + railway_train_timetable_stops.

    Args:
        cur: A psycopg2 cursor.
        trips: List of GTFS trip dicts.
        stop_times: List of GTFS stop_time dicts.
        route_map: Dict of {route_id: railway_id}.
        stop_map: Dict of {stop_id: (station_id, railway_id)}.
    """

    # Group stop_times by trip_id, sorted by stop_sequence
    trip_stops = {}
    for st in stop_times:
        tid = st["trip_id"]
        trip_stops.setdefault(tid, []).append(st)
    for v in trip_stops.values():
        v.sort(key=lambda x: int(x["stop_sequence"]))

    stop_batch = []
    BATCH_SIZE = 5000

    for trip in trips:
        trip_id = trip["trip_id"]
        route_id = trip["route_id"]
        railway_id = route_map.get(route_id)
        if not railway_id:
            continue

        timetable_id = f"{OPERATOR}.{trip_id}"
        calendar = CALENDAR_MAP.get(trip.get("service_id", ""), "Weekday")
        direction = DIRECTION_MAP.get(trip.get("direction_id", ""), "Outbound")

        # Infer origin / destination from stop_times
        stops_list = trip_stops.get(trip_id, [])
        if not stops_list:
            continue

        first_stop_id = stops_list[0]["stop_id"]
        last_stop_id = stops_list[-1]["stop_id"]
        origin = stop_map.get(first_stop_id, (None,))[0]
        destination = stop_map.get(last_stop_id, (None,))[0]

        cur.execute("""
            INSERT INTO railway_train_timetable
                (timetable_id, train_id, railway_id, calendar,
                 rail_direction, train_type, train_number,
                 origin_station, destination_station)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (timetable_id) DO NOTHING;
        """, (timetable_id, trip_id, railway_id, calendar,
              direction, f"{OPERATOR}.Local", trip_id,
              origin, destination))

        # Stop details
        for idx, st in enumerate(stops_list):
            sid = stop_map.get(st["stop_id"], (None,))[0]
            if not sid:
                continue
            arr = _normalize_time(st.get("arrival_time"))
            dep = _normalize_time(st.get("departure_time"))
            stop_batch.append((timetable_id, idx, sid, arr, dep))

            if len(stop_batch) >= BATCH_SIZE:
                cur.executemany("""
                    INSERT INTO railway_train_timetable_stops
                        (timetable_id, stop_order, station_id,
                         arrival_time, departure_time)
                    VALUES (%s, %s, %s, %s, %s);
                """, stop_batch)
                stop_batch.clear()

    if stop_batch:
        cur.executemany("""
            INSERT INTO railway_train_timetable_stops
                (timetable_id, stop_order, station_id,
                 arrival_time, departure_time)
            VALUES (%s, %s, %s, %s, %s);
        """, stop_batch)


# -- Main entry point ----------------------------------------------------------

def import_metro_gtfs(gtfs_dir=None):
    """Imports Tokyo Metro GTFS data (appends, does not delete existing data).

    Args:
        gtfs_dir: Path to TokyoMetro-Train-GTFS directory.
                  Defaults to data/tokyo/TokyoMetro-Train-GTFS/.
    """
    if gtfs_dir is None:
        base = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        gtfs_dir = os.path.join(base, "data", "tokyo", "TokyoMetro-Train-GTFS")

    print("=== Tokyo Metro GTFS import ===")

    # Read GTFS files
    routes = _read_csv(os.path.join(gtfs_dir, "routes.txt"))
    stops = _read_csv(os.path.join(gtfs_dir, "stops.txt"))
    trips = _read_csv(os.path.join(gtfs_dir, "trips.txt"))
    stop_times = _read_csv(os.path.join(gtfs_dir, "stop_times.txt"))

    # Build mappings
    route_map = _build_route_map(routes)
    stop_map = _build_stop_map(stops)
    en_names = _build_translation_map(gtfs_dir)

    conn = connect_db()
    cur = conn.cursor()

    try:
        _import_lines(cur, route_map)
        _import_stations(cur, stops, stop_map, en_names)
        _import_timetables(cur, trips, stop_times, route_map, stop_map)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

    # Report
    lines_n = table_row_count("railway_lines")
    stations_n = table_row_count("railway_stations")
    tt_n = table_row_count("railway_train_timetable")
    stops_n = table_row_count("railway_train_timetable_stops")
    print(f"  railway_lines:                 {lines_n} rows")
    print(f"  railway_stations:              {stations_n} rows")
    print(f"  railway_train_timetable:       {tt_n} rows")
    print(f"  railway_train_timetable_stops: {stops_n} rows")
    print("=== Tokyo Metro GTFS import complete ===\n")
