"""mini-tokyo-3d railway data import (41 operators excluding Toei/TokyoMetro)."""

import json
import os

from src.db import connect_db, table_row_count


# -- JSON reading --------------------------------------------------------------

def _load_json(filepath):
    """Reads a JSON file and returns the parsed data.

    Args:
        filepath: Path to the JSON file.

    Returns:
        Parsed JSON data.
    """
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


# -- Time normalization --------------------------------------------------------

def _normalize_time(hhmm):
    """Converts 'HH:MM' to 'HH:MM:00', with hour >= 24 taken modulo.

    Args:
        hhmm: Time string in HH:MM format, or None.

    Returns:
        Normalized time string in HH:MM:SS format, or None.
    """
    if not hhmm:
        return None
    parts = hhmm.split(":")
    h = int(parts[0]) % 24
    return f"{h:02d}:{parts[1]}:00"


# -- Calendar parsing ---------------------------------------------------------

def _parse_calendar(trip_id):
    """Extracts calendar from trip ID; returns None to skip.

    Format: Operator.Line.TrainNum.Calendar[.Variant]
    Calendar = 'Weekday' | 'SaturdayHoliday'
    Variant = numeric suffix (skip these)

    Args:
        trip_id: Trip identifier string.

    Returns:
        Calendar string ('Weekday' or 'SaturdayHoliday'), or None to skip.
    """
    parts = trip_id.split(".")
    if len(parts) < 4:
        return None
    cal = parts[3]
    if cal in ("Weekday", "SaturdayHoliday"):
        # Skip trips with numeric variant suffix
        if len(parts) >= 5 and parts[4].isdigit():
            return None
        return cal
    return None


# -- Operator filter -----------------------------------------------------------

_SKIP_PREFIXES = ("Toei.", "TokyoMetro.")


def _should_skip(item_id):
    """Returns True if item belongs to Toei or TokyoMetro.

    Args:
        item_id: An identifier string to check.

    Returns:
        True if the ID starts with a Toei or TokyoMetro prefix.
    """
    return item_id.startswith(_SKIP_PREFIXES)


# -- Import helpers ------------------------------------------------------------

def _import_lines(cur, railways):
    """Inserts railway_lines, returns set of imported railway_ids.

    Args:
        cur: A psycopg2 cursor.
        railways: List of railway dicts from railways.json.

    Returns:
        Set of inserted railway_id strings.
    """
    inserted = set()
    for r in railways:
        rid = r["id"]
        if _should_skip(rid):
            continue
        operator = rid.split(".")[0]
        cur.execute("""
            INSERT INTO railway_lines (railway_id, operator)
            VALUES (%s, %s)
            ON CONFLICT (railway_id) DO NOTHING;
        """, (rid, operator))
        inserted.add(rid)
    return inserted


def _import_stations(cur, stations, valid_railways):
    """Inserts railway_stations, returns set of imported station_ids.

    Args:
        cur: A psycopg2 cursor.
        stations: List of station dicts from stations.json.
        valid_railways: Set of valid railway_id strings.

    Returns:
        Set of inserted station_id strings.
    """
    inserted = set()
    for s in stations:
        sid = s["id"]
        if _should_skip(sid):
            continue
        coord = s.get("coord")
        if not coord:
            continue
        railway_id = s.get("railway", "")
        if railway_id not in valid_railways:
            continue

        lon, lat = coord[0], coord[1]
        name_ja = s.get("title", {}).get("ja", "")
        name_en = s.get("title", {}).get("en", "")

        cur.execute("""
            INSERT INTO railway_stations
                (station_id, station_code, name_ja, name_en,
                 latitude, longitude, geom, railway_id)
            VALUES (%s, NULL, %s, %s, %s, %s,
                    ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s)
            ON CONFLICT (station_id) DO NOTHING;
        """, (sid, name_ja, name_en, lat, lon, lon, lat, railway_id))
        inserted.add(sid)
    return inserted


def _import_timetables(cur, timetable_dir, valid_stations):
    """Iterates timetable JSON files and inserts timetable + stops.

    Args:
        cur: A psycopg2 cursor.
        timetable_dir: Path to the train-timetables directory.
        valid_stations: Set of valid station_id strings.

    Returns:
        A tuple of (trip_count, stop_count, skipped_count).
    """
    BATCH_SIZE = 5000
    stop_batch = []
    tt_count = 0
    stop_count = 0
    skipped_trips = 0

    files = sorted(f for f in os.listdir(timetable_dir) if f.endswith(".json"))
    for fname in files:
        # Skip Toei / TokyoMetro files
        lower = fname.lower()
        if lower.startswith("toei-") or lower.startswith("tokyometro-"):
            continue

        trips = _load_json(os.path.join(timetable_dir, fname))

        for trip in trips:
            trip_id = trip["id"]
            calendar = _parse_calendar(trip_id)
            if calendar is None:
                skipped_trips += 1
                continue

            railway_id = trip.get("r", "")
            if _should_skip(railway_id):
                skipped_trips += 1
                continue

            train_id = trip.get("t", "")
            rail_direction = trip.get("d", "")
            train_type = trip.get("y", "")
            train_number = trip.get("n", "")

            os_list = trip.get("os", [])
            ds_list = trip.get("ds", [])
            origin = os_list[0] if os_list else None
            destination = ds_list[0] if ds_list else None

            cur.execute("""
                INSERT INTO railway_train_timetable
                    (timetable_id, train_id, railway_id, calendar,
                     rail_direction, train_type, train_number,
                     origin_station, destination_station)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (timetable_id) DO NOTHING;
            """, (trip_id, train_id, railway_id, calendar,
                  rail_direction, train_type, train_number,
                  origin, destination))
            tt_count += 1

            # Stop details
            for idx, stop in enumerate(trip.get("tt", [])):
                station_id = stop.get("s", "")
                if station_id not in valid_stations:
                    continue
                arr = _normalize_time(stop.get("a"))
                dep = _normalize_time(stop.get("d"))
                stop_batch.append((trip_id, idx, station_id, arr, dep))
                stop_count += 1

                if len(stop_batch) >= BATCH_SIZE:
                    cur.executemany("""
                        INSERT INTO railway_train_timetable_stops
                            (timetable_id, stop_order, station_id,
                             arrival_time, departure_time)
                        VALUES (%s, %s, %s, %s, %s);
                    """, stop_batch)
                    stop_batch.clear()

    # Flush remaining
    if stop_batch:
        cur.executemany("""
            INSERT INTO railway_train_timetable_stops
                (timetable_id, stop_order, station_id,
                 arrival_time, departure_time)
            VALUES (%s, %s, %s, %s, %s);
        """, stop_batch)

    return tt_count, stop_count, skipped_trips


# -- Main entry point ----------------------------------------------------------

def import_minitokyo3d(data_dir=None):
    """Imports mini-tokyo-3d railway data (appends, does not delete existing data).

    Covers 41 operators excluding Toei and TokyoMetro.

    Args:
        data_dir: Path to mini-tokyo-3d/data/ directory.
                  Defaults to data/mini-tokyo-3d/data/.
    """
    if data_dir is None:
        base = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        data_dir = os.path.join(base, "data", "mini-tokyo-3d", "data")

    print("=== mini-tokyo-3d railway data import ===")

    # Read main data
    railways = _load_json(os.path.join(data_dir, "railways.json"))
    stations = _load_json(os.path.join(data_dir, "stations.json"))
    timetable_dir = os.path.join(data_dir, "train-timetables")

    conn = connect_db()
    cur = conn.cursor()

    try:
        # 1. Lines
        valid_railways = _import_lines(cur, railways)
        print(f"  Lines imported: {len(valid_railways)}")

        # 2. Stations
        valid_stations = _import_stations(cur, stations, valid_railways)
        print(f"  Stations imported: {len(valid_stations)}")

        # 3. Timetables + stops
        tt_count, stop_count, skipped = _import_timetables(
            cur, timetable_dir, valid_stations
        )
        print(f"  Trips imported: {tt_count} (skipped {skipped})")
        print(f"  Stops imported: {stop_count}")

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

    # Report totals
    lines_n = table_row_count("railway_lines")
    stations_n = table_row_count("railway_stations")
    tt_n = table_row_count("railway_train_timetable")
    stops_n = table_row_count("railway_train_timetable_stops")
    print(f"\n  DB totals:")
    print(f"    railway_lines:                 {lines_n} rows")
    print(f"    railway_stations:              {stations_n} rows")
    print(f"    railway_train_timetable:       {tt_n} rows")
    print(f"    railway_train_timetable_stops: {stops_n} rows")
    print("=== mini-tokyo-3d import complete ===\n")
