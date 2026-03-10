"""Railway data import from ODPT JSON files."""

import json
import os

from src.db import connect_db, is_table_empty, clear_table, table_row_count, execute_sql
from src.transit.odpt import (
    clean_railway_station_id,
    clean_railway_id,
    clean_operator,
    clean_calendar,
    clean_rail_direction,
    clean_train_type,
    strip_odpt_prefix,
)
from src.import_data.schema import (
    DROP_RAILWAY_TABLES,
    ALL_RAILWAY_DDL,
)


def create_railway_tables():
    """Creates all railway tables (drops existing first)."""
    execute_sql(DROP_RAILWAY_TABLES, message="  Dropped old railway tables")
    for ddl in ALL_RAILWAY_DDL:
        execute_sql(ddl)
    print("  Railway tables created")


# -- Railway stations + lines --------------------------------------------------

def import_railway_stations(json_file):
    """Imports railway stations and auto-extracts lines.

    Reads station data from ODPT JSON, extracts unique railway lines
    from the odpt:railway field, then inserts both lines and stations.

    Args:
        json_file: Path to railway_station_information.json.
    """
    if not is_table_empty("railway_stations"):
        clear_table("railway_stations")
    if not is_table_empty("railway_lines"):
        execute_sql("DELETE FROM railway_lines;")

    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    conn = connect_db()
    cur = conn.cursor()

    # Pass 1: collect and insert all railway_lines
    seen_railways = set()
    for station in data:
        raw_railway = station.get("odpt:railway", "")
        railway_id = clean_railway_id(raw_railway)
        if railway_id and railway_id not in seen_railways:
            operator = clean_operator(station.get("odpt:operator", ""))
            cur.execute("""
                INSERT INTO railway_lines (railway_id, operator)
                VALUES (%s, %s)
                ON CONFLICT (railway_id) DO NOTHING;
            """, (railway_id, operator))
            seen_railways.add(railway_id)

    # Pass 2: insert stations
    for station in data:
        station_id = clean_railway_station_id(station.get("owl:sameAs", ""))
        station_code = station.get("odpt:stationCode", None)
        name_ja = station.get("dc:title", "")
        title_obj = station.get("odpt:stationTitle", {})
        name_en = title_obj.get("en", "") if isinstance(title_obj, dict) else ""
        latitude = station.get("geo:lat", None)
        longitude = station.get("geo:long", None)
        railway_id = clean_railway_id(station.get("odpt:railway", ""))

        if latitude is None or longitude is None:
            print(f"  Skipping station without coordinates: {station_id}")
            continue

        cur.execute("""
            INSERT INTO railway_stations
                (station_id, station_code, name_ja, name_en,
                 latitude, longitude, geom, railway_id)
            VALUES (%s, %s, %s, %s, %s, %s,
                    ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s)
            ON CONFLICT (station_id) DO NOTHING;
        """, (station_id, station_code, name_ja, name_en,
              latitude, longitude, longitude, latitude, railway_id))

    conn.commit()
    cur.close()
    conn.close()
    lines_n = table_row_count("railway_lines")
    stations_n = table_row_count("railway_stations")
    print(f"  railway_lines imported ({lines_n} rows)")
    print(f"  railway_stations imported ({stations_n} rows)")


# -- Station timetable ---------------------------------------------------------

def import_railway_station_timetable(json_file):
    """Imports station-level departure timetables.

    Flattens nested stationTimetableObject arrays into one row per departure.

    Args:
        json_file: Path to railway_station_timetable.json.
    """
    if not is_table_empty("railway_station_timetable"):
        clear_table("railway_station_timetable")

    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    conn = connect_db()
    cur = conn.cursor()

    rows_inserted = 0
    batch = []
    BATCH_SIZE = 5000

    for tt in data:
        station_id = clean_railway_station_id(tt.get("odpt:station", ""))
        calendar = clean_calendar(tt.get("odpt:calendar", ""))
        direction = clean_rail_direction(tt.get("odpt:railDirection", ""))
        objects = tt.get("odpt:stationTimetableObject", [])

        for obj in objects:
            train_number = obj.get("odpt:trainNumber", "")
            train_type = clean_train_type(obj.get("odpt:trainType", ""))
            departure_time = obj.get("odpt:departureTime", None)

            # Destination may be a list
            dest_raw = obj.get("odpt:destinationStation", [])
            if isinstance(dest_raw, list) and dest_raw:
                destination = clean_railway_station_id(dest_raw[0])
            elif isinstance(dest_raw, str):
                destination = clean_railway_station_id(dest_raw)
            else:
                destination = None

            batch.append((station_id, calendar, direction,
                          train_number, train_type, departure_time,
                          destination))

            if len(batch) >= BATCH_SIZE:
                cur.executemany("""
                    INSERT INTO railway_station_timetable
                        (station_id, calendar, rail_direction,
                         train_number, train_type, departure_time,
                         destination_station)
                    VALUES (%s, %s, %s, %s, %s, %s, %s);
                """, batch)
                rows_inserted += len(batch)
                batch.clear()

    if batch:
        cur.executemany("""
            INSERT INTO railway_station_timetable
                (station_id, calendar, rail_direction,
                 train_number, train_type, departure_time,
                 destination_station)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """, batch)
        rows_inserted += len(batch)

    conn.commit()
    cur.close()
    conn.close()
    total = table_row_count("railway_station_timetable")
    print(f"  railway_station_timetable imported ({total} rows)")


# -- Train timetable -----------------------------------------------------------

def import_railway_train_timetable(json_file):
    """Imports train-level timetables with stop details.

    Populates two tables:
        railway_train_timetable: one row per train (~1000)
        railway_train_timetable_stops: one row per stop (~25000)

    Args:
        json_file: Path to railway_timetanle.json.
    """
    if not is_table_empty("railway_train_timetable_stops"):
        clear_table("railway_train_timetable_stops")
    if not is_table_empty("railway_train_timetable"):
        clear_table("railway_train_timetable")

    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    conn = connect_db()
    cur = conn.cursor()

    stop_batch = []
    BATCH_SIZE = 5000

    for train in data:
        timetable_id = strip_odpt_prefix(train.get("owl:sameAs", ""))
        train_id = strip_odpt_prefix(train.get("odpt:train", ""))
        railway_id = clean_railway_id(train.get("odpt:railway", ""))
        calendar = clean_calendar(train.get("odpt:calendar", ""))
        direction = clean_rail_direction(train.get("odpt:railDirection", ""))
        train_type = clean_train_type(train.get("odpt:trainType", ""))
        train_number = train.get("odpt:trainNumber", "")

        # Origin / destination may be lists
        origin_raw = train.get("odpt:originStation", [])
        origin = clean_railway_station_id(origin_raw[0]) if origin_raw else None

        dest_raw = train.get("odpt:destinationStation", [])
        destination = clean_railway_station_id(dest_raw[0]) if dest_raw else None

        if not timetable_id:
            continue

        # Insert timetable header
        cur.execute("""
            INSERT INTO railway_train_timetable
                (timetable_id, train_id, railway_id, calendar,
                 rail_direction, train_type, train_number,
                 origin_station, destination_station)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (timetable_id) DO NOTHING;
        """, (timetable_id, train_id, railway_id, calendar,
              direction, train_type, train_number, origin, destination))

        # Stop details
        objects = train.get("odpt:trainTimetableObject", [])
        for idx, stop in enumerate(objects):
            # Prefer departureStation, fallback to arrivalStation
            station_id = strip_odpt_prefix(
                stop.get("odpt:departureStation")
                or stop.get("odpt:arrivalStation", "")
            )
            arrival_time = stop.get("odpt:arrivalTime", None)
            departure_time = stop.get("odpt:departureTime", None)

            stop_batch.append((timetable_id, idx, station_id,
                               arrival_time, departure_time))

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

    conn.commit()
    cur.close()
    conn.close()
    tt_n = table_row_count("railway_train_timetable")
    stops_n = table_row_count("railway_train_timetable_stops")
    print(f"  railway_train_timetable imported ({tt_n} rows)")
    print(f"  railway_train_timetable_stops imported ({stops_n} rows)")


# -- Import all ----------------------------------------------------------------

def import_all_railway(data_dir):
    """Creates tables and imports all railway data in dependency order.

    Args:
        data_dir: Root data directory containing tokyo/railway/ subdirectory.
    """
    rail_dir = os.path.join(data_dir, "tokyo", "railway")
    print("=== Railway data import ===")
    create_railway_tables()
    import_railway_stations(
        os.path.join(rail_dir, "railway_station_information.json"))
    import_railway_station_timetable(
        os.path.join(rail_dir, "railway_station_timetable.json"))
    import_railway_train_timetable(
        os.path.join(rail_dir, "railway_timetanle.json"))
    print("=== Railway data import complete ===\n")
