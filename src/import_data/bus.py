"""Bus data import from ODPT JSON files."""

import json
import os

from src.db import connect_db, is_table_empty, clear_table, table_row_count
from src.transit.odpt import (
    clean_bus_stop_id,
    clean_bus_route_id,
    clean_busroute_pattern,
    clean_operator,
    strip_odpt_prefix,
)


# -- Bus stops -----------------------------------------------------------------

def import_bus_stops(json_file):
    """Imports bus stop locations from ODPT JSON.

    Args:
        json_file: Path to bus_stop_information.json.
    """
    if not is_table_empty("bus_stops"):
        clear_table("bus_stops")

    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    conn = connect_db()
    cur = conn.cursor()

    for stop in data:
        busstop_id = clean_bus_stop_id(stop.get("owl:sameAs", ""))
        name_ja = stop.get("dc:title", "")
        name_en = stop.get("title", {}).get("en", "Unknown") if isinstance(stop.get("title"), dict) else "Unknown"
        name_kana = stop.get("odpt:kana", None)
        latitude = stop.get("geo:lat", None)
        longitude = stop.get("geo:long", None)
        operator = clean_operator(stop.get("odpt:operator", None))
        pole_number = stop.get("odpt:busstopPoleNumber", None)
        busroute_pattern = clean_busroute_pattern(stop.get("odpt:busroutePattern", []))

        if latitude is None or longitude is None:
            continue

        cur.execute("""
            INSERT INTO bus_stops
                (busstop_id, name_ja, name_en, name_kana, latitude, longitude,
                 operator, busstop_pole_number, busroute_pattern, geom)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
                    ST_SetSRID(ST_MakePoint(%s, %s), 4326))
            ON CONFLICT (busstop_id) DO NOTHING;
        """, (busstop_id, name_ja, name_en, name_kana, latitude, longitude,
              operator, pole_number, busroute_pattern, longitude, latitude))

    conn.commit()
    cur.close()
    conn.close()
    print(f"  bus_stops imported ({table_row_count('bus_stops')} rows)")


# -- Bus routes ----------------------------------------------------------------

def import_bus_routes(json_file):
    """Imports bus route geometries from ODPT JSON.

    Args:
        json_file: Path to bus_route_patten.json.
    """
    if not is_table_empty("bus_routes"):
        clear_table("bus_routes")

    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    conn = connect_db()
    cur = conn.cursor()

    for route in data:
        pattern = route.get("odpt:pattern", None)
        direction = route.get("odpt:direction", None)
        route_id = f"{pattern}.{direction}"
        title = route.get("dc:title", "")
        operator = clean_operator(route.get("odpt:operator", ""))
        region = route.get("ug:region", None)

        if not pattern:
            continue

        route_geom = None
        if isinstance(region, dict) and "coordinates" in region:
            coords = region["coordinates"]
            if isinstance(coords, list) and len(coords) >= 2:
                route_geom = "LINESTRING(" + ", ".join(
                    f"{lon} {lat}" for lon, lat in coords
                ) + ")"

        cur.execute("""
            INSERT INTO bus_routes (route_id, title, operator, direction, geom)
            VALUES (%s, %s, %s, %s, ST_GeomFromText(%s, 4326))
            ON CONFLICT (route_id) DO UPDATE SET geom = EXCLUDED.geom;
        """, (route_id, title, operator, direction, route_geom))

    conn.commit()
    cur.close()
    conn.close()
    print(f"  bus_routes imported ({table_row_count('bus_routes')} rows)")


# -- Bus route-stop ordering ---------------------------------------------------

def import_bus_route_stops(json_file):
    """Imports bus route-stop ordering from ODPT JSON.

    Args:
        json_file: Path to bus_route_patten.json.
    """
    if not is_table_empty("bus_route_stops"):
        clear_table("bus_route_stops")

    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    conn = connect_db()
    cur = conn.cursor()

    for route in data:
        pattern = route.get("odpt:pattern", None)
        direction = route.get("odpt:direction", None)
        route_id = f"{pattern}.{direction}"
        stops = route.get("odpt:busstopPoleOrder", [])

        if not pattern:
            continue

        for stop in stops:
            stop_order = stop["odpt:index"]
            busstop_id = clean_bus_stop_id(stop["odpt:busstopPole"])

            cur.execute("""
                INSERT INTO bus_route_stops (route_id, busstop_id, stop_order)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING;
            """, (route_id, busstop_id, stop_order))

    conn.commit()
    cur.close()
    conn.close()
    print(f"  bus_route_stops imported ({table_row_count('bus_route_stops')} rows)")


# -- Bus timetable -------------------------------------------------------------

def import_bus_timetable(json_file):
    """Imports bus timetable entries from ODPT JSON.

    Args:
        json_file: Path to bus_timetable.json.
    """
    if not is_table_empty("bus_timetable"):
        clear_table("bus_timetable")

    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    conn = connect_db()
    cur = conn.cursor()

    for entry in data:
        timetable_id = strip_odpt_prefix(entry.get("owl:sameAs", ""))
        raw_pattern = strip_odpt_prefix(entry.get("odpt:busroutePattern", ""))
        parts = raw_pattern.split(".")
        route_id = ".".join(parts[-2:]) if len(parts) >= 2 else raw_pattern
        objects = entry.get("odpt:busTimetableObject", [])

        if not timetable_id or not objects:
            continue

        # Check if route exists
        cur.execute("SELECT 1 FROM bus_routes WHERE route_id = %s;", (route_id,))
        if not cur.fetchone():
            continue

        for stop in objects:
            stop_order = stop.get("odpt:index")
            raw_pole = strip_odpt_prefix(stop.get("odpt:busstopPole", ""))
            pole_parts = raw_pole.split(".")
            busstop_id = ".".join(pole_parts[-2:]) if len(pole_parts) >= 2 else raw_pole
            arrival_time = stop.get("odpt:arrivalTime", None)
            departure_time = stop.get("odpt:departureTime", None)

            # Check if stop exists
            cur.execute("SELECT 1 FROM bus_stops WHERE busstop_id = %s;", (busstop_id,))
            if not cur.fetchone():
                continue

            cur.execute("""
                INSERT INTO bus_timetable
                    (timetable_id, route_id, busstop_id, stop_order,
                     arrival_time, departure_time)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (timetable_id, busstop_id) DO NOTHING;
            """, (timetable_id, route_id, busstop_id, stop_order,
                  arrival_time, departure_time))

    conn.commit()
    cur.close()
    conn.close()
    print(f"  bus_timetable imported ({table_row_count('bus_timetable')} rows)")


# -- Import all ----------------------------------------------------------------

def import_all_bus(data_dir):
    """Imports all bus data in dependency order.

    Args:
        data_dir: Root data directory containing tokyo/bus/ subdirectory.
    """
    bus_dir = os.path.join(data_dir, "tokyo", "bus")
    print("=== Bus data import ===")
    import_bus_stops(os.path.join(bus_dir, "bus_stop_information.json"))
    import_bus_routes(os.path.join(bus_dir, "bus_route_patten.json"))
    import_bus_route_stops(os.path.join(bus_dir, "bus_route_patten.json"))
    import_bus_timetable(os.path.join(bus_dir, "bus_timetable.json"))
    print("=== Bus data import complete ===\n")
