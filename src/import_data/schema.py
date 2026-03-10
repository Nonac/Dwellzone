"""DDL constants for all transit database tables."""

from src.db import execute_sql

# -- Bus tables ---------------------------------------------------------------

DROP_BUS_TABLES = """
DROP TABLE IF EXISTS bus_timetable;
DROP TABLE IF EXISTS bus_route_stops;
DROP TABLE IF EXISTS bus_routes;
DROP TABLE IF EXISTS bus_stops;
"""

CREATE_BUS_STOPS = """
CREATE TABLE IF NOT EXISTS bus_stops (
    id SERIAL PRIMARY KEY,
    busstop_id TEXT UNIQUE,
    name_ja TEXT,
    name_en TEXT,
    name_kana TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    operator TEXT,
    busstop_pole_number TEXT,
    busroute_pattern TEXT[],
    geom GEOMETRY(Point, 4326)
);
"""

CREATE_BUS_ROUTES = """
CREATE TABLE IF NOT EXISTS bus_routes (
    route_id TEXT PRIMARY KEY,
    title TEXT,
    operator TEXT,
    direction TEXT,
    geom GEOMETRY(LineString, 4326)
);
"""

CREATE_BUS_ROUTE_STOPS = """
CREATE TABLE IF NOT EXISTS bus_route_stops (
    id SERIAL PRIMARY KEY,
    route_id TEXT REFERENCES bus_routes(route_id),
    busstop_id TEXT REFERENCES bus_stops(busstop_id),
    stop_order INT
);
"""

CREATE_BUS_TIMETABLE = """
CREATE TABLE IF NOT EXISTS bus_timetable (
    id SERIAL PRIMARY KEY,
    timetable_id TEXT,
    route_id TEXT REFERENCES bus_routes(route_id),
    busstop_id TEXT REFERENCES bus_stops(busstop_id),
    stop_order INT,
    arrival_time TIME,
    departure_time TIME,
    UNIQUE (timetable_id, busstop_id)
);
"""

CREATE_BUS_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_bus_routes_geom ON bus_routes USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_bus_stops_geom ON bus_stops USING GIST (geom);
"""

# -- Railway tables ------------------------------------------------------------

DROP_RAILWAY_TABLES = """
DROP TABLE IF EXISTS railway_train_timetable_stops;
DROP TABLE IF EXISTS railway_train_timetable;
DROP TABLE IF EXISTS railway_station_timetable;
DROP TABLE IF EXISTS railway_stations;
DROP TABLE IF EXISTS railway_lines;
DROP TABLE IF EXISTS railway_stops;
"""

CREATE_RAILWAY_LINES = """
CREATE TABLE IF NOT EXISTS railway_lines (
    railway_id TEXT PRIMARY KEY,
    operator TEXT
);
"""

CREATE_RAILWAY_STATIONS = """
CREATE TABLE IF NOT EXISTS railway_stations (
    id SERIAL PRIMARY KEY,
    station_id TEXT UNIQUE,
    station_code TEXT,
    name_ja TEXT,
    name_en TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    geom GEOMETRY(Point, 4326),
    railway_id TEXT REFERENCES railway_lines(railway_id)
);
"""

CREATE_RAILWAY_STATION_TIMETABLE = """
CREATE TABLE IF NOT EXISTS railway_station_timetable (
    id SERIAL PRIMARY KEY,
    station_id TEXT REFERENCES railway_stations(station_id),
    calendar TEXT,
    rail_direction TEXT,
    train_number TEXT,
    train_type TEXT,
    departure_time TIME,
    destination_station TEXT
);
"""

CREATE_RAILWAY_TRAIN_TIMETABLE = """
CREATE TABLE IF NOT EXISTS railway_train_timetable (
    id SERIAL PRIMARY KEY,
    timetable_id TEXT UNIQUE,
    train_id TEXT,
    railway_id TEXT,
    calendar TEXT,
    rail_direction TEXT,
    train_type TEXT,
    train_number TEXT,
    origin_station TEXT,
    destination_station TEXT
);
"""

CREATE_RAILWAY_TRAIN_TIMETABLE_STOPS = """
CREATE TABLE IF NOT EXISTS railway_train_timetable_stops (
    id SERIAL PRIMARY KEY,
    timetable_id TEXT REFERENCES railway_train_timetable(timetable_id),
    stop_order INT,
    station_id TEXT,
    arrival_time TIME,
    departure_time TIME
);
"""

CREATE_RAILWAY_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_railway_stations_geom ON railway_stations USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_railway_station_tt_station ON railway_station_timetable (station_id);
CREATE INDEX IF NOT EXISTS idx_railway_station_tt_calendar ON railway_station_timetable (calendar);
CREATE INDEX IF NOT EXISTS idx_railway_train_tt_stops_tid ON railway_train_timetable_stops (timetable_id);
"""

# -- Helper lists --------------------------------------------------------------

ALL_BUS_DDL = [
    CREATE_BUS_STOPS,
    CREATE_BUS_ROUTES,
    CREATE_BUS_ROUTE_STOPS,
    CREATE_BUS_TIMETABLE,
    CREATE_BUS_INDEXES,
]

ALL_RAILWAY_DDL = [
    CREATE_RAILWAY_LINES,
    CREATE_RAILWAY_STATIONS,
    CREATE_RAILWAY_STATION_TIMETABLE,
    CREATE_RAILWAY_TRAIN_TIMETABLE,
    CREATE_RAILWAY_TRAIN_TIMETABLE_STOPS,
    CREATE_RAILWAY_INDEXES,
]


def create_all_tables():
    """Creates all bus and railway tables with indexes.

    Drops existing tables first to avoid conflicts, then creates
    all tables in dependency order.
    """
    execute_sql(DROP_BUS_TABLES, message="  Dropped bus tables")
    for ddl in ALL_BUS_DDL:
        execute_sql(ddl)
    print("  Bus tables created")

    execute_sql(DROP_RAILWAY_TABLES, message="  Dropped railway tables")
    for ddl in ALL_RAILWAY_DDL:
        execute_sql(ddl)
    print("  Railway tables created")


def drop_all_tables():
    """Drops all bus and railway tables."""
    execute_sql(DROP_BUS_TABLES, message="  Dropped bus tables")
    execute_sql(DROP_RAILWAY_TABLES, message="  Dropped railway tables")
