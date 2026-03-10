-- Manual bootstrap script for tokyo_transit database.
-- For Python-driven setup, use: python scripts/init_db.py
--
-- The authoritative DDL source is src/import_data/schema.py.
-- This file is provided for manual psql usage only.

-- Terminate existing connections to the database
SELECT pg_terminate_backend(pg_stat_activity.pid)
FROM pg_stat_activity
WHERE pg_stat_activity.datname = 'tokyo_transit' AND pid <> pg_backend_pid();

-- Enable PostGIS extension
CREATE EXTENSION IF NOT EXISTS postgis;

-- Drop old bus tables (reverse dependency order)
DROP TABLE IF EXISTS bus_timetable;
DROP TABLE IF EXISTS bus_route_stops;
DROP TABLE IF EXISTS bus_routes;
DROP TABLE IF EXISTS bus_stops;

-- Bus stops table
CREATE TABLE bus_stops (
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

-- Bus routes table
CREATE TABLE bus_routes (
    route_id TEXT PRIMARY KEY,
    title TEXT,
    operator TEXT,
    direction TEXT,
    geom GEOMETRY(LineString, 4326)
);

-- Bus route-stop ordering
CREATE TABLE bus_route_stops (
    id SERIAL PRIMARY KEY,
    route_id TEXT REFERENCES bus_routes(route_id),
    busstop_id TEXT REFERENCES bus_stops(busstop_id),
    stop_order INT
);

-- Bus timetable
CREATE TABLE bus_timetable (
    id SERIAL PRIMARY KEY,
    timetable_id TEXT,
    route_id TEXT REFERENCES bus_routes(route_id),
    busstop_id TEXT REFERENCES bus_stops(busstop_id),
    stop_order INT,
    arrival_time TIME,
    departure_time TIME,
    UNIQUE (timetable_id, busstop_id)
);

-- Bus spatial indexes
CREATE INDEX idx_bus_routes_geom ON bus_routes USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_bus_stops_geom ON bus_stops USING GIST (geom);

-- -------------------------------------------------------------------------
-- Railway tables
-- -------------------------------------------------------------------------

-- Drop old railway tables (reverse dependency order)
DROP TABLE IF EXISTS railway_train_timetable_stops;
DROP TABLE IF EXISTS railway_train_timetable;
DROP TABLE IF EXISTS railway_station_timetable;
DROP TABLE IF EXISTS railway_stations;
DROP TABLE IF EXISTS railway_lines;
DROP TABLE IF EXISTS railway_stops;

-- Railway lines
CREATE TABLE railway_lines (
    railway_id TEXT PRIMARY KEY,
    operator TEXT
);

-- Railway stations
CREATE TABLE railway_stations (
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

-- Station-level departure timetable
CREATE TABLE railway_station_timetable (
    id SERIAL PRIMARY KEY,
    station_id TEXT REFERENCES railway_stations(station_id),
    calendar TEXT,
    rail_direction TEXT,
    train_number TEXT,
    train_type TEXT,
    departure_time TIME,
    destination_station TEXT
);

-- Train-level timetable (header)
CREATE TABLE railway_train_timetable (
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

-- Train timetable stop details
CREATE TABLE railway_train_timetable_stops (
    id SERIAL PRIMARY KEY,
    timetable_id TEXT REFERENCES railway_train_timetable(timetable_id),
    stop_order INT,
    station_id TEXT,
    arrival_time TIME,
    departure_time TIME
);

-- Railway indexes
CREATE INDEX idx_railway_stations_geom ON railway_stations USING GIST (geom);
CREATE INDEX idx_railway_station_tt_station ON railway_station_timetable (station_id);
CREATE INDEX idx_railway_station_tt_calendar ON railway_station_timetable (calendar);
CREATE INDEX idx_railway_train_tt_stops_tid ON railway_train_timetable_stops (timetable_id);
