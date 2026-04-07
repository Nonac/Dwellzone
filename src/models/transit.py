"""ORM models for transit tables (bus + railway).

Mirrors the existing schema in src/import_data/schema.py.
"""

from geoalchemy2 import Geometry
from sqlalchemy import (
    Column, Integer, String, Float, Text, Time,
    ForeignKey, UniqueConstraint, Index, ARRAY,
)

from src.models import TransitBase as Base


# -- Bus tables ----------------------------------------------------------------

class BusStop(Base):
    __tablename__ = "bus_stops"

    id = Column(Integer, primary_key=True)
    busstop_id = Column(Text, unique=True)
    name_ja = Column(Text)
    name_en = Column(Text)
    name_kana = Column(Text)
    latitude = Column(Float)
    longitude = Column(Float)
    operator = Column(Text)
    busstop_pole_number = Column(Text)
    busroute_pattern = Column(ARRAY(Text))
    geom = Column(Geometry("POINT", srid=4326))

    __table_args__ = (
        Index("idx_bus_stops_geom", geom, postgresql_using="gist"),
    )


class BusRoute(Base):
    __tablename__ = "bus_routes"

    route_id = Column(Text, primary_key=True)
    title = Column(Text)
    operator = Column(Text)
    direction = Column(Text)
    geom = Column(Geometry("LINESTRING", srid=4326))

    __table_args__ = (
        Index("idx_bus_routes_geom", geom, postgresql_using="gist"),
    )


class BusRouteStop(Base):
    __tablename__ = "bus_route_stops"

    id = Column(Integer, primary_key=True)
    route_id = Column(Text, ForeignKey("bus_routes.route_id"))
    busstop_id = Column(Text, ForeignKey("bus_stops.busstop_id"))
    stop_order = Column(Integer)


class BusTimetable(Base):
    __tablename__ = "bus_timetable"

    id = Column(Integer, primary_key=True)
    timetable_id = Column(Text)
    route_id = Column(Text, ForeignKey("bus_routes.route_id"))
    busstop_id = Column(Text, ForeignKey("bus_stops.busstop_id"))
    stop_order = Column(Integer)
    arrival_time = Column(Time)
    departure_time = Column(Time)

    __table_args__ = (
        UniqueConstraint("timetable_id", "busstop_id"),
    )


# -- Railway tables ------------------------------------------------------------

class RailwayLine(Base):
    __tablename__ = "railway_lines"

    railway_id = Column(Text, primary_key=True)
    operator = Column(Text)


class RailwayStation(Base):
    __tablename__ = "railway_stations"

    id = Column(Integer, primary_key=True)
    station_id = Column(Text, unique=True)
    station_code = Column(Text)
    name_ja = Column(Text)
    name_en = Column(Text)
    latitude = Column(Float)
    longitude = Column(Float)
    geom = Column(Geometry("POINT", srid=4326))
    railway_id = Column(Text, ForeignKey("railway_lines.railway_id"))

    __table_args__ = (
        Index("idx_railway_stations_geom", geom, postgresql_using="gist"),
    )


class RailwayStationTimetable(Base):
    __tablename__ = "railway_station_timetable"

    id = Column(Integer, primary_key=True)
    station_id = Column(Text, ForeignKey("railway_stations.station_id"))
    calendar = Column(Text)
    rail_direction = Column(Text)
    train_number = Column(Text)
    train_type = Column(Text)
    departure_time = Column(Time)
    destination_station = Column(Text)

    __table_args__ = (
        Index("idx_railway_station_tt_station", station_id),
        Index("idx_railway_station_tt_calendar", calendar),
    )


class RailwayTrainTimetable(Base):
    __tablename__ = "railway_train_timetable"

    id = Column(Integer, primary_key=True)
    timetable_id = Column(Text, unique=True)
    train_id = Column(Text)
    railway_id = Column(Text)
    calendar = Column(Text)
    rail_direction = Column(Text)
    train_type = Column(Text)
    train_number = Column(Text)
    origin_station = Column(Text)
    destination_station = Column(Text)


class RailwayTrainTimetableStop(Base):
    __tablename__ = "railway_train_timetable_stops"

    id = Column(Integer, primary_key=True)
    timetable_id = Column(Text, ForeignKey("railway_train_timetable.timetable_id"))
    stop_order = Column(Integer)
    station_id = Column(Text)
    arrival_time = Column(Time)
    departure_time = Column(Time)

    __table_args__ = (
        Index("idx_railway_train_tt_stops_tid", timetable_id),
    )
