"""SQLAlchemy ORM base, engine, and session factories.

Two separate databases:
- transit engine: tokyo_transit (existing, used by isochrone pipeline)
- suumo engine: suumo (new, housing data)
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from src.credentials import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
from src.settings import get_config


class TransitBase(DeclarativeBase):
    """Base for transit tables (tokyo_transit database)."""
    pass


class SuumoBase(DeclarativeBase):
    """Base for suumo tables (suumo database)."""
    pass


def _transit_url():
    return f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


def _suumo_url():
    cfg = get_config().get("suumo", {}).get("database", {})
    host = cfg.get("host", DB_HOST)
    port = cfg.get("port", DB_PORT)
    name = cfg.get("name", "suumo")
    user = cfg.get("user", DB_USER)
    password = cfg.get("password", DB_PASSWORD)
    return f"postgresql://{user}:{password}@{host}:{port}/{name}"


transit_engine = create_engine(_transit_url(), pool_pre_ping=True)
suumo_engine = create_engine(_suumo_url(), pool_pre_ping=True)

TransitSession = sessionmaker(bind=transit_engine)
SuumoSession = sessionmaker(bind=suumo_engine)


def init_suumo_db(reset=False):
    """Creates all suumo tables.

    Args:
        reset: If True, drops all tables first and recreates.
    """
    import src.models.suumo  # noqa: F401

    suumo_engine.dispose()  # Clear cached connections

    if reset:
        SuumoBase.metadata.drop_all(bind=suumo_engine)

    # Use raw DDL for idempotent index creation
    from sqlalchemy import text
    SuumoBase.metadata.create_all(bind=suumo_engine, checkfirst=True)
    print("[models] Suumo tables created/verified")


def init_transit_db():
    """Creates all transit tables. Safe to call repeatedly."""
    import src.models.transit  # noqa: F401
    TransitBase.metadata.create_all(bind=transit_engine, checkfirst=True)
    print("[models] Transit tables created/verified")
