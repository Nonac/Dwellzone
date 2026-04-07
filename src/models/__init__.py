"""SQLAlchemy ORM base, engine, and session factories.

Two separate databases:
- transit engine: tokyo_transit (used by isochrone pipeline)
- suumo engine: suumo (housing data)

Transit engine is lazy — only created when first accessed,
so the crawler can run without transit DB credentials.
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from src.settings import get_config


class TransitBase(DeclarativeBase):
    """Base for transit tables (tokyo_transit database)."""
    pass


class SuumoBase(DeclarativeBase):
    """Base for suumo tables (suumo database)."""
    pass


# -- Suumo engine (always available) ------------------------------------------

def _suumo_url():
    cfg = get_config().get("suumo", {}).get("database", {})
    host = cfg.get("host", "localhost")
    port = cfg.get("port", 5432)
    name = cfg.get("name", "suumo")
    user = cfg.get("user", "")
    password = cfg.get("password", "")
    return f"postgresql://{user}:{password}@{host}:{port}/{name}"


suumo_engine = create_engine(_suumo_url(), pool_pre_ping=True)
SuumoSession = sessionmaker(bind=suumo_engine)


# -- Transit engine (lazy, only for isochrone pipeline) ------------------------

_transit_engine = None
_TransitSession = None


def _get_transit_engine():
    global _transit_engine
    if _transit_engine is None:
        from src.credentials import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
        url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        _transit_engine = create_engine(url, pool_pre_ping=True)
    return _transit_engine


def get_transit_session_factory():
    global _TransitSession
    if _TransitSession is None:
        _TransitSession = sessionmaker(bind=_get_transit_engine())
    return _TransitSession


# Keep backward compat: transit_engine as property-like access
class _LazyTransitEngine:
    """Proxy that creates transit engine on first use."""
    def __getattr__(self, name):
        return getattr(_get_transit_engine(), name)

transit_engine = _LazyTransitEngine()


# -- Init functions ------------------------------------------------------------

def init_suumo_db(reset=False):
    """Creates all suumo tables."""
    import src.models.suumo  # noqa: F401

    suumo_engine.dispose()

    if reset:
        SuumoBase.metadata.drop_all(bind=suumo_engine)

    SuumoBase.metadata.create_all(bind=suumo_engine, checkfirst=True)
    print("[models] Suumo tables created/verified")


def init_transit_db():
    """Creates all transit tables. Safe to call repeatedly."""
    import src.models.transit  # noqa: F401
    engine = _get_transit_engine()
    TransitBase.metadata.create_all(bind=engine, checkfirst=True)
    print("[models] Transit tables created/verified")
