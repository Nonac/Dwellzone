"""ORM models for Suumo real estate listings.

Two separate tables:
- mansions: マンション (apartments) — 中古 + 新築
- kodates: 一戸建て (houses) — 中古 + 新築

Key design:
- content_hash: SHA256 of (address, price, area, floor_plan, listing_type)
  for deduplication. Suumo often lists the same property under multiple URLs.
- crawl_cycle_id: Links each listing to the crawl cycle that last saw it.
  Listings not seen in the current cycle are stale and should be purged.
- detail_fields: JSONB catch-all for all key-value pairs from detail page.
- images: JSONB array of image URLs from detail page.
"""

import hashlib
from datetime import datetime, timezone

from geoalchemy2 import Geometry
from sqlalchemy import (
    Column, Integer, BigInteger, Float, Text, Boolean,
    DateTime, ForeignKey, Index,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declared_attr

from src.models import SuumoBase


def _utcnow():
    return datetime.now(timezone.utc)


def compute_content_hash(address, price, area_sqm, floor_plan, listing_type):
    """Computes a SHA256 content hash for deduplication.

    Two listings with the same hash are considered the same property,
    even if they have different suumo_ids or URLs.

    Args:
        address: Full address string.
        price: Price in 万円.
        area_sqm: Area in m².
        floor_plan: Floor plan string (e.g. '3LDK').
        listing_type: 'mansion' or 'kodate'.

    Returns:
        Hex SHA256 string (64 chars).
    """
    parts = [
        str(address or ""),
        str(price or ""),
        f"{area_sqm:.1f}" if area_sqm else "",
        str(floor_plan or ""),
        str(listing_type or ""),
    ]
    raw = "|".join(parts).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def get_model_for_type(listing_type):
    """Returns the ORM model class for the given listing type."""
    return {"mansion": Mansion, "kodate": Kodate}[listing_type]


# -- Crawl cycle ---------------------------------------------------------------

class CrawlCycle(SuumoBase):
    """Represents one weekly crawl run.

    All listings reference the cycle that last confirmed them.
    After a new cycle completes, listings still pointing to the
    previous cycle are considered stale and deleted.
    """
    __tablename__ = "crawl_cycles"

    id = Column(Integer, primary_key=True)
    started_at = Column(DateTime, default=_utcnow)
    finished_at = Column(DateTime)
    status = Column(Text, default="running")  # running / completed / failed
    stats = Column(JSONB)  # {total, new, updated, duplicates, errors, ...}


# -- Listing mixin (shared columns) -------------------------------------------

class ListingMixin:
    """Shared columns for mansion and kodate tables."""

    # Identity
    suumo_id = Column(Text, nullable=False)
    content_hash = Column(Text, nullable=False, index=True)
    is_new = Column(Boolean)  # True = 新築, False = 中古

    # Basic property details
    title = Column(Text)
    price = Column(BigInteger)           # 万円
    price_per_sqm = Column(Integer)      # 万円/m²
    address = Column(Text)
    prefecture = Column(Text)
    city = Column(Text)
    town = Column(Text)
    latitude = Column(Float)
    longitude = Column(Float)

    @declared_attr
    def geom(cls):
        return Column(Geometry("POINT", srid=4326))

    station_access = Column(JSONB)       # [{line, station, walk_min}, ...]
    floor_plan = Column(Text)            # 間取り (3LDK etc)
    building_year = Column(Integer)      # 築年（西暦）
    building_age = Column(Integer)       # 築年数
    structure = Column(Text)             # 構造 (RC, SRC, 木造 etc)
    url = Column(Text)

    # Detail page data
    images = Column(JSONB)               # Array of image URLs
    description = Column(Text)           # Property description
    detail_fields = Column(JSONB)        # All key-value pairs from detail page
    raw_fields = Column(JSONB)           # Key-value pairs from list page
    detail_fetched_at = Column(DateTime) # When detail page was last crawled

    # Lifecycle
    @declared_attr
    def crawl_cycle_id(cls):
        return Column(Integer, ForeignKey("crawl_cycles.id"), nullable=False)

    status = Column(Text, default="active")  # active / delisted
    first_seen_at = Column(DateTime, default=_utcnow)
    last_seen_at = Column(DateTime, default=_utcnow)
    delisted_at = Column(DateTime)           # When listing disappeared from Suumo
    created_at = Column(DateTime, default=_utcnow)
    updated_at = Column(DateTime, default=_utcnow, onupdate=_utcnow)


# -- Mansion -------------------------------------------------------------------

class Mansion(ListingMixin, SuumoBase):
    """マンション (apartment/condo) listing table."""
    __tablename__ = "mansions"

    id = Column(Integer, primary_key=True)

    # Mansion-specific
    building_name = Column(Text)          # マンション名
    exclusive_area_sqm = Column(Float)    # 専有面積 m²
    balcony_area_sqm = Column(Float)      # バルコニー面積 m²
    floor_location = Column(Text)         # 所在階 (e.g. "5階")
    total_floors = Column(Text)           # 階建て (e.g. "10階建")
    management_fee = Column(Integer)      # 管理費 (円/月)
    repair_reserve = Column(Integer)      # 修繕積立金 (円/月)
    total_units = Column(Integer)         # 総戸数
    direction = Column(Text)              # 向き (南, 南西, etc.)

    __table_args__ = (
        Index("idx_mansions_suumo_id", "suumo_id"),
        Index("idx_mansions_pref_city", "prefecture", "city"),
        Index("idx_mansions_price", "price"),
        Index("idx_mansions_area", "exclusive_area_sqm"),
        Index("idx_mansions_year", "building_year"),
        Index("idx_mansions_cycle", "crawl_cycle_id"),
        Index("idx_mansions_hash_cycle", "content_hash", "crawl_cycle_id", unique=True),
    )


# -- Kodate -------------------------------------------------------------------

class Kodate(ListingMixin, SuumoBase):
    """一戸建て (detached house) listing table."""
    __tablename__ = "kodates"

    id = Column(Integer, primary_key=True)

    # Kodate-specific
    building_name = Column(Text)          # 物件名
    building_area_sqm = Column(Float)     # 建物面積 m²
    land_area_sqm = Column(Float)         # 土地面積 m²
    land_rights = Column(Text)            # 土地権利 (所有権, 借地権, etc.)
    road_access = Column(Text)            # 接道状況
    building_coverage = Column(Text)      # 建ぺい率
    floor_area_ratio = Column(Text)       # 容積率
    zoning = Column(Text)                 # 用途地域
    total_floors = Column(Text)           # 階建て

    __table_args__ = (
        Index("idx_kodates_suumo_id", "suumo_id"),
        Index("idx_kodates_pref_city", "prefecture", "city"),
        Index("idx_kodates_price", "price"),
        Index("idx_kodates_building_area", "building_area_sqm"),
        Index("idx_kodates_land_area", "land_area_sqm"),
        Index("idx_kodates_year", "building_year"),
        Index("idx_kodates_cycle", "crawl_cycle_id"),
        Index("idx_kodates_hash_cycle", "content_hash", "crawl_cycle_id", unique=True),
    )


# -- Price history -------------------------------------------------------------

class PriceHistory(SuumoBase):
    """Tracks price changes across crawl cycles for the same property."""
    __tablename__ = "price_history"

    id = Column(Integer, primary_key=True)
    content_hash = Column(Text, nullable=False, index=True)
    listing_type = Column(Text)          # 'mansion' or 'kodate'
    price = Column(BigInteger)
    crawl_cycle_id = Column(Integer, ForeignKey("crawl_cycles.id"))
    recorded_at = Column(DateTime, default=_utcnow)


# -- Geocode cache -------------------------------------------------------------

class GeocodeCache(SuumoBase):
    """Address → coordinates cache to avoid repeated GSI API calls."""
    __tablename__ = "geocode_cache"

    id = Column(Integer, primary_key=True)
    address = Column(Text, nullable=False, unique=True, index=True)
    latitude = Column(Float)
    longitude = Column(Float)
    created_at = Column(DateTime, default=_utcnow)


# -- Crawl detail log ----------------------------------------------------------

class CrawlLog(SuumoBase):
    """Per-query crawl log (one entry per prefecture × listing_type)."""
    __tablename__ = "crawl_logs"

    id = Column(Integer, primary_key=True)
    crawl_cycle_id = Column(Integer, ForeignKey("crawl_cycles.id"), nullable=False)
    prefecture = Column(Text)
    listing_type = Column(Text)
    is_new = Column(Boolean)
    total_pages = Column(Integer)
    total_items = Column(Integer)
    new_listings = Column(Integer, default=0)
    updated_listings = Column(Integer, default=0)
    duplicates_skipped = Column(Integer, default=0)
    errors = Column(Integer, default=0)
    started_at = Column(DateTime)
    finished_at = Column(DateTime)
    status = Column(Text)
