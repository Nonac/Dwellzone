"""Main crawl pipeline: cycle management, page iteration, upsert, TTL purge.

Workflow:
1. Start a new CrawlCycle
2. Phase 1 — List pages: for each (prefecture, listing_type):
   a. Iterate search result pages
   b. Parse listings, compute content_hash
   c. Upsert into mansions or kodates table
   d. Duplicates (same hash, same cycle) are skipped
3. Phase 2 — Detail pages: for each new/updated listing:
   a. Fetch the detail page URL
   b. Parse images, description, detail_fields
   c. Update the listing row
4. After all queries complete:
   a. Purge stale listings (crawl_cycle_id != current cycle)
   b. Mark cycle as completed
"""

import sys
import os
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.db import get_suumo_session
from src.models.suumo import (
    CrawlCycle, Mansion, Kodate, PriceHistory, CrawlLog,
    compute_content_hash, get_model_for_type,
)
from src.scraper.suumo_client import SuumoClient, BS_CODES
from src.scraper.parser import parse_listing_page, parse_total_pages, parse_detail_page
from src.scraper.geocoder import geocode_address
from src.settings import get_config


def _now():
    return datetime.now(timezone.utc)


def _parse_address(address, prefecture_code):
    """Splits address into (prefecture, city, town)."""
    import re
    pref_names = {13: "東京都", 14: "神奈川県", 11: "埼玉県", 12: "千葉県"}
    pref = pref_names.get(prefecture_code, "")
    city, town = "", ""
    if address and pref and address.startswith(pref):
        rest = address[len(pref):]
        m = re.match(r"(.+?[市区町村])(.*)", rest)
        if m:
            city, town = m.group(1), m.group(2)
        else:
            city = rest
    return pref, city, town


# -- Cycle management ----------------------------------------------------------

def start_cycle():
    """Creates a new CrawlCycle and returns its ID."""
    with get_suumo_session() as session:
        cycle = CrawlCycle(started_at=_now(), status="running")
        session.add(cycle)
        session.flush()
        cycle_id = cycle.id
    print(f"[crawl] Started cycle #{cycle_id}")
    return cycle_id


def finish_cycle(cycle_id, status="completed", stats=None, purge=False):
    """Marks a cycle as finished. Optionally purges stale listings.

    Args:
        cycle_id: Cycle ID.
        status: Final status string.
        stats: Stats dict to store.
        purge: If True, delete listings not seen in this cycle.
               Only set True for full crawls covering all types/prefectures.
    """
    with get_suumo_session() as session:
        cycle = session.query(CrawlCycle).get(cycle_id)
        if cycle:
            cycle.finished_at = _now()
            cycle.status = status
            cycle.stats = stats

    if purge:
        purged = purge_stale(cycle_id)
        print(f"[crawl] Cycle #{cycle_id} {status}. Purged {purged} stale listings.")
    else:
        print(f"[crawl] Cycle #{cycle_id} {status}.")


def purge_stale(current_cycle_id):
    """Deletes listings not updated in the current cycle from both tables.

    Returns:
        Number of deleted rows.
    """
    total = 0
    with get_suumo_session() as session:
        for Model in [Mansion, Kodate]:
            stale_hashes = (
                session.query(Model.content_hash)
                .filter(Model.crawl_cycle_id != current_cycle_id)
                .subquery()
            )
            session.query(PriceHistory).filter(
                PriceHistory.content_hash.in_(stale_hashes.select())
            ).delete(synchronize_session=False)

            count = (
                session.query(Model)
                .filter(Model.crawl_cycle_id != current_cycle_id)
                .delete(synchronize_session=False)
            )
            total += count
    return total


# -- Single query crawl --------------------------------------------------------

def crawl_query(cycle_id, prefecture, listing_type, is_new=False,
                max_pages=0, max_items=0):
    """Crawls one (prefecture, listing_type, is_new) combination.

    Uses unified search endpoint for both mansion and kodate.

    Args:
        cycle_id: Current CrawlCycle ID.
        prefecture: Prefecture code.
        listing_type: 'mansion' or 'kodate'.
        is_new: True for 新築.
        max_pages: 0 = unlimited.
        max_items: 0 = unlimited. Stops when total_items reaches this.

    Returns:
        Stats dict.
    """
    client = SuumoClient()
    stats = {"new": 0, "updated": 0, "duplicates": 0, "errors": 0,
             "total_pages": 0, "total_items": 0}
    log_started = _now()
    label = f"pref={prefecture} {'new_' if is_new else ''}{listing_type}"

    print(f"[crawl] {label}: starting...")
    url = client.build_search_url(prefecture, listing_type, is_new, page=1)
    try:
        html = client.fetch(url)
    except Exception as e:
        print(f"[crawl] {label}: failed: {e}")
        _save_log(cycle_id, prefecture, listing_type, is_new, stats, log_started, "failed")
        return stats

    total_pages = parse_total_pages(html)
    if max_pages > 0:
        total_pages = min(total_pages, max_pages)
    stats["total_pages"] = total_pages
    print(f"[crawl] {label}: {total_pages} pages")

    _process_page(html, cycle_id, prefecture, listing_type, is_new, stats)

    for page in range(2, total_pages + 1):
        if max_items > 0 and stats["total_items"] >= max_items:
            print(f"[crawl] {label}: reached max_items={max_items}, stopping")
            break
        url = client.build_search_url(prefecture, listing_type, is_new, page=page)
        try:
            html = client.fetch(url)
            _process_page(html, cycle_id, prefecture, listing_type, is_new, stats)
        except Exception as e:
            print(f"[crawl] {label}: page {page} error: {e}")
            stats["errors"] += 1
        if page % 10 == 0:
            print(f"[crawl] {label}: page {page}/{total_pages}")

    _save_log(cycle_id, prefecture, listing_type, is_new, stats, log_started, "success")
    print(f"[crawl] {label}: done — new={stats['new']} upd={stats['updated']} dup={stats['duplicates']}")
    return stats


def _process_page(html, cycle_id, prefecture, listing_type, is_new, stats):
    """Parses one page and upserts listings into the correct table."""
    items = parse_listing_page(html)
    stats["total_items"] += len(items)
    Model = get_model_for_type(listing_type)

    with get_suumo_session() as session:
        for item in items:
            suumo_id = item.get("suumo_id")
            if not suumo_id:
                continue

            pref, city, town = _parse_address(item.get("address", ""), prefecture)
            price = item.get("price")
            area = item.get("area_sqm")
            floor_plan = item.get("floor_plan")

            chash = compute_content_hash(
                item.get("address"), price, area, floor_plan, listing_type
            )

            # Check if already seen in this cycle (duplicate)
            existing = (
                session.query(Model)
                .filter_by(content_hash=chash)
                .first()
            )

            if existing and existing.crawl_cycle_id == cycle_id:
                stats["duplicates"] += 1
                continue

            # Geocode
            lat, lon, geom_wkt = None, None, None
            addr = item.get("address", "")
            if addr:
                lat, lon = geocode_address(addr)
                if lat and lon:
                    geom_wkt = f"SRID=4326;POINT({lon} {lat})"

            now = _now()

            if existing:
                # Update: bump cycle, check price change
                if price and existing.price and price != existing.price:
                    session.add(PriceHistory(
                        content_hash=chash,
                        listing_type=listing_type,
                        price=price,
                        crawl_cycle_id=cycle_id,
                        recorded_at=now,
                    ))

                existing.crawl_cycle_id = cycle_id
                existing.suumo_id = suumo_id
                existing.price = price or existing.price
                existing.url = item.get("url") or existing.url
                existing.last_seen_at = now
                existing.updated_at = now
                existing.raw_fields = item.get("raw_fields")
                if lat and lon:
                    existing.latitude = lat
                    existing.longitude = lon
                    existing.geom = geom_wkt
                # Reset detail_fetched_at so detail page is re-crawled
                existing.detail_fetched_at = None
                stats["updated"] += 1
            else:
                # Build common kwargs
                kwargs = dict(
                    suumo_id=suumo_id,
                    content_hash=chash,
                    is_new=is_new,
                    title=item.get("title"),
                    price=price,
                    address=addr,
                    prefecture=pref,
                    city=city,
                    town=town,
                    latitude=lat,
                    longitude=lon,
                    geom=geom_wkt,
                    station_access=item.get("station_access") or None,
                    floor_plan=floor_plan,
                    building_year=item.get("building_year"),
                    building_age=item.get("building_age"),
                    structure=item.get("structure"),
                    url=item.get("url"),
                    raw_fields=item.get("raw_fields"),
                    crawl_cycle_id=cycle_id,
                    first_seen_at=now,
                    created_at=now,
                    updated_at=now,
                )

                # Type-specific columns from list page
                if listing_type == "mansion":
                    kwargs["building_name"] = item.get("building_name")
                    kwargs["exclusive_area_sqm"] = item.get("area_sqm")
                    kwargs["balcony_area_sqm"] = item.get("balcony_sqm")
                    kwargs["floor_location"] = item.get("floors")
                elif listing_type == "kodate":
                    kwargs["building_name"] = item.get("building_name")
                    kwargs["building_area_sqm"] = item.get("area_sqm")
                    kwargs["land_area_sqm"] = item.get("land_area_sqm")

                listing = Model(**kwargs)
                session.add(listing)

                # Record initial price
                if price:
                    session.add(PriceHistory(
                        content_hash=chash,
                        listing_type=listing_type,
                        price=price,
                        crawl_cycle_id=cycle_id,
                        recorded_at=now,
                    ))
                stats["new"] += 1


# -- Detail page crawl ---------------------------------------------------------

def crawl_details_for_type(cycle_id, listing_type):
    """Fetches detail pages for listings missing detail data.

    Only processes listings from the current cycle that haven't
    had their detail page fetched yet.

    Args:
        cycle_id: Current CrawlCycle ID.
        listing_type: 'mansion' or 'kodate'.

    Returns:
        Stats dict with fetched/errors counts.
    """
    Model = get_model_for_type(listing_type)
    client = SuumoClient()
    stats = {"fetched": 0, "errors": 0, "total": 0}

    # Get IDs and URLs of listings needing detail fetch
    with get_suumo_session() as session:
        rows = (
            session.query(Model.id, Model.url)
            .filter(Model.crawl_cycle_id == cycle_id)
            .filter(Model.detail_fetched_at.is_(None))
            .all()
        )

    stats["total"] = len(rows)
    if not rows:
        print(f"[detail] {listing_type}: no listings need detail fetch")
        return stats

    print(f"[detail] {listing_type}: fetching {len(rows)} detail pages...")

    for i, (listing_id, url) in enumerate(rows, 1):
        if not url:
            stats["errors"] += 1
            continue

        try:
            html = client.fetch(url)
            detail = parse_detail_page(html)

            with get_suumo_session() as session:
                obj = session.query(Model).get(listing_id)
                if not obj:
                    continue

                # Common detail fields
                obj.images = detail.get("images") or None
                obj.description = detail.get("description")
                obj.detail_fields = detail.get("detail_fields") or None
                obj.detail_fetched_at = _now()

                # Supplement structure if missing
                if not obj.structure and detail.get("structure"):
                    obj.structure = detail["structure"]

                # Supplement building year if missing
                if not obj.building_year and detail.get("building_year"):
                    obj.building_year = detail["building_year"]
                    obj.building_age = detail.get("building_age")

                # Type-specific detail fields
                if listing_type == "mansion":
                    _apply_mansion_details(obj, detail)
                elif listing_type == "kodate":
                    _apply_kodate_details(obj, detail)

            stats["fetched"] += 1

        except Exception as e:
            print(f"[detail] Error for {url}: {e}")
            stats["errors"] += 1

        if i % 50 == 0:
            print(f"[detail] {listing_type}: {i}/{len(rows)} "
                  f"(fetched={stats['fetched']} errors={stats['errors']})")

    print(f"[detail] {listing_type}: done — "
          f"fetched={stats['fetched']} errors={stats['errors']}")
    return stats


def _apply_mansion_details(obj, detail):
    """Applies mansion-specific fields from detail page data."""
    if detail.get("management_fee") and not obj.management_fee:
        obj.management_fee = detail["management_fee"]
    if detail.get("repair_reserve") and not obj.repair_reserve:
        obj.repair_reserve = detail["repair_reserve"]
    if detail.get("total_units") and not obj.total_units:
        obj.total_units = detail["total_units"]
    if detail.get("direction") and not obj.direction:
        obj.direction = detail["direction"]
    if detail.get("floor_location") and not obj.floor_location:
        obj.floor_location = detail["floor_location"]
    if detail.get("total_floors") and not obj.total_floors:
        obj.total_floors = detail["total_floors"]
    if detail.get("area_sqm") and not obj.exclusive_area_sqm:
        obj.exclusive_area_sqm = detail["area_sqm"]
    if detail.get("building_name"):
        obj.building_name = detail.get("building_name") or obj.building_name


def _apply_kodate_details(obj, detail):
    """Applies kodate-specific fields from detail page data."""
    if detail.get("land_rights") and not obj.land_rights:
        obj.land_rights = detail["land_rights"]
    if detail.get("road_access") and not obj.road_access:
        obj.road_access = detail["road_access"]
    if detail.get("building_coverage") and not obj.building_coverage:
        obj.building_coverage = detail["building_coverage"]
    if detail.get("floor_area_ratio") and not obj.floor_area_ratio:
        obj.floor_area_ratio = detail["floor_area_ratio"]
    if detail.get("zoning") and not obj.zoning:
        obj.zoning = detail["zoning"]
    if detail.get("total_floors") and not obj.total_floors:
        obj.total_floors = detail["total_floors"]
    if detail.get("area_sqm") and not obj.building_area_sqm:
        obj.building_area_sqm = detail["area_sqm"]
    if detail.get("land_area_sqm") and not obj.land_area_sqm:
        obj.land_area_sqm = detail["land_area_sqm"]


def _save_log(cycle_id, prefecture, listing_type, is_new, stats, started_at, status):
    """Writes a CrawlLog entry."""
    pref_names = {13: "東京都", 14: "神奈川県", 11: "埼玉県", 12: "千葉県"}
    with get_suumo_session() as session:
        session.add(CrawlLog(
            crawl_cycle_id=cycle_id,
            prefecture=pref_names.get(prefecture, str(prefecture)),
            listing_type=listing_type,
            is_new=is_new,
            total_pages=stats.get("total_pages", 0),
            total_items=stats.get("total_items", 0),
            new_listings=stats.get("new", 0),
            updated_listings=stats.get("updated", 0),
            duplicates_skipped=stats.get("duplicates", 0),
            errors=stats.get("errors", 0),
            started_at=started_at,
            finished_at=_now(),
            status=status,
        ))


# -- Full crawl ----------------------------------------------------------------

def run_full_crawl(max_pages=0, max_items=0, skip_details=False):
    """Runs a complete weekly crawl cycle.

    1. Start new cycle
    2. Phase 1: Crawl list pages for all (prefecture, type) combos
    3. Phase 2: Crawl detail pages for new/updated listings
    4. Phase 3: Purge stale listings from previous cycle
    5. Mark cycle completed
    """
    cfg = get_config().get("suumo", {})
    prefectures = cfg.get("prefectures", [13])
    listing_types = cfg.get("listing_types", ["mansion", "kodate"])
    include_new = cfg.get("include_new", True)

    cycle_id = start_cycle()
    total_stats = {"new": 0, "updated": 0, "duplicates": 0, "errors": 0}

    # Phase 1: List pages
    print("\n[crawl] === Phase 1: List Pages ===")
    for pref in prefectures:
        for ltype in listing_types:
            # 中古
            s = crawl_query(cycle_id, pref, ltype, is_new=False,
                            max_pages=max_pages, max_items=max_items)
            for k in total_stats:
                total_stats[k] += s.get(k, 0)

            # 新築
            if include_new and (ltype, True) in BS_CODES:
                s = crawl_query(cycle_id, pref, ltype, is_new=True,
                                max_pages=max_pages, max_items=max_items)
                for k in total_stats:
                    total_stats[k] += s.get(k, 0)

    # Phase 2: Detail pages
    if not skip_details:
        print("\n[crawl] === Phase 2: Detail Pages ===")
        detail_stats = {"fetched": 0, "errors": 0}
        for ltype in listing_types:
            ds = crawl_details_for_type(cycle_id, ltype)
            detail_stats["fetched"] += ds.get("fetched", 0)
            detail_stats["errors"] += ds.get("errors", 0)
        total_stats["details_fetched"] = detail_stats["fetched"]
        total_stats["detail_errors"] = detail_stats["errors"]

    # Phase 3: Purge + finish
    print("\n[crawl] === Phase 3: Cleanup ===")
    status = "completed" if total_stats["errors"] == 0 else "partial"
    finish_cycle(cycle_id, status=status, stats=total_stats, purge=True)

    print(f"\n[crawl] === Cycle #{cycle_id} Summary ===")
    for k, v in total_stats.items():
        print(f"  {k}: {v}")


def run_details_only():
    """Fetches detail pages for existing listings without re-crawling list pages.

    Uses the most recent completed cycle to find listings needing detail data.
    """
    cfg = get_config().get("suumo", {})
    listing_types = cfg.get("listing_types", ["mansion", "kodate"])

    # Find the latest completed cycle
    with get_suumo_session() as session:
        cycle = (
            session.query(CrawlCycle)
            .filter(CrawlCycle.status.in_(["completed", "partial"]))
            .order_by(CrawlCycle.id.desc())
            .first()
        )
        if not cycle:
            print("[detail] No completed cycle found. Run a full crawl first.")
            return
        cycle_id = cycle.id

    print(f"[detail] Using cycle #{cycle_id}")
    for ltype in listing_types:
        crawl_details_for_type(cycle_id, ltype)


if __name__ == "__main__":
    from src.settings import load_config
    load_config()
    from src.models import init_suumo_db
    init_suumo_db()
    run_full_crawl()
