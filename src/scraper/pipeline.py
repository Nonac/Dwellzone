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
    CrawlCycle, Mansion, Kodate, PriceHistory, CrawlLog, GeocodeCache,
    compute_content_hash, get_model_for_type,
)
from src.scraper.suumo_client import SuumoClient, SuumoBannedException, BS_CODES
from src.scraper.parser import parse_listing_page, parse_total_pages, parse_detail_page
from src.scraper.geocoder import geocode_address
from src.scraper import notify
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


_geocode_stats = {"api": 0, "cache": 0, "skip": 0}


def _geocode_cached(session, address):
    """Geocodes an address, using DB cache to avoid repeated API calls.

    Returns:
        (lat, lon, geom_wkt) tuple.
    """
    if not address:
        return None, None, None

    # Check cache first
    cached = session.query(GeocodeCache).filter_by(address=address).first()
    if cached:
        _geocode_stats["cache"] += 1
        lat, lon = cached.latitude, cached.longitude
        if lat and lon:
            return lat, lon, f"SRID=4326;POINT({lon} {lat})"
        return None, None, None

    # Call GSI API
    _geocode_stats["api"] += 1
    lat, lon = geocode_address(address)

    # Store in cache (even if None, to avoid retrying bad addresses)
    session.add(GeocodeCache(address=address, latitude=lat, longitude=lon))
    session.flush()

    if lat and lon:
        return lat, lon, f"SRID=4326;POINT({lon} {lat})"
    return None, None, None


def print_geocode_stats():
    """Prints geocoding statistics."""
    s = _geocode_stats
    print(f"[geocode] api={s['api']} cache={s['cache']} skip_existing={s['skip']}")
    _geocode_stats.update({"api": 0, "cache": 0, "skip": 0})


def _count_active():
    """Returns total active listings across both tables."""
    with get_suumo_session() as session:
        total = 0
        for Model in [Mansion, Kodate]:
            total += session.query(Model).filter(Model.status == "active").count()
        return total


# -- Cycle management ----------------------------------------------------------

def start_cycle():
    """Creates a new CrawlCycle. Used by partial/test crawls."""
    with get_suumo_session() as session:
        cycle = CrawlCycle(started_at=_now(), status="running")
        session.add(cycle)
        session.flush()
        cycle_id = cycle.id
    print(f"[crawl] Started cycle #{cycle_id}")
    return cycle_id


def start_or_resume_cycle():
    """Resumes an interrupted cycle, or creates a new one.

    If a cycle with status='running' exists, it was interrupted.
    Resume it instead of creating a new one, so listings already
    crawled in that cycle keep their cycle_id.

    Returns:
        (cycle_id, resumed) tuple.
    """
    with get_suumo_session() as session:
        existing = (
            session.query(CrawlCycle)
            .filter(CrawlCycle.status == "running")
            .order_by(CrawlCycle.id.desc())
            .first()
        )
        if existing:
            print(f"[crawl] Resuming interrupted cycle #{existing.id}")
            return existing.id, True

        cycle = CrawlCycle(started_at=_now(), status="running")
        session.add(cycle)
        session.flush()
        cycle_id = cycle.id

    print(f"[crawl] Started cycle #{cycle_id}")
    return cycle_id, False


def _get_completed_queries(cycle_id):
    """Returns set of (prefecture, listing_type, is_new) already completed in this cycle."""
    pref_codes = {"東京都": 13, "神奈川県": 14, "埼玉県": 11, "千葉県": 12}
    completed = set()
    with get_suumo_session() as session:
        logs = (
            session.query(CrawlLog)
            .filter(CrawlLog.crawl_cycle_id == cycle_id)
            .filter(CrawlLog.status.in_(["success", "banned"]))
            .all()
        )
        for log in logs:
            pref_code = pref_codes.get(log.prefecture, log.prefecture)
            completed.add((pref_code, log.listing_type, log.is_new))
    return completed


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
        delisted = mark_delisted(cycle_id)
        print(f"[crawl] Cycle #{cycle_id} {status}. Delisted {delisted} stale listings.")
    else:
        print(f"[crawl] Cycle #{cycle_id} {status}.")


def mark_delisted(current_cycle_id):
    """Marks listings not seen in the current cycle as delisted.

    Listings that reappear in a future cycle will be set back to 'active'.

    Returns:
        Number of newly delisted rows.
    """
    now = _now()
    total = 0
    with get_suumo_session() as session:
        for Model in [Mansion, Kodate]:
            count = (
                session.query(Model)
                .filter(Model.crawl_cycle_id != current_cycle_id)
                .filter(Model.status == "active")
                .update({
                    Model.status: "delisted",
                    Model.delisted_at: now,
                }, synchronize_session=False)
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
    except SuumoBannedException as e:
        print(f"[crawl] {label}: BANNED — {e}")
        stats["banned"] = True
        _save_log(cycle_id, prefecture, listing_type, is_new, stats, log_started, "banned")
        return stats
    except Exception as e:
        print(f"[crawl] {label}: failed: {e}")
        _save_log(cycle_id, prefecture, listing_type, is_new, stats, log_started, "failed")
        return stats

    total_pages = parse_total_pages(html)
    if max_pages > 0:
        total_pages = min(total_pages, max_pages)
    stats["total_pages"] = total_pages
    print(f"[crawl] {label}: {total_pages} pages")

    if _process_page(html, cycle_id, prefecture, listing_type, is_new, stats):
        print(f"[crawl] {label}: price ceiling reached, stopping")
        _save_log(cycle_id, prefecture, listing_type, is_new, stats, log_started, "success")
        print(f"[crawl] {label}: done — new={stats['new']} upd={stats['updated']} dup={stats['duplicates']}")
        print_geocode_stats()
        return stats

    for page in range(2, total_pages + 1):
        if max_items > 0 and stats["total_items"] >= max_items:
            print(f"[crawl] {label}: reached max_items={max_items}, stopping")
            break
        url = client.build_search_url(prefecture, listing_type, is_new, page=page)
        try:
            html = client.fetch(url)
            if _process_page(html, cycle_id, prefecture, listing_type, is_new, stats):
                print(f"[crawl] {label}: price ceiling reached at page {page}, stopping")
                break
        except SuumoBannedException as e:
            print(f"[crawl] {label}: BANNED mid-crawl — {e}")
            stats["banned"] = True
            break
        except Exception as e:
            print(f"[crawl] {label}: page {page} error: {e}")
            stats["errors"] += 1
        if page % 10 == 0:
            print(f"[crawl] {label}: page {page}/{total_pages}")

    _save_log(cycle_id, prefecture, listing_type, is_new, stats, log_started, "success")
    print(f"[crawl] {label}: done — new={stats['new']} upd={stats['updated']} dup={stats['duplicates']}")
    print_geocode_stats()
    return stats


def _process_page(html, cycle_id, prefecture, listing_type, is_new, stats):
    """Parses one page and upserts listings into the correct table.

    Returns:
        True if price ceiling was hit (all remaining pages can be skipped).
    """
    cfg = get_config().get("suumo", {})
    price_ceiling = cfg.get("price_ceiling") or 0  # 万円, 0 = no limit

    items = parse_listing_page(html)
    stats["total_items"] += len(items)
    Model = get_model_for_type(listing_type)
    hit_ceiling = False

    with get_suumo_session() as session:
        for item in items:
            suumo_id = item.get("suumo_id")
            if not suumo_id:
                continue

            price = item.get("price")

            # Results are price-ascending; if over ceiling, skip rest
            if price_ceiling and price and price > price_ceiling:
                hit_ceiling = True
                continue

            pref, city, town = _parse_address(item.get("address", ""), prefecture)
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

            addr = item.get("address", "")
            now = _now()

            if existing:
                # Existing listing: reuse coordinates, skip geocoding
                _geocode_stats["skip"] += 1

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
                # Reactivate if previously delisted
                if existing.status == "delisted":
                    existing.status = "active"
                    existing.delisted_at = None
                # Reset detail_fetched_at so detail page is re-crawled
                existing.detail_fetched_at = None
                stats["updated"] += 1
            else:
                # New listing: geocode with cache
                lat, lon, geom_wkt = _geocode_cached(session, addr)

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

    return hit_ceiling


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
    """Runs a complete weekly crawl cycle with resume support.

    If a previous cycle was interrupted (status='running'), it resumes
    from where it left off. Already-completed (prefecture, type, is_new)
    combos are skipped based on crawl_logs.

    1. Start or resume cycle
    2. Phase 1: Crawl list pages (skip completed combos)
    3. Phase 2: Crawl detail pages (skip already-fetched)
    4. Phase 3: Safety check + mark delisted
    """
    cfg = get_config().get("suumo", {})
    prefectures = cfg.get("prefectures", [13])
    listing_types = cfg.get("listing_types", ["mansion", "kodate"])
    include_new = cfg.get("include_new", True)

    cycle_id, resumed = start_or_resume_cycle()

    if resumed:
        completed = _get_completed_queries(cycle_id)
        print(f"[crawl] Resuming: {len(completed)} queries already done")
        notify.crawl_started(cycle_id, prefectures, listing_types)
    else:
        completed = set()
        notify.crawl_started(cycle_id, prefectures, listing_types)

    total_stats = {"new": 0, "updated": 0, "duplicates": 0, "errors": 0}
    banned = False

    # Phase 1: List pages
    print("\n[crawl] === Phase 1: List Pages ===")
    for pref in prefectures:
        pref_stats = {"new": 0, "updated": 0, "duplicates": 0, "errors": 0}

        for ltype in listing_types:
            # 中古
            if (pref, ltype, False) not in completed:
                s = crawl_query(cycle_id, pref, ltype, is_new=False,
                                max_pages=max_pages, max_items=max_items)
                for k in total_stats:
                    total_stats[k] += s.get(k, 0)
                    pref_stats[k] += s.get(k, 0)

                if s.get("banned"):
                    banned = True
                    break
            else:
                print(f"[crawl] pref={pref} {ltype}: skipped (already done)")

            # 新築
            if include_new and (ltype, True) in BS_CODES:
                if (pref, ltype, True) not in completed:
                    s = crawl_query(cycle_id, pref, ltype, is_new=True,
                                    max_pages=max_pages, max_items=max_items)
                    for k in total_stats:
                        total_stats[k] += s.get(k, 0)
                        pref_stats[k] += s.get(k, 0)

                    if s.get("banned"):
                        banned = True
                        break
                else:
                    print(f"[crawl] pref={pref} new_{ltype}: skipped (already done)")

        if banned:
            break

        # Progress update after each prefecture
        notify.crawl_prefecture_done(cycle_id, pref, pref_stats, total_stats)

    # Ban detected → keep cycle as 'running' so next attempt resumes
    if banned:
        print("\n[crawl] === ABORT: Suumo ban detected ===")
        print("[crawl] Cycle stays 'running' for resume on next attempt.")
        notify.alert_banned(cycle_id, total_stats)
        return

    # Phase 2: Detail pages (detail_fetched_at IS NULL = natural resume)
    if not skip_details:
        print("\n[crawl] === Phase 2: Detail Pages ===")
        detail_stats = {"fetched": 0, "errors": 0}
        for ltype in listing_types:
            ds = crawl_details_for_type(cycle_id, ltype)
            detail_stats["fetched"] += ds.get("fetched", 0)
            detail_stats["errors"] += ds.get("errors", 0)
        total_stats["details_fetched"] = detail_stats["fetched"]
        total_stats["detail_errors"] = detail_stats["errors"]

    # Phase 3: Safety check + finish
    print("\n[crawl] === Phase 3: Cleanup ===")
    status = "completed" if total_stats["errors"] == 0 else "partial"

    active_count = _count_active()
    found_count = total_stats["new"] + total_stats["updated"]
    threshold = cfg.get("safety_threshold", 0.5)

    # On resume, found_count only reflects THIS run. Add listings already
    # in this cycle from previous runs.
    if resumed:
        with get_suumo_session() as session:
            for Model in [Mansion, Kodate]:
                found_count += (
                    session.query(Model)
                    .filter(Model.crawl_cycle_id == cycle_id)
                    .count()
                )

    if active_count > 0 and found_count < active_count * threshold:
        print(f"[crawl] SAFETY: found {found_count}/{active_count} listings "
              f"(< {threshold:.0%}), skipping delist")
        finish_cycle(cycle_id, status="suspicious", stats=total_stats, purge=False)
        notify.alert_suspicious(cycle_id, found_count, active_count, threshold)
    else:
        finish_cycle(cycle_id, status=status, stats=total_stats, purge=True)
        # Count delisted
        delisted = 0
        with get_suumo_session() as session:
            for Model in [Mansion, Kodate]:
                delisted += (
                    session.query(Model)
                    .filter(Model.status == "delisted",
                            Model.crawl_cycle_id != cycle_id)
                    .count()
                )
        notify.crawl_completed(cycle_id, status, total_stats, delisted)

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
