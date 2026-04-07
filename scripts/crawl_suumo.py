#!/usr/bin/env python
"""Suumo real estate listing crawler.

Usage:
    python scripts/crawl_suumo.py                          # Full weekly crawl
    python scripts/crawl_suumo.py --prefecture 13           # Tokyo only
    python scripts/crawl_suumo.py --type mansion            # Mansions only
    python scripts/crawl_suumo.py --max-pages 2             # Test run (2 pages)
    python scripts/crawl_suumo.py --skip-details            # List pages only, no detail fetch
    python scripts/crawl_suumo.py --details-only            # Fetch detail pages for existing listings
    python scripts/crawl_suumo.py --status                  # Show crawl history
    python scripts/crawl_suumo.py --init-db                 # Create tables
"""

import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.settings import load_config


def main():
    p = argparse.ArgumentParser(description="Suumo listing crawler")
    p.add_argument("--config", default=None, help="YAML config file path")
    p.add_argument("--prefecture", type=int, default=None,
                   help="Prefecture code (13=Tokyo, 14=Kanagawa, 11=Saitama, 12=Chiba)")
    p.add_argument("--type", default=None, choices=["mansion", "kodate"],
                   help="Listing type filter")
    p.add_argument("--max-pages", type=int, default=0,
                   help="Max pages per query (0=unlimited)")
    p.add_argument("--max-items", type=int, default=0,
                   help="Max items per (type, is_new) combo (0=unlimited)")
    p.add_argument("--skip-details", action="store_true",
                   help="Skip detail page fetching (list pages only)")
    p.add_argument("--details-only", action="store_true",
                   help="Only fetch detail pages for existing listings (no list crawl)")
    p.add_argument("--status", action="store_true", help="Show recent crawl status")
    p.add_argument("--init-db", action="store_true", help="Initialize suumo database tables")
    args = p.parse_args()

    load_config(args.config)

    if args.init_db:
        from src.models import init_suumo_db
        init_suumo_db()
        return

    if args.status:
        _show_status()
        return

    # Ensure tables exist
    from src.models import init_suumo_db
    init_suumo_db()

    if args.details_only:
        from src.scraper.pipeline import run_details_only
        run_details_only()
        return

    if args.prefecture or args.type:
        from src.scraper.pipeline import (
            start_cycle, finish_cycle, crawl_query, crawl_details_for_type,
        )
        from src.scraper.suumo_client import BS_CODES
        from src.scraper import notify

        cfg = load_config(args.config)
        suumo_cfg = cfg.get("suumo", {})
        prefectures = [args.prefecture] if args.prefecture else suumo_cfg.get("prefectures", [13])
        types = [args.type] if args.type else suumo_cfg.get("listing_types", ["mansion", "kodate"])
        include_new = suumo_cfg.get("include_new", True)

        cycle_id = start_cycle()
        notify.crawl_started(cycle_id, prefectures, types)

        total_stats = {"new": 0, "updated": 0, "duplicates": 0, "errors": 0}
        banned = False
        for pref in prefectures:
            for ltype in types:
                s = crawl_query(cycle_id, pref, ltype, is_new=False,
                                max_pages=args.max_pages, max_items=args.max_items)
                for k in total_stats:
                    total_stats[k] += s.get(k, 0)
                if s.get("banned"):
                    banned = True
                    break

                if not banned and include_new and (ltype, True) in BS_CODES:
                    s = crawl_query(cycle_id, pref, ltype, is_new=True,
                                    max_pages=args.max_pages, max_items=args.max_items)
                    for k in total_stats:
                        total_stats[k] += s.get(k, 0)
                    if s.get("banned"):
                        banned = True
                        break
            if banned:
                break

        if banned:
            notify.alert_banned(cycle_id, total_stats)
        else:
            if not args.skip_details:
                for ltype in types:
                    crawl_details_for_type(cycle_id, ltype)
            finish_cycle(cycle_id)
            notify.crawl_completed(cycle_id, "completed", total_stats)
    else:
        from src.scraper.pipeline import run_full_crawl
        run_full_crawl(max_pages=args.max_pages, max_items=args.max_items,
                       skip_details=args.skip_details)


def _show_status():
    from src.db import get_suumo_session
    from src.models.suumo import CrawlCycle, Mansion, Kodate

    with get_suumo_session() as session:
        # Recent cycles
        cycles = (session.query(CrawlCycle)
                  .order_by(CrawlCycle.id.desc())
                  .limit(5)
                  .all())

        if cycles:
            print("\n=== Recent Crawl Cycles ===")
            for c in cycles:
                duration = ""
                if c.finished_at and c.started_at:
                    dt = (c.finished_at - c.started_at).total_seconds()
                    duration = f" ({dt/60:.0f}min)"
                print(f"  #{c.id} {c.started_at} {c.status}{duration}")
                if c.stats:
                    print(f"       {c.stats}")
        else:
            print("  No crawl cycles yet.")

        # Current listing counts per table
        mansion_count = session.query(Mansion).count()
        kodate_count = session.query(Kodate).count()

        # Detail coverage
        mansion_with_detail = session.query(Mansion).filter(
            Mansion.detail_fetched_at.isnot(None)).count()
        kodate_with_detail = session.query(Kodate).filter(
            Kodate.detail_fetched_at.isnot(None)).count()

        print(f"\n=== Listings ===")
        print(f"  Mansions: {mansion_count} (detail: {mansion_with_detail})")
        print(f"  Kodates:  {kodate_count} (detail: {kodate_with_detail})")
        print(f"  Total:    {mansion_count + kodate_count}")


if __name__ == "__main__":
    main()
