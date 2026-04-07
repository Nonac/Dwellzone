"""Telegram notification module for crawl status updates.

Sends messages via Telegram Bot API. Configure bot_token and chat_id
in configs/default.yaml under suumo.telegram.

All methods are safe to call even if not configured (no-op).
"""

from datetime import datetime, timezone

import requests

from src.settings import get_config


def _send(text):
    cfg = get_config().get("suumo", {}).get("telegram", {})
    token = cfg.get("bot_token", "")
    chat_id = cfg.get("chat_id", "")
    if not token or not chat_id:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "Markdown",
            },
            timeout=10,
        )
    except Exception:
        pass


def _now_str():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


# -- Lifecycle messages --------------------------------------------------------

def crawl_started(cycle_id, prefectures, listing_types):
    pref_names = {13: "東京", 14: "神奈川", 11: "埼玉", 12: "千葉"}
    prefs = ", ".join(pref_names.get(p, str(p)) for p in prefectures)
    types = ", ".join(listing_types)
    _send(
        f"🕷️ *Crawl started* — cycle #{cycle_id}\n"
        f"Prefectures: {prefs}\n"
        f"Types: {types}\n"
        f"{_now_str()}"
    )


def crawl_progress(cycle_id, stats):
    _send(
        f"📊 Cycle #{cycle_id} progress\n"
        f"new={stats.get('new', 0)} "
        f"updated={stats.get('updated', 0)} "
        f"dup={stats.get('duplicates', 0)} "
        f"errors={stats.get('errors', 0)}"
    )


def crawl_completed(cycle_id, status, stats, delisted=0):
    details = (
        f"new={stats.get('new', 0)}, "
        f"updated={stats.get('updated', 0)}, "
        f"delisted={delisted}"
    )
    if stats.get("details_fetched"):
        details += f", detail_pages={stats['details_fetched']}"

    icon = "✅" if status == "completed" else "⚠️"
    _send(
        f"{icon} *Crawl {status}* — cycle #{cycle_id}\n"
        f"{details}\n"
        f"{_now_str()}"
    )


# -- Alert messages ------------------------------------------------------------

def alert_banned(cycle_id, stats):
    _send(
        f"🚨 *BAN DETECTED* — cycle #{cycle_id} ABORTED\n"
        f"Found {stats.get('new', 0)} listings before ban.\n"
        f"*No existing data was modified.*\n"
        f"Cycle stays running for resume.\n"
        f"{_now_str()}"
    )


def alert_suspicious(cycle_id, found, active, threshold):
    pct = found / active * 100 if active > 0 else 0
    _send(
        f"⚠️ *Suspicious crawl* — cycle #{cycle_id}\n"
        f"Found {found}/{active} listings ({pct:.0f}%)\n"
        f"Threshold: {threshold:.0%}\n"
        f"*Delist skipped to protect data.*\n"
        f"{_now_str()}"
    )


def alert_error(cycle_id, error_msg):
    _send(
        f"❌ *Crawl error* — cycle #{cycle_id}\n"
        f"{error_msg}\n"
        f"{_now_str()}"
    )
