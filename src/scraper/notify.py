"""Telegram notification module for crawl status updates.

Sends messages via Telegram Bot API. Configure bot_token and chat_id
in configs/default.yaml under suumo.telegram.

All methods are safe to call even if not configured (no-op).
"""

from datetime import datetime, timezone, timedelta

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


_JST = timezone(timedelta(hours=9))

def _now_str():
    return datetime.now(_JST).strftime("%Y-%m-%d %H:%M JST")


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


_PREF_NAMES = {13: "東京", 14: "神奈川", 11: "埼玉", 12: "千葉"}


def crawl_prefecture_done(cycle_id, prefecture, pref_stats, total_stats):
    """Sent after each prefecture completes."""
    pref_name = _PREF_NAMES.get(prefecture, str(prefecture))
    _send(
        f"📊 Cycle #{cycle_id} — *{pref_name}* done\n"
        f"  this: new={pref_stats.get('new', 0)} "
        f"upd={pref_stats.get('updated', 0)} "
        f"dup={pref_stats.get('duplicates', 0)}\n"
        f"  total: new={total_stats.get('new', 0)} "
        f"upd={total_stats.get('updated', 0)} "
        f"err={total_stats.get('errors', 0)}"
    )


def crawl_completed(cycle_id, status, stats, delisted=0):
    lines = [
        f"new={stats.get('new', 0)}",
        f"updated={stats.get('updated', 0)}",
        f"delisted={delisted}",
    ]
    if stats.get("details_fetched"):
        lines.append(f"detail pages={stats['details_fetched']}")
    if stats.get("errors", 0) > 0:
        lines.append(f"errors={stats['errors']}")

    icon = "✅" if status == "completed" else "⚠️"
    _send(
        f"{icon} *Crawl {status}* — cycle #{cycle_id}\n"
        f"{', '.join(lines)}\n"
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
