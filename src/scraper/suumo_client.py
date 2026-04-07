"""HTTP client for Suumo with rate limiting and retry.

Both mansion and kodate use unified search endpoint:
  /jj/bukken/ichiran/JJ010FJ001/

bs codes:
  011 = 中古マンション
  012 = 新築マンション
  021 = 中古一戸建て
  022 = 新築一戸建て

Price filter: kb=下限(万円), kt=上限(万円)
"""

import random
import time
from urllib.parse import urlencode

import requests

from src.settings import get_config

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0",
]

# Prefecture code -> slug mapping
PREF_SLUGS = {
    13: "tokyo",
    14: "kanagawa",
    11: "saitama",
    12: "chiba",
}

# bs codes for unified search
BS_CODES = {
    ("mansion", False): "011",   # 中古マンション
    ("mansion", True): "012",    # 新築マンション
    ("kodate", False): "021",    # 中古一戸建て
    ("kodate", True): "022",     # 新築一戸建て
}

# Kept for backward compat check in pipeline
AREA_BASED_PREFIXES = {
    ("kodate", False): "chukoikkodate",
    ("kodate", True): "ikkodate",
}


class SuumoClient:
    """HTTP client for fetching Suumo listing pages."""

    def __init__(self):
        cfg = get_config().get("suumo", {})
        self._delay = cfg.get("request_delay", [2, 5])
        self._session = requests.Session()
        self._session.headers.update({
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "ja,en;q=0.5",
        })

    def fetch(self, url, max_retries=3):
        """Fetches a URL with rate limiting and retry.

        Args:
            url: Target URL (with query string already included).
            max_retries: Max retry attempts.

        Returns:
            Response text (HTML string).
        """
        for attempt in range(max_retries):
            self._session.headers["User-Agent"] = random.choice(_USER_AGENTS)
            delay = random.uniform(self._delay[0], self._delay[1])
            time.sleep(delay)

            try:
                resp = self._session.get(url, timeout=30)
                resp.raise_for_status()
                return resp.text
            except requests.RequestException as e:
                if attempt < max_retries - 1:
                    wait = (attempt + 1) * 5
                    print(f"[suumo] Retry {attempt + 1}/{max_retries} after {wait}s: {e}")
                    time.sleep(wait)
                else:
                    raise

    # -- Unified search (mansion + kodate) -------------------------------------

    def build_search_url(self, prefecture, listing_type, is_new=False, page=1):
        """Returns a full URL for unified search.

        Works for both mansion and kodate.

        Args:
            prefecture: Prefecture code (13, 14, etc).
            listing_type: 'mansion' or 'kodate'.
            is_new: True for 新築.
            page: Page number.

        Returns:
            URL string with query params.
        """
        cfg = get_config().get("suumo", {})
        kb = cfg.get("price_min") or 0
        kt = cfg.get("price_max") or 9999999

        bs = BS_CODES.get((listing_type, is_new), "011")

        params = [
            ("ar", "030"),
            ("bs", bs),
            ("ta", str(prefecture)),
            ("jspIdFlg", "patternShikugun"),
            ("kb", str(kb)),
            ("kt", str(kt)),
        ]

        # Area params differ by type
        if listing_type == "mansion":
            params += [("mb", "0"), ("mt", "9999999")]
        else:
            params += [("tb", "0"), ("tt", "9999999"),
                       ("hb", "0"), ("ht", "9999999")]

        params += [
            ("ekTjCd", ""),
            ("ekTjNm", ""),
            ("tj", "0"),
            ("cnb", "0"),
            ("cn", "9999999"),
            ("srch_navi", "1"),
        ]

        if page > 1:
            params.append(("pn", str(page)))

        base = "https://suumo.jp/jj/bukken/ichiran/JJ010FJ001/"
        return base + "?" + urlencode(params)


def get_supported_types():
    """Returns all supported (listing_type, is_new) combinations."""
    return list(BS_CODES.keys())
