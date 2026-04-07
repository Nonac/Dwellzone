"""HTML parser for Suumo listing pages.

Suumo listing page structure (search results):
  .property_unit                    — one per listing
    h2.property_unit-title a        — title + detail URL (contains nc_XXXXX)
    .dottable--cassette             — key-value table
      .dottable-line dl             — each <dt>=label, <dd>=value
        物件名, 販売価格, 所在地, 沿線・駅,
        専有面積, 間取り, バルコニー, 築年月,
        土地面積, 建物面積, etc.

Suumo detail page structure:
  Property images in gallery area
  Detail tables with <th>/<td> pairs
  Description/comment sections
"""

import re

from bs4 import BeautifulSoup


# ==============================================================================
# List page parsing
# ==============================================================================

def parse_total_pages(html):
    """Extracts total page count from pagination.

    Args:
        html: Raw HTML string.

    Returns:
        Total page count (int).
    """
    soup = BeautifulSoup(html, "lxml")
    pagination = soup.select_one(".pagination_set")
    if not pagination:
        return 1

    max_page = 1
    for link in pagination.select("a"):
        text = link.get_text(strip=True)
        if text.isdigit():
            max_page = max(max_page, int(text))
    return max_page


def parse_listing_page(html):
    """Parses all listings from a search results page.

    Args:
        html: Raw HTML string from Suumo.

    Returns:
        List of dicts with extracted fields.
    """
    soup = BeautifulSoup(html, "lxml")
    listings = []

    for item in soup.select(".property_unit"):
        try:
            data = _parse_item(item)
            if data and data.get("suumo_id"):
                listings.append(data)
        except Exception as e:
            print(f"[parser] Error: {e}")

    return listings


def _parse_item(item):
    """Extracts all fields from one .property_unit element."""
    data = {}

    # -- Title & URL & suumo_id --
    link = item.select_one("h2.property_unit-title a")
    if link:
        href = link.get("href", "")
        data["title"] = link.get_text(strip=True)
        data["url"] = "https://suumo.jp" + href if href.startswith("/") else href
        m = re.search(r"nc_(\d+)", href)
        if m:
            data["suumo_id"] = "nc_" + m.group(1)

    if not data.get("suumo_id"):
        return None

    # -- Thumbnail image --
    img = item.select_one("img.property_unit-thumbnail, img.js-noContextMenu")
    if img:
        src = img.get("data-src") or img.get("src") or ""
        if src and not src.endswith("noimage.gif"):
            data["thumbnail"] = src

    # -- Key-value pairs from dottable --
    fields = {}
    dottable = item.select_one(".dottable--cassette")
    if dottable:
        seen_keys = set()
        for dl in dottable.select("dl"):
            dt = dl.select_one("dt")
            dd = dl.select_one("dd")
            if dt and dd:
                key = dt.get_text(strip=True)
                val = dd.get_text(strip=True)
                if key and key not in seen_keys:
                    fields[key] = val
                    seen_keys.add(key)

    # -- Map fields to data dict --

    # 物件名 (building/property name)
    if "物件名" in fields:
        data["building_name"] = fields["物件名"]

    # 販売価格
    if "販売価格" in fields:
        data["price"] = _parse_price(fields["販売価格"])

    # 所在地
    if "所在地" in fields:
        data["address"] = fields["所在地"]

    # 沿線・駅
    if "沿線・駅" in fields:
        data["station_access"] = _parse_station_access(fields["沿線・駅"])

    # 専有面積 (mansion)
    if "専有面積" in fields:
        data["area_sqm"] = _parse_area(fields["専有面積"])

    # 建物面積 (kodate)
    if "建物面積" in fields:
        data["area_sqm"] = data.get("area_sqm") or _parse_area(fields["建物面積"])

    # 土地面積
    if "土地面積" in fields:
        data["land_area_sqm"] = _parse_area(fields["土地面積"])

    # 間取り
    if "間取り" in fields:
        data["floor_plan"] = fields["間取り"]

    # 築年月
    if "築年月" in fields:
        year, age = _parse_building_year(fields["築年月"])
        if year:
            data["building_year"] = year
        if age is not None:
            data["building_age"] = age

    # バルコニー
    if "バルコニー" in fields:
        data["balcony_sqm"] = _parse_area(fields["バルコニー"])

    # 階数/階建 (for mansions: 所在階/構造)
    for key in ["階", "所在階", "構造・階建て"]:
        if key in fields:
            floors_info = fields[key]
            data["floors"] = floors_info
            structure = _parse_structure(floors_info)
            if structure:
                data["structure"] = structure

    # Store all raw fields for debugging
    data["raw_fields"] = fields

    return data


# ==============================================================================
# Detail page parsing
# ==============================================================================

def parse_detail_page(html):
    """Parses a Suumo property detail page.

    Extracts images, all key-value detail fields, and description text.
    Fields are stored both as a raw dict (detail_fields) and as typed
    extractions for known fields.

    Args:
        html: Raw HTML string from a Suumo detail page.

    Returns:
        dict with keys:
        - images: list of image URL strings
        - description: property description text
        - detail_fields: dict of all key-value pairs from detail tables
        - Plus extracted typed fields (management_fee, repair_reserve, etc.)
    """
    soup = BeautifulSoup(html, "lxml")
    data = {}

    # -- Images --
    data["images"] = _extract_images(soup)

    # -- Detail key-value fields --
    detail_fields = _extract_detail_fields(soup)
    data["detail_fields"] = detail_fields

    # -- Description --
    data["description"] = _extract_description(soup)

    # -- Parse typed fields from detail_fields --
    _extract_typed_fields(data, detail_fields)

    return data


def _extract_images(soup):
    """Extracts property photo URLs from the detail page.

    Suumo detail pages have two image sections:
    - div.w220.h165: Property photos (interior, exterior, equipment)
    - div.w296.h222: Neighborhood photos (stations, schools, shops) — excluded

    Each image has an alt attribute describing the photo (e.g. "リビング", "外観").
    We return a list of {url, alt, caption} dicts.
    """
    images = []
    seen = set()

    # Property photos are in div.w220.h165 containers
    for div in soup.select("div.w220.h165"):
        img = div.select_one("img")
        if not img:
            continue
        # URL can be in src, data-src, or rel (JS lazy-loaded)
        src = img.get("data-src") or img.get("src") or img.get("rel") or ""
        if "img01.suumo.com" not in src or "bukken" not in src:
            continue

        large_url = _to_large_image_url(src)
        if large_url in seen:
            continue
        seen.add(large_url)

        alt = img.get("alt", "")
        # Caption is in the parent's text
        caption_el = div.parent
        caption = ""
        if caption_el:
            # Get text excluding the img alt
            caption = caption_el.get_text(strip=True)
            # Remove the alt text prefix if present
            if alt and caption.startswith(alt):
                caption = caption[len(alt):].strip()

        # Skip promo/advertisement images
        if alt == "プレゼント":
            continue

        images.append({
            "url": large_url,
            "alt": alt,
            "caption": caption[:200] if caption else "",
        })

    return images


def _to_large_image_url(url):
    """Converts a Suumo thumbnail URL to a larger version.

    Input:  https://img01.suumo.com/jj/resizeImage?src=gazo%2Fbukken%2F...jpg&w=96&h=72
    Output: https://img01.suumo.com/jj/resizeImage?src=gazo%2Fbukken%2F...jpg&w=640&h=480
    """
    if "resizeImage" not in url:
        return url

    # Replace w= and h= params with larger values
    url = re.sub(r"[&?]w=\d+", "&w=640", url)
    url = re.sub(r"[&?]h=\d+", "&h=480", url)
    return url


def _extract_detail_fields(soup):
    """Extracts property key-value pairs from detail tables.

    Suumo detail pages use "ヒント" suffix on <th> for property fields
    (e.g. "価格ヒント", "管理費ヒント"). We strip the suffix and only
    keep fields that are actual property attributes, filtering out
    navigation, forms, and support/company info.
    """
    fields = {}

    # Property-relevant th keywords (with or without ヒント suffix)
    _PROPERTY_KEYS = {
        "物件名", "所在地", "住所", "交通",
        # Pricing
        "価格", "販売価格", "最多価格帯", "管理費", "管理費等",
        "修繕積立金", "修繕積立基金", "諸費用",
        # Area / layout
        "専有面積", "建物面積", "土地面積", "その他面積", "敷地面積",
        "間取り", "バルコニー",
        # Structure
        "所在階", "所在階/構造・階建", "構造・階建て", "構造・工法", "階建て",
        "完成時期（築年月）", "完成時期(築年月)", "築年月",
        # Direction / energy
        "向き", "エネルギー消費性能", "断熱性能", "目安光熱費",
        # Sales info
        "販売戸数", "総戸数", "引渡可能時期", "販売スケジュール",
        # Land / zoning
        "土地の権利形態", "敷地の権利形態", "土地権利", "用途地域",
        "建ぺい率・容積率", "建ぺい率", "容積率",
        "私道負担・道路", "接道状況", "地目", "都市計画",
        # Other
        "駐車場", "施工", "リフォーム", "情報提供日",
    }

    for table in soup.select("table"):
        for row in table.select("tr"):
            ths = row.select("th")
            tds = row.select("td")
            # Handle multi-column rows (multiple th/td pairs per row)
            for th, td in zip(ths, tds):
                raw_key = th.get_text(strip=True)
                val = td.get_text(strip=True)
                if not raw_key or not val or val == "-":
                    continue

                # Strip "ヒント" suffix
                key = raw_key.replace("ヒント", "").strip()

                if key in _PROPERTY_KEYS:
                    if key not in fields:
                        fields[key] = val

    return fields


def _extract_description(soup):
    """Extracts property description/comment text."""
    parts = []

    for selector in [
        ".property_view_note",
        ".property_view_detail-comment",
        '[class*="description"]',
        '[class*="comment"]',
        ".informationComment",
    ]:
        for el in soup.select(selector):
            text = el.get_text(strip=True)
            if text and len(text) > 10:
                parts.append(text)

    return "\n".join(parts) if parts else None


def _extract_typed_fields(data, fields):
    """Extracts known typed fields from detail_fields dict.

    Keys are already cleaned (ヒント suffix stripped) by _extract_detail_fields.
    """
    # -- Mansion fields --

    # 管理費 (management fee)
    if "管理費" in fields:
        data["management_fee"] = _parse_monthly_fee(fields["管理費"])

    # 修繕積立金 (repair reserve)
    if "修繕積立金" in fields:
        data["repair_reserve"] = _parse_monthly_fee(fields["修繕積立金"])

    # 総戸数 (total units)
    if "総戸数" in fields:
        data["total_units"] = _parse_integer(fields["総戸数"])

    # 向き (direction/orientation)
    if "向き" in fields:
        data["direction"] = fields["向き"]

    # 所在階 (floor location for mansion)
    for key in ["所在階", "所在階/構造・階建"]:
        if key in fields:
            data["floor_location"] = fields[key]
            break

    # 構造・階建て (structure + total floors)
    for key in ["構造・階建て", "構造・工法", "所在階/構造・階建"]:
        if key in fields:
            val = fields[key]
            data["total_floors"] = val
            structure = _parse_structure(val)
            if structure:
                data["structure"] = structure
            break

    # -- Kodate fields --

    # 土地権利
    for key in ["土地の権利形態", "敷地の権利形態", "土地権利"]:
        if key in fields:
            data["land_rights"] = fields[key]
            break

    # 接道状況 / 私道負担・道路
    for key in ["接道状況", "私道負担・道路"]:
        if key in fields:
            data["road_access"] = fields[key]
            break

    # 建ぺい率・容積率 (often combined as "60％・200％")
    if "建ぺい率・容積率" in fields:
        val = fields["建ぺい率・容積率"]
        parts = re.split(r"[・/]", val)
        if parts:
            data["building_coverage"] = parts[0].strip()
        if len(parts) > 1:
            data["floor_area_ratio"] = parts[1].strip()
    else:
        if "建ぺい率" in fields:
            data["building_coverage"] = fields["建ぺい率"]
        if "容積率" in fields:
            data["floor_area_ratio"] = fields["容積率"]

    # 用途地域
    if "用途地域" in fields:
        data["zoning"] = fields["用途地域"]

    # -- Common fields (supplement from detail page) --

    # 建物面積 / 専有面積
    for key in ["専有面積", "建物面積"]:
        if key in fields and "area_sqm" not in data:
            data["area_sqm"] = _parse_area(fields[key])

    # その他面積 (balcony etc)
    if "その他面積" in fields:
        data["balcony_area_text"] = fields["その他面積"]
        sqm = _parse_area(fields["その他面積"])
        if sqm:
            data["balcony_area_sqm"] = sqm

    # 土地面積
    if "土地面積" in fields and "land_area_sqm" not in data:
        data["land_area_sqm"] = _parse_area(fields["土地面積"])

    # 築年月 / 完成時期
    for key in ["完成時期（築年月）", "完成時期(築年月)", "築年月"]:
        if key in fields:
            year, age = _parse_building_year(fields[key])
            if year and "building_year" not in data:
                data["building_year"] = year
            if age is not None and "building_age" not in data:
                data["building_age"] = age
            break

    # 構造 (standalone, if not already set from composite field)
    for key in ["構造・工法", "構造・階建て"]:
        if key in fields and "structure" not in data:
            data["structure"] = _parse_structure(fields[key]) or fields[key]

    # 駐車場
    if "駐車場" in fields:
        data["parking"] = fields["駐車場"]


# ==============================================================================
# Field parsers
# ==============================================================================

def _parse_price(text):
    """Parses '1280万円' or '1億2000万円' to int (万円).

    For price ranges like '1億7120万円〜2億300万円', takes the lower value.
    """
    text = text.replace(",", "").replace("　", "").replace(" ", "")
    # Handle price ranges: take lower value
    if "〜" in text or "～" in text:
        text = re.split(r"[〜～]", text)[0]

    total = 0
    m_oku = re.search(r"(\d+)億", text)
    m_man = re.search(r"(\d+)万", text)
    if m_oku:
        total += int(m_oku.group(1)) * 10000
    if m_man:
        total += int(m_man.group(1))
    return total if total > 0 else None


def _parse_area(text):
    """Extracts area m² from '68.5m2（20.72坪）（壁芯）'."""
    m = re.search(r"([\d.]+)\s*m", text)
    if m:
        return float(m.group(1))
    return None


def _parse_building_year(text):
    """Parses '1986年8月' or '2025年3月（築1年）' or '2025年9月予定'.

    Returns:
        (year, age) tuple. age may be None.
    """
    year = None
    age = None

    m_year = re.search(r"(\d{4})年", text)
    if m_year:
        year = int(m_year.group(1))
        from datetime import datetime
        age = datetime.now().year - year

    if "新築" in text:
        age = 0

    m_age = re.search(r"築(\d+)年", text)
    if m_age:
        age = int(m_age.group(1))

    return year, age


def _parse_station_access(text):
    """Parses 'ＪＲ青梅線「河辺」徒歩9分' into structured data.

    Handles multiple stations separated by various delimiters.

    Returns:
        List of {line, station, walk_min} dicts.
    """
    results = []

    # Normalize fullwidth chars
    text = text.replace("（", "(").replace("）", ")").replace("　", " ")

    # Pattern: LINE「STATION」徒歩N分 or LINE「STATION」バスN分
    for m in re.finditer(
        r"([^「」\s/／]+)[「]([^」]+)[」]\s*(?:徒歩|バス)?(\d+)分",
        text,
    ):
        results.append({
            "line": m.group(1),
            "station": m.group(2),
            "walk_min": int(m.group(3)),
        })

    # Fallback: LINE/STATION駅 徒歩N分
    if not results:
        for m in re.finditer(
            r"([^/／\s]+)[/／]([^駅]+駅?)\s*徒歩(\d+)分",
            text,
        ):
            results.append({
                "line": m.group(1),
                "station": m.group(2),
                "walk_min": int(m.group(3)),
            })

    return results


def _parse_structure(text):
    """Extracts building structure from text like 'RC造10階建'."""
    m = re.search(r"(RC|SRC|S|木|鉄筋|鉄骨)造?", text)
    if m:
        return m.group(0)
    return None


def _parse_monthly_fee(text):
    """Parses '12,500円/月' or '1.25万円' to integer yen."""
    text = text.replace(",", "").replace("　", "").replace(" ", "")

    # 万円 unit
    m = re.search(r"([\d.]+)\s*万", text)
    if m:
        return int(float(m.group(1)) * 10000)

    # 円 unit
    m = re.search(r"([\d]+)\s*円", text)
    if m:
        return int(m.group(1))

    # Just digits
    m = re.search(r"(\d+)", text)
    if m and "不要" not in text and "なし" not in text and "-" not in text:
        return int(m.group(1))

    return None


def _parse_integer(text):
    """Extracts first integer from text like '150戸'."""
    m = re.search(r"(\d+)", text)
    if m:
        return int(m.group(1))
    return None
