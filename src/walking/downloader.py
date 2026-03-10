"""Downloads Kanto region OSM PBF and extracts the Tokyo walking network.

Steps:
    1. Download Geofabrik Kanto PBF (~434MB) with progress bar
    2. Clip to Tokyo bbox with osmium (2km buffer for boundary completeness)
    3. Convert PBF to OSM XML
    4. Build walk network graph via 3-pass streaming XML parse (highway ways only)

Dependencies: pip install osmnx    system: apt install osmium-tool
"""

import os
import sys
import time
import subprocess
import urllib.request

from src.config import TOKYO_BBOX, WALK_CACHE_DIR

GEOFABRIK_URL = "https://download.geofabrik.de/asia/japan/kanto-latest.osm.pbf"

# Expand bbox by ~2km to ensure boundary road nodes are complete
CLIP_BUFFER = 0.02


def _download_with_progress(url, dest):
    """Downloads a file via HTTP with a progress bar.

    Args:
        url: URL to download from.
        dest: Local file path to save to.
    """
    print(f"  URL:  {url}")
    print(f"  Dest: {dest}")

    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    resp = urllib.request.urlopen(req)
    total = int(resp.headers.get("Content-Length", 0))
    total_mb = total / (1024 * 1024) if total else 0

    downloaded = 0
    chunk_size = 1024 * 1024  # 1MB
    t0 = time.time()

    with open(dest, "wb") as f:
        while True:
            chunk = resp.read(chunk_size)
            if not chunk:
                break
            f.write(chunk)
            downloaded += len(chunk)

            elapsed = time.time() - t0
            speed = downloaded / elapsed if elapsed > 0 else 0
            speed_mb = speed / (1024 * 1024)
            dl_mb = downloaded / (1024 * 1024)

            if total:
                pct = downloaded / total
                bar_done = int(40 * pct)
                bar = "\u2588" * bar_done + "\u2591" * (40 - bar_done)
                eta = (total - downloaded) / speed if speed > 0 else 0
                print(
                    f"\r  [{bar}] {dl_mb:.0f}/{total_mb:.0f} MB "
                    f"({pct*100:.1f}%)  {speed_mb:.1f} MB/s  "
                    f"ETA {int(eta//60)}m{int(eta%60):02d}s   ",
                    end="",
                    flush=True,
                )
            else:
                print(
                    f"\r  {dl_mb:.0f} MB  {speed_mb:.1f} MB/s   ",
                    end="",
                    flush=True,
                )

    elapsed = time.time() - t0
    print(
        f"\n  Done: {downloaded/(1024*1024):.0f} MB  "
        f"in {int(elapsed//60)}m{int(elapsed%60):02d}s"
    )


def _build_graph_safe(xml_path):
    """Builds a walk network graph from OSM XML (highway ways only).

    Uses a 3-pass streaming parser to minimize memory usage:
      Pass 1: Scan ways, collect node IDs referenced by highway ways
      Pass 2: Scan nodes, add only highway-referenced nodes to graph
      Pass 3: Scan ways again, add edges

    Args:
        xml_path: Path to the OSM XML file.

    Returns:
        A NetworkX MultiDiGraph with edge lengths computed.
    """
    import xml.etree.ElementTree as ET
    import networkx as nx
    import osmnx as ox

    # Pass 1: collect node IDs from highway ways
    print("  Pass 1/3: Scanning ways, collecting node IDs...", flush=True)
    needed_nodes = set()
    way_count = 0

    for event, elem in ET.iterparse(xml_path, events=("end",)):
        if elem.tag == "way":
            tags = {}
            nd_refs = []
            for child in elem:
                if child.tag == "nd":
                    nd_refs.append(int(child.attrib["ref"]))
                elif child.tag == "tag":
                    tags[child.attrib["k"]] = child.attrib["v"]
            if "highway" in tags:
                needed_nodes.update(nd_refs)
                way_count += 1
            elem.clear()
        elif elem.tag == "node":
            elem.clear()

    print(f"  Highway ways: {way_count:,}  nodes needed: {len(needed_nodes):,}", flush=True)

    # Pass 2: add highway-referenced nodes
    print("  Pass 2/3: Loading highway nodes...", flush=True)
    G = nx.MultiDiGraph(crs="epsg:4326")
    node_coords = {}  # nid -> (lon, lat)

    for event, elem in ET.iterparse(xml_path, events=("end",)):
        if elem.tag == "node":
            nid = int(elem.attrib["id"])
            if nid in needed_nodes:
                lon = float(elem.attrib["lon"])
                lat = float(elem.attrib["lat"])
                G.add_node(nid, x=lon, y=lat)
                node_coords[nid] = (lon, lat)
            elem.clear()
        elif elem.tag in ("way", "relation"):
            elem.clear()

    print(f"  Highway nodes: {len(node_coords):,}", flush=True)

    # Pass 3: add edges
    print("  Pass 3/3: Building edges...", flush=True)
    edge_count = 0
    skipped = 0

    for event, elem in ET.iterparse(xml_path, events=("end",)):
        if elem.tag == "way":
            tags = {}
            nd_refs = []
            for child in elem:
                if child.tag == "nd":
                    nd_refs.append(int(child.attrib["ref"]))
                elif child.tag == "tag":
                    tags[child.attrib["k"]] = child.attrib["v"]

            if "highway" in tags:
                osmid = int(elem.attrib["id"])
                attrs = {"osmid": osmid, "highway": tags["highway"]}
                for u, v in zip(nd_refs[:-1], nd_refs[1:]):
                    if u in node_coords and v in node_coords:
                        G.add_edge(u, v, **attrs)
                        G.add_edge(v, u, **attrs)
                        edge_count += 2
                    else:
                        skipped += 1

            elem.clear()
        elif elem.tag == "node":
            elem.clear()

    print(f"  Edges: {edge_count:,}  (skipped {skipped} boundary-broken edges)", flush=True)

    # Compute edge lengths
    print("  Computing edge lengths...", flush=True)
    G = ox.distance.add_edge_lengths(G)

    return G


def download_and_build():
    """Downloads OSM data and builds the Tokyo walking network graph."""
    import osmnx as ox

    cache_path = os.path.join(WALK_CACHE_DIR, "tokyo_walk.graphml")

    if os.path.exists(cache_path):
        fsize = os.path.getsize(cache_path) / (1024 * 1024)
        print(f"Cache exists: {cache_path} ({fsize:.1f} MB)")
        print("Delete the file to re-download")
        return

    os.makedirs(WALK_CACHE_DIR, exist_ok=True)

    pbf_path = os.path.join(WALK_CACHE_DIR, "kanto-latest.osm.pbf")
    clipped_pbf = os.path.join(WALK_CACHE_DIR, "tokyo_clipped.osm.pbf")
    clipped_xml = os.path.join(WALK_CACHE_DIR, "tokyo_clipped.osm")

    t0 = time.time()

    # Step 1: Download PBF
    if os.path.exists(pbf_path):
        fsize = os.path.getsize(pbf_path) / (1024 * 1024)
        print(f"[1/4] PBF exists ({fsize:.0f} MB), skipping download")
    else:
        print("[1/4] Downloading Kanto region PBF...")
        _download_with_progress(GEOFABRIK_URL, pbf_path)

    # Step 2: Clip to expanded bbox
    if os.path.exists(clipped_pbf):
        fsize = os.path.getsize(clipped_pbf) / (1024 * 1024)
        print(f"\n[2/4] Clipped PBF exists ({fsize:.1f} MB), skipping")
    else:
        south, north, west, east = TOKYO_BBOX
        exp_south = south - CLIP_BUFFER
        exp_north = north + CLIP_BUFFER
        exp_west = west - CLIP_BUFFER
        exp_east = east + CLIP_BUFFER
        bbox_str = f"{exp_west},{exp_south},{exp_east},{exp_north}"
        print(f"\n[2/4] Clipping PBF (expanded bbox: {bbox_str})...")
        subprocess.run(
            ["osmium", "extract", f"--bbox={bbox_str}",
             "--strategy=complete_ways",
             pbf_path, "-o", clipped_pbf, "--overwrite"],
            check=True,
        )
        fsize = os.path.getsize(clipped_pbf) / (1024 * 1024)
        print(f"  Clipping done: {fsize:.1f} MB")

    # Step 3: Convert to OSM XML
    if os.path.exists(clipped_xml):
        fsize = os.path.getsize(clipped_xml) / (1024 * 1024)
        print(f"\n[3/4] OSM XML exists ({fsize:.1f} MB), skipping")
    else:
        print("\n[3/4] Converting PBF to OSM XML...")
        subprocess.run(
            ["osmium", "cat", clipped_pbf, "-o", clipped_xml, "--overwrite"],
            check=True,
        )
        fsize = os.path.getsize(clipped_xml) / (1024 * 1024)
        print(f"  Conversion done: {fsize:.1f} MB")

    # Step 4: Build walk network graph
    print("\n[4/4] Building walk network graph...")
    G = _build_graph_safe(clipped_xml)
    print(
        f"  Walk network: nodes {G.number_of_nodes():,}  edges {G.number_of_edges():,}"
    )

    print(f"\nSaving to {cache_path}...", flush=True)
    ox.save_graphml(G, cache_path)

    total_time = time.time() - t0
    fsize = os.path.getsize(cache_path) / (1024 * 1024)
    print(
        f"\nDone! Total time: {int(total_time // 60)}m{int(total_time % 60):02d}s  "
        f"File: {fsize:.1f} MB"
    )

    # Clean up intermediate files (keep Kanto PBF for re-processing)
    for tmp in (clipped_pbf, clipped_xml):
        if os.path.exists(tmp):
            os.remove(tmp)
            print(f"Cleaned up: {tmp}")
