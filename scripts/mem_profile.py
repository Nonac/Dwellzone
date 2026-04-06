#!/usr/bin/env python
"""Memory profiler: runs the isochrone pipeline with RSS tracking at each stage.

Usage:
    python scripts/mem_profile.py
    python scripts/mem_profile.py --duration 40 --interval 10
"""

import argparse
import os
import sys
import resource
import gc

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


def rss_gb():
    """Current process RSS in GB (does not include children)."""
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024 / 1024


def rss_children_gb():
    """Peak RSS of all child processes in GB."""
    return resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss / 1024 / 1024


def checkpoint(label):
    gc.collect()
    print(f"  [MEM] {label}: self={rss_gb():.2f} GB, children_peak={rss_children_gb():.2f} GB")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--duration", type=int, default=60)
    p.add_argument("--interval", type=int, default=10)
    args = p.parse_args()

    from datetime import datetime
    from src.settings import load_config
    from src.timer import reset_timer, elapsed

    cfg = load_config(None)
    iso = cfg.get("isochrone", {})
    lat = iso.get("lat", 35.7126349)
    lon = iso.get("lon", 139.785853)
    dep_time = datetime(2026, 4, 6, 8, 30, 0)
    dep_seconds = 8 * 3600 + 30 * 60
    duration = args.duration
    interval = args.interval
    max_dur_seconds = duration * 60
    bands_seconds = [b * 60 for b in range(interval, duration + 1, interval)]

    print(f"=== Memory Profile: {duration}min, {len(bands_seconds)} bands, interval={interval}min ===")
    checkpoint("startup")

    reset_timer()

    # Stage 1: Data load
    from src.transit.loader import load_all
    data = load_all(calendar="Weekday", max_walk_m=800)
    checkpoint("after data load")

    # Stage 2: Transit reachability
    from src.transit.graph import compute_reachable
    reachable, origin_node = compute_reachable(
        lat, lon, dep_seconds, max_dur_seconds, data,
        walk_speed_kmh=5.0, max_walk_m=800,
    )
    print(f"  Reachable stops: {len(reachable)}")
    checkpoint("after transit dijkstra")

    # Stage 3: Undirected graph
    from src.walking.network import get_undirected_graph
    G_undirected = get_undirected_graph(data["walk_graph"])
    checkpoint("after undirected graph")

    # Stage 4: Sparse matrix
    from src.walking.dijkstra import convert_graph_to_sparse
    sparse_mat, node_list, node_to_idx, coords_array, edges_array = \
        convert_graph_to_sparse(G_undirected)
    checkpoint("after sparse matrix")

    # Collect source nodes
    from src.config import DEFAULT_WALK_SPEED_KMH
    walk_speed_ms = DEFAULT_WALK_SPEED_KMH * 1000.0 / 3600.0
    max_walk_dist = max(bands_seconds) * walk_speed_ms

    source_nodes = []
    if origin_node is not None and origin_node in node_to_idx:
        source_nodes.append((origin_node, dep_seconds, node_to_idx[origin_node]))
    node_info = {}
    for sid, arr_s in reachable.items():
        osm_node = data.get("snapped", {}).get(sid)
        if osm_node is None or osm_node not in node_to_idx:
            continue
        if osm_node not in node_info or arr_s < node_info[osm_node]:
            node_info[osm_node] = arr_s
    for osm_node, arr_s in node_info.items():
        source_nodes.append((osm_node, arr_s, node_to_idx[osm_node]))
    print(f"  Source nodes: {len(source_nodes)}")

    # Free what we can before Dijkstra fork
    del node_list, node_to_idx, node_info
    gc.collect()
    checkpoint("before walk dijkstra fork")

    # Stage 5: Walk Dijkstra (FORK 60 workers)
    from src.walking.dijkstra import run_parallel_dijkstra
    band_edges = run_parallel_dijkstra(
        sparse_mat, coords_array, edges_array,
        source_nodes, bands_seconds,
        dep_seconds, origin_node,
        walk_speed_ms, max_walk_dist,
    )
    checkpoint("after walk dijkstra")
    for b in bands_seconds:
        print(f"    {b//60}min: {len(band_edges[b])} edges")

    # Stage 6: Edge buffer (FORK 60 workers)
    from src.geometry.buffer import generate_edge_buffers
    from src.config import BUFFER_METERS
    buffer_deg = BUFFER_METERS / 111320.0
    band_polygons = generate_edge_buffers(band_edges, bands_seconds, buffer_deg)
    checkpoint("after edge buffer")

    # Free band_edges before merge
    del band_edges
    gc.collect()
    checkpoint("after freeing band_edges")

    # Stage 7: Merge (FORK 60 workers level 1 + FORK ~5 workers level 2)
    from src.geometry.merge import merge_band_polygons
    merged = merge_band_polygons(band_polygons, bands_seconds)
    checkpoint("after merge")

    # Free intermediate polygons
    del band_polygons
    gc.collect()
    checkpoint("after freeing band_polygons chunks")

    # Stage 8: Simplify (FORK 6 workers) -- the NEW parallel simplify
    from src.geometry.geojson import _simplify_geom
    print(f"  [@{elapsed():.1f}s] Simplifying sequentially (to isolate memory)...")
    for band_s in bands_seconds:
        polygon = merged.get(band_s)
        if polygon is not None and not polygon.is_empty:
            from src.config import SIMPLIFY_TOLERANCE
            merged[band_s] = _simplify_geom(polygon, SIMPLIFY_TOLERANCE)
            checkpoint(f"after simplify {band_s//60}min")

    checkpoint("FINAL")
    print(f"\n[@{elapsed():.1f}s] Done.")


if __name__ == "__main__":
    main()
