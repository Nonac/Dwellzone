#!/usr/bin/env python
"""Minimal benchmark: 20min duration, 10min interval (2 bands only).

Isolates each stage with precise timing. Designed to complete in ~5 minutes.

Usage:
    python scripts/benchmark.py
    python scripts/benchmark.py --duration 30 --interval 10
"""

import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.settings import load_config


def fmt(s):
    return f"{s * 1000:.0f}ms" if s < 1 else f"{s:.1f}s"


def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--config", default=None)
    p.add_argument("--duration", type=int, default=20, help="Minutes (default 20)")
    p.add_argument("--interval", type=int, default=10, help="Interval (default 10)")
    args = p.parse_args()

    cfg = load_config(args.config)
    iso = cfg.get("isochrone", {})
    lat = iso.get("lat", 35.7114817)
    lon = iso.get("lon", 139.7856803)
    h, m = 8, 30
    dep_seconds = h * 3600 + m * 60
    duration = args.duration
    interval = args.interval
    bands_minutes = list(range(interval, duration + 1, interval))
    bands_seconds = [b * 60 for b in bands_minutes]
    calendar = "Weekday"

    from src.config import DEFAULT_WALK_SPEED_KMH, DEFAULT_MAX_WALK_M

    timings = {}
    wall_start = time.perf_counter()

    print(f"\n>>> Benchmark: {duration}min, {len(bands_seconds)} bands {bands_minutes}")

    # ── 1: Data loading ──
    t0 = time.perf_counter()
    from src.transit.loader import load_all
    data = load_all(calendar=calendar, max_walk_m=DEFAULT_MAX_WALK_M)
    timings["1_data_load"] = time.perf_counter() - t0

    # ── 2: Transit reachability ──
    t0 = time.perf_counter()
    from src.transit.graph import compute_reachable
    reachable, origin_node = compute_reachable(
        lat, lon, dep_seconds, duration * 60, data,
        walk_speed_kmh=DEFAULT_WALK_SPEED_KMH, max_walk_m=DEFAULT_MAX_WALK_M,
    )
    timings["2_transit_reach"] = time.perf_counter() - t0

    # ── 3: Undirected graph ──
    t0 = time.perf_counter()
    from src.walking.network import get_undirected_graph
    G_undirected = get_undirected_graph(data["walk_graph"])
    timings["3_undirected"] = time.perf_counter() - t0

    # ── 4: Sparse matrix ──
    t0 = time.perf_counter()
    from src.walking.dijkstra import convert_graph_to_sparse
    sparse_mat, node_list, node_to_idx, coords_array, edges_array = \
        convert_graph_to_sparse(G_undirected)
    timings["4_sparse"] = time.perf_counter() - t0

    # ── 5: Source nodes ──
    t0 = time.perf_counter()
    walk_speed_ms = DEFAULT_WALK_SPEED_KMH * 1000.0 / 3600.0
    max_walk_dist = max(bands_seconds) * walk_speed_ms
    snapped = data.get("snapped", {})
    source_nodes = []
    if origin_node is not None and origin_node in node_to_idx:
        source_nodes.append((origin_node, dep_seconds, node_to_idx[origin_node]))
    node_info = {}
    for sid, arr_s in reachable.items():
        osm_node = snapped.get(sid)
        if osm_node is None or osm_node not in node_to_idx:
            continue
        if osm_node not in node_info or arr_s < node_info[osm_node]:
            node_info[osm_node] = arr_s
    for osm_node, arr_s in node_info.items():
        source_nodes.append((osm_node, arr_s, node_to_idx[osm_node]))
    timings["5_sources"] = time.perf_counter() - t0
    print(f"  Source nodes: {len(source_nodes)}")

    # ── 6: Walk Dijkstra ──
    t0 = time.perf_counter()
    from src.walking.dijkstra import run_parallel_dijkstra
    band_edges = run_parallel_dijkstra(
        sparse_mat, coords_array, edges_array,
        source_nodes, bands_seconds,
        dep_seconds, origin_node, walk_speed_ms, max_walk_dist,
    )
    timings["6_dijkstra"] = time.perf_counter() - t0
    for b in bands_seconds:
        print(f"    {b // 60}min: {len(band_edges[b])} edges")

    # ── 7: Edge buffer ──
    t0 = time.perf_counter()
    from src.geometry.buffer import generate_edge_buffers
    from src.config import BUFFER_METERS
    buffer_deg = BUFFER_METERS / 111320.0
    band_polygons = generate_edge_buffers(band_edges, bands_seconds, buffer_deg)
    timings["7_buffer"] = time.perf_counter() - t0

    # ── 8: Merge ──
    t0 = time.perf_counter()
    from src.geometry.merge import merge_band_polygons
    merged = merge_band_polygons(band_polygons, bands_seconds)
    timings["8_merge"] = time.perf_counter() - t0
    for b in bands_seconds:
        poly = merged.get(b)
        verts = _count_verts(poly) if poly and not poly.is_empty else 0
        print(f"    {b // 60}min: {verts} vertices")

    # ── 9: Simplify (per band, timed individually) ──
    from src.geometry.geojson import _simplify_geom
    print("\n  Simplify per band:")
    simplify_total = 0
    simplified = {}
    for b in bands_seconds:
        poly = merged.get(b)
        if poly and not poly.is_empty:
            ts = time.perf_counter()
            from src.config import SIMPLIFY_TOLERANCE
            simplified[b] = _simplify_geom(poly, SIMPLIFY_TOLERANCE)
            dt = time.perf_counter() - ts
            simplify_total += dt
            v_before = _count_verts(poly)
            v_after = _count_verts(simplified[b])
            print(f"    {b // 60}min: {fmt(dt)}  ({v_before} -> {v_after} verts)")
        else:
            simplified[b] = poly
    timings["9_simplify"] = simplify_total

    # ── 10: GeoJSON ──
    t0 = time.perf_counter()
    from shapely.geometry import mapping
    import json
    features = []
    for b in bands_seconds:
        poly = simplified.get(b)
        if poly and not poly.is_empty:
            features.append({"type": "Feature", "geometry": mapping(poly),
                             "properties": {"duration_minutes": b // 60}})
    geojson_str = json.dumps({"type": "FeatureCollection", "features": features})
    timings["10_geojson"] = time.perf_counter() - t0
    print(f"\n  GeoJSON: {len(geojson_str) / 1024:.0f} KB")

    # ── Summary ──
    wall_total = time.perf_counter() - wall_start
    print("\n" + "=" * 55)
    print(f"BENCHMARK ({duration}min, {len(bands_seconds)} bands)")
    print("=" * 55)
    w = max(len(k) for k in timings)
    for name, t in timings.items():
        pct = t / wall_total * 100
        bar = "#" * int(pct / 2)
        print(f"  {name:<{w}}  {fmt(t):>8}  {pct:5.1f}%  {bar}")
    print(f"  {'─' * w}  {'─' * 8}  {'─' * 5}")
    print(f"  {'TOTAL':<{w}}  {fmt(wall_total):>8}  100.0%")
    print()


def _count_verts(geom):
    try:
        return len(geom.exterior.coords)
    except AttributeError:
        total = 0
        try:
            for part in geom.geoms:
                total += len(part.exterior.coords)
                for hole in part.interiors:
                    total += len(hole.coords)
        except Exception:
            pass
        return total


if __name__ == "__main__":
    main()
