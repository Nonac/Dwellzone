"""Walk neighbor computation: station-to-station walking distances via road network."""

import os
import pickle

import networkx as nx

from src.config import WALK_CACHE_DIR, walk_seconds as calc_walk_seconds
from src.walking.snap import snap_point


def build_walk_neighbors(G, stops, snapped, max_walk_m):
    """Computes walking-reachable neighbors for each stop via road network Dijkstra.

    Args:
        G: OSMnx walking network graph.
        stops: Dict of {stop_id: (lat, lon, type)}.
        snapped: Dict of {stop_id: osm_node_id}.
        max_walk_m: Maximum walking distance in meters.

    Returns:
        A dict of {stop_id: [(neighbor_id, walk_seconds), ...]}.

    Results are cached to data/walk_cache/walk_neighbors_{max_walk_m}m.pkl.
    """
    cache_path = os.path.join(
        WALK_CACHE_DIR, f"walk_neighbors_{max_walk_m}m.pkl"
    )
    if os.path.exists(cache_path):
        print("[walk_network] Loading walk neighbors from cache...")
        with open(cache_path, "rb") as f:
            return pickle.load(f)

    print(
        f"[walk_network] Computing walk neighbors "
        f"(max_walk={max_walk_m}m, {len(stops)} stops)..."
    )

    # Build osm_node -> [stop_id, ...] reverse index
    node_to_stops = {}
    for sid, osm_node in snapped.items():
        node_to_stops.setdefault(osm_node, []).append(sid)

    # Deduplicate: only run Dijkstra once per unique OSM node
    unique_nodes = set(snapped.values())
    print(f"[walk_network] Unique OSM nodes: {len(unique_nodes)}")

    node_reachable = {}  # osm_node -> {osm_node: distance_m}
    done = 0
    total = len(unique_nodes)
    for node in unique_nodes:
        try:
            lengths = nx.single_source_dijkstra_path_length(
                G, node, cutoff=max_walk_m, weight="length"
            )
            node_reachable[node] = lengths
        except nx.NodeNotFound:
            node_reachable[node] = {}

        done += 1
        if done % 500 == 0:
            print(f"[walk_network]   Dijkstra progress: {done}/{total}")

    # Convert to stop_id -> [(neighbor_id, walk_seconds)]
    walk_neighbors = {}
    for sid, osm_node in snapped.items():
        reachable = node_reachable.get(osm_node, {})
        neighbors = []
        for target_node, dist_m in reachable.items():
            if target_node == osm_node:
                continue
            for target_sid in node_to_stops.get(target_node, []):
                if target_sid == sid:
                    continue
                ws = calc_walk_seconds(dist_m)
                neighbors.append((target_sid, ws))
        if neighbors:
            walk_neighbors[sid] = neighbors

    os.makedirs(WALK_CACHE_DIR, exist_ok=True)
    with open(cache_path, "wb") as f:
        pickle.dump(walk_neighbors, f)
    print(
        f"[walk_network] Walk neighbors cached to {cache_path} "
        f"({len(walk_neighbors)} stops with neighbors)"
    )

    return walk_neighbors


def find_initial_stops_road(G, lat, lon, snapped, max_walk_m):
    """Finds all transit stops reachable by walking from a GPS origin via road network.

    Args:
        G: OSMnx walking network graph.
        lat: Origin latitude.
        lon: Origin longitude.
        snapped: Dict of {stop_id: osm_node_id}.
        max_walk_m: Maximum walking distance in meters.

    Returns:
        A tuple of (initial_list, origin_node):
            initial_list: [(stop_id, walk_seconds), ...]
            origin_node: The OSM node ID the origin was snapped to.
    """
    origin_node, _ = snap_point(G, lat, lon)

    try:
        lengths = nx.single_source_dijkstra_path_length(
            G, origin_node, cutoff=max_walk_m, weight="length"
        )
    except nx.NodeNotFound:
        return [], origin_node

    # Reverse lookup: osm_node -> stop_id
    node_to_stops = {}
    for sid, osm_node in snapped.items():
        node_to_stops.setdefault(osm_node, []).append(sid)

    initial = []
    for osm_node, dist_m in lengths.items():
        for sid in node_to_stops.get(osm_node, []):
            ws = calc_walk_seconds(dist_m)
            initial.append((sid, ws))

    return initial, origin_node
