"""KDTree-based spatial index for snapping GPS coordinates to walk network nodes."""

import os
import pickle

import numpy as np
from scipy.spatial import cKDTree

from src.config import WALK_CACHE_DIR

# Module-level memory caches
_snap_tree = None
_snap_node_ids = None
_snap_coords = None


def _build_snap_index(G):
    """Builds a cKDTree spatial index from the walk network nodes.

    Args:
        G: OSMnx walking network graph.

    Returns:
        A tuple of (tree, node_ids, (lat_scale, lon_scale)).
    """
    global _snap_tree, _snap_node_ids, _snap_coords
    if _snap_tree is not None:
        return _snap_tree, _snap_node_ids, _snap_coords

    node_ids = list(G.nodes)
    coords = np.array([(G.nodes[n]["y"], G.nodes[n]["x"]) for n in node_ids])

    # Convert to approximate meters for KDTree
    # 1 degree latitude ~ 111320m, 1 degree longitude ~ 111320*cos(35.68) ~ 90400m
    lat_scale = 111_320.0
    lon_scale = 111_320.0 * np.cos(np.radians(35.68))
    scaled = np.column_stack([coords[:, 0] * lat_scale, coords[:, 1] * lon_scale])

    tree = cKDTree(scaled)
    _snap_tree = tree
    _snap_node_ids = node_ids
    _snap_coords = (lat_scale, lon_scale)
    return tree, node_ids, (lat_scale, lon_scale)


def snap_point(G, lat, lon):
    """Snaps a single GPS coordinate to the nearest walk network node.

    Args:
        G: OSMnx walking network graph.
        lat: Latitude in decimal degrees.
        lon: Longitude in decimal degrees.

    Returns:
        A tuple of (osm_node_id, snap_distance_meters).
    """
    tree, node_ids, (lat_scale, lon_scale) = _build_snap_index(G)
    query = np.array([lat * lat_scale, lon * lon_scale])
    dist, idx = tree.query(query)
    return node_ids[idx], dist


def snap_all_stops(G, stops):
    """Batch-snaps all transit stops to walk network nodes.

    Args:
        G: OSMnx walking network graph.
        stops: Dict of {stop_id: (lat, lon, type)}.

    Returns:
        A dict of {stop_id: osm_node_id}.

    Results are cached to data/walk_cache/stop_snap.pkl.
    """
    cache_path = os.path.join(WALK_CACHE_DIR, "stop_snap.pkl")
    if os.path.exists(cache_path):
        print("[walk_network] Loading stop snap mapping from cache...")
        with open(cache_path, "rb") as f:
            return pickle.load(f)

    print(f"[walk_network] Batch snapping {len(stops)} stops to walk network...")
    tree, node_ids, (lat_scale, lon_scale) = _build_snap_index(G)

    stop_ids = list(stops.keys())
    coords = np.array(
        [[stops[sid][0] * lat_scale, stops[sid][1] * lon_scale] for sid in stop_ids]
    )
    _, indices = tree.query(coords)

    snapped = {}
    for i, sid in enumerate(stop_ids):
        snapped[sid] = node_ids[indices[i]]

    os.makedirs(WALK_CACHE_DIR, exist_ok=True)
    with open(cache_path, "wb") as f:
        pickle.dump(snapped, f)
    print(f"[walk_network] Stop snap mapping cached to {cache_path}")

    return snapped
