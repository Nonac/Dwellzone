"""Walking network graph loading, caching, and undirected conversion."""

import os
import pickle

import osmnx as ox

from src.config import TOKYO_BBOX, WALK_CACHE_DIR
from src.timer import elapsed

# Module-level memory caches
_walk_graph = None
_undirected_graph = None


def get_walk_graph():
    """Loads the Greater Tokyo walking network graph from cache or downloads it.

    Bbox: TOKYO_BBOX (35.50-35.85, 139.40-139.95).
    Prefers pickle cache (fastest), falls back to graphml, then downloads.

    Returns:
        A networkx.MultiDiGraph of the walking network.
    """
    global _walk_graph
    if _walk_graph is not None:
        return _walk_graph

    os.makedirs(WALK_CACHE_DIR, exist_ok=True)
    pkl_path = os.path.join(WALK_CACHE_DIR, "tokyo_walk.pkl")
    graphml_path = os.path.join(WALK_CACHE_DIR, "tokyo_walk.graphml")

    if os.path.exists(pkl_path):
        print("[walk_network] Loading walk graph from pickle cache...")
        with open(pkl_path, "rb") as f:
            G = pickle.load(f)
    elif os.path.exists(graphml_path):
        print("[walk_network] Loading walk graph from graphml cache...")
        G = ox.load_graphml(graphml_path)
        print("[walk_network] Saving pickle cache...")
        with open(pkl_path, "wb") as f:
            pickle.dump(G, f, protocol=pickle.HIGHEST_PROTOCOL)
    else:
        south, north, west, east = TOKYO_BBOX
        print(
            f"[walk_network] Downloading OSM walk network "
            f"bbox=({south}, {north}, {west}, {east})..."
        )
        G = ox.graph_from_bbox(
            bbox=(north, south, east, west),
            network_type="walk",
        )
        ox.save_graphml(G, graphml_path)
        with open(pkl_path, "wb") as f:
            pickle.dump(G, f, protocol=pickle.HIGHEST_PROTOCOL)
        print("[walk_network] Walk graph cached")

    print(
        f"[walk_network] Walk graph nodes: {G.number_of_nodes()}, "
        f"edges: {G.number_of_edges()}"
    )
    _walk_graph = G
    return G


def get_undirected_graph(G):
    """Returns an undirected version of the graph (with memory and disk cache).

    Args:
        G: The directed walking network graph.

    Returns:
        An undirected networkx.Graph.
    """
    global _undirected_graph
    if _undirected_graph is not None:
        return _undirected_graph

    pkl_path = os.path.join(WALK_CACHE_DIR, "tokyo_walk_undirected.pkl")

    if os.path.exists(pkl_path):
        print(f"[@{elapsed():.1f}s] Loading undirected graph from cache...")
        with open(pkl_path, "rb") as f:
            _undirected_graph = pickle.load(f)
        print(f"[@{elapsed():.1f}s] Undirected graph loaded")
        return _undirected_graph

    print(f"[@{elapsed():.1f}s] Converting to undirected graph...")
    _undirected_graph = G.to_undirected()
    print(f"[@{elapsed():.1f}s] Undirected graph conversion done")

    print(f"[@{elapsed():.1f}s] Saving undirected graph cache...")
    with open(pkl_path, "wb") as f:
        pickle.dump(_undirected_graph, f, protocol=pickle.HIGHEST_PROTOCOL)
    print(f"[@{elapsed():.1f}s] Undirected graph cache saved")

    return _undirected_graph
