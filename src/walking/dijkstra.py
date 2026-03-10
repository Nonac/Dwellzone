"""Sparse matrix construction and parallel scipy Dijkstra for walk isochrones."""

import multiprocessing as mp_lib
from dataclasses import dataclass

import numpy as np
from scipy.sparse import csr_matrix
from scipy.sparse.csgraph import dijkstra as scipy_dijkstra

from src.config import DEFAULT_WALK_SPEED_KMH, NUM_WORKERS
from src.timer import elapsed


@dataclass
class DijkstraContext:
    """Shared state for fork-mode parallel Dijkstra workers."""

    sparse_mat: csr_matrix
    coords_array: np.ndarray
    edges_array: np.ndarray
    bands_seconds: list
    dep_seconds: int
    origin_node: object  # OSM node ID or None
    walk_speed_ms: float
    max_walk_dist: float


# Single module-level context (inherited by fork-mode child processes)
_ctx = None


def convert_graph_to_sparse(G):
    """Converts a NetworkX graph to a scipy sparse matrix.

    Args:
        G: An undirected NetworkX graph.

    Returns:
        A tuple of (sparse_matrix, node_list, node_to_idx, coords_array, edges_array).
    """
    print(f"[@{elapsed():.1f}s] Converting to sparse matrix...")

    node_list = list(G.nodes())
    node_to_idx = {n: i for i, n in enumerate(node_list)}
    n_nodes = len(node_list)

    # Extract coordinates
    coords_array = np.array([
        (G.nodes[n]["x"], G.nodes[n]["y"]) for n in node_list
    ], dtype=np.float64)

    # Build sparse matrix
    rows, cols, data = [], [], []
    edges_set = set()

    for u, v, d in G.edges(data=True):
        i, j = node_to_idx[u], node_to_idx[v]
        weight = d.get("length", 1.0)
        rows.append(i)
        cols.append(j)
        data.append(weight)
        # Undirected: add reverse edge
        rows.append(j)
        cols.append(i)
        data.append(weight)
        # Record edge (canonical order)
        if i < j:
            edges_set.add((i, j))
        else:
            edges_set.add((j, i))

    sparse_matrix = csr_matrix((data, (rows, cols)), shape=(n_nodes, n_nodes))

    edges_array = np.array(list(edges_set), dtype=np.int32)
    print(f"  Nodes: {n_nodes}, edges: {len(edges_set)}")

    return sparse_matrix, node_list, node_to_idx, coords_array, edges_array


def _dijkstra_worker(batch):
    """Worker function: runs Dijkstra for a batch of source nodes.

    Uses module-level _ctx (inherited via fork mode, avoiding pickle overhead).
    Uses numpy vectorized operations instead of Python loops where possible.

    Args:
        batch: List of (osm_node, arrival_seconds, matrix_index) tuples.

    Returns:
        A dict of {band_seconds: set of ((x1,y1),(x2,y2)) edge coordinate pairs}.
    """
    global _ctx

    indices = [s[2] for s in batch]

    dist_matrix = scipy_dijkstra(
        _ctx.sparse_mat,
        directed=False,
        indices=indices,
        limit=_ctx.max_walk_dist,
        return_predecessors=False
    )

    edge_i_arr = _ctx.edges_array[:, 0]
    edge_j_arr = _ctx.edges_array[:, 1]

    # Collect edges grouped by band
    batch_edges = {b: set() for b in _ctx.bands_seconds}

    for i, (osm_node, arr_s, idx) in enumerate(batch):
        distances = dist_matrix[i]

        for band_s in _ctx.bands_seconds:
            if osm_node == _ctx.origin_node:
                remaining_s = band_s
            else:
                remaining_s = (_ctx.dep_seconds + band_s) - arr_s

            if remaining_s < 30:
                continue

            band_dist = remaining_s * _ctx.walk_speed_ms

            # Vectorized: find edges where both endpoints are reachable
            reachable_mask = distances <= band_dist
            edge_mask = reachable_mask[edge_i_arr] & reachable_mask[edge_j_arr]

            valid_indices = np.nonzero(edge_mask)[0]

            for idx in valid_indices:
                ei, ej = edge_i_arr[idx], edge_j_arr[idx]
                coord_i = tuple(_ctx.coords_array[ei])
                coord_j = tuple(_ctx.coords_array[ej])
                batch_edges[band_s].add((coord_i, coord_j))

    return batch_edges


def run_parallel_dijkstra(
    sparse_mat,
    coords_array,
    edges_array,
    source_nodes,
    bands_seconds,
    dep_seconds,
    origin_node,
    walk_speed_ms,
    max_walk_dist,
):
    """Runs parallel scipy Dijkstra across multiple source nodes.

    Args:
        sparse_mat: scipy sparse matrix of the walk network.
        coords_array: Numpy array of (x, y) coordinates for each node.
        edges_array: Numpy array of (i, j) edge index pairs.
        source_nodes: List of (osm_node, arrival_seconds, matrix_index) tuples.
        bands_seconds: List of band durations in seconds.
        dep_seconds: Departure time in seconds.
        origin_node: OSM node ID of the origin point (or None).
        walk_speed_ms: Walking speed in meters per second.
        max_walk_dist: Maximum walking distance in meters.

    Returns:
        A dict of {band_seconds: set of ((x1,y1),(x2,y2)) edge coordinate pairs}.
    """
    global _ctx

    total = len(source_nodes)
    print(f"[walk_network] scipy Dijkstra computing {total} source nodes ({NUM_WORKERS} workers)...")

    # Split into batches
    batch_size = max(1, total // NUM_WORKERS)
    batches = []
    for batch_start in range(0, total, batch_size):
        batch_end = min(batch_start + batch_size, total)
        batches.append(source_nodes[batch_start:batch_end])

    # Set context (inherited by child processes via fork)
    _ctx = DijkstraContext(
        sparse_mat=sparse_mat,
        coords_array=coords_array,
        edges_array=edges_array,
        bands_seconds=bands_seconds,
        dep_seconds=dep_seconds,
        origin_node=origin_node,
        walk_speed_ms=walk_speed_ms,
        max_walk_dist=max_walk_dist,
    )

    print(f"[walk_network]   Split into {len(batches)} batches for parallel processing...")

    ctx = mp_lib.get_context('fork')
    with ctx.Pool(processes=min(NUM_WORKERS, len(batches))) as pool:
        results = pool.map(_dijkstra_worker, batches)

    # Merge results
    band_edges = {b: set() for b in bands_seconds}
    for batch_result in results:
        for band_s in bands_seconds:
            band_edges[band_s].update(batch_result[band_s])

    print(f"  [@{elapsed():.1f}s] Dijkstra done")
    return band_edges
