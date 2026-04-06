"""Multi-band walk isochrone polygon generation.

Orchestrates: sparse matrix Dijkstra -> edge buffer -> polygon merge.
"""

from src.config import DEFAULT_WALK_SPEED_KMH, BUFFER_METERS
from src.timer import elapsed
from src.walking.dijkstra import convert_graph_to_sparse, run_parallel_dijkstra
from src.geometry.buffer import generate_edge_buffers
from src.geometry.merge import merge_band_polygons


def batch_walk_isochrone_multi_band(
    G_undirected,
    reachable,
    snapped,
    dep_seconds,
    bands_seconds,
    origin_node=None,
    walk_speed_kmh=DEFAULT_WALK_SPEED_KMH,
    buffer_meters=BUFFER_METERS,
):
    """Generates walk isochrone polygons for multiple time bands.

    Args:
        G_undirected: Undirected walking network graph.
        reachable: Dict of {stop_id: arrival_seconds}.
        snapped: Dict of {stop_id: osm_node_id}.
        dep_seconds: Departure time in seconds.
        bands_seconds: List of band durations in seconds.
        origin_node: OSM node ID of the origin point (or None).
        walk_speed_kmh: Walking speed in km/h.
        buffer_meters: Buffer distance around edges in meters.

    Returns:
        A dict of {band_seconds: Shapely polygon or None}.
    """
    walk_speed_ms = walk_speed_kmh * 1000.0 / 3600.0
    max_dur = max(bands_seconds)
    max_walk_dist = max_dur * walk_speed_ms
    buffer_deg = buffer_meters / 111320.0

    # Convert to sparse matrix
    sparse_mat, _, node_to_idx, coords_array, edges_array = \
        convert_graph_to_sparse(G_undirected)
    print(f"  [@{elapsed():.1f}s] Sparse matrix conversion done")

    # Collect source nodes
    source_nodes = []  # [(osm_node, arr_s, matrix_idx)]

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

    # Parallel Dijkstra
    band_edges = run_parallel_dijkstra(
        sparse_mat, coords_array, edges_array,
        source_nodes, bands_seconds,
        dep_seconds, origin_node,
        walk_speed_ms, max_walk_dist,
    )

    # Parallel edge buffer
    band_polygons = generate_edge_buffers(band_edges, bands_seconds, buffer_deg)

    # Parallel polygon merge
    result = merge_band_polygons(band_polygons, bands_seconds)

    # Print edge counts per band
    for band_s in bands_seconds:
        edge_count = len(band_edges[band_s])
        print(f"[walk_network]   {band_s // 60}min: {edge_count} edges")

    return result


def batch_walk_isochrone_polygons(
    G_undirected,
    reachable,
    snapped,
    dep_seconds,
    dur_seconds,
    origin_node=None,
    walk_speed_kmh=DEFAULT_WALK_SPEED_KMH,
):
    """Single-band walk isochrone polygon generation (backward-compatible wrapper).

    Args:
        G_undirected: Undirected walking network graph.
        reachable: Dict of {stop_id: arrival_seconds}.
        snapped: Dict of {stop_id: osm_node_id}.
        dep_seconds: Departure time in seconds.
        dur_seconds: Duration in seconds.
        origin_node: OSM node ID of the origin point (or None).
        walk_speed_kmh: Walking speed in km/h.

    Returns:
        A list containing a single polygon, or an empty list.
    """
    result = batch_walk_isochrone_multi_band(
        G_undirected, reachable, snapped,
        dep_seconds, [dur_seconds],
        origin_node=origin_node,
        walk_speed_kmh=walk_speed_kmh,
    )
    poly = result.get(dur_seconds)
    if poly is None or poly.is_empty:
        return []
    return [poly]
