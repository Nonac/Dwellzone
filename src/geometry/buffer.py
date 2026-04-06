"""Edge buffer generation: creates polygon buffers around walk network edges."""

import multiprocessing as mp_lib

from src.config import NUM_WORKERS, CHUNK_SIZE
from src.timer import elapsed


def _buffer_edges_chunk(args):
    """Worker: buffers a chunk of edges and merges them into a single geometry.

    Args:
        args: Tuple of (chunk_id, edges_list, buffer_deg).

    Returns:
        A tuple of (chunk_id, Shapely polygon or None).
    """
    chunk_id, edges, buffer_deg = args
    if not edges:
        return (chunk_id, None)

    from shapely import buffer as shp_buffer
    from shapely.geometry import MultiLineString

    lines = [((p1[0], p1[1]), (p2[0], p2[1])) for p1, p2 in edges]
    multi_line = MultiLineString(lines)
    buffered = shp_buffer(multi_line, buffer_deg, cap_style='round', quad_segs=4)
    return (chunk_id, buffered)


def generate_edge_buffers(band_edges, bands_seconds, buffer_deg):
    """Generates buffered polygons from edge coordinate sets, in parallel.

    Args:
        band_edges: Dict of {band_seconds: set of ((x1,y1),(x2,y2)) edge pairs}.
        bands_seconds: List of band durations in seconds.
        buffer_deg: Buffer distance in degrees.

    Returns:
        A dict of {band_seconds: list of Shapely polygons (one per chunk)}.
    """
    total_edges = sum(len(band_edges[b]) for b in bands_seconds)
    print(f"[walk_network] Parallel edge buffer ({total_edges} edges, {NUM_WORKERS} workers)...")

    chunk_size = CHUNK_SIZE
    buffer_tasks = []
    task_to_band = {}  # chunk_id -> band_s

    chunk_id = 0
    for band_s in bands_seconds:
        edges_list = list(band_edges[band_s])
        if not edges_list:
            task_to_band[chunk_id] = band_s
            buffer_tasks.append((chunk_id, [], buffer_deg))
            chunk_id += 1
        else:
            for i in range(0, len(edges_list), chunk_size):
                chunk = edges_list[i:i + chunk_size]
                task_to_band[chunk_id] = band_s
                buffer_tasks.append((chunk_id, chunk, buffer_deg))
                chunk_id += 1

    print(f"[walk_network]   Split into {len(buffer_tasks)} chunks for parallel processing...")

    ctx = mp_lib.get_context('fork')
    with ctx.Pool(processes=NUM_WORKERS) as pool:
        chunk_results = pool.map(_buffer_edges_chunk, buffer_tasks)

    print(f"  [@{elapsed():.1f}s] Chunk buffer done")

    # Group chunk results by band
    band_polygons = {b: [] for b in bands_seconds}
    for cid, poly in chunk_results:
        band_s = task_to_band[cid]
        if poly is not None and not poly.is_empty:
            band_polygons[band_s].append(poly)

    for b in bands_seconds:
        print(f"  {b // 60}min: {len(band_polygons[b])} chunks to merge")

    return band_polygons
