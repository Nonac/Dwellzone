"""Polygon merging: combines buffered chunks into final band polygons.

Uses two-level parallel merge to utilize all CPU cores:
Level 1: Split large bands into sub-groups, merge each sub-group in parallel.
Level 2: Merge the sub-group results for each band.
"""

import multiprocessing as mp_lib

from src.config import NUM_WORKERS
from src.timer import elapsed

# Sub-groups of this size are merged in parallel in level 1
_MERGE_GROUP_SIZE = 30


def _merge_worker(args):
    """Worker: merges a list of polygons via unary_union."""
    from shapely.ops import unary_union
    task_id, polys = args
    if not polys:
        return (task_id, None)
    if len(polys) == 1:
        return (task_id, polys[0])
    return (task_id, unary_union(polys))


def merge_band_polygons(band_polygons, bands_seconds):
    """Merges polygon chunks into one polygon per band, using two-level parallelism.

    Level 1: All bands' chunks are split into sub-groups and merged in parallel
             across NUM_WORKERS processes.
    Level 2: Sub-group results for each band are merged in a second parallel pass.

    Args:
        band_polygons: Dict of {band_seconds: list of Shapely polygons}.
        bands_seconds: List of band durations in seconds.

    Returns:
        A dict of {band_seconds: merged Shapely polygon or None}.
    """
    # Build level-1 tasks: split each band into sub-groups
    tasks = []
    task_map = {}  # task_id -> band_s

    task_id = 0
    for band_s in bands_seconds:
        polys = band_polygons[band_s]
        if len(polys) <= _MERGE_GROUP_SIZE:
            # Small band: single task
            tasks.append((task_id, polys))
            task_map[task_id] = band_s
            task_id += 1
        else:
            # Large band: split into sub-groups
            for i in range(0, len(polys), _MERGE_GROUP_SIZE):
                group = polys[i:i + _MERGE_GROUP_SIZE]
                tasks.append((task_id, group))
                task_map[task_id] = band_s
                task_id += 1

    total_tasks = len(tasks)
    print(f"[@{elapsed():.1f}s] Merging polygons (level 1: {total_tasks} tasks, {NUM_WORKERS} workers)...")

    ctx = mp_lib.get_context('fork')
    with ctx.Pool(processes=NUM_WORKERS) as pool:
        level1_results = pool.map(_merge_worker, tasks)

    # Collect level-1 results per band
    band_intermediates = {b: [] for b in bands_seconds}
    for tid, poly in level1_results:
        if poly is not None and not poly.is_empty:
            band_intermediates[task_map[tid]].append(poly)

    print(f"  [@{elapsed():.1f}s] Level 1 done")

    # Level 2: merge sub-group results for each band
    level2_tasks = []
    for band_s in bands_seconds:
        intermediates = band_intermediates[band_s]
        if len(intermediates) > 1:
            level2_tasks.append((band_s, intermediates))

    if level2_tasks:
        print(f"  Merging level 2 ({len(level2_tasks)} bands with multiple sub-groups)...")
        with ctx.Pool(processes=len(level2_tasks)) as pool:
            level2_results = pool.map(_merge_worker, level2_tasks)
        for band_s, poly in level2_results:
            band_intermediates[band_s] = [poly] if poly is not None else []

    result = {}
    for band_s in bands_seconds:
        intermediates = band_intermediates[band_s]
        result[band_s] = intermediates[0] if intermediates else None

    print(f"  [@{elapsed():.1f}s] Merge done")
    return result
