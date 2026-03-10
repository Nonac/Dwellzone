"""Polygon merging: combines buffered chunks into final band polygons.

Currently uses unary_union (O(n^2)). Ready for hierarchical binary merge
optimization (O(n log n)) as a future improvement.
"""

import multiprocessing as mp_lib

from src.timer import elapsed


def _merge_band_worker(args):
    """Worker: merges all polygons for a single band.

    Args:
        args: Tuple of (band_seconds, list_of_polygons).

    Returns:
        A tuple of (band_seconds, merged Shapely polygon or None).
    """
    from shapely.ops import unary_union
    band_s, polys = args
    if not polys:
        return (band_s, None)
    if len(polys) == 1:
        return (band_s, polys[0])
    return (band_s, unary_union(polys))


def merge_band_polygons(band_polygons, bands_seconds):
    """Merges polygon chunks into one polygon per band, in parallel.

    Args:
        band_polygons: Dict of {band_seconds: list of Shapely polygons}.
        bands_seconds: List of band durations in seconds.

    Returns:
        A dict of {band_seconds: merged Shapely polygon or None}.
    """
    print(f"[@{elapsed():.1f}s] Merging polygons ({len(bands_seconds)} bands in parallel)...")

    merge_tasks = [(b, band_polygons[b]) for b in bands_seconds]

    ctx = mp_lib.get_context('fork')
    with ctx.Pool(processes=len(bands_seconds)) as pool:
        merge_results = pool.map(_merge_band_worker, merge_tasks)

    result = {}
    for band_s, poly in merge_results:
        result[band_s] = poly

    print(f"  [@{elapsed():.1f}s] Merge done")
    return result
