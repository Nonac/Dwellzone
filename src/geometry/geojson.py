"""GeoJSON Feature construction and serialization."""

import json

from shapely.geometry import Point, mapping
from shapely.ops import unary_union

from src.config import DEFAULT_WALK_SPEED_KMH, meters_to_degrees
from src.timer import elapsed


def build_isochrone_geojson(
    reachable,
    stops,
    dep_seconds,
    dur_seconds,
    origin_lat=None,
    origin_lon=None,
    walk_speed_kmh=DEFAULT_WALK_SPEED_KMH,
    simplify_tolerance=0.0005,
    walk_graph=None,
    snapped=None,
    origin_node=None,
):
    """Builds a single-band isochrone GeoJSON Feature.

    Uses road network when available, otherwise falls back to circular buffers.

    Args:
        reachable: Dict of {stop_id: arrival_seconds}.
        stops: Dict of {stop_id: (lat, lon, type)}.
        dep_seconds: Departure time in seconds.
        dur_seconds: Duration in seconds.
        origin_lat: Origin latitude (for circular fallback).
        origin_lon: Origin longitude (for circular fallback).
        walk_speed_kmh: Walking speed.
        simplify_tolerance: Polygon simplification tolerance in degrees.
        walk_graph: OSMnx walk graph (None for circular fallback).
        snapped: Dict of {stop_id: osm_node_id} (None for circular fallback).
        origin_node: Origin OSM node ID.

    Returns:
        A GeoJSON Feature dict.
    """
    if walk_graph is not None and snapped is not None:
        return _build_isochrone_road(
            reachable, stops, dep_seconds, dur_seconds,
            walk_graph, snapped, origin_node,
            walk_speed_kmh=walk_speed_kmh,
            simplify_tolerance=simplify_tolerance,
        )

    return _build_isochrone_circular(
        reachable, stops, dep_seconds, dur_seconds,
        origin_lat=origin_lat, origin_lon=origin_lon,
        walk_speed_kmh=walk_speed_kmh,
        simplify_tolerance=simplify_tolerance,
    )


def _build_isochrone_road(
    reachable,
    stops,
    dep_seconds,
    dur_seconds,
    walk_graph,
    snapped,
    origin_node,
    walk_speed_kmh=DEFAULT_WALK_SPEED_KMH,
    simplify_tolerance=0.0005,
):
    """Builds isochrone polygon using road network edge buffers.

    Args:
        reachable: Dict of {stop_id: arrival_seconds}.
        stops: Dict of {stop_id: (lat, lon, type)}.
        dep_seconds: Departure time in seconds.
        dur_seconds: Duration in seconds.
        walk_graph: OSMnx walk graph.
        snapped: Dict of {stop_id: osm_node_id}.
        origin_node: Origin OSM node ID.
        walk_speed_kmh: Walking speed in km/h.
        simplify_tolerance: Polygon simplification tolerance in degrees.

    Returns:
        A GeoJSON Feature dict.
    """
    from src.walking.network import get_undirected_graph
    from src.walking.isochrone_builder import batch_walk_isochrone_polygons

    if not reachable and origin_node is None:
        return _empty_feature(dep_seconds, dur_seconds)

    G_undirected = get_undirected_graph(walk_graph)

    polygons = batch_walk_isochrone_polygons(
        G_undirected, reachable, snapped,
        dep_seconds, dur_seconds,
        origin_node=origin_node,
        walk_speed_kmh=walk_speed_kmh,
    )

    if not polygons:
        return _empty_feature(dep_seconds, dur_seconds)

    merged = unary_union(polygons)

    if simplify_tolerance > 0:
        merged = merged.simplify(simplify_tolerance, preserve_topology=True)

    feature = {
        "type": "Feature",
        "geometry": mapping(merged),
        "properties": {
            "departure_seconds": dep_seconds,
            "duration_seconds": dur_seconds,
            "reachable_stops": len(reachable),
            "walk_speed_kmh": walk_speed_kmh,
            "method": "road_network",
        },
    }
    return feature


def _build_isochrone_circular(
    reachable,
    stops,
    dep_seconds,
    dur_seconds,
    origin_lat=None,
    origin_lon=None,
    walk_speed_kmh=DEFAULT_WALK_SPEED_KMH,
    simplify_tolerance=0.0005,
):
    """Builds isochrone polygon using circular buffers (fallback).

    Args:
        reachable: Dict of {stop_id: arrival_seconds}.
        stops: Dict of {stop_id: (lat, lon, type)}.
        dep_seconds: Departure time in seconds.
        dur_seconds: Duration in seconds.
        origin_lat: Origin latitude.
        origin_lon: Origin longitude.
        walk_speed_kmh: Walking speed in km/h.
        simplify_tolerance: Polygon simplification tolerance in degrees.

    Returns:
        A GeoJSON Feature dict.
    """
    deadline = dep_seconds + dur_seconds
    walk_speed_ms = walk_speed_kmh * 1000.0 / 3600.0

    buffers = []

    # Origin walking circle: entire duration available for walking
    if origin_lat is not None and origin_lon is not None:
        origin_radius_m = dur_seconds * walk_speed_ms
        origin_radius_deg = meters_to_degrees(origin_radius_m)
        origin_buf = Point(origin_lon, origin_lat).buffer(origin_radius_deg, resolution=16)
        buffers.append(origin_buf)

    # Buffer for each reachable stop
    for sid, arr_s in reachable.items():
        if sid not in stops:
            continue

        lat, lon, _typ = stops[sid]
        remaining_s = deadline - arr_s
        if remaining_s <= 0:
            continue

        walk_radius_m = remaining_s * walk_speed_ms
        radius_deg = meters_to_degrees(walk_radius_m)

        if radius_deg > 0:
            pt = Point(lon, lat)
            buf = pt.buffer(radius_deg, resolution=8)
            buffers.append(buf)

    if not buffers:
        return _empty_feature(dep_seconds, dur_seconds)

    merged = unary_union(buffers)

    if simplify_tolerance > 0:
        merged = merged.simplify(simplify_tolerance, preserve_topology=True)

    feature = {
        "type": "Feature",
        "geometry": mapping(merged),
        "properties": {
            "departure_seconds": dep_seconds,
            "duration_seconds": dur_seconds,
            "reachable_stops": len(reachable),
            "walk_speed_kmh": walk_speed_kmh,
            "method": "circular_buffer",
        },
    }
    return feature


def _empty_feature(dep_seconds, dur_seconds):
    """Returns an empty GeoJSON Feature when no stops are reachable.

    Args:
        dep_seconds: Departure time in seconds.
        dur_seconds: Duration in seconds.

    Returns:
        A GeoJSON Feature dict with empty geometry.
    """
    return {
        "type": "Feature",
        "geometry": {"type": "Polygon", "coordinates": []},
        "properties": {
            "departure_seconds": dep_seconds,
            "duration_seconds": dur_seconds,
            "reachable_stops": 0,
        },
    }


def save_geojson(feature, filepath):
    """Wraps a GeoJSON Feature in a FeatureCollection and writes to file.

    Args:
        feature: A GeoJSON Feature dict.
        filepath: Output file path.
    """
    fc = {
        "type": "FeatureCollection",
        "features": [feature],
    }
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(fc, f, ensure_ascii=False, indent=2)
    print(f"[geometry] GeoJSON saved to {filepath}")


def build_isochrone_bands_geojson(
    reachable,
    stops,
    dep_seconds,
    bands_seconds,
    origin_lat=None,
    origin_lon=None,
    walk_speed_kmh=DEFAULT_WALK_SPEED_KMH,
    simplify_tolerance=0.0005,
    walk_graph=None,
    snapped=None,
    origin_node=None,
):
    """Generates GeoJSON Features for multiple time bands in one pass.

    Optimized: runs Dijkstra once, then generates polygons for each band.

    Args:
        reachable: Dict of {stop_id: arrival_seconds}.
        stops: Dict of {stop_id: (lat, lon, type)}.
        dep_seconds: Departure time in seconds.
        bands_seconds: List of band durations in seconds [600, 1200, ...].
        origin_lat: Origin latitude.
        origin_lon: Origin longitude.
        walk_speed_kmh: Walking speed.
        simplify_tolerance: Polygon simplification tolerance in degrees.
        walk_graph: OSMnx walk graph (None for circular fallback).
        snapped: Dict of {stop_id: osm_node_id}.
        origin_node: Origin OSM node ID.

    Returns:
        A list of GeoJSON Feature dicts, one per band.
    """
    if walk_graph is not None and snapped is not None:
        from src.walking.network import get_undirected_graph
        from src.walking.isochrone_builder import batch_walk_isochrone_multi_band

        G_undirected = get_undirected_graph(walk_graph)

        # Compute all bands at once
        band_polygons = batch_walk_isochrone_multi_band(
            G_undirected, reachable, snapped,
            dep_seconds, bands_seconds,
            origin_node=origin_node,
            walk_speed_kmh=walk_speed_kmh,
        )

        print(f"[@{elapsed():.1f}s] Building GeoJSON features...")
        features = []
        for band_s in bands_seconds:
            polygon = band_polygons.get(band_s)

            if polygon is None or polygon.is_empty:
                feature = _empty_feature(dep_seconds, band_s)
            else:
                if simplify_tolerance > 0:
                    polygon = polygon.simplify(simplify_tolerance, preserve_topology=True)

                deadline = dep_seconds + band_s
                stops_in_band = sum(1 for arr in reachable.values() if arr <= deadline)

                feature = {
                    "type": "Feature",
                    "geometry": mapping(polygon),
                    "properties": {
                        "departure_seconds": dep_seconds,
                        "duration_seconds": band_s,
                        "reachable_stops": stops_in_band,
                        "walk_speed_kmh": walk_speed_kmh,
                        "method": "edge_buffer",
                    },
                }

            features.append(feature)

        print(f"[@{elapsed():.1f}s] GeoJSON features done")
        return features

    # Fallback: circular buffer for each band individually
    features = []
    for band_s in bands_seconds:
        feature = _build_isochrone_circular(
            reachable, stops, dep_seconds, band_s,
            origin_lat=origin_lat, origin_lon=origin_lon,
            walk_speed_kmh=walk_speed_kmh,
            simplify_tolerance=simplify_tolerance,
        )
        features.append(feature)

    return features
