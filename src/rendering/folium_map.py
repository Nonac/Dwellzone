"""Folium map rendering for isochrone visualization."""

import json
import os

import folium

# High-contrast color scale (colorblind-friendly, dark tones for light basemap)
# Based on ColorBrewer "Dark2" + curated for deuteranopia/protanopia safety
_CONTOUR_COLORS = [
    "#d62728",  # 10min - strong red
    "#e6550d",  # 20min - dark orange
    "#8b6508",  # 30min - dark gold
    "#2ca02c",  # 40min - green
    "#1f78b4",  # 50min - strong blue
    "#6a3d9a",  # 60min - deep purple
]


def render_single_band(feature, lat, lon, departure_time, duration_minutes, output):
    """Renders a single-band isochrone to an HTML map.

    Args:
        feature: GeoJSON Feature dict from compute_isochrone().
        lat: Origin latitude.
        lon: Origin longitude.
        departure_time: datetime of departure.
        duration_minutes: Duration in minutes.
        output: Output HTML file path.
    """
    geojson_fc = {"type": "FeatureCollection", "features": [feature]}

    m = folium.Map(
        location=[lat, lon],
        zoom_start=12,
        tiles="https://cyberjapandata.gsi.go.jp/xyz/std/{z}/{x}/{y}.png",
        attr="GSI Japan",
    )

    folium.GeoJson(
        geojson_fc,
        name=f"{duration_minutes}min isochrone",
        style_function=lambda f: {
            "fillColor": "#3388ff",
            "color": "#2255aa",
            "weight": 2,
            "fillOpacity": 0.25,
        },
    ).add_to(m)

    time_label = departure_time.strftime("%H:%M")
    folium.Marker(
        location=[lat, lon],
        popup=f"Origin ({time_label})",
        icon=folium.Icon(color="red", icon="play", prefix="fa"),
    ).add_to(m)

    folium.LayerControl().add_to(m)

    os.makedirs(os.path.dirname(os.path.abspath(output)), exist_ok=True)
    m.save(output)
    stops = feature["properties"]["reachable_stops"]
    print(f"[map] Saved to {output}  (reachable stops: {stops})")


def save_geojson(features, output_path):
    """Saves GeoJSON FeatureCollection to file.

    Args:
        features: A single Feature dict or a list of Feature dicts.
        output_path: Output file path (.geojson).
    """
    if isinstance(features, dict):
        features = [features]
    fc = {"type": "FeatureCollection", "features": features}
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(fc, f, ensure_ascii=False)
    print(f"[map] GeoJSON saved to {output_path}")


def render_contour(features, lat, lon, departure_time, band_interval, output):
    """Renders multi-band isochrone contours to an HTML map.

    Args:
        features: List of GeoJSON Feature dicts from compute_isochrone_bands().
        lat: Origin latitude.
        lon: Origin longitude.
        departure_time: datetime of departure.
        band_interval: Interval between bands in minutes.
        output: Output HTML file path.
    """
    m = folium.Map(
        location=[lat, lon],
        zoom_start=12,
        tiles="https://cyberjapandata.gsi.go.jp/xyz/std/{z}/{x}/{y}.png",
        attr="GSI Japan",
    )

    # Draw from largest to smallest so smaller bands appear on top
    num_bands = len(features)
    for feature in reversed(features):
        band_min = feature["properties"]["duration_minutes"]
        idx = band_min // band_interval - 1
        color = _CONTOUR_COLORS[idx] if idx < len(_CONTOUR_COLORS) else _CONTOUR_COLORS[-1]

        fc = {"type": "FeatureCollection", "features": [feature]}
        folium.GeoJson(
            fc,
            name=f"{band_min}min",
            style_function=lambda f, c=color: {
                "fillColor": c,
                "color": c,
                "weight": 1.5,
                "fillOpacity": 0.6,
            },
            tooltip=f"{band_min}min",
        ).add_to(m)

    time_label = departure_time.strftime("%H:%M")
    folium.Marker(
        location=[lat, lon],
        popup=f"Origin ({time_label})",
        icon=folium.Icon(color="red", icon="play", prefix="fa"),
    ).add_to(m)

    folium.LayerControl().add_to(m)

    os.makedirs(os.path.dirname(os.path.abspath(output)), exist_ok=True)
    m.save(output)
    print(f"[map] Contour map saved to {output}  ({num_bands} bands)")
