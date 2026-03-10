# Tokyo Transit Isochrone Map

Generate isochrone maps for Tokyo's public transit system. Given a starting point and departure time, computes reachable areas within a time budget using buses, railways, metro, and walking.

## Features

- Multi-modal transit: JR, private railways, Tokyo Metro (GTFS), Toei, buses (ODPT)
- Walking network from OpenStreetMap (Kanto region)
- Contour mode with configurable time bands (e.g. 10/20/30/40/50/60 min)
- Outputs: interactive Folium HTML map + GeoJSON
- Parallel Dijkstra for large-scale graph search

## Prerequisites

- Python 3.10+
- PostgreSQL with PostGIS extension
- [ODPT API key](https://developer.odpt.org/) (for fetching transit data)

## Setup

```bash
# Install system dependencies (Debian/Ubuntu)
sudo apt install -y libpq-dev python3-dev gcc

# Install Python packages
pip install -r requirements.txt

# Copy config template and fill in your credentials
cp configs/default.yaml.example configs/default.yaml
# Edit configs/default.yaml with your database and ODPT API settings
```

### Database

Set up a PostGIS-enabled PostgreSQL instance. Example with Docker:

```yaml
services:
  postgis:
    image: postgis/postgis
    ports:
      - "5432:5432"
    environment:
      POSTGRES_USER: your_user
      POSTGRES_PASSWORD: your_password
      POSTGRES_DB: tokyo_transit
```

## Usage

### 1. Initialize database schema

```bash
python scripts/init_db.py
```

### 2. Import transit data

```bash
# Import everything at once
python scripts/import_all.py

# Or import individually
python scripts/import_bus.py
python scripts/import_railway.py
python scripts/import_metro.py
python scripts/import_minitokyo3d.py
```

### 3. Download walking network

```bash
python scripts/download_walk_network.py
```

### 4. Generate isochrone map

```bash
# Use config defaults
python scripts/generate_map.py

# Override parameters
python scripts/generate_map.py --lat 35.68 --lon 139.77 --duration 45 --contour
```

Output goes to `outputs/` by default.

## Configuration

All settings are in `configs/default.yaml`. See `configs/default.yaml.example` for the full template with comments.

## Project Structure

```
scripts/          # Thin CLI entry points
src/
  import_data/    # DB schema + data importers (bus, railway, metro, mini-tokyo-3d)
  transit/        # ODPT API client + timetable loader
  walking/        # OSM graph download + Dijkstra
  geometry/       # Buffer, merge, GeoJSON generation
  rendering/      # Folium map rendering
  isochrone.py    # Core isochrone computation
  config.py       # Constants (walk speed, haversine, etc.)
  settings.py     # YAML config loader
  db.py           # PostgreSQL connection
configs/          # YAML config files
sql/              # Reference SQL scripts
outputs/          # Generated maps (git-ignored)
```

## License

MIT
