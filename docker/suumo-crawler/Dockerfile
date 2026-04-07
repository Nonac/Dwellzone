FROM python:3.11-slim

WORKDIR /app

# Install system deps for psycopg2 and lxml
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev libxml2-dev libxslt1-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements-crawler.txt .
RUN pip install --no-cache-dir -r requirements-crawler.txt

COPY src/ src/
COPY scripts/ scripts/
COPY configs/ configs/

CMD ["python", "-m", "src.scraper.pipeline"]
