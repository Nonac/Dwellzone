#!/bin/bash
# VPS deployment script for Suumo crawler
#
# Prerequisites on VPS:
#   - Docker + Docker Compose installed
#   - ZeroTier joined to same network as local DB server
#
# Prerequisites on local DB server:
#   - PostgreSQL pg_hba.conf allows ZeroTier subnet:
#     host  suumo  nonac  10.147.0.0/16  md5
#   - Reload: sudo systemctl reload postgresql
#
# Usage:
#   1. Copy this project to VPS
#   2. Create docker/suumo-crawler/.env from .env.example
#   3. Run: bash scripts/deploy-vps.sh setup
#   4. Test: bash scripts/deploy-vps.sh test
#   5. Enable cron: bash scripts/deploy-vps.sh cron

set -e

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE_FILE="$PROJECT_DIR/docker/suumo-crawler/docker-compose.yml"
ENV_FILE="$PROJECT_DIR/docker/suumo-crawler/.env"
LOG_DIR="$PROJECT_DIR/logs"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

case "${1:-help}" in
  setup)
    echo "=== Building Docker image ==="
    docker compose -f "$COMPOSE_FILE" build

    echo ""
    if [ ! -f "$ENV_FILE" ]; then
      echo -e "${RED}Missing .env file!${NC}"
      echo "Create it from the example:"
      echo "  cp $PROJECT_DIR/docker/suumo-crawler/.env.example $ENV_FILE"
      echo "  vim $ENV_FILE  # fill in ZeroTier IP and DB password"
      exit 1
    fi

    mkdir -p "$LOG_DIR"
    echo -e "${GREEN}Setup complete.${NC}"
    echo "Next: bash $0 test"
    ;;

  test)
    echo "=== Test run: 1 page mansion, Tokyo only ==="
    docker compose -f "$COMPOSE_FILE" run --rm suumo-crawler \
      python scripts/crawl_suumo.py --type mansion --prefecture 13 --max-items 10 --skip-details
    echo -e "${GREEN}Test complete. Check DB for results.${NC}"
    ;;

  run)
    echo "=== Full crawl ==="
    mkdir -p "$LOG_DIR"
    docker compose -f "$COMPOSE_FILE" run --rm suumo-crawler \
      python scripts/crawl_suumo.py 2>&1 | tee "$LOG_DIR/crawl_$(date +%Y%m%d_%H%M%S).log"
    ;;

  cron)
    # Install crontab entry: every Sunday at 03:00 JST (18:00 UTC Saturday)
    CRON_CMD="0 18 * * 6 cd $PROJECT_DIR && bash scripts/deploy-vps.sh run >> $LOG_DIR/cron.log 2>&1"
    (crontab -l 2>/dev/null | grep -v "deploy-vps.sh"; echo "$CRON_CMD") | crontab -
    echo -e "${GREEN}Cron installed:${NC}"
    echo "  $CRON_CMD"
    echo ""
    echo "Current crontab:"
    crontab -l
    ;;

  cron-remove)
    crontab -l 2>/dev/null | grep -v "deploy-vps.sh" | crontab -
    echo "Cron entry removed."
    ;;

  logs)
    ls -lt "$LOG_DIR"/*.log 2>/dev/null | head -10
    ;;

  help|*)
    echo "Usage: bash $0 <command>"
    echo ""
    echo "Commands:"
    echo "  setup        Build Docker image, check .env"
    echo "  test         Test run (10 items, Tokyo mansion, no details)"
    echo "  run          Full crawl (all prefectures, all types, with details)"
    echo "  cron         Install weekly cron job (Sunday 03:00 JST)"
    echo "  cron-remove  Remove cron job"
    echo "  logs         List recent log files"
    ;;
esac
