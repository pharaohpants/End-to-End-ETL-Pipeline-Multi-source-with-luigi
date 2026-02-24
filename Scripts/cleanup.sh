#!/bin/bash
# ============================================================
# cleanup.sh - Delete old ETL log files (>30 days)
# ============================================================

LOG_DIR="$(dirname "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)")/logs"

echo "Cleaning logs older than 30 days..."
find "$LOG_DIR" -name "etl_*.log" -mtime +30 -delete -print 2>/dev/null | wc -l | xargs -I{} echo "Deleted {} log files"
find "$LOG_DIR" -name "cron_*.log" -mtime +30 -delete 2>/dev/null

echo "Current usage:"
du -sh "$LOG_DIR" 2>/dev/null
echo "Done."