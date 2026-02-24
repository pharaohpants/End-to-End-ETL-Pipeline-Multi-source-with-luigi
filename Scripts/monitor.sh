#!/bin/bash
# ============================================================
# monitor.sh - Simple ETL Pipeline Monitor
# ============================================================

LOG_DIR="$(dirname "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)")/logs"

case "${1:-status}" in
    status)
        echo "=== ETL Pipeline Monitor ==="
        echo ""
        echo "Cron schedule:"
        crontab -l 2>/dev/null | grep "run_etl" || echo "  No cron job found"
        echo ""
        echo "Recent runs:"
        ls -t "$LOG_DIR"/etl_pipeline_*.log 2>/dev/null | head -5 | while read f; do
            if grep -q "COMPLETED SUCCESSFULLY" "$f" 2>/dev/null; then
                echo "  [OK]   $(basename $f)"
            elif grep -q "FAILED" "$f" 2>/dev/null; then
                echo "  [FAIL] $(basename $f)"
            else
                echo "  [????] $(basename $f)"
            fi
        done
        echo ""
        echo "Disk usage:"
        du -sh "$LOG_DIR" 2>/dev/null
        ;;
    tail)
        LATEST=$(ls -t "$LOG_DIR"/etl_pipeline_*.log 2>/dev/null | head -1)
        [ -n "$LATEST" ] && tail -f "$LATEST" || echo "No logs found"
        ;;
    *)
        echo "Usage: $0 {status|tail}"
        ;;
esac