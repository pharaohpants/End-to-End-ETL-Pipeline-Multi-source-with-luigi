#!/bin/bash
# ============================================================
# setup_crontab.sh - Setup Cron Schedule for ETL Pipeline
# ============================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_SCRIPT="$SCRIPT_DIR/run_etl.sh"
LOG_DIR="$(dirname "$SCRIPT_DIR")/logs"

chmod +x "$RUN_SCRIPT"

# Backup existing crontab
crontab -l > "/tmp/crontab_backup_$(date +%Y%m%d).txt" 2>/dev/null || true

# Add ETL cron job (skip if already exists)
if crontab -l 2>/dev/null | grep -q "run_etl.sh"; then
    echo "Cron job already exists:"
    crontab -l | grep "run_etl.sh"
else
    # Default: daily at 02:00 AM
    #(crontab -l 2>/dev/null; echo "0 2 * * * $RUN_SCRIPT >> $LOG_DIR/cron_\$(date +\%Y\%m\%d).log 2>&1") | crontab -
    #echo "Cron job installed: daily at 02:00 AM" 
    (crontab -l 2>/dev/null; echo "*/5 * * * * $RUN_SCRIPT >> $LOG_DIR/cron_\$(date +\%Y\%m\%d).log 2>&1") | crontab -
fi

echo ""
echo "Current cron schedule:"
crontab -l 2>/dev/null | grep -v "^#" | grep -v "^$"
echo ""
echo "Commands: crontab -l (view) | crontab -e (edit) | crontab -r (remove)"