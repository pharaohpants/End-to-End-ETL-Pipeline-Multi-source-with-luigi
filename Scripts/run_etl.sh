#!/bin/bash
# ============================================================
# run_etl.sh - ETL Pipeline Runner (Linux/WSL)
# Perusahaan XYZ
# ============================================================
set -e

# Auto-detect project directory (parent of Scripts/)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
VENV_DIR="$PROJECT_DIR/venv"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/etl_$(date +%Y%m%d_%H%M%S).log"

mkdir -p "$LOG_DIR"

log() { echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"; }

# ---- Pre-flight checks ----
log "=== ETL PIPELINE START ==="
log "Project: $PROJECT_DIR"

[ ! -f "$PROJECT_DIR/main.py" ] && log "ERROR: main.py not found!" && exit 1
[ ! -f "$PROJECT_DIR/.env" ]    && log "ERROR: .env not found!"    && exit 1

# ---- Activate venv (if exists) ----
if [ -d "$VENV_DIR/bin" ]; then
    source "$VENV_DIR/bin/activate"
    log "venv activated"
fi

# ---- Clean Luigi locks ----
rm -rf "$PROJECT_DIR/luigi-tmp-"* 2>/dev/null || true

# ---- Run pipeline ----
START_TIME=$(date +%s)
cd "$PROJECT_DIR"

if python -u main.py 2>&1 | tee -a "$LOG_FILE"; then
    DURATION=$(( $(date +%s) - START_TIME ))
    log "=== ETL SUCCESS (${DURATION}s) ==="
    deactivate 2>/dev/null || true
    exit 0
else
    DURATION=$(( $(date +%s) - START_TIME ))
    log "=== ETL FAILED (${DURATION}s) ==="
    deactivate 2>/dev/null || true
    exit 1
fi