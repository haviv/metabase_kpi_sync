#!/usr/bin/env bash
# Daily sync: fetch Azure cost (and optional flags) into Postgres for Metabase.
#
# Metabase does NOT receive a separate push — it queries the same Postgres DB.
# Ensure PG_HOST in .env points to the database your Metabase instance uses.
#
# Usage:
#   ./scripts/run_daily_sync.sh
#   ./scripts/run_daily_sync.sh --with-ado   # also run one ADO/Jira cycle
#
# Schedule with launchd (macOS):
#   cp scripts/com.pathlock.metabase-daily-sync.plist ~/Library/LaunchAgents/
#   launchctl load ~/Library/LaunchAgents/com.pathlock.metabase-daily-sync.plist

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

mkdir -p logs
LOG="logs/daily_sync.log"
WITH_ADO=false

for arg in "$@"; do
  case "$arg" in
    --with-ado) WITH_ADO=true ;;
  esac
done

# Load .env
if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

export PYTHONUNBUFFERED=1
export INCLUDE_AZURE_COST="${INCLUDE_AZURE_COST:-true}"

echo "=== Daily sync started $(date -u '+%Y-%m-%d %H:%M:%S UTC') ===" | tee -a "$LOG"

# Azure cost: standalone is enough (24h gate also exists inside export_ado if you use that path)
if [[ "${INCLUDE_AZURE_COST}" == "true" || "${INCLUDE_AZURE_COST}" == "1" ]]; then
  echo "------> Azure cost sync" | tee -a "$LOG"
  python3 export_azure_cost.py 2>&1 | tee -a "$LOG"
fi

# Optional: one full export_ado cycle (ADO + Jira + Azure if flags set)
if [[ "$WITH_ADO" == "true" ]]; then
  echo "------> export_ado one-shot cycle" | tee -a "$LOG"
  RUN_ONCE=true \
  DISABLE_FULL_SYNC=true \
  SKIP_ADO_HISTORY="${SKIP_ADO_HISTORY:-true}" \
  SKIP_GITHUB_METRICS="${SKIP_GITHUB_METRICS:-true}" \
  python3 export_ado.py --once \
    ${INCLUDE_JIRA:+--include-jira} \
    ${INCLUDE_AZURE_COST:+--include-azure-cost} \
    2>&1 | tee -a "$LOG"
fi

echo "=== Daily sync finished $(date -u '+%Y-%m-%d %H:%M:%S UTC') ===" | tee -a "$LOG"
