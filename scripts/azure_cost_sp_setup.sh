#!/usr/bin/env bash
# Finish Azure cost service principal setup (run as subscription Owner/User Access Admin).
#
# App already created: metabase-kpi-azure-cost-sync
# Client ID: 69ca2ac3-c8ca-47bd-8f74-2e918e6bbb15
# Tenant ID: 57cdd3cf-ae8b-4672-aac8-72237a79b841
#
# Already assigned: dev + staging (Cost Management Reader)
# Still needed: pre-prod + prod

set -euo pipefail

APP_ID="${AZURE_CLIENT_ID:-69ca2ac3-c8ca-47bd-8f74-2e918e6bbb15}"
ROLE="Cost Management Reader"

assign_cost_reader() {
  local sub_id="$1"
  local sub_name="$2"
  echo "Assigning $ROLE on $sub_name ($sub_id)"
  az role assignment create \
    --assignee "$APP_ID" \
    --role "$ROLE" \
    --scope "/subscriptions/$sub_id"
}

# Pre-prod + prod (failed for non-admin user)
assign_cost_reader "17291027-e2ea-4049-b41b-debd997e7ed1" "PathlockCloud-Preprod"
assign_cost_reader "84bd96c6-0c2a-4b9b-87c1-6e375c3d0a09" "PathlockCloud-Prod"

echo "Done. Verify:"
az role assignment list --assignee "$APP_ID" \
  --query "[].{role:roleDefinitionName, scope:scope}" -o table
