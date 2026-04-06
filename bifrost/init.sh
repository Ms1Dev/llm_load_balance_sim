#!/bin/sh
set -e

# Start Bifrost in the background using its normal entrypoint
/app/docker-entrypoint.sh /app/main &
BIFROST_PID=$!

# Wait for Bifrost to accept requests
echo "[init] Waiting for Bifrost..."
until curl -sf http://localhost:8080/api/health > /dev/null 2>&1; do
    sleep 1
done
echo "[init] Bifrost ready"

# Check whether governance is already enabled
GOVERNANCE=$(curl -sf http://localhost:8080/api/config | grep -o '"enable_governance":true' || true)

if [ -z "$GOVERNANCE" ]; then
    echo "[init] Enabling governance..."
    curl -sf -X PUT http://localhost:8080/api/config \
        -H "Content-Type: application/json" \
        -d '{"client_config":{"enable_governance":true,"log_retention_days":365}}'
    echo "[init] Governance enabled — restarting Bifrost"
    kill "$BIFROST_PID"
    wait "$BIFROST_PID" 2>/dev/null || true
    # Re-exec replaces this shell with Bifrost so Docker tracks it correctly
    exec /app/docker-entrypoint.sh /app/main
else
    echo "[init] Governance already enabled"
    wait "$BIFROST_PID"
fi
