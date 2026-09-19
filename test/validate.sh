#!/bin/bash
set -e

echo "Running anti-oversell validation..."

# Wait for all orders to be processed
echo "Waiting for worker to process queue..."
sleep 10

EVENT="${1:-flash-sale-001}"
MAX_ORDERS="${2:-1000}"

# Execute SQL query inside the postgres container
# Get the count of orders for the flash sale sku
QUERY="SELECT COUNT(*) FROM orders WHERE sku = '${EVENT}:ticket';"
TOTAL=$(docker exec $(docker ps -qf "name=postgres") psql -U openwar -d openwar -t -c "$QUERY" | tr -d ' ')

if [ -z "$TOTAL" ]; then
    echo "ERROR: Failed to retrieve order count."
    exit 1
fi

if [ "$TOTAL" -gt "$MAX_ORDERS" ]; then
    echo "❌ FAIL: Oversell detected! Total orders: $TOTAL (Max Allowed: $MAX_ORDERS)"
    exit 1
else
    echo "✅ SUCCESS: Anti-oversell passed. Total orders: $TOTAL (Max Allowed: $MAX_ORDERS)"
    exit 0
fi
