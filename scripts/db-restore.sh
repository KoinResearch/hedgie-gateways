#!/bin/bash
set -e

if [ -z "$1" ]; then
    echo "❌ Usage: $0 <backup_file>"
    echo "📁 Available backups:"
    ls -la ./backups/
    exit 1
fi

if [ ! -f "$1" ]; then
    echo "❌ File $1 not found!"
    exit 1
fi

echo "🔄 Restoring database from $1..."
docker exec -i hedgie-gateways-pg psql -U admin -d deribit_trades < $1
echo "✅ Database restore completed!"
