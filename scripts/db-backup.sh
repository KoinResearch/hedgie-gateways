#!/bin/bash
set -e

BACKUP_DIR="./backups"
DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_FILE="$BACKUP_DIR/backup_$DATE.sql"

mkdir -p $BACKUP_DIR

echo "🗄️  Creating database backup..."
docker exec hedgie-gateways-pg pg_dump -U admin -d deribit_trades > $BACKUP_FILE

echo "✅ Backup created: $BACKUP_FILE"
echo "📊 Backup size: $(du -h $BACKUP_FILE | cut -f1)"
