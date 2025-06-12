#!/bin/bash
echo "📊 Hedgie Gateways Status:"
echo "=========================="
docker-compose ps

echo -e "\n🗄️  Database Status:"
echo "==================="
docker exec hedgie-gateways-pg pg_isready -U admin -d deribit_trades

echo -e "\n📈 Trading Data Statistics:"
echo "==========================="
docker exec hedgie-gateways-pg psql -U admin -d deribit_trades -c "
SELECT
    'all_btc_trades' as table_name,
    COUNT(*) as records,
    MIN(timestamp) as oldest_record,
    MAX(timestamp) as newest_record
FROM all_btc_trades
UNION ALL
SELECT
    'all_eth_trades' as table_name,
    COUNT(*) as records,
    MIN(timestamp) as oldest_record,
    MAX(timestamp) as newest_record
FROM all_eth_trades;
"
