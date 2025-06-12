#!/bin/bash
echo "🗄️  Connecting to PostgreSQL..."
docker exec -it hedgie-gateways-pg psql -U admin -d deribit_trades
