#!/bin/bash
set -e

echo "🚀 Starting Hedgie Gateways..."

if [ ! -f .env ]; then
    echo "❌ .env file not found! Copying from .env.example"
    cp .env.example .env
    echo "📝 Please edit .env file with your settings"
    exit 1
fi

mkdir -p logs backups

docker-compose up --build -d

echo "✅ Services started!"
echo "🌐 PgAdmin: http://localhost:8080"
echo "🗄️  Database: localhost:5432"
echo "📊 Logs: docker-compose logs -f"
