import asyncpg
import logging
from typing import Optional
from .config import config

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None

    async def init(self):
        try:
            await self._create_connection_pool()
            await self._run_migrations()
            logger.info("Database initialization completed successfully")
        except Exception as error:
            logger.error(f"Database initialization failed: {error}")
            raise error

    async def _create_connection_pool(self):
        try:
            self.pool = await asyncpg.create_pool(
                config.database.get_connection_string(),
                min_size=1,
                max_size=20
            )
            logger.info("Connection pool created successfully")
        except Exception as error:
            logger.error(f"Error creating connection pool: {error}")
            raise error

    async def _run_migrations(self):
        try:
            async with self.pool.acquire() as conn:
                with open('migrations/init.sql', 'r') as f:
                    migration_sql = f.read()

                logger.info("Running database migrations...")
                await conn.execute(migration_sql)
                logger.info("Database migrations completed successfully")
        except Exception as error:
            logger.error(f"Error running migrations: {error}")
            raise error

    async def close(self):
        if self.pool:
            await self.pool.close()
            logger.info("Database connection pool closed")

db_manager = DatabaseManager()
