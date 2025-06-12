import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool
import logging
from typing import Optional
from contextlib import contextmanager
from .config import config

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        self.pool: Optional[ThreadedConnectionPool] = None

    async def init(self):
        """Инициализация пула соединений и создание базы данных"""
        try:
            await self._create_database_if_not_exists()
            await self._create_connection_pool()
            await self._run_migrations()
            logger.info("Database initialization completed successfully")
        except Exception as error:
            logger.error(f"Database initialization failed: {error}")
            raise error

    async def _create_database_if_not_exists(self):
        """Создание базы данных если она не существует"""
        admin_conn_str = f"postgresql://{config.database.user}:{config.database.password}@{config.database.host}:{config.database.port}/postgres"

        try:
            conn = psycopg2.connect(admin_conn_str)
            conn.autocommit = True
            cursor = conn.cursor()

            logger.info("Connected to PostgreSQL server")

            # Проверяем существование базы данных
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (config.database.database,))
            exists = cursor.fetchone()

            if not exists:
                logger.info(f"Creating database: {config.database.database}")
                cursor.execute(f'CREATE DATABASE "{config.database.database}"')
                logger.info(f"Database {config.database.database} created successfully")
            else:
                logger.info(f"Database {config.database.database} already exists")

        except Exception as error:
            logger.error(f"Error creating database: {error}")
            raise error
        finally:
            if 'conn' in locals():
                conn.close()

    async def _create_connection_pool(self):
        """Создание пула соединений"""
        try:
            self.pool = ThreadedConnectionPool(
                minconn=1,
                maxconn=20,
                dsn=config.database.get_connection_string(),
                cursor_factory=RealDictCursor
            )
            logger.info("Connection pool created successfully")
        except Exception as error:
            logger.error(f"Error creating connection pool: {error}")
            raise error

    async def _run_migrations(self):
        """Запуск миграций"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Читаем и выполняем миграции
                    with open('migrations/init.sql', 'r') as f:
                        migration_sql = f.read()

                    logger.info("Running database migrations...")
                    cursor.execute(migration_sql)
                    conn.commit()
                    logger.info("Database migrations completed successfully")
        except Exception as error:
            logger.error(f"Error running migrations: {error}")
            raise error

    @contextmanager
    def get_connection(self):
        """Контекстный менеджер для получения соединения из пула"""
        if not self.pool:
            raise RuntimeError("Database pool not initialized")

        conn = None
        try:
            conn = self.pool.getconn()
            yield conn
        except Exception as error:
            if conn:
                conn.rollback()
            raise error
        finally:
            if conn:
                self.pool.putconn(conn)

    def close(self):
        """Закрытие пула соединений"""
        if self.pool:
            self.pool.closeall()
            logger.info("Database connection pool closed")

# Глобальный экземпляр менеджера базы данных
db_manager = DatabaseManager()
