-- init.sql - Базовая схема для проекта Deribit Options Analytics
-- Создает все необходимые таблицы с проверкой существования

-- =============================================================================
-- ПОСЛЕДОВАТЕЛЬНОСТИ (SEQUENCES)
-- =============================================================================

-- Последовательность для пользователей
CREATE SEQUENCE IF NOT EXISTS users_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- Последовательность для ролей
CREATE SEQUENCE IF NOT EXISTS roles_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- Последовательность для refresh tokens
CREATE SEQUENCE IF NOT EXISTS refresh_tokens_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- =============================================================================
-- ТОРГОВЫЕ ТАБЛИЦЫ - DERIBIT
-- =============================================================================

-- Все BTC торги на Deribit
CREATE TABLE IF NOT EXISTS all_btc_trades (
    trade_id TEXT NOT NULL,
    block_trade_leg_count TEXT,
    contracts NUMERIC,
    block_trade_id TEXT,
    combo_id TEXT,
    tick_direction INTEGER,
    mark_price NUMERIC,
    amount NUMERIC,
    trade_seq INTEGER,
    instrument_name TEXT,
    index_price NUMERIC,
    direction TEXT,
    price NUMERIC,
    iv NUMERIC,
    liquidation TEXT,
    combo_trade_id TEXT,
    timestamp TIMESTAMP WITHOUT TIME ZONE,
    CONSTRAINT all_btc_trades_pkey PRIMARY KEY (trade_id)
);

-- Все ETH торги на Deribit
CREATE TABLE IF NOT EXISTS all_eth_trades (
    trade_id TEXT NOT NULL,
    block_trade_leg_count TEXT,
    contracts NUMERIC,
    block_trade_id TEXT,
    combo_id TEXT,
    tick_direction INTEGER,
    mark_price NUMERIC,
    amount NUMERIC,
    trade_seq INTEGER,
    instrument_name TEXT,
    index_price NUMERIC,
    direction TEXT,
    price NUMERIC,
    iv NUMERIC,
    liquidation TEXT,
    combo_trade_id TEXT,
    timestamp TIMESTAMP WITHOUT TIME ZONE,
    CONSTRAINT all_eth_trades_pkey PRIMARY KEY (trade_id)
);

-- BTC блок-торги на Deribit
CREATE TABLE IF NOT EXISTS btc_block_trades (
    trade_id TEXT NOT NULL,
    block_trade_leg_count TEXT,
    contracts NUMERIC,
    block_trade_id TEXT,
    combo_id TEXT,
    tick_direction INTEGER,
    mark_price NUMERIC,
    amount NUMERIC,
    trade_seq INTEGER,
    instrument_name TEXT,
    index_price NUMERIC,
    direction TEXT,
    price NUMERIC,
    iv NUMERIC,
    liquidation TEXT,
    combo_trade_id TEXT,
    timestamp TIMESTAMP WITHOUT TIME ZONE,
    CONSTRAINT btc_block_trades_pkey PRIMARY KEY (trade_id)
);

-- ETH блок-торги на Deribit
CREATE TABLE IF NOT EXISTS eth_block_trades (
    trade_id TEXT NOT NULL,
    block_trade_leg_count TEXT,
    contracts NUMERIC,
    block_trade_id TEXT,
    combo_id TEXT,
    tick_direction INTEGER,
    mark_price NUMERIC,
    amount NUMERIC,
    trade_seq INTEGER,
    instrument_name TEXT,
    index_price NUMERIC,
    direction TEXT,
    price NUMERIC,
    iv NUMERIC,
    liquidation TEXT,
    combo_trade_id TEXT,
    timestamp TIMESTAMP WITHOUT TIME ZONE,
    CONSTRAINT eth_block_trades_pkey PRIMARY KEY (trade_id)
);

-- =============================================================================
-- ТОРГОВЫЕ ТАБЛИЦЫ - OKX
-- =============================================================================

-- BTC торги на OKX
CREATE TABLE IF NOT EXISTS okx_btc_trades (
    trade_id TEXT NOT NULL,
    mark_price NUMERIC,
    amount NUMERIC,
    instrument_name TEXT,
    index_price NUMERIC,
    direction TEXT,
    price NUMERIC,
    iv NUMERIC,
    timestamp TIMESTAMP WITHOUT TIME ZONE,
    CONSTRAINT okx_btc_trades_pkey PRIMARY KEY (trade_id)
);

-- ETH торги на OKX
CREATE TABLE IF NOT EXISTS okx_eth_trades (
    trade_id TEXT NOT NULL,
    mark_price NUMERIC,
    amount NUMERIC,
    instrument_name TEXT,
    index_price NUMERIC,
    direction TEXT,
    price NUMERIC,
    iv NUMERIC,
    timestamp TIMESTAMP WITHOUT TIME ZONE,
    CONSTRAINT okx_eth_trades_pkey PRIMARY KEY (trade_id)
);

-- =============================================================================
-- СИСТЕМА ПОЛЬЗОВАТЕЛЕЙ И АУТЕНТИФИКАЦИИ
-- =============================================================================

-- Роли пользователей
CREATE TABLE IF NOT EXISTS roles (
    id INTEGER NOT NULL DEFAULT nextval('roles_id_seq'::regclass),
    name CHARACTER VARYING(50) NOT NULL,
    description CHARACTER VARYING(255),
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT roles_pkey PRIMARY KEY (id),
    CONSTRAINT roles_name_key UNIQUE (name)
);

-- Связка последовательности с таблицей
ALTER SEQUENCE roles_id_seq OWNED BY roles.id;

-- Пользователи
CREATE TABLE IF NOT EXISTS users (
    id INTEGER NOT NULL DEFAULT nextval('users_id_seq'::regclass),
    email CHARACTER VARYING(255) NOT NULL,
    password CHARACTER VARYING(255) NOT NULL,
    first_name CHARACTER VARYING(255),
    last_name CHARACTER VARYING(255),
    is_active BOOLEAN DEFAULT true,
    is_email_verified BOOLEAN DEFAULT false,
    verification_token CHARACTER VARYING(255),
    reset_password_token CHARACTER VARYING(255),
    reset_password_expires TIMESTAMP WITHOUT TIME ZONE,
    login_attempts INTEGER DEFAULT 0,
    lock_until TIMESTAMP WITHOUT TIME ZONE,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT users_pkey PRIMARY KEY (id),
    CONSTRAINT users_email_key UNIQUE (email)
);

-- Связка последовательности с таблицей
ALTER SEQUENCE users_id_seq OWNED BY users.id;

-- Связь пользователей и ролей (многие ко многим)
CREATE TABLE IF NOT EXISTS user_roles (
    user_id INTEGER NOT NULL,
    role_id INTEGER NOT NULL,
    CONSTRAINT user_roles_pkey PRIMARY KEY (user_id, role_id),
    CONSTRAINT user_roles_user_id_fkey FOREIGN KEY (user_id)
        REFERENCES users(id) ON DELETE CASCADE,
    CONSTRAINT user_roles_role_id_fkey FOREIGN KEY (role_id)
        REFERENCES roles(id) ON DELETE CASCADE
);

-- Refresh токены для аутентификации
CREATE TABLE IF NOT EXISTS refresh_tokens (
    id INTEGER NOT NULL DEFAULT nextval('refresh_tokens_id_seq'::regclass),
    user_id INTEGER,
    token CHARACTER VARYING(255) NOT NULL,
    expires_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT refresh_tokens_pkey PRIMARY KEY (id),
    CONSTRAINT refresh_tokens_user_id_fkey FOREIGN KEY (user_id)
        REFERENCES users(id) ON DELETE CASCADE
);

-- Связка последовательности с таблицей
ALTER SEQUENCE refresh_tokens_id_seq OWNED BY refresh_tokens.id;

-- =============================================================================
-- ИНДЕКСЫ ДЛЯ ОПТИМИЗАЦИИ ПРОИЗВОДИТЕЛЬНОСТИ
-- =============================================================================

-- Индексы для торговых таблиц по времени (наиболее частые запросы)
CREATE INDEX IF NOT EXISTS idx_all_btc_trades_timestamp ON all_btc_trades (timestamp);
CREATE INDEX IF NOT EXISTS idx_all_eth_trades_timestamp ON all_eth_trades (timestamp);
CREATE INDEX IF NOT EXISTS idx_btc_block_trades_timestamp ON btc_block_trades (timestamp);
CREATE INDEX IF NOT EXISTS idx_eth_block_trades_timestamp ON eth_block_trades (timestamp);
CREATE INDEX IF NOT EXISTS idx_okx_btc_trades_timestamp ON okx_btc_trades (timestamp);
CREATE INDEX IF NOT EXISTS idx_okx_eth_trades_timestamp ON okx_eth_trades (timestamp);

-- Индексы по инструментам (для фильтрации по типам опционов)
CREATE INDEX IF NOT EXISTS idx_all_btc_trades_instrument ON all_btc_trades (instrument_name);
CREATE INDEX IF NOT EXISTS idx_all_eth_trades_instrument ON all_eth_trades (instrument_name);
CREATE INDEX IF NOT EXISTS idx_btc_block_trades_instrument ON btc_block_trades (instrument_name);
CREATE INDEX IF NOT EXISTS idx_eth_block_trades_instrument ON eth_block_trades (instrument_name);
CREATE INDEX IF NOT EXISTS idx_okx_btc_trades_instrument ON okx_btc_trades (instrument_name);
CREATE INDEX IF NOT EXISTS idx_okx_eth_trades_instrument ON okx_eth_trades (instrument_name);

-- Индексы по направлению торгов
CREATE INDEX IF NOT EXISTS idx_all_btc_trades_direction ON all_btc_trades (direction);
CREATE INDEX IF NOT EXISTS idx_all_eth_trades_direction ON all_eth_trades (direction);

-- Индексы для пользовательской системы
CREATE INDEX IF NOT EXISTS idx_users_email ON users (email);
CREATE INDEX IF NOT EXISTS idx_users_verification_token ON users (verification_token);
CREATE INDEX IF NOT EXISTS idx_users_reset_password_token ON users (reset_password_token);
CREATE INDEX IF NOT EXISTS idx_refresh_tokens_user_id ON refresh_tokens (user_id);
CREATE INDEX IF NOT EXISTS idx_refresh_tokens_expires_at ON refresh_tokens (expires_at);

-- =============================================================================
-- БАЗОВЫЕ ДАННЫЕ
-- =============================================================================

-- Создаем базовые роли
INSERT INTO roles (name, description)
SELECT 'ADMIN', 'System Administrator'
WHERE NOT EXISTS (SELECT 1 FROM roles WHERE name = 'ADMIN');

INSERT INTO roles (name, description)
SELECT 'USER', 'Regular User'
WHERE NOT EXISTS (SELECT 1 FROM roles WHERE name = 'USER');

INSERT INTO roles (name, description)
SELECT 'ANALYST', 'Data Analyst'
WHERE NOT EXISTS (SELECT 1 FROM roles WHERE name = 'ANALYST');

-- =============================================================================
-- КОММЕНТАРИИ К ТАБЛИЦАМ (ДОКУМЕНТАЦИЯ)
-- =============================================================================

COMMENT ON TABLE all_btc_trades IS 'Все BTC торги с биржи Deribit';
COMMENT ON TABLE all_eth_trades IS 'Все ETH торги с биржи Deribit';
COMMENT ON TABLE btc_block_trades IS 'BTC блок-торги с биржи Deribit';
COMMENT ON TABLE eth_block_trades IS 'ETH блок-торги с биржи Deribit';
COMMENT ON TABLE okx_btc_trades IS 'BTC торги с биржи OKX';
COMMENT ON TABLE okx_eth_trades IS 'ETH торги с биржи OKX';
COMMENT ON TABLE users IS 'Пользователи системы';
COMMENT ON TABLE roles IS 'Роли пользователей';
COMMENT ON TABLE user_roles IS 'Связь пользователей и ролей';
COMMENT ON TABLE refresh_tokens IS 'Refresh токены для аутентификации';

-- Готово! Базовая схема создана.
