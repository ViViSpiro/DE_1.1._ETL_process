-- Создание схемы DS
CREATE SCHEMA IF NOT EXISTS DS;

-- Создание схемы для логов
CREATE SCHEMA IF NOT EXISTS LOGS;

-- Создание таблицы для логов
CREATE TABLE IF NOT EXISTS LOGS.etl_logs (
    log_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status VARCHAR(20) NOT NULL,
    records_processed INTEGER DEFAULT 0,
    error_message TEXT
);