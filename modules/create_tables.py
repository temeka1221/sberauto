"""
Модуль содержит функции, необходимые для создания базы данных и таблиц в ней.
- create_tables: создает таблицы sessions и hits в базе данных, если они еще не существуют.
- create_database_if_not_exists: проверяет наличие базы данных и создает ее в случае отсутствия.
- Используется библиотека psycopg2 для взаимодействия с базой данных PostgreSQL.
- Конфигурационные параметры для подключения к базе данных импортируются из модуля user_pass.
"""
import logging

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from config.user_pass import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT


def create_tables(connection):
    """Создание таблиц в бд"""
    try:
        with connection.cursor() as cursor:
            # Таблица sessions
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sessions (
                    session_id VARCHAR PRIMARY KEY,
                    utm_source VARCHAR NOT NULL DEFAULT 'unknown',
                    utm_medium VARCHAR NOT NULL DEFAULT 'unknown',
                    visit_date DATE NOT NULL,
                    visit_number INTEGER NOT NULL CHECK (visit_number > 0),
                    device_os VARCHAR NOT NULL DEFAULT 'unknown',
                    device_brand VARCHAR NOT NULL DEFAULT 'unknown',
                    device_model VARCHAR NOT NULL DEFAULT 'unknown'
            )
            """)
            # Таблица hits
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS hits (
                    session_id VARCHAR NOT NULL,
                    hit_date DATE NOT NULL,
                    hit_number INTEGER NOT NULL CHECK (hit_number > 0),
                    event_label VARCHAR NOT NULL,
                    PRIMARY KEY (session_id, hit_number),
                    FOREIGN KEY (session_id)
                        REFERENCES sessions (session_id)
                        ON DELETE CASCADE
                )
            """)
            # Индексы для оптимизации запросов
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_sessions_visit_date
                ON sessions(visit_date)
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_hits_hit_date
                ON hits(hit_date)
            """)

            connection.commit()
            logging.info('Таблицы успешно созданы')

    except Exception as e:
        logging.error('Ошибка при создании таблиц: %s', e)
        raise


def create_database_if_not_exists():
    """Создание бд, если она не существует"""
    try:
        conn = psycopg2.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            dbname='postgres'
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        with conn.cursor() as cursor:
            DB_NAME = 'sberdb'
            cursor.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{DB_NAME}'")
            exists = cursor.fetchone()
            if not exists:
                cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(DB_NAME)))
                logging.info('База данных %s успешно создана', DB_NAME)
            else:
                logging.info('База данных %s уже существует', DB_NAME)
    except Exception as e:
        logging.error('Ошибка при создании бд: %s', e)
        raise
    finally:
        if conn:
            conn.close()
