"""
Содержит функцию get_db_connection для подключения к базе данных.

"""
import logging
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from config.user_pass import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT


def get_db_connection(DB_NAME):
    """Подключение к бд"""
    # DB_USER, DB_PASSWORD от рута к postgres
    if None in [DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME]:
        logging.error('Одна или несколько переменных окружения не установлены')
        raise ValueError('Неверные параметры подключения к базе данных')

    try:
        connection = psycopg2.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME
        )
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        logging.info('Подключение к базе успешно установлено')
        return connection

    except Exception as e:
        logging.error('Ошибка подключения к базе данных: %s', e)
        raise
