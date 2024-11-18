"""
Модуль предназначен для предварительной обработки и загрузки данных в базу данных.
Он обеспечивает загрузку данных из файлов, их очистку от выбросов,
приведение типов данных к ожидаемым форматам, удаление дубликатов
и оптимизацию вставки данных в базу данных.


Функции:
- process_data: Основная функция, которая загружает данные из файлов,
  обрабатывает их, удаляет дубликаты, некорректные данные и строки с некорректными типами,
  и загружает в базу данных.
- clean_string_columns: Приводит столбцы датафрейма к нижнему регистру и удаляет пробелы.
- validate_data_types: Проверяет и приводит типы данных в столбцах датафрейма к ожидаемым,
  логгируя предупреждения в случае несоответствий.
- remove_outliers: Удаляет выбросы из датафрейма на основе межквартильного размаха,
  чтобы улучшить качество анализа данных.
- bulk_insert: Выполняет массовую вставку данных в таблицу базы данных
с использованием батч-обработки,
  что позволяет эффективно загружать большие объемы данных.
"""

import logging
import warnings
import os

import yaml

import pandas as pd

from psycopg2.extras import execute_values

from config.db_config import get_db_connection
from modules.create_tables import create_tables, create_database_if_not_exists

warnings.filterwarnings('ignore')

path = os.environ.get('PROJECT_PATH', '.')


def clean_string_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Приведение к нижнему регистру и удаление пробелов"""
    for col in df.select_dtypes(include=['object']).columns:
        # Проверка на содержание только строк
        if df[col].dtype == 'object' and df[col].notna().all():
            try:
                df[col] = df[col].str.lower().str.strip()
            except AttributeError:
                logging.warning('%s имеет некорректный тип', col)
                continue
    return df


def load_validation_config(cfg_path: str) -> dict:
    """Загружает конфигурацию валидации из YAML-файла"""
    with open(cfg_path, 'r') as f:
        return yaml.safe_load(f)


def validate_data_types(df: pd.DataFrame, expected_dtypes: dict) -> pd.DataFrame:
    """Проверка и приведение типов данных"""
    for col, dtype in expected_dtypes.items():
        if df[col].dtype != dtype:
            logging.warning(
                '%s имеет некорректный тип. Ожидался %s, а получен %s',
                col, dtype, df[col].dtype)
            try:
                df[col] = df[col].astype(dtype, errors='ignore')
            except Exception as e:
                logging.error('Произошла ошибка при приведении %s к типу %s: %s', col, dtype, e)
    return df


def remove_outliers(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Удаление выбросов"""
    q_one = df[column].quantile(0.25)
    q_three = df[column].quantile(0.75)
    iqr = q_three - q_one
    df = df[(df[column] >= (q_one - 1.5 * iqr)) & (df[column] <= (q_three + 1.5 * iqr))]
    return df


def bulk_insert(
        cursor,
        query: str, data: list,
        batch_size: int = 10000) -> None:
    """Массовое добавление данных в таблицу"""
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        execute_values(cursor, query, batch)


def load_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Обработка данных"""
    logging.info('Начало обработки данных')

    # Загрузка данных с обработкой ошибок
    try:
        df_sessions = pd.read_pickle('~/Downloads/ga_sessions.pkl')
        df_hits = pd.read_pickle('~/Downloads/ga_hits.pkl')
    except FileNotFoundError as e:
        logging.error('Файл не найден: %s', e)
        return
    except pd.errors.EmptyDataError:
        logging.error('Файл пуст')
        return
    except Exception as e:
        logging.error('Произошла ошибка: %s', e)
        return

    logging.info('Данные успешно загружены')

    # Удаление дубликатов
    df_sessions = df_sessions.drop_duplicates()
    df_hits = df_hits.drop_duplicates()

    # Преобразование к datetime
    df_sessions['visit_date'] = pd.to_datetime(df_sessions['visit_date'], errors='coerce')
    df_hits['hit_date'] = pd.to_datetime(df_hits['hit_date'], errors='coerce')

    # Удаление строк с некорректными датами
    df_sessions = df_sessions[pd.to_datetime(df_sessions['visit_date'], errors='coerce').notna()]
    df_hits = df_hits[pd.to_datetime(df_hits['hit_date'], errors='coerce').notna()]

    # Очистка строковых столбцов
    df_sessions = clean_string_columns(df_sessions)
    df_hits = clean_string_columns(df_hits)

    # Приведение столбцов к ожидаемым типам данных
    validation_cfg = load_validation_config(path + '/config/validation_cfg.yaml')

    df_sessions = validate_data_types(df_sessions, validation_cfg['validation']['sessions'])
    df_hits = validate_data_types(df_hits, validation_cfg['validation']['hits'])

    # Проверка логической целостности данных
    df_sessions = df_sessions[df_sessions['utm_medium'].isin([
        'organic', 'blogger_channel', 'blogger_stories', 'banner', 'cpc',
        'referral', 'cpm', '(none)', 'app', 'email', 'smm', 'vk_smm',
        'push', 'stories', 'tg', 'smartbanner'
    ])]

    # Фильтруем df_hits по существующим session_id из df_sessions
    valid_session_ids = set(df_sessions['session_id'].unique())
    df_hits = df_hits[df_hits['session_id'].isin(valid_session_ids)]

    # Заполнение пустых значений
    df_sessions.fillna({
        'utm_source': 'unknown',
        'utm_medium': 'unknown',
        'utm_campaign': 'unknown',
        'utm_adcontent': 'unknown',
        'utm_keyword': 'unknown',
        'device_os': 'unknown',
        'device_brand': 'unknown',
        'device_model': 'unknown'
    }, inplace=True)

    df_hits.fillna({
        'hit_referer': 'unknown',
        'event_label': 'unknown',
        'event_value': 'unknown'
    }, inplace=True)

    # Удаление аномальных значений
    df_sessions = remove_outliers(df_sessions, 'visit_number')
    df_hits = remove_outliers(df_hits, 'hit_number')

    logging.info('Обработка данных завершена')
    return df_sessions, df_hits


def insert_data() -> None:
    """Вставка данных в бд"""
    df_sessions, df_hits = load_data()

    logging.info('Загрузка данных в бд')
    # Создание бд и таблиц
    try:
        create_database_if_not_exists()
        connection = get_db_connection('sberdb')
        create_tables(connection)

        with connection.cursor() as cursor:
            # Загружаем известные сессии
            insert_sessions_query = """
                INSERT INTO sessions (
                    session_id, utm_source, utm_medium, 
                    visit_date, visit_number, device_os, 
                    device_brand, device_model
                )
                VALUES %s
                ON CONFLICT (session_id) DO NOTHING
            """
            sessions_data = [(
                row['session_id'],
                row['utm_source'],
                row['utm_medium'],
                row['visit_date'],
                row['visit_number'],
                row['device_os'],
                row['device_brand'],
                row['device_model']
            ) for index, row in df_sessions.iterrows()]

            bulk_insert(cursor, insert_sessions_query, sessions_data)
            connection.commit()

            # Находим недостающие айди сессий
            cursor.execute("SELECT session_id FROM sessions")
            existing_sessions = set(row[0] for row in cursor.fetchall())
            missing_sessions = set(df_hits['session_id']) - existing_sessions

            if missing_sessions:
                logging.info('Создаём %d недостающих sessions', len(missing_sessions))

                # Находим минимальную дату для недостающих сессий
                min_dates = df_hits.groupby('session_id')['hit_date'].min().to_dict()
                # Создаём записи для отсутствующих сессий
                missing_sessions_data = [(
                    session_id,
                    'unknown',  # utm_source
                    'unknown',  # utm_medium
                    min_dates.get(session_id),  # visit_date
                    1,  # visit_number
                    'unknown',  # device_os
                    'unknown',  # device_brand
                    'unknown'  # device_model
                ) for session_id in missing_sessions]

                bulk_insert(cursor, insert_sessions_query, missing_sessions_data)
                connection.commit()
                logging.info('Создание недостающих sessions завершено')

            # Удаляем дубликаты
            df_hits = df_hits.drop_duplicates(subset=['session_id', 'hit_number'])

            # Загрузка хитов
            logging.info('Загрузка хитов')
            hits_data = [(
                row['session_id'],
                row['hit_date'],
                row['hit_number'],
                row['event_label']
            ) for index, row in df_hits.iterrows()]

            insert_hits_query = """
                INSERT INTO hits (session_id, hit_date, hit_number, event_label)
                VALUES %s
                ON CONFLICT (session_id, hit_number) DO UPDATE 
                SET 
                    hit_date = EXCLUDED.hit_date,
                    event_label = EXCLUDED.event_label
            """

            bulk_insert(cursor, insert_hits_query, hits_data)
            connection.commit()
            logging.info('Загрузка хитов завершена')

            # Выводим статистику
            cursor.execute("SELECT COUNT(*) FROM sessions")
            total_sessions = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM hits")
            total_hits = cursor.fetchone()[0]

            logging.info('''
                'Итоги загрузки:
                Всего sessions в базе: %d
                Добавлено автоматически sessions: %d
                Всего hits в базе: %d''',
                total_sessions, len(missing_sessions), total_hits
            )

    except Exception as e:
        logging.error('Произошла ошибка при загрузке данных: %s', e)
        connection.rollback()
    finally:
        if connection:
            connection.close()
