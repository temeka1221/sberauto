"""
Этот модуль предназначен для обработки и загрузки данных в базу данных из JSON файлов.
Он включает в себя функции для обработки данных сессий и хитов, а также их загрузки в базу данных.
Функции в этом модуле:
- process_sessions_data: Обрабатывает данные сессий из JSON и
возвращает данные для загрузки и идентификаторы отсутствующих сессий.
- process_hits_data: Обрабатывает данные хитов из JSON и возвращает отфильтрованный список хитов.
- process_and_load_json_data: Основная функция,
которая обрабатывает JSON файлы и загружает данные в базу данных.
"""

import json
import os
import logging

from datetime import datetime
from glob import glob

from config.db_config import get_db_connection
from modules.create_tables import create_tables, create_database_if_not_exists
from modules.data_pipeline import bulk_insert

path = os.environ.get('PROJECT_PATH', '.')


def process_sessions_data(data: dict) -> tuple[list, set]:
    """Обработка данных сессий из json"""
    session_data = []
    missing_sessions_ids = set()

    for _, sessions in data.items():
        for session in sessions:
            session_id = session['session_id']
            utm_source = session.get('utm_source', 'unknown')
            utm_medium = session.get('utm_medium', 'unknown')
            visit_date = datetime.strptime(session['visit_date'], '%Y-%m-%d').date()
            visit_number = session['visit_number']
            device_os = session.get('device_os', 'unknown')
            device_brand = session.get('device_brand', 'unknown') \
                if session.get('device_brand') not in [None, 'NaN'] else 'unknown'
            device_model = session.get('device_model', 'unknown') \
                if session.get('device_model') not in [None, 'NaN'] else 'unknown'

            session_data.append((
                session_id, utm_source, utm_medium,
                visit_date, visit_number,
                device_os, device_brand, device_model
            ))

            missing_sessions_ids.add(session_id)

    return session_data, missing_sessions_ids


def process_hits_data(data: dict, valid_sessions_ids: set) -> list:
    """Обработка данных хитов из json"""
    hits_data = []
    for _, sessions in data.items():
        for hit in sessions:
            session_id = hit['session_id']
            hit_date = datetime.strptime(hit['hit_date'], '%Y-%m-%d').date()
            hit_number = hit['hit_number']
            event_label = hit['event_label']

            if session_id in valid_sessions_ids:
                hits_data.append((session_id, hit_date, hit_number, event_label))

    return hits_data


def process_and_load_json_data(data_dir=path + '/json_data') -> None:
    """Обработка и загрузка данных в бд из json"""
    try:
        # Коннект к бд
        create_database_if_not_exists()
        connection = get_db_connection('sberdb')
        create_tables(connection)

        # Обработка json
        for filename in glob(os.path.join(data_dir, '*.json')):
            logging.info('Открываю файл: %s', filename)
            with open(filename, 'r') as f:
                data = json.load(f)

                if 'ga_sessions' in filename:
                    sessions_data, missing_sessions_ids = process_sessions_data(data)
                    logging.info(
                        'Обработано %s сессий, найдено %s недостающих сессий',
                        len(sessions_data), len(missing_sessions_ids)
                    )
                    # Загрузка сессий в бд
                    insert_sessions_query = """
                                    INSERT INTO sessions (
                                        session_id, utm_source, utm_medium, 
                                        visit_date, visit_number, device_os, 
                                        device_brand, device_model
                                    )
                                    VALUES %s
                                    ON CONFLICT (session_id) DO UPDATE
                                    SET
                                        utm_source = EXCLUDED.utm_source,
                                        utm_medium = EXCLUDED.utm_medium,
                                        visit_date = EXCLUDED.visit_date,
                                        visit_number = EXCLUDED.visit_number,
                                        device_os = EXCLUDED.device_os,
                                        device_brand = EXCLUDED.device_brand,
                                        device_model = EXCLUDED.device_model
                    """
                    bulk_insert(connection.cursor(), insert_sessions_query, sessions_data)
                    connection.commit()

                elif 'ga_hits' in filename:
                    with connection.cursor() as cursor:
                        cursor.execute('SELECT session_id FROM sessions')
                        existing_sessions = set(row[0] for row in cursor.fetchall())

                    hits_data = process_hits_data(data, existing_sessions)
                    logging.info('Обработано %s хитов', len(hits_data))

                    # Загрузка хитов в бд
                    insert_hits_query = """
                        INSERT INTO hits (session_id, hit_date, hit_number, event_label)
                        VALUES %s
                        ON CONFLICT (session_id, hit_number) DO UPDATE
                        SET
                            hit_date = EXCLUDED.hit_date,
                            event_label = EXCLUDED.event_label
                        
                    """
                    bulk_insert(connection.cursor(), insert_hits_query, hits_data)
                    connection.commit()

            logging.info('Обработка json завершена, все данные загружены')

    except Exception as e:
        logging.error('Произошла ошибка: %s', e)
        connection.rollback()

    finally:
        if connection:
            connection.close()
