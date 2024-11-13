"""
Модуль config.user_pass.py содержит переменные, которые используются для подключения к базе данных.
В нем содержатся:
- DB_USER: имя пользователя от рута psql
- DB_PASSWORD: пароль от рута psql
- DB_HOST: хост, на котором находится бд
- DB_PORT: порт, на котором находится бд
"""
import os

path = os.environ.get('PROJECT_PATH', '.')

DB_USER = 'your_user'
DB_PASSWORD = 'your_password'
DB_HOST = 'localhost'
DB_PORT = '5432'
