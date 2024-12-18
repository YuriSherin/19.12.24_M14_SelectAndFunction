import logging
import sqlite3

def create_table(name_bd):
    """Функция создает таблицу Users"""
    try:
        logging.info((f'Попытка подключения к БД {name_bd}'))
        with sqlite3.connect(name_bd) as connection:
            logging.info((f'Подключение к БД {name_bd} выполнено УСЕШНО!'))
            cursor = connection.cursor()
            logging.info((f'Попытка выполнить команду создания таблицы Users'))
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS Users(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            email TEXT NOT NULL,
            age INTEGER,
            balance INTEGER NOT NULL)
            ''')
            connection.commit()
            logging.info((f'Команда создания таблицы Users выполнена УСПЕШНО!'))
    except sqlite3.Error as e:
        logging.error(f'Ошибка при работе с БД: {e}')
    finally:
        logging.info(f'Подключение к БД закрыто УСПЕШНО!')

def insert_one_line(name_bd:str, username:str, email:str, age:int, balance:int):
    """Функция вставляет одну запись в таблицу"""
    try:
        logging.info((f'Попытка подключения к БД {name_bd}'))
        with sqlite3.connect(name_bd) as connection:
            logging.info((f'Подключение к БД {name_bd} выполнено УСЕШНО!'))
            cursor = connection.cursor()
            logging.info((f'Попытка выполнить команду вставить запись <{username}, {email}, {age}, {balance}> в таблицу Users'))
            cursor.execute('INSERT INTO Users (username, email, age, balance) VALUES (?, ?, ?, ?)', (username, email, age, balance))
            connection.commit()
            logging.info((f'Количество УСПЕШНО вставленных записей в таблицу Users: {cursor.rowcount}'))
    except sqlite3.Error as e:
        logging.error(f'Ошибка при работе с БД: {e}')
    finally:
        logging.info(f'Подключение к БД закрыто УСПЕШНО!')


def insert_multiple_lines(name_bd:str, lines:list):
    """Функция вставляет несколько записей в таблицу"""
    try:
        logging.info((f'Попытка подключения к БД {name_bd}'))
        with sqlite3.connect(name_bd) as connection:
            logging.info((f'Подключение к БД {name_bd} выполнено УСЕШНО!'))
            cursor = connection.cursor()
            logging.info((f'Попытка выполнить команду вставить несколько записей в таблицу Users'))

            cursor.executemany('INSERT INTO Users (username, email, age, balance) VALUES (?, ?, ?, ?)', lines)
            connection.commit()
            logging.info((f'Количество УСПЕШНО вставленных записей в таблицу Users: {cursor.rowcount}'))
    except sqlite3.Error as e:
        logging.error(f'Ошибка при работе с БД: {e}')
    finally:
        logging.info(f'Подключение к БД закрыто УСПЕШНО!')


def update_one_line(name_bd:str, username:str, balance:int):
    """Функция обновляет одну запись в таблице"""
    try:
        logging.info((f'Попытка подключения к БД {name_bd}'))
        with sqlite3.connect(name_bd) as connection:
            logging.info((f'Подключение к БД {name_bd} выполнено УСЕШНО!'))
            cursor = connection.cursor()
            logging.info((f'Попытка выполнить команду обновить <{balance} для {username}> в таблице Users'))
            cursor.execute('UPDATE Users SET balance = ? WHERE username = ?', (balance, username))
            connection.commit()
            logging.info((f'Количество УСПЕШНО обновленных записей в таблице Users: {cursor.rowcount}'))
    except sqlite3.Error as e:
        logging.error(f'Ошибка при работе с БД: {e}')
    finally:
        logging.info(f'Подключение к БД закрыто УСПЕШНО!')


def update_multiple_lines(name_bd:str, lines:list):
    """Функция обновляет несколько записей в таблице"""
    try:
        logging.info((f'Попытка подключения к БД {name_bd}'))
        with sqlite3.connect(name_bd) as connection:
            logging.info((f'Подключение к БД {name_bd} выполнено УСЕШНО!'))
            cursor = connection.cursor()
            logging.info((f'Попытка выполнить команду обновить несколько записей в таблице Users'))

            cursor.executemany('UPDATE Users SET balance = ? WHERE username = ?', lines)
            connection.commit()
            logging.info((f'Количество УСПЕШНО обновленных записей в таблице Users: {cursor.rowcount}'))
    except sqlite3.Error as e:
        logging.error(f'Ошибка при работе с БД: {e}')
    finally:
        logging.info(f'Подключение к БД закрыто УСПЕШНО!')


def delete_one_line(name_bd:str, username:str):
    """Функция удаляет одну запись из таблицы"""
    try:
        logging.info((f'Попытка подключения к БД {name_bd}'))
        with sqlite3.connect(name_bd) as connection:
            logging.info((f'Подключение к БД {name_bd} выполнено УСЕШНО!'))
            cursor = connection.cursor()
            logging.info((f'Попытка выполнить команду удалить запись для {username}> из таблицы Users'))
            cursor.execute('DELETE FROM Users WHERE username = ?', (username,))
            connection.commit()
            logging.info((f'Количество УСПЕШНО удаленных записей из таблицы Users: {cursor.rowcount}'))
    except sqlite3.Error as e:
        logging.error(f'Ошибка при работе с БД: {e}')
    finally:
        logging.info(f'Подключение к БД закрыто УСПЕШНО!')


def delete_multiple_lines(name_bd:str, lines:list):
    """Функция удаляет несколько записей их таблицы"""
    try:
        logging.info((f'Попытка подключения к БД {name_bd}'))
        with sqlite3.connect(name_bd) as connection:
            logging.info((f'Подключение к БД {name_bd} выполнено УСЕШНО!'))
            cursor = connection.cursor()
            logging.info((f'Попытка выполнить команду удалить несколько записей из таблицы Users'))

            cursor.executemany('DELETE FROM Users WHERE username = ?', lines)
            connection.commit()
            logging.info((f'Количество УСПЕШНО удаленных записей из таблицы Users: {cursor.rowcount}'))
    except sqlite3.Error as e:
        logging.error(f'Ошибка при работе с БД: {e}')
    finally:
        logging.info(f'Подключение к БД закрыто УСПЕШНО!')


def select_all_where(name_bd, age):
    """Функция выбирает все записи из таблицы, где возраст не равен 60"""
    try:
        logging.info((f'Попытка подключения к БД {name_bd}'))
        with sqlite3.connect(name_bd) as connection:
            logging.info((f'Подключение к БД {name_bd} выполнено УСЕШНО!'))
            cursor = connection.cursor()
            logging.info((f'Попытка выполнить команду выбрать записи из таблицы Users, где возраст не равен {age}'))
            cursor.execute('SELECT * FROM Users WHERE age != ?', (age,))
            rows = cursor.fetchall()
            logging.info(f'Всего получено записей из таблицы Users: {len(rows)}')
            result = []         # формируем список строк
            for row in rows:
                s = f'Имя: {row[1]} | Почта: {row[2]} | Возраст: {row[3]}  | Баланс: {row[3]}'
                result.append(s)
            connection.commit()
    except sqlite3.Error as e:
        logging.error(f'Ошибка при работе с БД: {e}')
    finally:
        logging.info(f'Подключение к БД закрыто УСПЕШНО!')
        return result


def delete_one(name_bd:str, cmd:str, param):
    """Функция удаляет одну запись из таблицы,
    где cmd - строка команды, param - дополнительный параметр"""
    try:
        logging.info((f'Попытка подключения к БД {name_bd}'))
        with sqlite3.connect(name_bd) as connection:
            logging.info((f'Подключение к БД {name_bd} выполнено УСЕШНО!'))
            cursor = connection.cursor()
            logging.info((f'Попытка выполнить команду удалить запись из таблицы Users'))
            cursor.execute(cmd, (param,))
            connection.commit()
            logging.info((f'Количество УСПЕШНО удаленных записей из таблицы Users: {cursor.rowcount}'))
    except sqlite3.Error as e:
        logging.error(f'Ошибка при работе с БД: {e}')
    finally:
        logging.info(f'Подключение к БД закрыто УСПЕШНО!')


def function(name_bd:str, cmd:str):
    """Функция выполняет команду, переданную ей в аргументах,
    где cmd - строка команды, param - дополнительный параметр"""
    result = None
    try:
        logging.info((f'Попытка подключения к БД {name_bd}'))
        with sqlite3.connect(name_bd) as connection:
            logging.info((f'Подключение к БД {name_bd} выполнено УСЕШНО!'))
            cursor = connection.cursor()
            logging.info((f'Попытка выполнить функцию применительно к таблице Users'))
            cursor.execute(cmd)
            result = cursor.fetchone()[0]
            connection.commit()
            logging.info((f'функция выполнена УСПЕШНО применительно к таблице Users. Результат: {result}'))
    except sqlite3.Error as e:
        logging.error(f'Ошибка при работе с БД: {e}')
        result = None
    finally:
        logging.info(f'Подключение к БД закрыто УСПЕШНО!')
        return result
