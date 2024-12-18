import BD_SQLite3 as bd
import logging
NAME_BD = 'not_telegram.db'     # имя БД

def previous_task():
    """Функция решения предыдущей задачи"""
    bd.create_table(NAME_BD)    # создадим БД

    # формируем списки для вставки, обновления и удаления записей в БД
    lst_insert = [(f'User{str(i)}', f'example{str(i)}@gmail.com', i*10, 1000) for i in range(1, 11)]
    lst_update = [(500, f'User{str(i)}') for i in range(1, 11, 2)]
    lst_delete = [(f'User{str(i)}',) for i in range(1, 11, 3)]

    bd.insert_multiple_lines(NAME_BD, lst_insert)       # вставили записи в БД из списка
    bd.update_multiple_lines(NAME_BD, lst_update)       # обновили записи в БД из списка
    bd.delete_multiple_lines(NAME_BD, lst_delete)       # удалили записи из БД по списку

def main():
    """Функция решения текущей задачи"""
    str_command = 'DELETE FROM Users WHERE id = ?'      # удалим из БД пользователя с id = 6
    bd.delete_one(NAME_BD, str_command, 6)

    str_command = 'SELECT COUNT(*) FROM Users'          # получим из БД количество записей пользователей
    count_users = bd.function(NAME_BD, str_command)

    str_command = 'SELECT SUM(balance) FROM Users'      #  получим из БД сумму баланса всех пользователей
    sum_balance_users = bd.function(NAME_BD, str_command)

    str_command = 'SELECT AVG(balance) FROM Users'      # получим из БД среднее значение для баланса пользователей
    avg_balance_users = bd.function(NAME_BD, str_command)
    print(f'Средний баланс всех пользователей: {avg_balance_users}')    # выведем рузальтат на печать



if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, filename="py_log.log",filemode="w", encoding='utf-8',
                        format="%(asctime)s %(levelname)s %(message)s")
    # previous_task()   # функция решения предыдущей задачи
    main()              # функция решения текущей задачи



