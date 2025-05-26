from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
import os
from dotenv import load_dotenv
import asyncpg
from asyncpg.pool import Pool
from datetime import datetime, timedelta
import pandas as pd
import requests
import logging
import io
import typing
import json

if os.path.exists('.env'):
    load_dotenv('.env')


class Hepl_work_by_postgre:
    """
    Класс риализующий дополнительную логику работы с базой данных пользователей ТГ бота

    :param alch_pg: не асинхронное соединение с БД Postgres
    :type alch_pg: Engine

    :param dct_user_state: словарь собираемый в момент инициализации экземпляра класса
                           и необходимый для проверки состояния пользователей
    :type dct_user_state: dict

    :param pool_aeforecast: пул асинхронных соединений для работы с бд Postgre Агроэкспорт
    :type pool_aeforecast: Pool | null

    """

    def __init__(self):
        self.alch_pg = create_engine(
            f"postgresql://{os.getenv('USER_NAME_PG')}:{os.getenv('PASSWORD_PG')}@{os.getenv('HOST_PG')}:{os.getenv('PORT_PG')}/{os.getenv('DATABASE_PG')}")
        self.dct_user_state = self.create_dct()
        self.pool_aeforecast = None
        self.dict_dag_state = {'success': '✅',
                               'running': '▶️',
                               'failed': '⛔️'}

    def create_dct(self) -> dict:
        """
        Возвращает словарь состояний каждого из пользователей роли admin
        необходимо для валидации состояния в callback_query_handler

        :return: словарь состояний пользователей
        """
        df_state = pd.read_sql("""SELECT * 
                                      FROM ot 
                                      WHERE chat_id in (SELECT chat_id FROM ot WHERE role_id IN (2, 4))""",
                               con=self.alch_pg)
        return {k: v for k, v in zip(df_state.chat_id.tolist(), df_state.current_state.tolist())}

    async def create_pool(self):
        """
        Создает асинхронный пул соединений для баз данных.
        Если такие еще не были созданы
        """
        if self.pool_aeforecast is None:
            self.pool_aeforecast = await asyncpg.create_pool(user=os.getenv('USER_NAME_PG'),
                                                             password=os.getenv('PASSWORD_PG'),
                                                             host=os.getenv('HOST_PG'),
                                                             port=os.getenv('PORT_PG'),
                                                             database=os.getenv('DATABASE_PG'), max_size=2, min_size=1)
        else:
            pass

    @staticmethod
    async def write_hello_func() -> str:
        """
        Рассчитывает приветственную фразу в зависимости от текущего времени
        :return: строку приветствия
        """
        hour = datetime.now().hour
        if 1 < hour <= 9:
            return 'Доброе утро'
        elif 9 < hour <= 13:
            return 'Добрый день'
        elif 13 < hour <= 20:
            return 'Добрый вечер'
        elif 20 <= hour <= 1:
            return 'Доброй ночи'

    async def check_user(self, user_id: int) -> bool:
        """
        Проверяет есть ли такой пользователь и является ли он администратором/пользователем-администратором

        :param user_id: chat_id пользователя

        :return: True or False
        """
        await self.create_pool()

        async with self.pool_aeforecast.acquire() as conn:
            return await conn.fetchval(
                """SELECT EXISTS(SELECT 1 FROM ot 
                   WHERE chat_id = $1 
                   AND uu_user_id IS NOT NULL 
                   AND role_id IN (2, 4)
                   AND active_user)""",
                user_id)

    async def update_state_user(self, user_id: int, state: str, update_dag_tag_func: typing.Optional[bool] = False):
        """
        Обновляет состояние пользователя(администратора) в БД и словаре
        для каждого бота используется своя таблица состояний

        :param user_id: chat_id администратора

        :param state: состояние в котором будет находиться администратора

        :param update_dag_tag_func: переменная необходимая для обновления dag_tag_func_state,
                                    параметра отвечающего за информацию о возвращении в определенное меню
                                    (предыдущее состоияние из которого мы попали в interaction_with_dag)
        """
        await self.create_pool()

        async with self.pool_aeforecast.acquire() as conn:
            if update_dag_tag_func:
                await conn.execute(
                    """UPDATE ot SET dag_tag_func_state = current_state, current_state = $1 WHERE chat_id = $2""",
                    state, user_id)
            else:
                await conn.execute(
                    """UPDATE ot SET current_state = $1 WHERE chat_id = $2""",
                    state, user_id)
        self.dct_user_state[user_id] = state

    async def get_dag_tag_func_state(self, user_id: int) -> str:
        """
        :param user_id: chat_id пользователя

        :return: значение из столбца dag_tag_func_state, т.е. значение state
                 из какой функции было вызвано меню управления DAG
        """
        async with self.pool_aeforecast.acquire() as conn:
            return await conn.fetchval("""SELECT dag_tag_func_state 
                                    FROM ot
                                    WHERE chat_id = $1""", user_id)

    async def get_access_section(self, user_id: int, message_text: str) -> bool:
        """
        Провермяем есть указанные раздел в доступе у роли, которой наделен пользователь

        :param user_id: chat_id пользователя

        :param message_text: текст сообщения пользователя (наименование раздела в боте)

        :return: True or False
        """
        if len(message_text) == 0:
            return False
        await self.create_pool()
        async with self.pool_aeforecast.acquire() as conn:
            return await conn.fetchval("""SELECT EXISTS(SELECT 1 FROM le 
                    WHERE role_id = (SELECT role_id FROM ot WHERE chat_id = $1) AND button_names LIKE('%' || $2 || '%'))""",
                                       user_id, message_text)

    async def get_pagination_status(self, user_id: int, status: str = 'default') -> int:
        """
        Возвращает пагинацию клавиатуры для указанного пользователя в зависимости от переданного статуса
        - default: пользователь на первой страницу
        - next: итерируется дальше по списку страниц
        - back: итерируется к началу по списку страниц

        :param user_id: chat_id пользователя

        :param status: положение, куда двигается пользователь или только начинает работу с клавитатурой
                       (default)

        :return: номер страницы
        """
        await self.create_pool()
        if status == 'default':
            async with self.pool_aeforecast.acquire() as conn:
                await conn.execute(
                    """UPDATE us SET status_user_etl_bot = 0 WHERE chat_id = $1""", user_id)
                return 0
        elif status == 'next':
            async with self.pool_aeforecast.acquire() as conn:
                pagen = await conn.fetchval(
                    """SELECT status_user_etl_bot FROM us WHERE chat_id = $1""", user_id)
                pagen += 1
                await conn.execute(
                    """UPDATE us SET status_user_etl_bot = $2 WHERE chat_id = $1""", user_id,
                    pagen)
                return pagen
        elif status == 'back':
            async with self.pool_aeforecast.acquire() as conn:
                pagen = await conn.fetchval(
                    """SELECT status_user_etl_bot FROM us WHERE chat_id = $1""", user_id)
                pagen = 0 if pagen == 0 else pagen - 1
                await conn.execute(
                    """UPDATE us SET status_user_etl_bot = $2 WHERE chat_id = $1""", user_id,
                    pagen)
                return pagen
        elif status == 'current_value':
            async with self.pool_aeforecast.acquire() as conn:
                pagen = await conn.fetchval(
                    """SELECT status_user_etl_bot FROM us WHERE chat_id = $1""", user_id)
                return pagen

    async def check_timeout_operation(self, operation_name: str, field: str, timeout: int) -> bool:
        """
        Проверяем, прошел ли тайм-аут на переданной операции

        :param operation_name: наименоване операции (пример, Проверка_Сертификатов)

        :param field: какую часть из времени мы вытаскиваем (day, hour, minute и тд.)

        :param timeout: сколько должно было пройти времени с момента последнего запуска операции

        :return: True or False
        """

        async with self.pool_aeforecast.acquire() as conn:
            value = await conn.fetchval("""SELECT CASE 
                                        WHEN $2 = 'day' 
                                            THEN DATE_PART($2, NOW() - timeout_operation) >= $3
                                        WHEN $2 = 'hour'
                                            THEN (DATE_PART($2, NOW() - timeout_operation) + DATE_PART('day', NOW() - timeout_operation) * 24) >= $3
                                        WHEN $2 = 'minute'
                                            THEN (DATE_PART($2, NOW() - timeout_operation) + DATE_PART('hour', NOW() - timeout_operation) * 60 + DATE_PART('day', NOW() - timeout_operation) * 1440) >= $3
                                        END
                                        FROM on 
                                        WHERE operation_name = $1""", operation_name, field, timeout)
            return True if value is None else value

    @staticmethod
    async def trigger_dag(dag_id: str, json_conf: dict) -> list[str | int]:
        """
        Триггер DAG, id которого был передан

        :param dag_id: id DAG, который будет запускать в ручную

        :param json_conf: набор параметров для конкретного DAG

        :return: список с состоянием, удачно был запущен DAG или что-то пошло не так
        """

        head = {'Content-Type': 'application/json'}
        json_conf = {'conf': json_conf}
        response_json = requests.post(os.getenv('TRIGGER_URL_DAG').format(dag_id=dag_id),
                                      auth=(os.getenv('AIRFLOW_USER'), os.getenv('AIRFLOW_PASSWORD')), headers=head,
                                      json=json_conf).json()

        if 'status' in response_json:
            return [response_json['detail'], response_json['status']]
        else:
            return [response_json['run_type']]

    @staticmethod
    async def delete_last_dag_run(dag_id: str) -> str:
        """
        Удаляет последний dag_run переданного dag

        :param dag_id: id дага, чей последний запуск нужно удалить

        :return: сообщение со статусом операции
        """

        need_datetime = (datetime.now() - timedelta(days=10)).isoformat() + "Z"
        head = {'Content-Type': 'application/json'}

        list_dag_runs = requests.get(os.getenv('TRIGGER_URL_DAG').format(dag_id=dag_id),
                                     auth=(os.getenv('AIRFLOW_USER'), os.getenv('AIRFLOW_PASSWORD')),
                                     headers=head,
                                     params={'start_date_gte': need_datetime}).json()['dag_runs']
        if len(list_dag_runs) == 0:
            return 'За последние 🔟 дней было 0️⃣ запусков DAG'
        else:
            last_dag_run_id = list_dag_runs[-1]['dag_run_id']
            response_delete = requests.delete(os.getenv('DELETE_DAG_RUN').format(dag_id=dag_id, dag_run_id=last_dag_run_id),
                                              auth=(os.getenv('AIRFLOW_ADMIN'), os.getenv('AIRFLOW_PASS_ADMIN')),
                                              headers=head)
            return (f'✅ Удаление {last_dag_run_id} прошло успешно' if response_delete.text == ''
                    else f"status = {response_delete.json()['status']} \ntitle = {response_delete.json()['title']} \ntype = {response_delete.json()['type']}")

    async def get_alert_description(self, user_id: int) -> list[dict]:
        """
        Пример возвращаемых данных:
            [{'alert_id': 2, 'type_alert': 'Обновление данных в БД', 'status_alert': True},
             {'alert_id': 1, 'type_alert': 'Ветеринарные сертификаты', 'status_alert': True}]

        :param user_id: chat_id пользователя

        :return: список словарей с необходимой информацией для каждого пользователя
        """
        await self.create_pool()
        async with self.pool_aeforecast.acquire() as conn:
            query_result = await conn.fetch("""SELECT alert_id, type_alert, status_alert 
                                          FROM on 
                                          JOIN on USING(alert_id)
                                          WHERE chat_id = $1""", user_id)

        return [dict(row) for row in query_result]

    async def alert_status_update(self, user_id: int, alert_id: int):
        """
        Меняет статус алерта на противположные:
        Пользователь был подписан на алерт(True), выбрал соответствующую кнопку и изменил свой статус на отписан(False)

        :param user_id: chat_id пользователя

        :param alert_id: идентификатор нужного алерта

        :return:
        """
        async with self.pool_aeforecast.acquire() as conn:
            await conn.execute(
                f"""UPDATE on 
                   SET status_alert = CASE WHEN status_alert = True THEN False ELSE True END 
                   WHERE alert_id = $1 AND chat_id = $2""", alert_id, user_id)

    async def get_missing_countries(self, bytes_path: bytes) -> str:
        """
        Находит страны, которые есть в ITC, но которых нет в БД в конкретном году

        :param bytes_path: скаченный фалй в telegram ботом

        :return: название файла, куда был сохранен результат
        """
        txt_bytes = io.BytesIO(bytes_path)

        # Данные из ITC
        df_itc = pd.read_table(txt_bytes).iloc[:, :13].rename(columns=lambda x: str(x).replace(' ', '_'))

        # Данные из БД
        df_countrys_in_db = pd.read_sql("""SELECT year, name_itc 
                                           FROM on l
                                           LEFT JOIN (SELECT code, name_itc 
                                           FROM on 
                                           WHERE length(name_itc) > 0
                                           GROUP BY code, name_itc ) r ON l.reporter_code = r.code""",
                                        con=self.alch_pg, dtype={'year': str})

        # Итерируемся по колонкам датафрейма itc и просматриваем каждую страну
        # если такой нет в списке, до добавляем в словарь
        dct_need_country = {'year': [], 'country': []}
        for column in list(df_itc.columns)[1:]:
            list_country_by_year = list(df_countrys_in_db.query('year == @column').name_itc)
            for country in list(df_itc[df_itc[f'{column}'] == 1].Countries_and_Territories):
                try:
                    if country not in list_country_by_year and country not in dct_need_country['country']:
                        dct_need_country['year'].append(column)
                        dct_need_country['country'].append(country)
                except KeyError:
                    continue

        # Сохраняем результат в файл
        exlsx_df = pd.DataFrame(dct_need_country)
        with pd.ExcelWriter('new_country_itc.xlsx') as writer:
            exlsx_df.to_excel(writer, sheet_name='Перечень стран', index=False, na_rep='NaN')
            for column in exlsx_df:
                column_width = max(exlsx_df[column].astype(str).map(len).max(), len(column))
                col_idx = exlsx_df.columns.get_loc(column)
                writer.sheets['Перечень стран'].set_column(col_idx, col_idx, column_width)
            writer.sheets['Перечень стран'].set_default_row(30)
        return 'new_country_itc.xlsx'

    async def update_etl_choose_dag(self, user_id: int, choose_operation_name: str, dag_id: str):
        """
        Обновляем название DAG и его id в сервисной таблице

        :param user_id: chat_id пользователя

        :param choose_operation_name: название операции, как она записана в БД

        :param dag_id: id DAG в airflow

        :return:
        """
        async with self.pool_aeforecast.acquire() as conn:
            await conn.execute("""UPDATE se 
                            SET choose_operation_name = $1,
                            dag_id = $2
                            WHERE chat_id = $3""", choose_operation_name, dag_id, user_id)

    async def update_etl_choose_variable(self, user_id: int, variable_name: typing.Optional[str] = None,
                                         variable_value: typing.Optional[str] = None):
        """
        Обновление параметров DAG в зависимости от переданных значений

        :param user_id: chat_id пользователя

        :param variable_name: необязательная переменная - имя меняемого параметра

        :param variable_value: необязательная переменная - новое значение выбранного параметра

        :return:
        """
        async with self.pool_aeforecast.acquire() as conn:
            # Обновляем имя параметра
            if variable_name:
                await conn.execute("""UPDATE se 
                            SET variable_name = $1
                            WHERE chat_id = $2""", variable_name, user_id)
            # Обновляем значение параметра
            else:
                await conn.execute("""UPDATE se 
                                            SET variable_value = $1
                                            WHERE chat_id = $2""", variable_value, user_id)

    async def get_choose_operation_name(self, user_id: int) -> str:
        """
        :param user_id: chat_id пользователя

        :return: выбранный пользователем DAG
        """
        await self.create_pool()

        async with self.pool_aeforecast.acquire() as conn:
            return await conn.fetchval("""SELECT choose_operation_name 
                                          FROM se 
                                          WHERE chat_id = $1""", user_id)

    async def get_dag_id(self, user_id: int) -> str:
        """
        :param user_id: chat_id пользователя

        :return: id DAG, который указан в airflow
        """
        async with self.pool_aeforecast.acquire() as conn:
            return await conn.fetchval("""SELECT dag_id 
                                          FROM se 
                                          WHERE chat_id = $1""", user_id)

    async def get_variables_dag(self, operation_name: str, user_id: int) -> dict:
        """
        :param operation_name: выбранный пользователем DAG

        :param user_id: chat_id пользователя

        :return: сформированный словарь параметров для запуска DAG
        """
        async with self.pool_aeforecast.acquire() as conn:
            await conn.set_type_codec(
                'json',
                encoder=json.dumps,
                decoder=json.loads,
                schema=''
            )

            dict_variables = await conn.fetchval("""SELECT variables_dag::json 
                                          FROM on 
                                          WHERE operation_name = $1""", operation_name)

        # Для отправки сообщения конкретному пользователю
        if 'chat_id' in dict_variables:
            dict_variables['chat_id'] = user_id
        return dict_variables

    async def get_timeout_operation_value(self, operation_name: str) -> dict:
        """
        :param operation_name: выбранный пользователем DAG

        :return: словарь с параметрами timeout триггера DAG
        """
        async with self.pool_aeforecast.acquire() as conn:
            await conn.set_type_codec(
                'json',
                encoder=json.dumps,
                decoder=json.loads,
                schema=''
            )

            return await conn.fetchval("""SELECT timeout_operation_value::json
                                          FROM on 
                                          WHERE operation_name = $1""", operation_name)

    async def get_list_variables_dag(self, operation_name: str) -> list:
        """
        :param operation_name: выбранный пользователем DAG

        :return: список параметов запуска DAG
        """
        async with self.pool_aeforecast.acquire() as conn:
            await conn.set_type_codec(
                'json',
                encoder=json.dumps,
                decoder=json.loads,
                schema=''
            )

            variables_dag = await conn.fetchval("""SELECT variables_dag::json
                                          FROM on 
                                          WHERE operation_name = $1""", operation_name)
        return [i for i in variables_dag]

    async def get_variable_name(self, user_id: int) -> str:
        """
        :param user_id: chat_id пользователя

        :return: значение параметра, который выбрал пользователь
        """
        await self.create_pool()

        async with self.pool_aeforecast.acquire() as conn:
            return await conn.fetchval("""SELECT variable_name 
                                          FROM se 
                                          WHERE chat_id = $1""", user_id)

    async def get_variable_value(self, user_id: int) -> str:
        """
        :param user_id: chat_id пользователя

        :return: новое значение выбранного параметра
        """
        async with self.pool_aeforecast.acquire() as conn:
            return await conn.fetchval("""SELECT variable_value 
                                          FROM se 
                                          WHERE chat_id = $1""", user_id)

    async def update_variables_dag(self, user_id: int, flag_update: str):
        """
        Функция обновляет значения выбранных параметров в зависимости от переданного flag_update

        :param user_id: chat_id пользователя

        :param flag_update: значение какого столбца мы обновляем (variables_dag/timeout_operation_value)

        :return:
        """
        variable_name = await self.get_variable_name(user_id)
        variable_value = await self.get_variable_value(user_id)
        operation_name = await self.get_choose_operation_name(user_id)
        json_path = [variable_name]

        # Тернарный оператор для обработки строкового значения
        # так как asyncpg требует преобразовать его к типу json
        json_value = variable_value if variable_value.isdigit() else json.dumps(variable_value)

        async with self.pool_aeforecast.acquire() as conn:
            if flag_update == 'variables_dag':
                await conn.execute("""UPDATE on
                                SET
                                 variables_dag = jsonb_set(
                                                            variables_dag,
                                                            $2,
                                                            $3
                                                           )
                                WHERE operation_name = $1""", operation_name, json_path, json_value)
            else:
                await conn.execute("""UPDATE on
                                                SET
                                                 timeout_operation_value = jsonb_set(
                                                                            timeout_operation_value,
                                                                            $2,
                                                                            $3
                                                                           )
                                                WHERE operation_name = $1""", operation_name, json_path, json_value)

    async def get_current_variable_value(self, user_id: int, name_columns: str,
                                         flag_return: typing.Optional[bool] = False) -> str | int | list:
        """
        :param user_id: chat_id пользователя

        :param name_columns: имя столбца откуда мы будем брать значение

        :param flag_return: флаг отвечающий за тип возвращаемой переменной,
                            True - возвращаем список
                            False - возвращаем строку или число

        :return: текущее значение переменной
        """
        variable_name = await self.get_variable_name(user_id)
        operation_name = await self.get_choose_operation_name(user_id)

        async with self.pool_aeforecast.acquire() as conn:
            value = await conn.fetchval(f"""SELECT {name_columns} ->> $1
                                       FROM on WHERE operation_name = $2""", variable_name,
                                        operation_name)
            return list(map(str, json.loads(value))) if flag_return else value

    async def get_current_twin_variable_value(self, user_id: int, name_columns: str,
                                              variable_name: str) -> str | int:
        """
        :param user_id: chat_id пользователя

        :param name_columns: имя столбца откуда мы будем брать значение

        :param variable_name: название парной переменной

        :return: текущее значение парной переменной
        """
        operation_name = await self.get_choose_operation_name(user_id)

        async with self.pool_aeforecast.acquire() as conn:
            return await conn.fetchval(f"""SELECT {name_columns} ->> $1
                                       FROM on WHERE operation_name = $2""", variable_name,
                                       operation_name)

    async def get_list_timeout_operation_value(self, operation_name: str) -> list:
        """
        :param operation_name: выбранный пользователем DAG

        :return: список параметов интервала запусков DAG
        """
        async with self.pool_aeforecast.acquire() as conn:
            await conn.set_type_codec(
                'json',
                encoder=json.dumps,
                decoder=json.loads,
                schema='pg_catalog'
            )

            timeout_operation_value_dag = await conn.fetchval("""SELECT timeout_operation_value::json
                                          FROM on 
                                          WHERE operation_name = $1""", operation_name)
        return [i for i in timeout_operation_value_dag]

    async def get_status_dag(self) -> str:
        """
        Функция проходится по каждому DAG и получает его текущий статус

        :return: итоговое сообщение со статусом всех DAG
        """
        need_datetime = (datetime.now() - timedelta(days=10)).isoformat() + "Z"
        message = ''
        head = {'Content-Type': 'application/json'}
        # Получаем список dag_id
        responce_dags = requests.get(os.getenv('URL_DAG_LIST'),
                                     auth=(os.getenv('AIRFLOW_USER'), os.getenv('AIRFLOW_PASSWORD')),
                                     headers=head).json()['dags']
        list_dag_id = [dag['dag_id'] for dag in responce_dags if dag['is_paused'] == False]

        # Составляем сообщение со статусами каждого dag в данный момент
        for dag_id in list_dag_id:
            try:
                rez = requests.get(os.getenv('TRIGGER_URL_DAG').format(dag_id=dag_id),
                                   auth=(os.getenv('AIRFLOW_USER'), os.getenv('AIRFLOW_PASSWORD')),
                                   headers=head,
                                   params={'start_date_gte': need_datetime}).json()['dag_runs'][-1]
                message += f"{self.dict_dag_state[rez['state']]} {dag_id}\n\n"
            except IndexError:
                logging.info(f'Пустой список по dag = {dag_id}')
        return message

    async def get_many_or_one_value(self, operation_name: str) -> bool:
        """

        :param operation_name: название операции, которое мы дали ей в БД

        :return: булево значение, какой тип выбора параметров установлен в данный момент
                 (True- одиночный, False - множественный)
        """

        async with self.pool_aeforecast.acquire() as conn:
            return await conn.fetchval("""SELECT many_or_one_value 
                                          FROM on 
                                          WHERE operation_name = $1""", operation_name)

    async def switch_type_choose_variable(self, operation_name) -> bool:
        """
        Меняет характер выбора значений параметра множественный/одиночный

        :param operation_name: название операции, которое мы дали ей в БД

        :return: булево значение, какой тип выбора параметров установлен в данный момент
                 (True- одиночный, False - множественный)
        """

        type_choose_variable = not await self.get_many_or_one_value(operation_name)
        list_name_variables = await self.get_list_variables_dag(operation_name)

        async with self.pool_aeforecast.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    f"""UPDATE on 
                       SET many_or_one_value = $1 
                       WHERE operation_name = $2""", type_choose_variable, operation_name)

                for var in list_name_variables:
                    await conn.execute("""UPDATE on
                                SET
                                 variables_dag = jsonb_set(
                                                            variables_dag,
                                                            $2::text[],
                                                            $3::jsonb
                                                           )
                                WHERE operation_name = $1""", operation_name, [var],
                                       '""' if type_choose_variable else '[]')

        return type_choose_variable

    async def get_only_one_choose_value(self, operation_name: str) -> bool:
        """
        Возвращает значение параметра only_one_choose_value, который сообщает, какой тип выбора доступен для операции
        (True- одиночный, False - множественный и одиночный). Записывается при добавлении операции в БД

        :param operation_name: название операции, которое мы дали ей в БД

        :return: булево значение, какой Допустимый тип выбора параметров у операции
                 (True- одиночный, False - множественный и одиночный)
        """

        async with self.pool_aeforecast.acquire() as conn:
            return await conn.fetchval("""SELECT only_one_choose_value 
                                          FROM on 
                                          WHERE operation_name = $1""", operation_name)

    async def update_etl_choose_variable_value_many_choose(self, user_id: int,
                                                           variable_name: typing.Optional[str] = None,
                                                           operation_name: typing.Optional[str] = None,
                                                           flag_update: typing.Optional[bool] = None,
                                                           variable_value: typing.Optional[list] = None):
        """
        Обновление параметров DAG в зависимости от переданных значений

        :param user_id: chat_id пользователя

        :param variable_name: необязательная переменная - имя меняемого параметра

        :param operation_name: текущее значение параметра для корректрой работы логики

        :param flag_update: параметр управляющий характером обновления
                            True - перезаписываем параметр из основной таблицы (status_operation) в служеюную (etl_choose)
                            False - обновляем параметр в служебной таблице (etl_choose)

        :param variable_value: новое значение параметра

        :return:
        """

        async with self.pool_aeforecast.acquire() as conn:
            # Обновляем имя параметра
            if flag_update:
                await conn.execute("""UPDATE se 
                            SET variable_value_many_choose = (SELECT variables_dag -> $1 FROM on WHERE operation_name = $2)
                            WHERE chat_id = $3""", variable_name, operation_name, user_id)
            else:
                await conn.execute("""UPDATE se
                                   SET variable_value_many_choose = $1::jsonb
                                   WHERE chat_id = $2""", json.dumps(variable_value), user_id)

    async def get_variable_value_many_choose(self, user_id: int) -> list:
        """
        Возвращает значение переменной записанное в сервисной таблице (etl_choose)

        :param user_id: chat_id пользователя

        :return: текущее значение переменной
        """

        async with self.pool_aeforecast.acquire() as conn:
            value = await conn.fetchval(f"""SELECT variable_value_many_choose::jsonb
                                       FROM se WHERE chat_id = $1""", user_id)
            return list(map(str, json.loads(value)))

    async def update_status_operation_many_value(self, user_id: int):
        """
        Сохраняет выбранные параметры для основной таблицы (status_operation)

        :param user_id: chat_id пользователя

        :return:
        """

        variable_name = await self.get_variable_name(user_id)
        operation_name = await self.get_choose_operation_name(user_id)

        async with self.pool_aeforecast.acquire() as conn:
            await conn.execute("""UPDATE on
                                    SET variables_dag = jsonb_set(
                                                variables_dag,
                                                $1,
                                                (SELECT variable_value_many_choose::jsonb
                                                                   FROM se WHERE chat_id = $2)
                                    )
                                    WHERE operation_name = $3""", [variable_name], user_id, operation_name)

    async def get_description(self, operation_name: str, variable_name: typing.Optional[str] = None,
                              flag_desc: typing.Optional[str] = None) -> str:
        """
        Возвращает в зависимости от переданного флага описание переменной или описание операции

        :param operation_name: имя операции в таблице status_operation

        :param variable_name: имя параметра если необходимо

        :param flag_desc: от значения зависит, какое описание возвращаем

        :return: текст описания
        """

        async with self.pool_aeforecast.acquire() as conn:
            if flag_desc == 'var':
                desc = await conn.fetchval("""SELECT variables_desc ->> $1
                                              FROM on 
                                              WHERE operation_name = $2""", variable_name, operation_name)
            else:
                desc = await conn.fetchval("""SELECT operation_desc
                                              FROM on 
                                              WHERE operation_name = $1""", operation_name)
        return desc

    async def update_captcha(self, user_id: int, text_captcha: str):
        """
        Обновляем текст капчи на введенный пользователем и флаг капчи на True

        :param user_id: chat_id пользователя

        :param text_captcha: текст капчи с картинки

        :return:
        """
        async with self.pool_aeforecast.acquire() as conn:
            await conn.execute("""UPDATE se 
                                  SET captcha_text = $1,
                                      captcha_flag = True  
                                  WHERE chat_id = $2""",
                               text_captcha, user_id)

    async def update_partner_flag(self, user_id: int) -> bool:
        """
        Обновляем значение partner_flag на противоположное

        :param user_id: chat_id пользователя

        :return: текущее значение флага
        """
        async with self.pool_aeforecast.acquire() as conn:
            await conn.execute("""UPDATE se 
                            SET partner_flag = (CASE WHEN (SELECT partner_flag FROM se  WHERE chat_id = $1) = True THEN False ELSE True END) 
                            WHERE chat_id = $1""", user_id)

            current_partner_flag = await conn.fetchval("""(SELECT partner_flag FROM se  WHERE chat_id = $1)""",
                                                       user_id)

        return current_partner_flag

    async def get_captha_message_id(self, user_id: int) -> str:
        """
        :param user_id: chat_id пользователя

        :return: id последней капчи с itc
        """
        async with self.pool_aeforecast.acquire() as conn:
            return await conn.fetchval("""SELECT captha_message_id 
                                          FROM se 
                                          WHERE chat_id = $1""", user_id)

