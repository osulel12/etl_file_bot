import typing
from datetime import datetime

# Доступные значеения для каждого из параметров DAG
params_dag_list = {'eac': ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12'],
                   'nnnn': ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12'],
                   'mptrg': ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12'],
                   'chat_id': ['👻', '🎃'],
                   'field': ['day', 'hour', 'minute'],
                   'timeout': [str(i) for i in range(1, 101)],
                   'all_cleare': ['all cleare', 'no all cleare'],
                   'name_table_by_sql': ['', '', '',
                                         '', '', '',
                                         '', '', 'all'],
                   'name_table_by_run_func': ['', '', 'all'],
                   'year_lower_border': [str(i) for i in range(2010, 2041)],
                   'year_upper_border': [str(i) for i in range(2010, 2041)],
                   'is_shown_lower_border': [str(i) for i in range(2010, 2041)],
                   'is_shown_upper_border': [str(i) for i in range(2010, 2041)],
                   'flag_truncate': ['True', 'False'],
                   'year_datamart': [str(i) for i in range(2017, 2031)],
                   'year_or_period': ['by_year', 'by_period'],
                   'year_to_date_from_months': [str(i) for i in range(2017, 2031)],
                   'month_India': [str(i) for i in range(1, 13)],
                   'year': [str(i) for i in range(2017, 2031)],
                   'year_not_mirror': [str(i) for i in range(2017, 2031)],
                   'month_value': [str(i) for i in range(1, 13)],
                   'name_tables_ref': ['', '', '',
                                       '', ''],
                   'flag_var': ['True', 'False'],
                   'full_refresh': ['True', 'False']}

# Доступные значеения для каждого из параметров DAG, связанные со временем
params_dag_list_from_date = {'date_start': [], 'date_end': []}

# Количество элементов на странице для каждого параметра
element_on_page_value = {'eac': 12,
                         'nnnn': 12,
                         'mptrg': 12,
                         'date_end': 12,
                         'date_start': 12,
                         'field': 10,
                         'timeout': 25,
                         'chat_id': 2,
                         'all_cleare': 2,
                         'name_table_by_sql': 10,
                         'name_table_by_run_func': 10,
                         'year_lower_border': 10,
                         'year_upper_border': 10,
                         'is_shown_lower_border': 10,
                         'is_shown_upper_border': 10,
                         'flag_truncate': 5,
                         'year_datamart': 12,
                         'year_or_period': 5,
                         'year_to_date_from_months': 12,
                         'month_India': 15,
                         'year': 12,
                         'year_not_mirror': 12,
                         'month_value': 13,
                         'name_tables_ref': 5,
                         'flag_var': 2,
                         'full_refresh': 2}


def get_twin_param_name(operation_name: str, param_choos: str) -> str:
    dct_operation = {'Отчет-Количество Справок': ['date_end', 'date_start']}
    return [param for param in dct_operation[operation_name] if param != param_choos][0]


def get_month_starts() -> list:
    """
    :return: список из дат начала каждого месяца в заданном интервале
    """
    start_year = 2020
    end_year = 2030
    month_starts = []

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            month_starts.append(datetime(year, month, 1))
    return month_starts


def get_need_value_list(key: str, current_value: str, current_value_twin: typing.Optional[str] = None) -> list:
    """
    :param key: название параметра, который мы редактируем

    :param current_value: текущее значение этого параметра

    :param current_value_twin: текущее значение парного параметра (используется для интервала дат)

    :return: преобразованный к нужному виду список с доступными значениями для переданного парамера(key)
    """
    if key in params_dag_list:
        return [value for value in params_dag_list[key] if value != current_value]
    elif key in params_dag_list_from_date:
        current_value = datetime.strptime(current_value, '%Y.%m.%d')
        current_value_twin = datetime.strptime(current_value_twin, '%Y.%m.%d')
        if key == 'date_start':
            return [date.strftime('%Y.%m.%d') for date in get_month_starts() if
                    date != current_value and date < current_value_twin]
        else:
            return [date.strftime('%Y.%m.%d') for date in get_month_starts() if
                    date != current_value and date > current_value_twin]


def get_many_choose_variables(key: str, current_value: list) -> list:
    """
    Функция возвращает список с помеченными значениями параметра, которые пользователь уже выбрал
    (Необходима для множественного выбора значений)

    :param key: название параметра, который мы редактируем

    :param current_value: текущее значение этого параметра

    :return: преобразованный к нужному виду список с доступными значениями для переданного парамера(key)
    """

    return ['🔘 ' + var if var in current_value else var for var in params_dag_list[key]]
