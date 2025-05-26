from telebot import types
import typing


def create_replay_markup(message_text: str, state: str,
                         flag_one_choose_value: typing.Optional[bool] = True) -> types.ReplyKeyboardMarkup:
    """
    Генерирует набор кнопок клавиатуры в зависимости от переданных параметров

    :param message_text: сообщение от пользователя

    :param state: состояние, в котором сейчас находится пользователь

    :param flag_one_choose_value: флаг, сиглизирующий какой тип выбора пораметров доступен для операции
                                  (True- одиночный, False - множественный и одиночный)

    :return: сгенерированную клавиатуру
    """

    if message_text == '' and state == 'main':
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
        btn1 = types.KeyboardButton('📩 Управление рассылкой')
        btn2 = types.KeyboardButton('📺 Витрины данных')
        btn3 = types.KeyboardButton('🗂 Отчеты по справкам')
        btn4 = types.KeyboardButton('📡 Парсеры')
        btn5 = types.KeyboardButton('🔬 Тестовые витрины данных')
        btn6 = types.KeyboardButton('🖥 Bash commands')
        btn7 = types.KeyboardButton('ℹ️ Статусы DAG`s')
        btn8 = types.KeyboardButton('🐘 Postgres')
        btn12 = types.KeyboardButton('📲 Outer_tg_bot')
        markup.add(btn2, btn5, btn4, btn3, btn8, btn6, btn7, btn12)
        markup.row(btn1)
        return markup
    elif message_text in ['🗺 ITC', '📖 Перечень отсутствующих стран', ''] and state == 'bloc_ITC':
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2, one_time_keyboard=True)
        btn1 = types.KeyboardButton('📖 Перечень отсутствующих стран')
        btn2 = types.KeyboardButton('🔖 Ввести капчу')
        btn3 = types.KeyboardButton('🌀 Формирование списка партнеров')
        btn10 = types.KeyboardButton('🔙 Назад')
        markup.add(btn1, btn2, btn3, btn10)
        return markup
    elif message_text == '' and state == 'datamart_menue':
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
        btn1 = types.KeyboardButton('💸 Внешняя торговля')
        btn2 = types.KeyboardButton('💹 Datalens')
        btn3 = types.KeyboardButton('⚖️ Балансы')
        btn10 = types.KeyboardButton('🚪 В главное меню')
        markup.add(btn1, btn2, btn3, btn10)
        return markup
    elif message_text == '' and state == 'certificates_menue':
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
        btn1 = types.KeyboardButton('Актуальные Даты Справок')
        btn2 = types.KeyboardButton('Отчет-Количество Справок')
        btn3 = types.KeyboardButton('Новые справки с момента последнего запуска')
        btn10 = types.KeyboardButton('🚪 В главное меню')
        markup.add(btn1, btn2, btn3, btn10)
        return markup
    elif message_text == '' and state == 'Parser_menue':
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
        btn1 = types.KeyboardButton('💹 MOEX')
        btn3 = types.KeyboardButton('🍃 USDA')
        btn4 = types.KeyboardButton('🌾 FAO')
        btn5 = types.KeyboardButton('🗺 ITC')
        btn2 = types.KeyboardButton('🇮🇳 Индия')
        bth6 = types.KeyboardButton('🇹🇭 Таиланд')
        btn10 = types.KeyboardButton('🚪 В главное меню')
        markup.add(btn1, btn3, btn4, btn5, btn2, bth6, btn10)
        return markup
    elif message_text == '' and state == 'bloc_USDA':
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
        btn1 = types.KeyboardButton('Обновление таблицы psd')
        btn2 = types.KeyboardButton('Обновление таблицы reference_data')
        btn10 = types.KeyboardButton('🔙 Назад')
        markup.add(btn1, btn2, btn10)
        return markup
    elif message_text == '' and state == 'bloc_FAO':
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
        btn1 = types.KeyboardButton('Обновление таблиц FAO')
        btn10 = types.KeyboardButton('🔙 Назад')
        markup.add(btn1, btn10)
        return markup
    elif message_text == '' and state == 'test_datamart_menue':
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
        btn1 = types.KeyboardButton('Тест Витрина Year_Data')
        btn2 = types.KeyboardButton('Тест Витрина Month_Data')
        btn3 = types.KeyboardButton('Тест Статические Справочники')
        btn4 = types.KeyboardButton('Тест Витрина Регионов РФ')
        btn5 = types.KeyboardButton('Тест Обновление ref_balances')
        btn6 = types.KeyboardButton('Тест Обновление balance_master')
        btn7 = types.KeyboardButton('Тест Обновление balance_trade_master')
        btn8 = types.KeyboardButton('Тест Справочники для web_app_logs')
        btn10 = types.KeyboardButton('🚪 В главное меню')
        markup.add(btn1, btn2, btn3, btn4, btn5, btn6, btn7, btn8, btn10)
        return markup
    elif message_text == '' and state == 'bash_menue':
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
        btn1 = types.KeyboardButton('Перезапуск VM')
        btn10 = types.KeyboardButton('🚪 В главное меню')
        markup.add(btn1, btn10)
        return markup
    elif message_text == '' and state == 'postgres_menue':
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
        btn1 = types.KeyboardButton('Представление Годовых данных')
        btn2 = types.KeyboardButton('Представление Месячных данных')
        btn3 = types.KeyboardButton('Перенос данных из customs_vanilla в customs')
        btn4 = types.KeyboardButton('Перенос данных из fts в customs_month')
        btn5 = types.KeyboardButton('Перенос данных из customs_vanilla в customs_month')
        btn6 = types.KeyboardButton('Перенос ref_country на MH')
        btn7 = types.KeyboardButton('Перенос данных из fts в customs')
        btn10 = types.KeyboardButton('🚪 В главное меню')
        markup.add(btn1, btn2, btn3, btn4, btn5, btn6, btn7, btn10)
        return markup
    elif message_text == '' and state == 'outer_tg_bot_section':
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
        btn1 = types.KeyboardButton('🚧 Тарифные/Нетарифные барьеры')
        btn10 = types.KeyboardButton('🚪 В главное меню')
        markup.add(btn1, btn10)
        return markup
    elif message_text == '' and state == 'barier':
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
        btn1 = types.KeyboardButton('Проверка_Сертификатов')
        btn2 = types.KeyboardButton('Chinaport_parser')
        btn10 = types.KeyboardButton('🔙 Назад')
        markup.add(btn1, btn2, btn10)
        return markup
    elif message_text == '' and state == 'India_menue':
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
        btn1 = types.KeyboardButton('Индия tradestat_commerce')
        btn2 = types.KeyboardButton('Tradestat_commerce new month')
        btn10 = types.KeyboardButton('🔙 Назад')
        markup.add(btn1, btn2, btn10)
        return markup
    elif message_text == '' and state == 'Thailand_menue':
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
        btn1 = types.KeyboardButton('Thailand_customs')
        btn2 = types.KeyboardButton('Thailand_customs new month')
        btn10 = types.KeyboardButton('🔙 Назад')
        markup.add(btn1, btn2, btn10)
        return markup
    elif message_text == '' and state == 'MOEX_menue':
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
        btn1 = types.KeyboardButton('Парсинг MOEX')
        btn10 = types.KeyboardButton('🔙 Назад')
        markup.add(btn1, btn10)
        return markup
    elif message_text == '' and state == 'balance_menue':
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
        btn1 = types.KeyboardButton('Обновление ref_balances')
        btn2 = types.KeyboardButton('Обновление balance_master')
        btn3 = types.KeyboardButton('Обновление balance_trade_master')
        btn10 = types.KeyboardButton('🔙 Назад')
        markup.add(btn1, btn2, btn3, btn10)
        return markup
    elif message_text == '' and state == 'foreign_trade_menue':
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
        btn1 = types.KeyboardButton('Витрина Year_Data')
        btn2 = types.KeyboardButton('Витрина Month_Data')
        btn3 = types.KeyboardButton('Статические справочники')
        btn10 = types.KeyboardButton('🔙 Назад')
        markup.add(btn1, btn2, btn3, btn10)
        return markup
    elif message_text == '' and state == 'datalens_menue':
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
        btn1 = types.KeyboardButton('Витрина Регионов РФ')
        btn2 = types.KeyboardButton('Витрина World Trade')
        btn3 = types.KeyboardButton('Витрина outer_tg_bot')
        btn4 = types.KeyboardButton('Витрины логов Аналитической платформы')
        btn5 = types.KeyboardButton('Справочники для web_app_logs')
        btn10 = types.KeyboardButton('🔙 Назад')
        markup.add(btn1, btn2, btn3, btn4, btn5, btn10)
        return markup
    elif message_text == '' and state == 'interaction_with_dag':
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
        btn1 = types.KeyboardButton('▶️ Запуск DAG')
        btn2 = types.KeyboardButton('🔄 Редактирование параметров запуска')
        btn3 = types.KeyboardButton('⏳ Редактирование параметров timeout')
        btn4 = types.KeyboardButton('⏺ Тип выбора параметров')
        btn5 = types.KeyboardButton('🚮 Удалить последний dag_run')
        btn6 = types.KeyboardButton('🔙 Назад')
        if flag_one_choose_value:
            markup.add(btn1, btn2, btn3, btn5, btn6)
        else:
            markup.add(btn1, btn2, btn3, btn4, btn5, btn6)
        return markup


def create_inline_markup(state: str,
                         list_itemns: typing.Optional[list | dict] = None,
                         pagen: typing.Optional[int] = 0,
                         element_on_page: typing.Optional[int] = 21,
                         country_state: typing.Optional[str] = None) -> types.InlineKeyboardMarkup:
    """
    Возвращает инлайн клавиатуру в зависимости от переданных параметров

    :param state: состояние пользователя

    :param list_itemns: набор элементов. Либо список стран/дат указанных в названии страновых справок,
                        либо словарь со странами

    :param pagen: значение на какой странице клавиатуры находится пользователь

    :param element_on_page: количество кнопок на одной странице клавиатуры

    :param country_state: страна, с которой пользователь работал крайний раз
                          необходим, для формирования уникальных callbacck
                          и предотвращения путаницы в параметрах кнопок

    :return: сформированную инлайн клавиатуру
    """

    if state in ['choos_value_dag', 'update_new_value_dag', 'choos_timeout_dag', 'update_new_timeout_value']:

        # Список списков для создания пагинации клавитауры
        lst_pagen_value = [list_itemns[i:i + element_on_page] for i in range(0, len(list_itemns), element_on_page)]
        btns = []

        # Наполняем список кнопками с названиями стран
        for var in lst_pagen_value[pagen]:
            var = str(var)
            btns.append(types.InlineKeyboardButton(text=var, callback_data=var))
        markup = types.InlineKeyboardMarkup()
        markup.add(*btns)

        # Формируем элементы навигации по клавиатуре
        if len(lst_pagen_value) == 1:
            markup.row(types.InlineKeyboardButton(text=f'{pagen + 1}/{len(lst_pagen_value)}', callback_data=f' '),
                       types.InlineKeyboardButton(text=f'🔙 Назад', callback_data=f'back_menue'))
        elif pagen + 1 == 1 and len(lst_pagen_value) > 1:
            markup.row(types.InlineKeyboardButton(text=f'🔙 Назад', callback_data=f'back_menue'),
                       types.InlineKeyboardButton(text=f'{pagen + 1}/{len(lst_pagen_value)}', callback_data=f' '),
                       types.InlineKeyboardButton(text=f'➡', callback_data=f'next'))
        elif pagen + 1 == len(lst_pagen_value) and len(lst_pagen_value) > 1:
            markup.row(types.InlineKeyboardButton(text=f'⬅', callback_data=f'back'),
                       types.InlineKeyboardButton(text=f'{pagen + 1}/{len(lst_pagen_value)}', callback_data=f' '),
                       types.InlineKeyboardButton(text=f'🔙 Назад', callback_data=f'back_menue'))
        elif 1 < pagen + 1 < len(lst_pagen_value) and len(lst_pagen_value) > 1:
            markup.row(types.InlineKeyboardButton(text=f'⬅', callback_data=f'back'),
                       types.InlineKeyboardButton(text=f'{pagen + 1}/{len(lst_pagen_value)}', callback_data=f' '),
                       types.InlineKeyboardButton(text=f'➡', callback_data=f'next'))
            markup.row(types.InlineKeyboardButton(text=f'🔙 Назад', callback_data=f'back_menue'))

        return markup

    elif state in ['form2_version_country', 'form1_version_country', 'form1_version_group', 'form2_version_group']:
        lst_country = list_itemns
        lst_pagen_country = [lst_country[i:i + element_on_page] for i in range(0, len(lst_country), element_on_page)]
        btns = []

        for date in lst_pagen_country[pagen]:
            btns.append(types.InlineKeyboardButton(text=date.replace('.', '-').replace('_', '-'), callback_data=date))
        markup = types.InlineKeyboardMarkup()
        markup.add(*btns)

        if len(lst_pagen_country) == 1:
            markup.add(types.InlineKeyboardButton(text=f'{pagen + 1}/{len(lst_pagen_country)}', callback_data=f' '))
        elif pagen + 1 == 1:
            markup.add(types.InlineKeyboardButton(text=f'{pagen + 1}/{len(lst_pagen_country)}', callback_data=f' '),
                       types.InlineKeyboardButton(text=f'➡', callback_data=f'next'))
        elif pagen + 1 == len(lst_pagen_country):
            markup.add(types.InlineKeyboardButton(text=f'⬅', callback_data=f'back'),
                       types.InlineKeyboardButton(text=f'{pagen + 1}/{len(lst_pagen_country)}', callback_data=f' '))
        else:
            markup.add(types.InlineKeyboardButton(text=f'⬅', callback_data=f'back'),
                       types.InlineKeyboardButton(text=f'{pagen + 1}/{len(lst_pagen_country)}', callback_data=f' '),
                       types.InlineKeyboardButton(text=f'➡', callback_data=f'next'))
        return markup

    elif state in ['barier_country']:
        lst_country = [k for k, v in list_itemns.items()]
        lst_pagen_country = [lst_country[i:i + element_on_page] for i in range(0, len(lst_country), element_on_page)]
        btns = []

        for country in lst_pagen_country[pagen]:
            btns.append(types.InlineKeyboardButton(text=country, callback_data=country))
        markup = types.InlineKeyboardMarkup()
        markup.add(*btns)

        if pagen + 1 == 1:
            markup.add(types.InlineKeyboardButton(text=f'{pagen + 1}/{len(lst_pagen_country)}', callback_data=f' '),
                       types.InlineKeyboardButton(text=f'➡', callback_data=f'next'))
        elif pagen + 1 == len(lst_pagen_country):
            markup.add(types.InlineKeyboardButton(text=f'⬅', callback_data=f'back'),
                       types.InlineKeyboardButton(text=f'{pagen + 1}/{len(lst_pagen_country)}', callback_data=f' '))
        else:
            markup.add(types.InlineKeyboardButton(text=f'⬅', callback_data=f'back'),
                       types.InlineKeyboardButton(text=f'{pagen + 1}/{len(lst_pagen_country)}', callback_data=f' '),
                       types.InlineKeyboardButton(text=f'➡', callback_data=f'next'))
        return markup

    elif state in ['region_reference']:
        list_region = [k for k, v in list_itemns.items()]
        lst_pagen_country = [list_region[i:i + element_on_page] for i in range(0, len(list_region), element_on_page)]
        btns = []

        for country in lst_pagen_country[pagen]:
            btns.append(types.InlineKeyboardButton(text=country, callback_data=country))
        markup = types.InlineKeyboardMarkup()
        markup.add(*btns)

        if pagen + 1 == 1:
            markup.add(types.InlineKeyboardButton(text=f'{pagen + 1}/{len(lst_pagen_country)}', callback_data=f' '),
                       types.InlineKeyboardButton(text=f'➡', callback_data=f'next'))
        elif pagen + 1 == len(lst_pagen_country):
            markup.add(types.InlineKeyboardButton(text=f'⬅', callback_data=f'back'),
                       types.InlineKeyboardButton(text=f'{pagen + 1}/{len(lst_pagen_country)}', callback_data=f' '))
        else:
            markup.add(types.InlineKeyboardButton(text=f'⬅', callback_data=f'back'),
                       types.InlineKeyboardButton(text=f'{pagen + 1}/{len(lst_pagen_country)}', callback_data=f' '),
                       types.InlineKeyboardButton(text=f'➡', callback_data=f'next'))
        return markup

    elif state in ['get_subscribe_user']:
        btns = []
        # {'alert_id': 2, 'type_alert': 'Обновление данных в БД', 'status_alert': True}
        lst_pagen_alert = [list_itemns[i:i + element_on_page] for i in range(0, len(list_itemns), element_on_page)]
        for i in lst_pagen_alert[pagen]:
            btns.append(types.InlineKeyboardButton(text=f"{'✅' if i['status_alert'] else '❌'} {i['type_alert']}",
                                                   callback_data=i['alert_id']))
        btns = sorted(btns, key=lambda x: len(x.text))
        markup = types.InlineKeyboardMarkup(row_width=1)
        markup.add(*btns)

        if len(lst_pagen_alert) == 1:
            markup.row(types.InlineKeyboardButton(text=f'{pagen + 1}/{len(lst_pagen_alert)}', callback_data=f' '),
                       types.InlineKeyboardButton(text=f'🚪 В главное меню', callback_data=f'main'))
        elif pagen + 1 == 1 and len(lst_pagen_alert) > 1:
            markup.row(types.InlineKeyboardButton(text=f'🚪 В главное меню', callback_data=f'main'),
                       types.InlineKeyboardButton(text=f'{pagen + 1}/{len(lst_pagen_alert)}', callback_data=f' '),
                       types.InlineKeyboardButton(text=f'➡', callback_data=f'next'))
        elif pagen + 1 == len(lst_pagen_alert) and len(lst_pagen_alert) > 1:
            markup.row(types.InlineKeyboardButton(text=f'⬅', callback_data=f'back'),
                       types.InlineKeyboardButton(text=f'{pagen + 1}/{len(lst_pagen_alert)}', callback_data=f' '),
                       types.InlineKeyboardButton(text=f'🚪 В главное меню', callback_data=f'main'))
        elif 1 < pagen + 1 < len(lst_pagen_alert) and len(lst_pagen_alert) > 1:
            markup.row(types.InlineKeyboardButton(text=f'⬅', callback_data=f'back'),
                       types.InlineKeyboardButton(text=f'{pagen + 1}/{len(lst_pagen_alert)}', callback_data=f' '),
                       types.InlineKeyboardButton(text=f'➡', callback_data=f'next'))
            markup.row(types.InlineKeyboardButton(text=f'🚪 В главное меню', callback_data=f'main'))

        return markup

    elif state in ['many_choose_update_new_value_dag']:
        btns = []
        lst_pagen_alert = [list_itemns[i:i + element_on_page] for i in range(0, len(list_itemns), element_on_page)]
        for i in lst_pagen_alert[pagen]:
            btns.append(types.InlineKeyboardButton(text=i,
                                                   callback_data=i.split('🔘 ')[1] if '🔘' in i else i))

        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.add(*btns)

        if len(lst_pagen_alert) == 1:
            markup.row(types.InlineKeyboardButton(text=f'☑️ Сохранить', callback_data=f'save'),
                       types.InlineKeyboardButton(text=f'{pagen + 1}/{len(lst_pagen_alert)}', callback_data=f' '),
                       types.InlineKeyboardButton(text=f'🔙 Назад', callback_data=f'back_menue'))
        elif pagen + 1 == 1 and len(lst_pagen_alert) > 1:
            markup.row(types.InlineKeyboardButton(text=f'🔙 Назад', callback_data=f'back_menue'),
                       types.InlineKeyboardButton(text=f'{pagen + 1}/{len(lst_pagen_alert)}', callback_data=f' '),
                       types.InlineKeyboardButton(text=f'➡', callback_data=f'next'))
            markup.row(types.InlineKeyboardButton(text=f'☑️ Сохранить', callback_data=f'save'))
        elif pagen + 1 == len(lst_pagen_alert) and len(lst_pagen_alert) > 1:
            markup.row(types.InlineKeyboardButton(text=f'⬅', callback_data=f'back'),
                       types.InlineKeyboardButton(text=f'{pagen + 1}/{len(lst_pagen_alert)}', callback_data=f' '),
                       types.InlineKeyboardButton(text=f'🔙 Назад', callback_data=f'back_menue'))
            markup.row(types.InlineKeyboardButton(text=f'☑️ Сохранить', callback_data=f'save'))
        elif 1 < pagen + 1 < len(lst_pagen_alert) and len(lst_pagen_alert) > 1:
            markup.row(types.InlineKeyboardButton(text=f'⬅', callback_data=f'back'),
                       types.InlineKeyboardButton(text=f'{pagen + 1}/{len(lst_pagen_alert)}', callback_data=f' '),
                       types.InlineKeyboardButton(text=f'➡', callback_data=f'next'))
            markup.row(types.InlineKeyboardButton(text=f'☑️ Сохранить', callback_data=f'save'),
                       types.InlineKeyboardButton(text=f'🔙 Назад', callback_data=f'back_menue'))
        return markup

    elif state in ['approve_remove_dag_run', 'approve_swap_type_params']:
        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.row(types.InlineKeyboardButton(text=f'✔️ Да', callback_data=f'yes'),
                   types.InlineKeyboardButton(text=f'✖️ Нет', callback_data=f'no'))

        return markup
