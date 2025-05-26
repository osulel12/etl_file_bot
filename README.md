# ETL_bot

## Добавление новых операций (Dag)

1. Добавьте новую операцию в таблицу алертов ***bot***:
    - [ ] alert_id - значение текущего максимального id лерта + 1
    - [ ] type_alert - название алерта, можно назвать так же, как добавляемая операция
    - [ ] role_access - список ролей, которым доступна подписка на алерты для добавляемой операции
   

2. Добавить новую операцию в таблицу **on:**
    - [ ] operation_name (параметр не меняется) - указать название, которое будет использоваться для кнопки в боте
    - [ ] variables_dag - указать переменные и если есть, их значения (если для операции присущь только одиночный выбор, указать это значение как сторка или число, если присущ и множественный и одиночный выбор, то указать это значение можно и как списко)
    - [ ] timeout_operation_value - указать параметры, через какое время можно запусть операцию
    - [ ] many_or_one_value - поставить True по умолчанию
    - [ ] only_one_choose_value (параметр не меняется) - True: разрешен только одиночный тип выбора, False: разрешен и множественный и одиночный тип выбора
    - [ ] variables_desc - добавить, если нужно описание переменных у определенной операции
    - [ ] operation_desc - добавить описание операции - что делает (Патерн: Операция выполняет <что делает>)

3. В файле ***create_keyboard.py*** добавьте кнопку с операцией в новый раздел:
   - [ ] Если раздел под операцию уже существует, то добавьте в него кнопку:
     ```   
      elif message_text == '' and state == 'datamart_menue':
            markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
            btn1 = types.KeyboardButton('Витрина Регионов РФ')
            btn2 = types.KeyboardButton('Витрина Year_Data')
            btn3 = types.KeyboardButton('ВАША ОПЕРАЦИЯ')
            btn10 = types.KeyboardButton('🚪 В главное меню')
            markup.add(btn1, btn2, btn3, btn10)
     ```
   - [ ] Если раздела не существуейт, то создайте раздел и добавьте в него новые кнопки, а так же введите имя state соответствующее раздела и добавть кнопку разделя в главное меню(state=main)
     ```   
      elif message_text == '' and state == 'ВАШ_РАЗДЕЛ':
            markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
            btn1 = types.KeyboardButton('ВАША ОПЕРАЦИЯ 1')
            btn2 = types.KeyboardButton('ВАША ОПЕРАЦИЯ 2')
            btn3 = types.KeyboardButton('🚪 В главное меню')
            markup.add(btn1, btn2, btn3)
     ``` 
   

4. В файле ***help_variable.py*** добавьте нужные параметры и их значения, если таких параметров еще нет в словарях:
   - [ ] В словарь params_dag_list. Важно, чтобы название ключей совпадали с названиями параметрами занесенными в базу данных. Так же все параметры должны иметь тип ***str***:
      ```
     params_dag_list = {
                        'НОВОЕ_ЗНАЧЕНИЕ_ПАРАМЕТРА 1': ['0', '1', '2', '3', '4'],
                        'НОВОЕ_ЗНАЧЕНИЕ_ПАРАМЕТРА 2': ['а', 'б', 'в', 'г', 'д']
                       }
     
     ``` 
   - [ ] Если параметры связаны с типом дата (год, месяц, число) и являются парными (дата старта, дата окончания), то их добавить необходимо в словарь ***params_dag_list_from_date***


5. В файле ***main.py*** необходиом интегрировать код для новой операции. Но перед этим если наша операция носит закрытый характер и доступна только для главных администраторов, то необходимо через админ_бота добавить название кнопку к роли admin и после этого раздел будет доступен только пользователям с этой ролью(листинг 1.). Если же операция не носит закрытый характер, то можно не добавлять кнопку к роли (листинг 2.):
   - [ ] Если мы добавляем операцию в уже существующий раздел, то находим его в файли и вставляем следующий код вводя нужные значения
      ```
     # Листинг 1. Добавление операции для закрытой роли
      elif await need_example_class.get_access_section(chat_id, message_text[
                                                              2:]) and message_text == 'ВАША ОПЕРАЦИЯ':
          await need_example_class.update_etl_choose_dag(chat_id, message_text, 'DAG_ID_IN_AIRFLOW')
          await need_example_class.update_state_user(chat_id, 'interaction_with_dag', True)
          await bot.send_message(chat_id, 'Выберите действие 👇',
                               reply_markup=create_replay_markup('', 'interaction_with_dag', 
                                                                flag_one_choose_value=await need_example_class.get_only_one_choose_value(message_text)))
     ``` 
     
    ```
         # Листинг 2. Добавление операции для всех пользователей
          elif await need_example_class.check_user(chat_id) and message_text == 'ВАША ОПЕРАЦИЯ':
              await need_example_class.update_etl_choose_dag(chat_id, message_text, 'DAG_ID_IN_AIRFLOW')
              await need_example_class.update_state_user(chat_id, 'interaction_with_dag', True)
              await bot.send_message(chat_id, 'Выберите действие 👇',
                                   reply_markup=create_replay_markup('', 'interaction_with_dag', 
                                                                    flag_one_choose_value=await need_example_class.get_only_one_choose_value(message_text)))
    ``` 
   - [ ] Если мы добавляем новый блок и новую операацию, то можно скопировать код любой функции уже имеющегося блока и в нем:
     - Заменить название функции на название нового блока 
     - Заменить state на название нового блока
     - Подставить нужную конструкцию 
       ```
          if 
             ...
          elif 
             ...
          else
             ...
       ``` 
