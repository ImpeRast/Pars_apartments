# Pars_apartments (Py + SQL)

Проект направленный на сбор данных с объявлениями о продаже квартир (по заданной ссылке "url_c" с предустановленными фильтрами с сайта циан), предобработку данных и загрузку в БД (используется диалект PostgreSQL)

1. создаём папку для хранения первых страниц с сылками
2. меняем данные юзер-агент (@@@)
3. парсим бесплатные прокси с открытого сайта, записываем их в файл lst_proxy.txt
4. читаем файл lst_proxy.txt, проверяем рабочие прокси, возвращаем рабочий прокси\
5. подключаемся к указанной целевой странице "url_c" получаем страницу с сылками и сохраняем её c указанием текущей даты и времени (@@@)
6. проходимся по сохранённой странице (файлу) с сылками и создаём кортеж из ссылок на объявления (@@@)
7. непосредственно парсим данные (цена, метраж, метро, город, площадь кухни, год постройки и т.п.)
8. очищаем некоторые полученные значения от мусора: '\\xa0' и т.п., записываем в json_файл (@@@)
9. подготовка js файлов к загрузке в БД (добавляеи несуществующие значения по именам полей в БД со значением Null)
10. загружаем файлы в БД (инкрементальная загрузка)
11. отработанные файлы сохраняются в "archive" с пометкой "backup"

для работоспособности необходимо:
1. указать целевую ссылку в main.py - "url_c";
2. подставить корректные значения в conn_config.txt - конфиги для БД;
3. проверить (установить) необходимые библиотеки: fake_useragent, bs4, requests, psycorg2, sqlalchemy;
4. указать количество страниц для парсига под текущем прокси в функции "data_pars" (строка -- link_counter % 2).

Существующие проблемы:
1. Периодически не отрабатывает парсер по одним и тем-же ссылкам, решение: перезапуск;
2. Периодически не отрабатывает парсер по странице "url_c" - с сылками на объявления, решение: перезапуск.
