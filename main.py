import py_scripts as ps
import sql_scripts as ss
import os
import os
from pathlib import Path
url_c = 'https://www.cian.ru/cat.php?deal_type=sale&electronic_trading=2&engine_version=2&flat_share=2&offer_type=flat&only_flat=1&region=1&room1=1'  #here used to be site for scrab


#____________________________execution_____________________________________________
ps.create_dir() #создаём папку для хранения первых страниц с сылками
headers = ps.get_user_agent() #меняем данные юзер-агент (@@@)
proxy_lst = ps.get_proxies() # парсим бесплатные прокси с открытого сайта, записываем их в файл lst_proxy.txt
checked_proxy = ps.check_proxy(proxy_lst) # читаем файл lst_proxy.txt, проверяем рабочие прокси, возвращаем рабочий прокси
file_name = ps.get_page(headers, url_c, checked_proxy) # получаем страницу с сылками и сохраняем её (@@@)
file_name = 'C:\ZPy_training\Pars_Cian\data_scrb\d_page_(10.09,19,38).txt'
links = ps.pars_links(file_name) #проходимся по сохранённой странице (файлу) с сылками и создаём кортеж из ссылок (@@@)
objs = ps.data_pars(headers, links) #непосредственно парсим данные (@@@)
ps.cleaner(objs) #очищаем некоторое значения от '\\xa0' и т.п., , записываем в json_файл (@@@)
objs = ps.json_preparing() #подготовка js файлов к загрузке в БД




#_____________________________________DB_block________________________________________
conf = ps.use_conn_config() #1
conn = ps.create_conn(conf) #2
ps.execute_query(conn, ss.create_shcema) #3 creating shcema
ps.execute_query(conn, ss.set_path) #4 set path to shcema
ps.execute_query(conn, ss.create_FACT_table) #5 create FACT table
ps.execute_query(conn, ss.create_tmp_table) #6 create STG_tmp_inc table

#counter = ps.num_of_starts() # for choose num of starts (return counter) делет вис щит иф нои ниид


#----------------------------insertion---------------------------------
""" This func load files into BD for each script start """
ps.json_to_db(conn, objs) #загрузка js в БД


""" This block about incremental loading"""
ps.execute_query(conn, ss.inc_views) #create view in DB

ps.execute_query(conn, ss.tmp_new_rows) #create tmp_new_rows
ps.execute_query(conn, ss.tmp_updated_rows) #create tmp_updated_rows

ps.execute_query(conn, ss.inc_insert1) #insert into FACT from tmp_new_rows
ps.execute_query(conn, ss.inc_update1) #update to duplicate entries (dlt_flg, date)
ps.execute_query(conn, ss.inc_insert2) #insert into FACT from STG_tmp_apart_data_updated_rows


""" tmp_drop block """
ps.del_all_tmp_tables(ss.delete_tmp_view)  # delete tmp_'view' tables
ps.del_all_tmp_tables(ss.delete_tmp_1st)  # delete STG_tmp tables
ps.del_all_tmp_tables(ss.delete_tmp_2nd) # delete STG_tmp/new/updated_rows


#___________________________moving processed files to archive_________________________________________
lst_of_json = list(Path('data_scrb').glob('*.json')) #заменить на отдельную функцию в py_scripts
for js in lst_of_json:
    ps.file_processing(js)

lst_of_d_page = list(Path('data_scrb').glob('*.txt'))
for page in lst_of_d_page:
    ps.file_processing(page)
