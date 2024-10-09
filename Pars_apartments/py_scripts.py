import pathlib
import random
from fake_useragent import UserAgent
from bs4 import BeautifulSoup as bs
from datetime import datetime
import requests
import time
import os
import re
import json

import psycopg2
import shutil
from pathlib import Path
from psycopg2 import OperationalError
from sqlalchemy import create_engine




url_c = 'https://www.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=flat&p=2&region=1&room1=1'  #here used to be site for scrab
#!!!!!!!! DON"T FORGET TO DELETE SITE

def create_dir():
    """create folder to downloaded pages"""
    my_cwd = os.getcwd()
    new_dir = '../data_scrb'
    path = os.path.join(my_cwd, new_dir)
    try:
        os.mkdir(path)
    except:
        print(f'{new_dir} is already exist!')

def get_user_agent():
    headers = UserAgent().getRandom
    for key, value in headers.items():
        headers[key] = str(value)
    headers['User-Agent'] = headers.pop('useragent')
    return headers

def get_proxies():
    """Get list of free proxies"""
    if os.path.isfile('lst_proxy.txt') == False:
        url_p = 'https://free-proxy-list.net/'
        resp = requests.get(url_p).text
        output = open('lst_proxy.txt', 'w')
        output.write(resp)
        output.close()
    temp_proxy_lst, proxy_lst = [], []
    with open('lst_proxy.txt', 'r') as f:
        for line in f:
            temp_proxy_lst.append(re.findall(r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d{1,4})', line))
        for lst in temp_proxy_lst:
            if len(lst) > 0:
                proxy_lst.append(*lst)
    return proxy_lst

def check_proxy(proxy_lst):
    """Check proxy valid"""
    for cur_proxy in proxy_lst:
        print(f'The checking proxy is {cur_proxy}')
        time.sleep(1)
        proxies = {
              'http' :  f'http://{cur_proxy}',
              'https' :  f'https://{cur_proxy}'
              }
        try:
            resp = requests.get('https://api.ipify.org/',
                             proxies=proxies)
            soup = bs(resp.text, 'lxml')
            without_port = re.findall(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}', soup.text) #Checking IP replacement
            without_port = str(without_port[0])
            if without_port in cur_proxy:
                checked_proxy = cur_proxy
                return checked_proxy
                print(f'checked_proxy is {checked_proxy}')
        except:
            print(f'try next')

def get_page(headers, url, checked_proxy=None):
    """Get receives a landing page for processing and save it"""
    if checked_proxy is None:
        print('Connection without proxy')
        response = requests.get(url=url, headers=headers)
    else:
        try:
            proxies = {
                'http': f'http://{checked_proxy}',
                'https': f'https://{checked_proxy}'
            }
            print(f'Connection with proxy: {checked_proxy}')
            response = requests.get(url=url,
                                headers=headers, proxies=proxies)
        except:
            print('Try next proxy from proxy_lst')
            checked_proxy = check_proxy(proxy_lst)
            response = requests.get(url=url, headers=headers, proxies=checked_proxy)
    texted_resp = response.text
    output = open(f'data_scrb\d_page_{datetime.now().strftime("(%m.%d,%H,%M)")}.txt', 'w', encoding='UTF-8')
    file_name = f'data_scrb\d_page_{datetime.now().strftime("(%m.%d,%H,%M)")}.txt'
    output.write(texted_resp)
    print('We get a file:')
    print(file_name)
    output.close()
    return file_name

def pars_links (file_name):
    """create tuple of links from first page"""
    with open(file_name, 'r', encoding='utf - 8') as file:
        links_tuple = ( )
        file_name = file.read()
        soup = bs(file_name, 'lxml')
        time.sleep(1)
        links = [cur_link.get('href') for cur_link in soup.find_all('a',
                                                                    attrs={'class':'_93444fe79c--media--9P6wN',
                                                                           'href': re.compile("^https://")})]
        # if len(links) <1:
        #     #file = pathlib.Path(file_name) реализовать удаление
        #     #file.unlink()
        #     file_name = get_page(headers, url_c, checked_proxy=None)
        #     links = pars_links(file_name)
        print(f' the links : {links}')
        return links

def data_pars(headers, links, proxy_lst= None, checked_proxy= None):
    """Get info about patametrs: #total_area, living_area, floor, total_floors... ect. and writing it into a dict"""
    objs = {}  # dict whitch will collect data from current link
    link_counter = 0
    for link in range(1, len(links)):
        if 'flat' in links[link] or 'klupit' in links[link]:
            print(f'pars from {links[link]}')
            time.sleep(random.randint(5,10))
            if checked_proxy is None:
                print('Connection without proxy')
                response = requests.get(url=links[link], headers=headers)
            else:
                try:
                    proxies = {
                        'http': f'http://{checked_proxy}',
                        'https': f'https://{checked_proxy}'
                    }
                    print(f'Connection with proxy: {checked_proxy}')
                    response = requests.get(url=links[link],
                                            headers=headers, proxies=proxies)
                except OperationalError as e:
                    print(f'The error "{e}" occured')

            time.sleep(3)
            soup = bs(response.text, 'lxml')

            # ______________city___________________
            try:
                city = soup.find('a', 'a10a3f92e9--address--SMU25'
                                 ).text
                city = {'city': city}
            except:
                print("Can't get city")
                city = {'city' : ''}

            # ______________rooms___________________
            try:
                rooms = soup.find_all('h1', 'a10a3f92e9--title--vlZwT'
                                      )
                r_rooms = 'ся (.*?комн.)'
                rooms = re.findall(r_rooms, str(rooms))
                rooms = {'rooms' : str(*rooms)}
            except:
                print("Can't get rooms")
                rooms = {'rooms' : ''}

            #______________price___________________
            try:
                price = soup.find(
                    'div', attrs={'data-testid': 'price-amount'}
                ).text
                price = {'price': price}
            except:
                print("Can't get price")
                price = {'price' : ''}

            # ______________metro___________________
            try:
                metro = soup.find('a', 'a10a3f92e9--underground_link--VnUVj').text
                metro = {'metro' : metro}
            except:
                print(f"Can't get metro")
                metro = {'metro' : ''}

            # ______________to_the_metro___________________
                # - I should make it later

            # ______________oter_params___________________
            try:
                other_params = soup.find_all(
                    'div', attrs={"a10a3f92e9--text--eplgM"})
                keys, vals = [], []
                r_keys, r_vals = 'U5">(.*?)</sp', '2px">(.*?)</sp'  # regular expression for searching parameters and their values
                keys.append(re.findall(r_keys, str(other_params)))
                vals.append(re.findall(r_vals, str(other_params)))
                if len(vals) < 1:
                    r_vals = 'e4SBY">(.*?)</sp'
                    vals.append(re.findall(r_vals, str(other_params)))
                other_params = dict(zip(*keys, *vals))
                if 'Год постройки' in other_params.keys():
                    other_params['Год постройки'] = str(other_params['Год постройки']) + '-01-01' # change format on date
                if 'Год сдачи' in other_params.keys():
                    other_params['Год сдачи'] = str(other_params['Год сдачи']) + '-01-01'  #change format on date
                #other_params.update()
            except:
                print("Can't get other_params")
                other_params = {'other_params': ''}

            link = {'link' : links[link]}
            objs.update({link_counter : {**link, **city, **metro, **price, **rooms, **other_params}})
            link_counter += 1

            print(link_counter)
            if link_counter % 2 == 0: #changing proxy for every num requests
                break
    print(objs)
    return objs

def cleaner(objs):
  """clean objs from '\\xa0' ect. and write into json """
  for details in objs.values():
    for key,value in details.items():
        cleaned_value = value.replace('\xa0', '').replace('м²', '').replace('₽', '').replace('-комн.', '').replace(',', '.')
        details[key] = cleaned_value
        json_string = json.dumps(objs, indent=4)
    try:
        with open(f'data_scrb\j_objs{datetime.now().strftime("(%m.%d,%H,%M)")}.json', "w", encoding='utf-8') as json_file:
            json.dump(objs, json_file, indent=4)
    except:
        print("not loaded")

#_____________________________________DB_block________________________________________

def use_conn_config():
    """reading and filling connecting into a dictionarry 'conf' - configurations from conn_config.txt"""
    with open('conn_config.txt') as configuations:
        conf = {}
        for line in configuations:
            key, value = line.strip().split(' : ')
            conf[key] = value
        return conf

def create_conn(configurations):
    """ create connection to DB using 'conn_config.txt' """
    conn = None
    try:
        conn = psycopg2.connect(**configurations)
        print('Connection to PostgreSQL DB successful')
    except OperationalError as e:
        print(f'The error "{e}" occurred')
    return conn

def execute_query(conn, query):
    """ cursor creating and executing query"""
    conn.autocommit = True
    global cursor
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        print(f'Querry {query} executed')
    except OperationalError as e:
        print(f'The error "{e}" occured')

def json_preparing():
    """For each json in current dir add kyes (and value for them : None)  from db_colums for next insert js in DB"""
    db = ['link', 'Год постройки', 'Год сдачи', 'Дом', 'Жилая площадь',
          'Общая площадь', 'Отделка', 'Площадь кухни', 'Этаж', 'city', 'rooms', 'metro', 'price']
    lst_of_json = Path('data_scrb').glob('*.json')
    for js in lst_of_json:
        with open(f'{js}', 'r') as read_file:
            objs = json.load(read_file)
            for obj, nested_dict in objs.items():
                for col_db in db:
                    if col_db not in nested_dict.keys():
                        nested_dict.update({col_db: None})
        return objs


def json_to_db(conn, objs):
    """This func load files into BD for each script start
    insert data from objs into STG_tmp_apart_data"""
    for obj in objs.values():
        print(f'boj is {obj}, obj type is {type(obj)}')
        try:
            cursor.execute(
                """
                    INSERT INTO STG_tmp_apart_data (link, "Год постройки", "Год сдачи", "Дом", "Жилая площадь",
          "Общая площадь", "Отделка", "Площадь кухни", "Этаж", "city", "rooms", "metro", "price")
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, [obj['link'], obj['Год постройки'], obj['Год сдачи'], obj['Дом'], obj['Жилая площадь'],
          obj['Общая площадь'], obj['Отделка'], obj['Площадь кухни'], obj['Этаж'], obj['city'], obj['rooms'], obj['metro'], obj['price']]
        )
        except:
            print(f'{obj} - INSERT Error')


def file_processing(file_name):
    """ renames used files and replaces an archive directory"""
    if not os.path.exists('archive'):
        os.makedirs('archive')
        print(f'Catalog archive is created.')
    else:
        print(f'Catalog archive is already exists.')
    try:
        archive_path = os.path.join(os.getcwd(), 'archive')
        new_file_name = shutil.move(file_name, f'{file_name}' + '.backup')
        if os.path.exists(os.path.join(archive_path, new_file_name)):
            os.remove(os.path.join(archive_path, new_file_name))

        shutil.move(new_file_name, archive_path)
        print(f'file {file_name} removed to archive')
    except Exception as e:
        print(e)
    pass


def del_all_tmp_tables(querry):
    """dlt all tmp tables and views"""
    cursor.execute(querry)
    print('All tmp tables are deleted!')
    pass


#__________________________________________test_func___________________________________________
def test_request(url, headers, retry=5):
    """func from PythonToday to handle errors on requests, make the specified number of attempts"""
    try:
        response = requests.get(url = url, headers= {'accept': '*/*', 'user-agent':UserAgent}, proxies=checked_proxy)
        print(f'[+] {url}{response.status_code}')
    except Exception as ex:
        time.sleep(random.randint(5,21)) #отложенное выполнение в диапазоне
        if retry:
            print(f'[INFO] retry = {retry} => {url}')
            return test_request(url, retry = retry - 1)
        else:
            raise
    else:
        return response