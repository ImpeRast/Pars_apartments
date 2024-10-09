import random
from fake_useragent import UserAgent
from bs4 import BeautifulSoup as bs
from datetime import datetime
import requests
import time
import os
import re
import csv
import json

def check_proxy(proxy_lst):
    """Check proxy valid"""
    for cur_proxy in proxy_lst:
        print(f'The checking proxy is {cur_proxy}')
        time.sleep(3)
        proxies = {
              'http' :  f'http://{cur_proxy}',
              'https' :  f'https://{cur_proxy}'
              }
        try:
            resp = requests.get('https://api.ipify.org/',
                             proxies=proxies)
            print(resp)
            soup = bs(resp.text, 'lxml')
            if cur_proxy in soup:
                checked_proxy = cur_proxy
                return checked_proxy
        except:
            print(f'try next')

#check_proxy(proxy_lst)

def get_user_agent():
    headers = UserAgent().getRandom
    for key, value in headers.items():
        headers[key] = str(value)
    headers['User-Agent'] = headers.pop('useragent')
    return headers

headers = get_user_agent()
"""
cur_proxy = '189.17.29.100:4230'
proxies = {
    'http': f'http://{cur_proxy}',
    'https': f'https://{cur_proxy}'
    }
resp = requests.get('https://api.ipify.org/', headers=headers,
                         proxies=proxies)
print(resp.text)
soup = bs(resp.text, 'lxml')
time.sleep(1)
without_port = re.findall(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}', soup.text) #Checking IP replacement

print(without_port)
without_port = str(without_port[0])

if without_port in cur_proxy:
    checked_proxy = cur_proxy

proxies = {
        'https': f'https://{checked_proxy}',
        'http': f'http://{checked_proxy}'
    }

print(f'Connection with proxy: {checked_proxy}')
"""

#response = requests.get(url='https://pythonist.ru/python-requests/',
  #                  headers=headers)


"""create tuple of links from first page"""
#the links : ['https://www.cian.ru/sale/flat/306341785/', 'https://www.cian.ru/sale/flat/304785312/', 'https://www.cian.ru/sale/flat/308077448/', 'https://www.cian.ru/sale/flat/306541560/', 'https://www.cian.ru/sale/flat/299890518/', 'https://www.cian.ru/sale/flat/306337748/', 'https://www.cian.ru/sale/flat/307196104/', 'https://www.cian.ru/sale/flat/301550653/', 'https://www.cian.ru/sale/flat/307544992/', 'https://www.cian.ru/sale/flat/303859434/', 'https://www.cian.ru/sale/flat/305679800/', 'https://www.cian.ru/sale/flat/307144720/', 'https://www.cian.ru/sale/flat/304753663/', 'https://www.cian.ru/sale/flat/307014462/', 'https://www.cian.ru/sale/flat/305952762/', 'https://www.cian.ru/sale/flat/296921064/', 'https://www.cian.ru/sale/flat/307553258/', 'https://www.cian.ru/sale/flat/305498891/', 'https://www.cian.ru/sale/flat/306687240/', 'https://www.cian.ru/sale/flat/307490180/', 'https://www.cian.ru/sale/flat/303293513/', 'https://www.cian.ru/sale/flat/308088559/', 'https://www.cian.ru/sale/flat/307537755/', 'https://www.cian.ru/sale/flat/307709150/', 'https://www.cian.ru/sale/flat/293713671/', 'https://www.cian.ru/sale/flat/308160720/', 'https://www.cian.ru/sale/flat/308238615/', 'https://www.cian.ru/sale/flat/307273456/']

def get_user_agent():
    headers = UserAgent().getRandom
    for key, value in headers.items():
        headers[key] = str(value)
    headers['User-Agent'] = headers.pop('useragent')
    return headers

headers = get_user_agent()



response = requests.get(url='https://www.cian.ru/sale/flat/304785312/', headers=headers)
time.sleep(3)
soup = bs(response.text, 'lxml')

city = soup.find('a', 'a10a3f92e9--address--SMU25'
                 ).text
print(city)
city = {'city': city}
print("Can't get city")
city = {'city' : None}

# ______________rooms___________________
rooms = soup.find_all('h1', 'a10a3f92e9--title--vlZwT'
                      )
r_rooms = 'ся (.*?комн.)'
rooms = re.findall(r_rooms, str(rooms))
print(rooms)
rooms = {'rooms' : str(*rooms)}
print("Can't get rooms")
rooms = {'rooms' : None}

#______________price___________________
price = soup.find(
    'div', attrs={'data-testid': 'price-amount'}
).text
print(price)
price = {'price': price}
print("Can't get price")
price = {'price' : None}

# ______________metro___________________
metro = soup.find('a', 'a10a3f92e9--underground_link--VnUVj').text
print(metro)
metro = {'metro' : metro}
print(f"Can't get metro")
metro = {'metro' : None}

# ______________to_the_metro___________________
# - I should make it later

# ______________oter_params___________________
other_params = soup.find_all(
    'div', attrs={"a10a3f92e9--text--eplgM"})
keys, vals = [], []
r_keys, r_vals = 'U5">(.*?)</sp', '2px">(.*?)</sp'  # regular expression for searching parameters and their values
keys.append(re.findall(r_keys, str(other_params)))
vals.append(re.findall(r_vals, str(other_params)))
other_params = dict(zip(*keys, *vals))
if 'Год постройки' in other_params.keys():
    other_params['Год постройки'] = str(other_params['Год постройки']) + '-01-01'  # change format on date
if 'Год сдачи' in other_params.keys():
    other_params['Год сдачи'] = str(other_params['Год сдачи']) + '-01-01'  # change format on date
print(other_params)
