import requests
from bs4 import BeautifulSoup
import csv
import json
import re
import time
from datetime import date, datetime, timedelta
import pandas as pd
import random
import os
import sys
import multiprocessing
from multiprocessing import Manager
import pyarrow
import pyarrow.parquet as pq
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains 
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException, NoSuchElementException

pd.options.display.width=None
pd.options.display.max_columns=None
pd.set_option('display.max_rows', 10000)
pd.set_option('display.max_columns', 20)
pd.set_option('display.max_colwidth', 100)


def read_coincarp_symbols(filepath):
    symbols = []
    ids = []
    with open(filepath, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            symbols.append(row[0])
            ids.append(row[1])

    return symbols, ids


def get_coincarp_exchange_flow(chrome_path):
    today = (datetime.today() - timedelta(hours=4)).strftime('%Y-%m-%d')
    df_head = ['#', 'Name', 'Symbol', 'On Exchange', '% Supply On Exchange', '1D Change', '1D Change %', '7D Change', '7D Change %',  '30D Change', '30D Change %']
    df_data = []
    symbols = []
    ids = []
    links = []

    webdriver_service = Service(chrome_path)
    chrome_options = Options()
    driver = webdriver.Chrome(service=webdriver_service, options=chrome_options)
    driver.minimize_window()

    for i in range(1,11):
        url = f'https://www.coincarp.com/exchangeflow/pn_{i}.html'
        driver.get(url)
        time.sleep(1)
        table = driver.find_element(By.ID, 'coinlistTable')
        
        raw_rows = table.find_elements(By.TAG_NAME, 'tr')[1:]
        rows = [r for r in raw_rows if 'sponsored-td' not in r.get_attribute('class')]
        for row in rows:
            rank = row.find_element(By.CLASS_NAME, 'td1').text
            coin_id = row.get_attribute('id')
            link = 'https://www.coincarp.com/currencies/' + coin_id + '/'
            name = row.find_element(By.CLASS_NAME, 'td2').find_element(By.CLASS_NAME, 'fullname').text
            symbol = row.find_element(By.CLASS_NAME, 'td2').find_element(By.CLASS_NAME, 'symbo').text
            sym_len = len(symbol) + 1
            cb = row.find_element(By.CLASS_NAME, 'td3')
            cex_bal = float(cb.text.replace(',', '').split(' ')[0]) if cb.text != '--' else 0.00
            psp = row.find_element(By.CLASS_NAME, 'td4')
            pct_supply = float(psp.text[:-1]) if psp.text != '--' else 0.00
            chgs = row.find_elements(By.TAG_NAME, 'td')[4:]
            chgs_values = [float(c.get_attribute('data-order')) for c in chgs]
            chgs_pcts = [float(c.find_element(By.CLASS_NAME, 'my-0').text[:-1]) if c.get_attribute('data-order') != '0' else 0.00 for c in chgs]
            row_data = [rank, name, symbol, cex_bal, pct_supply, chgs_values[0], chgs_pcts[0], chgs_values[1], chgs_pcts[1], chgs_values[2], chgs_pcts[2]]
            
            df_data.append(row_data)
            symbols.append(symbol)
            ids.append(coin_id)
            links.append(link)
        print(f'Successfully fetched data from page {i}!')
            
    df = pd.DataFrame(data=df_data, columns=df_head)
    print(df) 
    sub = 'exchange flow'
    os.makedirs(sub, exist_ok=True)
    filepath = os.path.join(sub, f'cryptos_exchange_flow_{today}.csv')
    df.to_csv(filepath, index=False)
    
    filepath2 = os.path.join(sub, f'cryptos_links_{today}.csv')
    with open(filepath2, 'w', newline='') as file:
        for sym, cid, link in zip(symbols, ids, links):
            file.write(sym + ',' + cid + ',' + link + '\n')
        file.close()
    
    return df, symbols, ids, links


def fetch_coincarp_price_trends_job(sym, cid, shared_list):
    types = ['m', '3m', '6m', 'y']  #w
    heads = ['1m', '3m', '6m', '1y']  #7d
    result_dict = {'symbol': sym, 'id': cid}
    for t, h in zip(types, heads):
        url = f'https://sapi.coincarp.com/api/v1/his/coin/trend?code={cid}&type={t}'
        response = requests.get(url)
        data = response.json()['data'] if response.status_code == 200 and 'data' in list(response.json().keys()) else []
        result_dict[h] = data
        time.sleep(0.2)
        
    shared_list.append(result_dict)
    return result_dict


def get_coincarp_price_trends(symbols, ids, core_num):
    sub = 'price trends'
    os.makedirs(sub, exist_ok=True)
    today = (datetime.today() - timedelta(hours=4)).strftime('%Y-%m-%d')
        
    pool = multiprocessing.Pool(processes=core_num)
    jobs = []
    manager = Manager()
    dict_list = manager.list()
    
    def callback(result):
        heads = ['1m', '3m', '6m', '1y']
        if any(h in list(result.keys()) for h in heads):
            result_text = f'Successfully fetched price trends data of {result["symbol"]} | Period keys count: {len(list(result.keys())[2:])}'
            print(result_text)
        else:
            result_text = f'Failed to fetch price trends data of {result["symbol"]}.'
            print(result_text)
    
    for sym, cid in zip(symbols, ids):
        job = pool.apply_async(fetch_coincarp_price_trends_job, args=(sym, cid, dict_list), callback=callback)
        jobs.append(job)
        
    for job in jobs:
        job.get()
    
    result_list = list(dict_list)
    filename = f'cryptos_price_trends_{today}.json'
    filepath = os.path.join(sub, filename)
    with open(filepath, 'w') as json_file:
        json.dump(result_list, json_file)

    price_trends_to_parquet(filename, filepath)
    print('Prices trend data of all crypto tokens successfully fetched!')
    os.remove(filepath)
    return result_list


def fetch_coincarp_cex_bal_job(sym, cid, shared_list):
    chaintypes = ['', 'ethereum', 'bscscan']
    result_dict = {'symbol': sym, 'id': cid}
    for t in chaintypes:
        url = f'https://sapi.coincarp.com/api/v1/market/walletscreen/coin/wallet?code={cid}&chainType={t}&page=1&pageSize=50&lang=en-US&isexchange=false'
        response = requests.get(url)
        raw = response.json()['data'] if response.status_code == 200 and 'data' in list(response.json().keys()) else None
        data = raw['list'] if type(raw) == dict and 'list' in list(raw.keys()) else []
        if data != []:
            t = 'mainnet' if t not in chaintypes[1:] else t
            result_dict[t] = data
        time.sleep(0.2)
    
    shared_list.append(result_dict)
    result_text = f'Successfully fetched detailed cex balances of {sym}!' if response.status_code == 200 else f'Failed to fetch detailed cex balances of {sym}.'
    return [response.status_code, result_text, url]


def get_coincarp_detailed_cex_balances(symbols, ids, core_num):
    sub = 'exchange balances'
    os.makedirs(sub, exist_ok=True)
    today = (datetime.today() - timedelta(hours=4)).strftime('%Y-%m-%d')
    
    pool = multiprocessing.Pool(processes=core_num)
    jobs = []
    manager = Manager()
    dict_list = manager.list()    

    def callback(result):
        print(result)
        
    for sym, cid in zip(symbols, ids):
        job = pool.apply_async(fetch_coincarp_cex_bal_job, args=(sym, cid, dict_list), callback=callback)
        jobs.append(job)
        
    for job in jobs:
        job.get()
        
    result_list = list(dict_list)
    filepath = os.path.join(sub, f'cryptos_cex_balances_{today}.json')
    with open(filepath, 'w') as json_file:
        json.dump(result_list, json_file)
    
    print('Detailed cex balances of all crypto tokens successfully fetched!')
    return result_list


def fetch_coincarp_socials_job(sym, cid, shared_list):
    result_dict = {'symbol': sym, 'id': cid}
    url = f'https://sapi.coincarp.com/api/v1/his/coin/coinsocial?code={cid}'
    response = requests.get(url)
    data = response.json()['data'] if response.status_code == 200 and 'data' in list(response.json().keys()) else []
    result_dict['socials'] = data
    time.sleep(0.2)
    shared_list.append(result_dict)
    result_text = f'Successfully fetched socials data of {sym}!' if response.status_code == 200 else f'Failed to fetch socials data of {sym}.'
    return [response.status_code, result_text, url]


def get_coincarp_socials(symbols, ids, core_num):
    sub = 'socials'
    os.makedirs(sub, exist_ok=True)
    today = (datetime.today() - timedelta(hours=4)).strftime('%Y-%m-%d')
    
    pool = multiprocessing.Pool(processes=core_num)
    jobs = []
    manager = Manager()
    dict_list = manager.list()    

    def callback(result):
        print(result)    
        
    for sym, cid in zip(symbols, ids):
        job = pool.apply_async(fetch_coincarp_socials_job, args=(sym, cid, dict_list), callback=callback)
        jobs.append(job)
        
    for job in jobs:
        job.get()
        
    result_list = list(dict_list)
    filepath = os.path.join(sub, f'cryptos_socials_{today}.json')
    with open(filepath, 'w') as json_file:
        json.dump(result_list, json_file)
    
    print('Socials data of all crypto tokens successfully fetched!')
    return result_list


def fetch_coincarp_holders_dist_job(sym, cid):
    sub = 'holders'
    os.makedirs(sub, exist_ok=True)
    symbol_path = os.path.join(sub, cid)
    os.makedirs(symbol_path, exist_ok=True)
    today = (datetime.today() - timedelta(hours=4)).strftime('%Y-%m-%d')
    keys = ['timestamp', 'holders', 'top 10 (%)', 'top 20 (%)', 'top 50 (%)', 'top 100 (%)']
    result_dict = {'symbol': sym, 'id': cid, 'keys': keys}
    data_dict = {}
    chaintypes = ['', 'ethereum', 'bscscan', 'solana', 'polygonscan', 'tron', 'fantom', 'avalanche', 'arbitrum', 'cronos']
    results = []
    for t in chaintypes:
        url = f'https://sapi.coincarp.com/api/v1/his/coin/chainsummary?code={cid}&platform={t}&type=1y'
        response = requests.get(url)
        raw_data = response.json()['data'] if response.status_code == 200 and 'data' in list(response.json().keys()) else None

        if type(raw_data) == list and len(raw_data) >= 1:
            t = 'mainnet' if t not in chaintypes[1:] else t
            dict_data = raw_data[-1]
            df = pd.DataFrame(data=raw_data, columns=keys)
            filepath = os.path.join(symbol_path, f'{sym}_holders_distribution_{t}_{today}.csv')
            df.to_csv(filepath, index=False)
            data_dict[t] = dict_data
            result_text = 'Success'
            results.append({t: result_text})
            time.sleep(0.2)

        elif type(raw_data) == list and len(raw_data) == 0:
            result_text = 'No Data'
            results.append({t: result_text})
            time.sleep(0.2)

        else:
            result_text = 'Failed'
            results.append({t: result_text})
            time.sleep(0.2)
    
    # result_dict['data'] = data_dict
    # shared_list.append(result_dict)
    result_text_list = {sym: results}
    return result_text_list


def get_coincarp_holders_distribution(symbols, ids, core_num):
    sub = 'holders'
    os.makedirs(sub, exist_ok=True)
    today = (datetime.today() - timedelta(hours=4)).strftime('%Y-%m-%d')
    
    pool = multiprocessing.Pool(processes=core_num)
    jobs = []
    # manager = Manager()
    # dict_list = manager.list()

    def callback(result):
        print(result)   
        
    for sym, cid in zip(symbols, ids):
        job = pool.apply_async(fetch_coincarp_holders_dist_job, args=(sym, cid), callback=callback)
        jobs.append(job)
        
    for job in jobs:
        job.get()
    
    # result_list = list(dict_list)
    # filepath = os.path.join(sub, f'cryptos_holders_distribution_{today}.json')
    # with open(filepath, 'w') as json_file:
    #     json.dump(result_list, json_file)
    print('Holders distribution data of all crypto tokens successfully fetched!')
    

def fetch_coincarp_news_job(sym, cid, path):
    today = (datetime.today() - timedelta(hours=4)).strftime('%Y-%m-%d')
    news = []
    for i in range(1, 51):
        url = f'https://sapi.coincarp.com/api/v1/news/coin/news?coincode={cid}&page={i}&pagesize=100&lang=en-US'
        response = requests.get(url)
        data = response.json()['data'] if response.status_code == 200 and 'data' in list(response.json().keys()) else None
        if response.status_code == 200 and type(data) == list and len(data) == 0:
            break
        news.extend(data)
        time.sleep(0.2)
        
    filepath = os.path.join(path, f'{sym}_news_history_{today}.json')
    with open(filepath, 'w') as json_file:
        json.dump(news, json_file)
    result_text = f'Successfully fetched news history of {cid}!' if len(news) >= 1 else f'No news history found for {cid}.'
    return result_text
        

def get_coincarp_news_hist(symbols, ids, core_num):
    sub = 'news'
    os.makedirs(sub, exist_ok=True)
    today = (datetime.today() - timedelta(hours=4)).strftime('%Y-%m-%d')

    pool = multiprocessing.Pool(processes=core_num)
    jobs = []
    
    def callback(result):
        print(result)
        
    for sym, cid in zip(symbols, ids):
        job = pool.apply_async(fetch_coincarp_news_job, args=(sym, cid, sub), callback=callback)
        jobs.append(job)
        
    for job in jobs:
        job.get()
    
    print('News data of all crypto tokens successfully fetched!')


def price_trends_to_parquet(filename, filepath):
    with open(filepath, 'r') as json_file:
        json_data = json.load(json_file)
        df = pd.DataFrame(json_data)
        table = pyarrow.Table.from_pandas(df)
        result_path = f'price trends\\{filename[:-5]}.parquet'
        pq.write_table(table, result_path)
        json_file.close()

    
    
if __name__ == '__main__':

    # Define variables
    chrome_path = r'C:\Users\user\Downloads\chromedriver-win32\chromedriver.exe'
    core = 10
    today = (datetime.today() - timedelta(hours=4)).strftime('%Y-%m-%d')

    # Main Flow
    coincarp_cex_flow, crypto_symbols, crypto_ids, crypto_links = get_coincarp_exchange_flow(chrome_path)
    crypto_symbols, crypto_ids = read_coincarp_symbols(f'exchange flow\\cryptos_links_{today}.csv')
    coincarp_cex_bals = get_coincarp_detailed_cex_balances(crypto_symbols, crypto_ids, core)
    coincarp_socials = get_coincarp_socials(crypto_symbols, crypto_ids, core)
    get_coincarp_holders_distribution(crypto_symbols, crypto_ids, 20)

    # Execute once a week, on Sunday
    # coincarp_price_trends = get_coincarp_price_trends(crypto_symbols, crypto_ids, core)
    # get_coincarp_news_hist(crypto_symbols, crypto_ids, core)