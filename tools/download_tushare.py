import os
import argparse
from datetime import date,datetime, timedelta
import time
import pandas as pd
import tushare as ts

from api import yahoo
import logging

def get_tusharedata(ticker=None, start_date=None, end_date=None):    
    tspro = ts.pro_api()
    df = pd.DataFrame()

    if end_date is None:
        yesterday = date.today() - timedelta(days=1)
        end_date =  yesterday.strftime('%Y%m%d')
             
    # 获取新数据
    for attempt in range(1, 4):
        try:
            df = tspro.daily(ts_code=ticker, start_date=start_date, end_date=end_date)
            #df = tspro.pro_bar(ts_code=ticker, adj='qfq', start_date=start_date, end_date=end_date)
            break
        except Exception as e:
            print(f"Error fetching data for {ticker}: {e}. Retrying {attempt}/3...")
            time.sleep(5)  # 等待5秒后重试

    if not df.empty:
        df = df.rename(columns={
            'trade_date': 'date',
            'open': 'open',
            'high': 'high',
            'low': 'low',
            'close': 'close',
            'vol': 'volume'
        })
        df = df[['date', 'open', 'high', 'low', 'close', 'volume']]
        df = df.sort_values(by='date', ascending=True)

        #每只股票，休息1秒，防止频率过快
        time.sleep(1) 
    
    return df

def create_list(fpath=None):
    tspro = ts.pro_api()

    if os.path.exists(fpath):
        df = tspro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
        df = df[['ts_code', 'symbol', 'name', 'area', 'industry', 'list_date']]                    
        df.to_csv(fpath, sep=',', encoding='utf-8-sig', index=False)
    else:
        print(f'Path {fpath} not exists, please create it first.')

def save_all(tickers, startdate=20250101, enddate=20250601, fpath=None):
    total_items = len(tickers)
    # df = tspro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    # df = df[['ts_code', 'symbol', 'name', 'area', 'industry', 'list_date']]                    
    # df.to_csv(os.path.join(os.path.dirname(__file__), '../data/price/stocklist.csv'), index=False)
    for idx, (ticker, listdate) in enumerate(tickers):
        idx1 = idx + 1
        filename = os.path.join(fpath, '{ticker}.csv'.format(ticker=ticker))

        #if os.path.exists(filename):
            #continue
        
        if listdate > startdate:
            start_date = str(listdate) #listdate是numpy.int64格式，一定要强制转换为str
        else:
            start_date = str(startdate)

        print(f'Ticker:{ticker}, listdate:{listdate} startdate:{start_date}, Progress: {idx1}/{total_items}')
        
        df = get_tusharedata(ticker=ticker, start_date=start_date, end_date=str(enddate))
        if not df.empty:
            df.to_csv(path_or_buf=filename, sep=',', encoding='utf_8_sig', index=False) 
        

def update_all(tickers, startdate=20250101, enddate=20250601, fpath=None):
    total_items = len(tickers)

    for idx, (ticker, listdate) in enumerate(tickers): 
        idx1 = idx + 1    
        filename = os.path.join(fpath, '{ticker}.csv'.format(ticker=ticker))
        try:
            df = pd.read_csv(filename)
            lastdate = datetime.strptime(str(df.iloc[-1, 0]), '%Y%m%d').date()  # 获取最后一行的日期，并转换为date对象
            #if len(df) < 400:
                #print(f'File {ticker} re-download it.')
        except (IOError, IndexError):
            if listdate > startdate:
                start_date = str(listdate) #listdate是numpy.int64格式，一定要强制转换为str
            else:
                start_date = str(startdate)

            print(f'File not found, Ticker:{ticker} download')
            
            df = get_tusharedata(ticker, start_date=start_date)
            if not df.empty:
                df.to_csv(path_or_buf=filename, sep=',', encoding='utf_8_sig', index=False) 
            
            continue
        
        # 确定新数据的开始日期（从上次最后日期的第二天开始）
        start_date = lastdate + timedelta(days=1)
        end_date = datetime.strptime(str(enddate), '%Y%m%d').date()
        if start_date >= end_date: #date.today():
            print(f"Last Record date: {lastdate}. No update required. Progress: {idx1}/{total_items}。")
            continue
        else:
            new_data_df = get_tusharedata(ticker, start_date.strftime('%Y%m%d'), str(enddate))
            if new_data_df.empty:
                print(f"No new data available. Progress: {idx1}/{total_items}。")
            else:
                # 将新数据追加到文件末尾
                # mode='a' 表示追加模式
                # header=False 表示不写入列头
                # index=False 表示不写入索引
                new_data_df.to_csv(path_or_buf=filename, sep=',', mode='a', encoding='utf_8_sig', header=False, index=False)
                print(f"Add {len(new_data_df)} datas {filename}, Progress:{idx1}/{total_items}。")

def check_all(tickers, startdate=20250101, fpath=None):
    total_items = len(tickers)

    for idx, (ticker, listdate) in enumerate(tickers): 
        idx1 = idx + 1    
        filename = os.path.join(fpath, '{ticker}.csv'.format(ticker=ticker))
        try:
            df = pd.read_csv(filename)
            if len(df) < 1650:
                print(f'File {ticker}--{len(df)} re-download it.')
        except  Exception as e:
            print(f"Error for {ticker}: {e}...")
            time.sleep(5)  # 等待5秒后重试
            

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser()
    PARSER.add_argument('-f', '--fpath', required=True)
    PARSER.add_argument('-s', '--start', required=False, default=20250101, type=int,
                        help='start date for a stock, format is YYYYMMDD')
    PARSER.add_argument('-e', '--end', required=False, default=0, type=int,
                        help='end date for a stock, format is YYYYMMDD, None means to yesterday')
    PARSER.add_argument('--save', action='store_true', required=False,
                        help='save all stocks for specified date, so both start&end are required')
    PARSER.add_argument('--update', action='store_true', required=False,
                        help='update all stocks upto yesterday')
    PARSER.add_argument('--list', action='store_true', required=False,
                        help='create/update a stock list')
    PARSER.add_argument('--check', action='store_true', required=False,
                        help='check stock have enough data')
    

    ARGS = PARSER.parse_args()
    ARG_ITEMS = vars(ARGS)
    
    ts.set_token('665ecc81b76150c0ae793d389dbf298f5545abe110d5e862211430df')
    # print(ts.__version__)
    # 配置日志级别为DEBUG
    #logging.basicConfig(level=logging.DEBUG)
    
    filepath = os.path.dirname(ARG_ITEMS['fpath'])
    TICKER_CSV_PATH = os.path.join(filepath, 'stocklist.csv')
    
    if ARG_ITEMS['list'] == True:
        create_list(fpath=TICKER_CSV_PATH)
    else:
        if os.path.exists(TICKER_CSV_PATH):
            # 多列转换为tuple列表
            TICKERS_DF = pd.read_csv(TICKER_CSV_PATH, usecols=[0,5], skiprows=1, header=None) #read_csv返回的DF数据格式
            # 单列转换
            # TICKERS = TICKERS_DF.iloc[1:, 0].tolist()   #第0列，取第一列数据，并转为list格式 
            TICKERS_records = TICKERS_DF.to_records(index=False)
            TICKERS = list(TICKERS_records)
            if ARG_ITEMS['save'] == True:
                save_all(TICKERS, startdate=ARG_ITEMS['start'], enddate=ARG_ITEMS['end'], fpath=ARG_ITEMS['fpath'])
            elif ARG_ITEMS['update'] == True:                
                update_all(TICKERS, startdate=ARG_ITEMS['start'], enddate=ARG_ITEMS['end'], fpath=ARG_ITEMS['fpath'])
            elif ARG_ITEMS['check'] == True:
                check_all(TICKERS, startdate=ARG_ITEMS['start'], fpath=ARG_ITEMS['fpath'])
        else:
            print(f'File {TICKER_CSV_PATH} not exists, please run with --list first to create it.')
