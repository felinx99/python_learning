import argparse
import itertools
import numpy as np
import psutil
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import talib as ta

from enum import IntEnum
from datetime import datetime, timedelta
from multiprocessing import Pool
from pathlib import Path

from pytdx.reader import TdxDailyBarReader, TdxLCMinBarReader, TdxFileNotFoundException
from api.timeprofile import TimeProfile
from backtest.util import datafeed

from common import CONFIG, DATAFRAME


def prepare_date(df=None):
    method = 'sma'
    method_short = f'{method}{CONFIG.SHORT}'
    method_mid = f'{method}{CONFIG.MID}'
    method_long = f'{method}{CONFIG.LONG}'

    df['ohlc'] = df.eval('(high + 2*open + 2*close + low) / 6')
    #calc_ols(df=df, column='ohlc', method=method_short)                
    #calc_ols(df=df, column='ohlc', method=method_mid)
    #calc_ols(df=df, column='ohlc', method=method_long)

    return df

def save_csvfile(path=None, data=None, period=None):
    datefmt = CONFIG.date_fmt[period]
    
    if not path.exists():
        data['date'] = data['date'].astype('datetime64[s]')
        data.to_csv(path, sep=',', encoding='utf-8-sig', index=False, date_format=datefmt, float_format='%.2f')
        return data
    else:
        try:
            df_dst = pd.read_csv(path, dtype=CONFIG.stock_csvtype, parse_dates=['date'])
        except Exception as e:
            return None

        df_dst['date'] = df_dst['date'].astype('datetime64[s]')      
        new_data_to_add = data[data['date'] > df_dst.iloc[-1, 0]]

        if not new_data_to_add.empty:
            df_dst = pd.concat([df_dst, new_data_to_add], ignore_index=True)
            df_dst.to_csv(path, sep=',', encoding='utf-8-sig', index=False, date_format=datefmt, float_format='%.2f')
            
        if period == DATAFRAME.DAY:
            return df_dst
        else:
            return None
 

def read_tdx_files(args):
    srcfile, dateframe = args
    tdxread = TdxDailyBarReader() if dateframe == DATAFRAME.DAY else TdxLCMinBarReader()

    try:
        df_src = tdxread.get_df(f"{srcfile}")
    except Exception as e:
        return None
    
    symbol = f"{srcfile.stem[2:]}.{srcfile.stem[:2].upper()}"
    dstfile = Path(CONFIG.tdx_data_path[dateframe]) / f"{symbol}.csv"

    # 转换处理
    df_src = df_src.drop('amount', axis=1)
    
    new_pbrows = save_csvfile(dstfile, df_src, dateframe)
    if new_pbrows is not None:
        new_pbrows['symbol'] = symbol
        return new_pbrows

    return None

def download_stock():
    task_filelist = []

    for exchange, dateframe in itertools.product(CONFIG.EXCHANGE, DATAFRAME):
        directory = CONFIG.inferred_path['TDX_INSTALL_DATADIR']/exchange/CONFIG.src_dir[dateframe]  
        new_filelist = [(p, dateframe) for p in directory.glob(CONFIG.file_extension[dateframe])]
        task_filelist.extend(new_filelist)

    #获取物理核心数，多进程处理
    physical_cores = psutil.cpu_count(logical=False)
    pb_data = []

    with Pool(physical_cores) as p:
        #starmap会自动将task_filelist解包传给函数
        
        results = p.imap_unordered(read_tdx_files, task_filelist, chunksize=200)
        for i, res in enumerate(results):
            if res is not None:
                res = prepare_date(res)
                pb_data.append(res)

            print(f"已处理: {i+1}/{len(task_filelist)}...", end='\r')

    #将pb_data写入Parquet
    if pb_data:
        final_df = pd.concat(pb_data, ignore_index=True)
        for col, dtype in CONFIG.stock_csvtype.items():
            final_df[col] = final_df[col].astype(dtype)
        
        pb_path = Path(CONFIG.tdx_data_path[DATAFRAME.DAY]) /'all_stock_daily.parquet'
        header = not Path(pb_path).exists()
        table = pa.Table.from_pandas(final_df)
        
        with pq.ParquetWriter(pb_path, table.schema, compression='snappy') as writer:
            writer.write_table(table)


def download_sector_list(datafeed=None, listfile=None, sector=None):
    #每周更新一次概念板块list
    downloadsignal = False
    weekday = datetime.now().weekday()
    
    if listfile.exists() == False or weekday == 5: 
            downloadsignal = True

    if downloadsignal:
        sector_list_df = datafeed.get_sector_list(sector_type=CONFIG.sector_type[sector])
        sector_list_df.to_csv(listfile, index=False, encoding='utf-8-sig')

def download_sector_daily(datafeed=None, listfile=None, sector=None):
    #每天更新概念板块指数
    assert listfile.exists(), f"Error: '{listfile}'"
    sector_list_df = pd.read_csv(listfile, skiprows=1, header=None)
    end_date = '' #到今天为止
    start_date = '20200101'
    sector_daily = []

    for _, sector_code in sector_list_df.iterrows():
        symbol = f"{sector_code[0]}"
        dstfile = CONFIG.inferred_path['TDX_SECTOR_PATH']/sector/'daily'/f"{symbol}.csv"

        if dstfile.exists():
            #如果文件存在，仅下载最近5天数据
            start_date = (datetime.now() - timedelta(days=5)).strftime('%Y%m%d') 

        try:
            hist = datafeed.get_daily(symbol=sector_code[0], start_date=start_date, end_date=end_date)    
        except Exception as e:
            print(f"获取数据{sector_code[0]}失败: {e}")
            continue
    
        new_pbrows = save_csvfile(path=dstfile, data=hist, period=DATAFRAME.DAY)

        if new_pbrows is not None:
            new_pbrows['symbol'] = symbol
            new_pbrows = prepare_date(new_pbrows)
            sector_daily.append(new_pbrows)
    
    return sector_daily

def update_sector_daily():
    #每天上传到通达信板块指数内
    pass

def download_sector(sectorlist=[]):
    feed = datafeed.FeedManager.register('tdx')
    feed.init_feed()
    
    pb_data = []

    for sector in sectorlist:
        sectorfile = CONFIG.inferred_path['TDX_SECTOR_PATH']/sector/f"{sector}_list.csv"
        download_sector_list(datafeed=feed, listfile=sectorfile, sector=sector)
        res = download_sector_daily(datafeed=feed, listfile=sectorfile, sector=sector)
        if len(res) > 0:
            pb_data += res

    if pb_data:
        final_df = pd.concat(pb_data, ignore_index=True)
        for col, dtype in CONFIG.stock_csvtype.items():
            final_df[col] = final_df[col].astype(dtype)
        
        pb_path = CONFIG.inferred_path['TDX_SECTOR_PATH'] /'all_sector_dailys.parquet'
        table = pa.Table.from_pandas(final_df)
        
        with pq.ParquetWriter(pb_path, table.schema, compression='snappy') as writer:
            writer.write_table(table)

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser()
    #PARSER.add_argument('-f', '--fpath', required=True)
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
    
     
    #filepath = Path(ARG_ITEMS['fpath'])
    #TICKERLIST_PATH_SRC = Path(ARG_ITEMS['fpath'])/'stocklist_all.csv'
    #TICKERLIST_PATH_DEST = Path(ARG_ITEMS['fpath'])/'stocklist.csv'

    TICKERLIST_PATH_SRC = CONFIG.base_path['TDX_DATA_PATH']/'stocklist_all.csv'
    TICKERLIST_PATH_DEST = CONFIG.base_path['TDX_DATA_PATH']/'stocklist.csv'


    assert TICKERLIST_PATH_SRC.exists(), f"Error: '{TICKERLIST_PATH_SRC}'"
    
    TICKERS_DF = pd.read_csv(TICKERLIST_PATH_SRC, skiprows=1, header=None)				
    
    with TimeProfile():
        download_stock()
        download_sector(sectorlist=['concept', 'l1', 'l2', 'l3'])




 
