import argparse
import itertools
import psutil
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from enum import IntEnum
from datetime import datetime, timedelta
from multiprocessing import Pool
from pathlib import Path

from pytdx.reader import TdxDailyBarReader, TdxLCMinBarReader, TdxFileNotFoundException
from api.timeprofile import TimeProfile
from backtest.util import datafeed


DATA_SRCPATH = 'D:\\new_tdx\\vipdoc'
PERIOD = ['lday', 'fzline', 'minline'] #日线，5分钟，1分钟
EXCHANGE = ['sh', 'sz', 'bj']

SECTOR_PATH = 'E:\\datas\\tdx\\sector'

class DATAFRAME(IntEnum):
    DAY = 0
    MINUTE5 = 1
    MINUTE1 = 2

src_dir = {
    DATAFRAME.DAY: 'lday',
    DATAFRAME.MINUTE5: 'fzline',
    DATAFRAME.MINUTE1: 'minline',
}

file_extension = {
    DATAFRAME.DAY: '*.day',
    DATAFRAME.MINUTE5: '*.lc5',
    DATAFRAME.MINUTE1: '*.lc1',
}

dst_dir = {
    DATAFRAME.DAY: 'E:\\datas\\tdx\\day_2018_2025',
    DATAFRAME.MINUTE5: 'E:\\datas\\tdx\\5m_2025',
    DATAFRAME.MINUTE1: 'E:\\datas\\tdx\\1m_2025',
}

data_frame = [DATAFRAME.DAY, DATAFRAME.MINUTE5, DATAFRAME.MINUTE1]

date_fmt = {
    DATAFRAME.DAY: '%Y-%m-%d',
    DATAFRAME.MINUTE5: '%Y-%m-%d %H:%M',
    DATAFRAME.MINUTE1: '%Y-%m-%d %H:%M',
}

stock_csvtype = {
    'open': 'float32',
    'high': 'float32',
    'low': 'float32',
    'close': 'float32',
    'volume': 'float64',
}

sector_type = {
    'self' : 'SECTOR_SELF', #自选股
    'hold' : 'SECTOR_HOLD', #持仓股
    'all' : 'SECTOR_ALL',   #所有A股
    'concept' : 'SECTOR_CONCEPT',   #概念板块
    'l1' : 'SECTOR_L1',  #行业一级
    'l2' : 'SECTOR_L2',
    'l3' : 'SECTOR_L3'
}

def save_csvfile(path=None, data=None, period=None):
    datefmt = date_fmt[period]
    
    if not path.exists():
        data['date'] = data['date'].astype('datetime64[s]')
        data.to_csv(path, sep=',', encoding='utf-8-sig', index=False, date_format=datefmt, float_format='%.2f')
        return data
    else:
        try:
            df_dst = pd.read_csv(path, dtype=stock_csvtype, parse_dates=['date'])
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
    dstfile = Path(dst_dir[dateframe]) / f"{symbol}.csv"

    # 转换处理
    df_src = df_src.drop('amount', axis=1)
    
    new_pbrows = save_csvfile(dstfile, df_src, dateframe)
    if new_pbrows is not None:
        new_pbrows['symbol'] = symbol
        return new_pbrows

    return None

def download_stock():
    task_filelist = []

    for exchange, dateframe in itertools.product(EXCHANGE, data_frame):
        directory = Path(DATA_SRCPATH)/exchange/src_dir[dateframe]  
        new_filelist = [(p, dateframe) for p in directory.glob(file_extension[dateframe])]
        task_filelist.extend(new_filelist)

    #获取物理核心数，多进程处理
    physical_cores = psutil.cpu_count(logical=False)
    pb_data = []

    with Pool(physical_cores) as p:
        #starmap会自动将task_filelist解包传给函数
        
        results = p.imap_unordered(read_tdx_files, task_filelist, chunksize=200)
        for i, res in enumerate(results):
            if res is not None:
                pb_data.append(res)
            print(f"已处理: {i+1}/{len(task_filelist)}...", end='\r')

    #将pb_data写入Parquet
    if pb_data:
        final_df = pd.concat(pb_data, ignore_index=True)
        for col, dtype in stock_csvtype.items():
            final_df[col] = final_df[col].astype(dtype)
        
        pb_path = Path(dst_dir[DATAFRAME.DAY]) /'all_stock_daily.parquet'
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
        concept_list_df = datafeed.get_sector_list(sector_type=sector_type[sector])
        concept_list_df.to_csv(listfile, index=False, encoding='utf-8-sig')

def download_sector_daily(datafeed=None, listfile=None, sector=None):
    #每天更新概念板块指数
    assert listfile.exists(), f"Error: '{listfile}'"
    concept_list_df = pd.read_csv(listfile, skiprows=1, header=None)
    end_date = '' #到今天为止
    start_date = '20200101'

    for _, sector_code in concept_list_df.iterrows():
        symbol = f"{sector_code[0]}"
        dstfile = Path(SECTOR_PATH)/sector/'daily'/f"{symbol}.csv"

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
            return new_pbrows

def update_sector_daily():
    #每天上传到通达信板块指数内
    pass

def download_sector(sectorlist=[]):
    feed = datafeed.FeedManager.register('tdx')
    feed.init_feed()
    
    pb_data = []

    for sector in sectorlist:
        sectorfile = Path(SECTOR_PATH)/sector/f"{sector}_list.csv"
        download_sector_list(datafeed=feed, listfile=sectorfile, sector=sector)
        res = download_sector_daily(datafeed=feed, listfile=sectorfile, sector=sector)
        if res is not None:
            pb_data.append(res)

    if pb_data:
        final_df = pd.concat(pb_data, ignore_index=True)
        for col, dtype in stock_csvtype.items():
            final_df[col] = final_df[col].astype(dtype)
        
        pb_path = Path(SECTOR_PATH) /'all_sector_daily.parquet'
        table = pa.Table.from_pandas(final_df)
        
        with pq.ParquetWriter(pb_path, table.schema, compression='snappy') as writer:
            writer.write_table(table)

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
    
     
    filepath = Path(ARG_ITEMS['fpath'])
    TICKERLIST_PATH_SRC = Path(ARG_ITEMS['fpath'])/'stocklist_all.csv'
    TICKERLIST_PATH_DEST = Path(ARG_ITEMS['fpath'])/'stocklist.csv'

    assert TICKERLIST_PATH_SRC.exists(), f"Error: '{TICKERLIST_PATH_SRC}'"
    
    TICKERS_DF = pd.read_csv(TICKERLIST_PATH_SRC, skiprows=1, header=None)				
    
    with TimeProfile():
        download_stock()
        download_sector(sectorlist=['concept', 'l1', 'l2', 'l3'])




 
