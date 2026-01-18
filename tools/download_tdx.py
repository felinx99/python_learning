
import argparse
import itertools
import psutil
import pandas as pd
import time
import tushare as ts

from enum import IntEnum
from datetime import date, datetime
from multiprocessing import Pool
from pathlib import Path

from pytdx.reader import TdxDailyBarReader, TdxLCMinBarReader, TdxFileNotFoundException
from api.timeprofile import TimeProfile


DATA_SRCPATH = 'D:\\new_tdx\\vipdoc'
PERIOD = ['lday', 'fzline', 'minline'] #日线，5分钟，1分钟
EXCHANGE = ['sh', 'sz', 'bj']

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

def read_tdx_files(srcfile:Path, dateframe:DATAFRAME):
    tdxread = TdxDailyBarReader() if dateframe == DATAFRAME.DAY else TdxLCMinBarReader()

    try:
        df_src = tdxread.get_df(f"{srcfile}")
    except Exception as e:
        return False

    fname = f"{srcfile.stem[2:]}.{srcfile.stem[:2].upper()}.csv"
    dstfile = Path(dst_dir[dateframe]) / fname

    # 转换处理
    df_src = df_src.drop('amount', axis=1)
    
    if not dstfile.exists():
        df_src.to_csv(dstfile, sep=',', encoding='utf-8-sig', index=False, date_format=date_fmt[date], float_format='%.2f') 
    else:
        df_dst = pd.read_csv(dstfile)
        df_dst['date'] = pd.to_datetime(df_dst['date'], format=date_fmt[dateframe])
        
        new_data_to_add = df_src[df_src['date'] > df_dst.iloc[-1, 0]]

        if not new_data_to_add.empty:
            df_dst = pd.concat([df_dst, new_data_to_add], ignore_index=True)
            df_dst.to_csv(dstfile, sep=',', encoding='utf-8-sig', index=False, date_format=date_fmt[dateframe], float_format='%.2f')
    return True

def init_tdx():
    task_filelist = []

    for exchange, dateframe in itertools.product(EXCHANGE, data_frame):
        directory = Path(DATA_SRCPATH)/exchange/src_dir[dateframe]  
        new_filelist = [(p, dateframe) for p in directory.glob(file_extension[dateframe])]
        task_filelist.extend(new_filelist)

    #获取物理核心数，多进程处理
    physical_cores = psutil.cpu_count(logical=False)
    if __name__ == '__main__':
        with Pool(physical_cores) as p:
            #starmap会自动将task_filelist解包传给函数
            results = p.starmap(read_tdx_files, task_filelist)
            print(f"processed {len(results)}/{len(task_filelist)} files, failed {len(task_filelist)-sum(results)}.")



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
    new_list = []					

    '''
    for fname in read_tdx_files(0):
        matching_rows_df = TICKERS_DF[TICKERS_DF.iloc[:, 0] == fname]
        if not matching_rows_df.empty:
            new_list = new_list + matching_rows_df.apply(tuple, axis=1).tolist()

    print(f'newlist len: {len(new_list)}')
    df = pd.DataFrame(data=new_list, columns=('ts_code', 'symbol', 'name', 'area', 'industry', 'list_date'))
    df.to_csv(TICKERLIST_PATH_DEST, sep=',', encoding='utf-8-sig', index=False)
    '''
    
    with TimeProfile():
        init_tdx()
    #update_tdx()



 
