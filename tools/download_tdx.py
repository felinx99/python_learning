from pathlib import Path
import argparse
from datetime import date,datetime, timedelta
import time
import pandas as pd
import tushare as ts

from pytdx.reader import TdxDailyBarReader, TdxLCMinBarReader, TdxFileNotFoundException

DATA_SRCPATH = 'c:\\new_tdx\\vipdoc'
DATA_DESTDAYPATH = 'E:\\datas\\tdx\\day_2018_2025'
DATA_DEST5MPATH = 'E:\\datas\\tdx\\5m_2025'
DATA_DEST1MPATH = 'E:\\datas\\tdx\\1m_2025'
PERIOD = ['lday', 'fzline', 'minline'] #日线，5分钟，1分钟
EXCHANGE = ['sh', 'sz', 'bj']


def read_tdx_files(period_type=0):
    if period_type == 0:
        reader = TdxDailyBarReader()
        for exchange_dir in EXCHANGE:
            directory = Path(DATA_SRCPATH)/exchange_dir/'lday'
            csv_files = list(directory.glob('*.day'))

            total_items = len(csv_files)
            for idx,file_path in enumerate(csv_files):
                idx1 = idx + 1
                #rename = f"{file_path.stem[2:]}.{file_path.stem[:2].upper()}"
                fname = f"{file_path.stem[2:]}.{file_path.stem[:2].upper()}" +f".csv"
                dst_fpath= Path(DATA_DESTDAYPATH)/fname
                try:
                    df = reader.get_df(f"{file_path}")
                except Exception as e:
                    continue
                # 转换处理
                df = df.drop('amount', axis=1)
                df['date'] = df['date'].str.replace('-', '').astype(int)  
                if not dst_fpath.exists():
                    df.to_csv(dst_fpath, sep=',', encoding='utf-8-sig', index=False) 
                else:
                    df_dst = pd.read_csv(dst_fpath)
                    lastdate = df_dst.iloc[-1, 0]
                    new_data_to_add = df[df['date'] > lastdate]
                    if not new_data_to_add.empty:
                        df_dst = pd.concat([df_dst, new_data_to_add], ignore_index=True)
                        df_dst.to_csv(dst_fpath, sep=',', encoding='utf-8-sig', index=False) 
                        print(f"Add {len(new_data_to_add)} datas {dst_fpath}, Progress:{idx1}/{total_items}。")

                #yield rename
        print('day finished')
    elif period_type == 1:
        reader = TdxLCMinBarReader()
        for exchange_dir in EXCHANGE:
            directory = Path(DATA_SRCPATH)/exchange_dir/'fzline'
            csv_files = list(directory.glob('*.lc5'))

            total_items = len(csv_files)
            for idx,file_path in enumerate(csv_files):
                idx1 = idx + 1
                fname = f"{file_path.stem[2:]}.{file_path.stem[:2].upper()}" +f".csv"
                dst_fpath= Path(DATA_DEST5MPATH)/fname
                try:
                    df = reader.get_df(file_path)
                except Exception as e:
                    continue
                df = df.drop('amount', axis=1)
                if not dst_fpath.exists():
                    df.to_csv(f'{dst_fpath}', sep=',', encoding='utf-8-sig', index=False) 
                else:
                    df_dst = pd.read_csv(dst_fpath)
                    lastdate = datetime.strptime(str(df_dst.iloc[-1, 0]), '%Y-%m-%d %H:%M')
                    src_datetime_series = pd.to_datetime(df['date'], format='%Y-%m-%d %H:%M')

                    new_data_to_add = df[src_datetime_series > lastdate]
                    if not new_data_to_add.empty:
                        df_dst = pd.concat([df_dst, new_data_to_add], ignore_index=True)
                        df_dst.to_csv(dst_fpath, sep=',', encoding='utf-8-sig', index=False)
                        print(f"Add {len(new_data_to_add)} datas {dst_fpath}, Progress:{idx1}/{total_items}。")
        print('5min finished')   
    elif period_type == 2:
        reader = TdxLCMinBarReader()
        for exchange_dir in EXCHANGE:
            directory = Path(DATA_SRCPATH)/exchange_dir/'minline'
            csv_files = list(directory.glob('*.lc1'))
        
            total_items = len(csv_files)
            for idx,file_path in enumerate(csv_files):
                idx1 = idx + 1
                fname = f"{file_path.stem[2:]}.{file_path.stem[:2].upper()}" +f".csv"
                dst_fpath= Path(DATA_DEST1MPATH)/fname
                try:
                    df = reader.get_df(f"{file_path}")                           
                except Exception as e:
                    continue
                df = df.drop('amount', axis=1)  
                if not dst_fpath.exists():
                    df.to_csv(dst_fpath, sep=',', encoding='utf-8-sig', index=False) 
                else:
                    df_dst = pd.read_csv(dst_fpath)
                    lastdate = datetime.strptime(str(df_dst.iloc[-1, 0]), '%Y-%m-%d %H:%M')
                    src_datetime_series = pd.to_datetime(df['date'], format='%Y-%m-%d %H:%M')

                    new_data_to_add = df[src_datetime_series > lastdate]
                    if not new_data_to_add.empty:
                        df_dst = pd.concat([df_dst, new_data_to_add], ignore_index=True)
                        df_dst.to_csv(dst_fpath, sep=',', encoding='utf-8-sig', index=False)
                        print(f"Add {len(new_data_to_add)} datas {dst_fpath}, Progress:{idx1}/{total_items}。")
        print('1min finished') 

def init_tdx():
    read_tdx_files(0)
    read_tdx_files(1)
    read_tdx_files(2)


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
    

    init_tdx()
    #update_tdx()



 
