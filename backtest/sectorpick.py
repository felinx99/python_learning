import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import talib as ta

from pathlib import Path
from .util import datafeed

SECTOR_PATH = 'E:\\datas\\tdx\\sector'
SLOPE_THRESHOLD = 0.015

sector_type = {
    'self' : 'SECTOR_SELF', #自选股
    'hold' : 'SECTOR_HOLD', #持仓股
    'all' : 'SECTOR_ALL',   #所有A股
    'concept' : 'SECTOR_CONCEPT',   #概念板块
    'l1' : 'SECTOR_L1',  #行业一级
    'l2' : 'SECTOR_L2',
    'l3' : 'SECTOR_L3'
}

stock_csvtype = {
    'open': 'float32',
    'high': 'float32',
    'low': 'float32',
    'close': 'float32',
    'volume': 'float64',
}

def calc_ols(df, column='', method=''):
    windows_len = 5
    N = int(''.join(filter(str.isdigit, method)))
    # 提取数据,并价格转换为对数值，对数体现价格的等比变化程序
    df[method] = ta.SMA(df[column].values.astype('float64'), timeperiod=N)
    y = np.log(df[method].values)
    n_rows = len(y)   
    k_array = np.full(n_rows, np.nan)
    x = np.arange(windows_len)
    x_mean = np.mean(x)
    sum_x2_minus_mean = np.sum((x - x_mean)**2) # 对应公式的分母
    
    # 只有当索引大于等于 N + (N-1) 时，才有足够的数据计算
    start_idx = N + windows_len - 1 
    
    # 使用 NumPy 的滑动窗口视图提高效率
    from numpy.lib.stride_tricks import sliding_window_view
    
    if n_rows >= start_idx:
        windows = sliding_window_view(y, window_shape=windows_len)
        # 计算斜率 k: sum((x - x_mean) * (y - y_mean)) / sum((x - x_mean)^2)
        x_centered = x - x_mean
        k_values = np.dot(windows, x_centered) / sum_x2_minus_mean    
        k_array[windows_len-1:] = k_values
    # 赋值给 DataFrame
    column_name = f"{method}_slope"
    df[column_name] = k_array*10
  
    return df

def select_sector(df):
    df['avg_slope'] = 0.4*df['sma5_slope'] + 0.2*df['sma10_slope'] + 0.4*df['sma20_slope']
    #筛选条件 
    df['slect_res'] = df['avg_slope'] > SLOPE_THRESHOLD

    return df['slect_res'].iloc[-1], df['avg_slope'].iloc[-1]

def get_up_sector(sectorlist=[], ret=''):

    assert ret in ['today', 'daily'], f"Params Error: 'ret={ret}'"
    df_result = None
    df_list = []
    daily_treandup_sector_df = None

    for sector in sectorlist:
        sectorlistpath = Path(SECTOR_PATH)/sector/f"{sector}_list.csv"
        sectorlist_df = pd.read_csv(sectorlistpath, skiprows=1, header=None)
        for _, sector_code in sectorlist_df.iterrows():
            symbol = f"{sector_code[0]}"
            sector_file = Path(SECTOR_PATH)/sector/'daily'/f"{symbol}.csv"
            sector_df = pd.read_csv(sector_file, dtype=stock_csvtype, parse_dates=['date'])
            sector_df['ohlc'] = sector_df.eval('(high + 2*open + 2*close + low) / 6')
            calc_ols(df=sector_df, column='ohlc', method='sma5')                
            calc_ols(df=sector_df, column='ohlc', method='sma10')
            calc_ols(df=sector_df, column='ohlc', method='sma20')
            sector_df['symbol'] = symbol
            sector_df['name'] = f"{sector_code[1]}"
            #dst_file = Path(SECTOR_PATH)/sector/'daily'/f"{symbol}_slope.csv"
            #sector_df.to_csv(dst_file, sep=',', encoding='utf-8-sig', index=False, date_format='%Y-%m-%d', float_format='%.3f')
            select_sector(sector_df)
            df_list.append(sector_df.loc[sector_df['avg_slope'] > SLOPE_THRESHOLD, ['date', 'symbol', 'name', 'avg_slope']].copy())
            

    df_result = pd.concat(df_list , ignore_index=True)
    df_result = df_result.sort_values(['date', 'avg_slope'], ascending=False)

    pb_path = Path(SECTOR_PATH) /'trendup_sectors_everydaily.parquet'
    table = pa.Table.from_pandas(df_result)
    
    with pq.ParquetWriter(pb_path, table.schema, compression='snappy') as writer:
        writer.write_table(table)

    for daily_date, group in df_result.groupby('date'):
        file_path = Path(SECTOR_PATH)/'daily'/f"trendup_{daily_date.strftime('%Y-%m-%d')}.csv"
        daily_treandup_sector_df = group[['symbol','name', 'avg_slope']].sort_values('avg_slope', ascending=False)
        daily_treandup_sector_df.to_csv(file_path, sep=',', encoding='utf-8-sig', index=False, date_format='%Y-%m-%d', float_format='%.3f')

    sector_filepath = Path(SECTOR_PATH)/f"trendup_secotr_today.txt"
    sector_list = list(daily_treandup_sector_df['symbol'])
    #sectorname_list = list(daily_treandup_sector_df['name'])
     
    with open(sector_filepath, 'w', encoding='utf-8-sig') as fw:
        fw.write('\n'.join(sector_list))

    return_list = sector_list if ret=='today' else df_result
    return return_list#, sectorname_list

def get_list_in_sector(sectorlist=[]):
    stock_list = set()
    for sector in sectorlist:
        templist_df = feed.get_stocklist_in_index(sector=sector)
        #print(f'sector:{sector}, stock len: {len(templist_df)}')
        stock_list.update(list(templist_df['Code']))
    return list(stock_list)

def get_daily_list_in_sector(sectorlist_df=None):
    daily_list = []
    for daily_date, group in sectorlist_df.groupby('date'):
        sector_list = list(group['symbol'])
        stocklist = get_list_in_sector(sector_list)
        daily_list.append(
                    {'date':daily_date, 'stocklist': stocklist}
                )

    return pd.DataFrame(daily_list)

if __name__ == '__main__':
    feed = datafeed.FeedManager.register('tdx')
    feed.init_feed()
    sectors_list = get_up_sector(sectorlist=['concept', 'l3'], ret='today')
    sectorslist_df = get_up_sector(sectorlist=['concept', 'l3'], ret='daily')
    feed.update_block(block_code='ZFBK', stock_list=sectors_list)
    stocklist = get_list_in_sector(sectorlist=sectors_list)
    stocklist_df = get_daily_list_in_sector(sectorlist_df=sectorslist_df)