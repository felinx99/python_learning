import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from common import CONFIG
import talib as ta
from pathlib import Path
from .datafeed import FeedManager
from numpy.lib.stride_tricks import sliding_window_view

class Sector:
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

    def __init__(self, type='tdx'):
        feed = FeedManager.register(type)
        assert feed is not None
        self.feed = feed
        self.feed.init_feed()

    @classmethod
    def calc_ols(cls, df=None, column='', method=''):
        windows_len = CONFIG.params['SLOPE_WINDOWS']
        N = int(''.join(filter(str.isdigit, method)))
        # 提取数据,并价格转换为对数值，对数体现价格的等比变化程序
        method_df = ta.SMA(df[column].values.astype('float64'), timeperiod=N)
        y = np.log(method_df)
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
        #column_name = f"{method}_slope"
        method_slope_df = k_array*10
    
        return method_df, method_slope_df

    @classmethod
    def calc_R(cls, df, column='ohlc', window=5):
        y = df[column].values.astype('float64')
        n = window
        if len(y) < n:
            return np.full(len(y), np.nan)

        y_wins = sliding_window_view(y, window_shape=n)

        x = np.arange(n)
        x_sum = n * (n - 1) / 2
        x_sum_sq = n * (n - 1) * (2 * n - 1) / 6
        x_var_part = n * x_sum_sq - x_sum**2 
        
        y_sum = np.sum(y_wins, axis=1)
        y_sum_sq = np.sum(y_wins**2, axis=1)

        xy_sum = np.dot(y_wins, x)
        numerator = n * xy_sum - x_sum * y_sum

        y_var_part = n * y_sum_sq - y_sum**2
        
        denominator_sq = x_var_part * y_var_part
        
        r2 = np.divide(numerator**2, denominator_sq, 
                    out=np.zeros_like(numerator), 
                    where=denominator_sq > 1e-12)
    
        result = np.full(len(y), np.nan)
        result[n-1:] = r2
        return result


    def select_sector(self, df):
        sma5_df,sma5_slope_df =  Sector.calc_ols(df=df, column='ohlc', method='sma5')                
        sma10_df,sma10_slope_df = Sector.calc_ols(df=df, column='ohlc', method='sma10')
        sma20_df,sma20_slope_df = Sector.calc_ols(df=df, column='ohlc', method='sma20')
        df['avg_slope'] = 0.4*sma5_slope_df + 0.2*sma10_slope_df + 0.4*sma20_slope_df
        weight = Sector.calc_R(df, column='ohlc', window=5)
        #筛选条件 df['avg_slope'] > CONFIG.params['SLOPE_THRESHOLD'] 权重大于2
        #k=0.02,R2=0.4,a=15*R2, threshold = np.degress(np.arctan(a*k)
        slect_res_df = (np.degrees(np.arctan(15 * weight * df['avg_slope']))   > CONFIG.params['SLOPE_THRESHOLD']) & (sma20_slope_df > 0.001)

        return slect_res_df

    def get_up_sector(self, sectorlist=[], ret=''):

        assert ret in ['today', 'daily'], f"Params Error: 'ret={ret}'"
        df_result = None
        df_list = []
        daily_treandup_sector_df = None

        for sector in sectorlist:
            sectorlistpath = CONFIG.inferred_path['TDX_SECTOR_PATH']/sector/f"{sector}_list.csv"
            sectorlist_df = pd.read_csv(sectorlistpath, skiprows=1, header=None)
            for _, sector_code in sectorlist_df.iterrows():
                symbol = f"{sector_code[0]}"
                sectorlist = self.get_list_in_sector(symbol)
                if len(sectorlist) < 6:
                    continue
                sector_file = CONFIG.inferred_path['TDX_SECTOR_PATH']/sector/'daily'/f"{symbol}.csv"
                sector_df = pd.read_csv(sector_file, dtype=self.stock_csvtype, parse_dates=['date'])
                sector_df['ohlc'] = sector_df.eval('(high + 2*open + 2*close + low) / 6')
                sector_df['symbol'] = symbol
                sector_df['name'] = f"{sector_code[1]}"

                #dst_file = Path(SECTOR_PATH)/sector/'daily'/f"{symbol}_slope.csv"
                #sector_df.to_csv(dst_file, sep=',', encoding='utf-8-sig', index=False, date_format='%Y-%m-%d', float_format='%.3f')
                slect_res_df = self.select_sector(sector_df)
                df_list.append(sector_df.loc[slect_res_df == True, ['date', 'symbol', 'name', 'avg_slope']].copy())
                

        df_result = pd.concat(df_list , ignore_index=True)
        df_result = df_result.sort_values(['date', 'avg_slope'], ascending=False)

        pb_path = CONFIG.inferred_path['TDX_SECTOR_PATH']/'trendup_sectors_everydaily.parquet'
        table = pa.Table.from_pandas(df_result)
        
        with pq.ParquetWriter(pb_path, table.schema, compression='snappy') as writer:
            writer.write_table(table)

        for daily_date, group in df_result.groupby('date'):
            file_path = CONFIG.inferred_path['TDX_SECTOR_PATH']/'daily'/f"trendup_{daily_date.strftime('%Y-%m-%d')}.csv"
            daily_treandup_sector_df = group[['symbol','name', 'avg_slope']].sort_values('avg_slope', ascending=False)
            daily_treandup_sector_df.to_csv(file_path, sep=',', encoding='utf-8-sig', index=False, date_format='%Y-%m-%d', float_format='%.3f')

        sector_filepath = CONFIG.inferred_path['TDX_SECTOR_PATH']/f"trendup_secotr_today.txt"
        sector_list = list(daily_treandup_sector_df['symbol'])
        #sectorname_list = list(daily_treandup_sector_df['name'])
        
        with open(sector_filepath, 'w', encoding='utf-8-sig') as fw:
            fw.write('\n'.join(sector_list))

        return_list = sector_list if ret=='today' else df_result
        return return_list#, sectorname_list

    def get_list_in_sector(self, sectorlist=[]):
        stocklist_df = None
        if type(sectorlist) is not list:
            sectorlist = [sectorlist]
        for sector in sectorlist:
            templist_df = self.feed.get_stocklist_in_index(sector=sector)
            #print(f'sector:{sector}, stock len: {len(templist_df)}')
            stocklist_df = pd.concat([stocklist_df, templist_df], ignore_index=True)

        stocklist_df = stocklist_df.drop_duplicates(subset=['Code'], keep='first')
        return stocklist_df

    def get_daily_list_in_sector(self, sectorlist_df=None):
        daily_list = []
        for daily_date, group in sectorlist_df.groupby('date'):
            sector_list = list(group['symbol'])
            stocklist = self.get_list_in_sector(self, sector_list)
            daily_list.append(
                        {'date':daily_date, 'stocklist': stocklist}
                    )

        return pd.DataFrame(daily_list)

    def update_sector(self, block_code='', update_list=''):
        self.feed.update_block(block_code=block_code, stock_list=update_list)

    def get_feed(self):
        return self.feed
    
    def get_L2Vol(self,symbol=None,start_date=None,end_date=None):
        #stocklist = symbol['Code'].tolist()
        stocklist = ['000935.SZ','600426.SH']

        stock_l2 = self.feed.get_L2Vol(symbol=stocklist, start_date=start_date, end_date=end_date)
        print(stock_l2)
