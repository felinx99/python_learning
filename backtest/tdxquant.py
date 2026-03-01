import sys
sys.path.append('D:/new_tdx_test/PYPlugins/user')
import pandas as pd
import talib as ta
import numpy as np
import time
from datetime import datetime, timedelta
from tqcenter import tq # type: ignore



WATCHLIST_PATH = "E:\\output\\Astock\\stockpicking\\test.csv"  # 周日扫描出的潜力股池

def calculate_average_correlation_numpy(df):
    # 1. 将 DataFrame 转换为 NumPy 数组
    data = df[['ma10', 'ma20', 'ma30']].values
    # 统计每一行中非 NaN 值的数量
    valid_counts = np.count_nonzero(~np.isnan(data), axis=1)
    
    # 2. 计算每个点标准差和变异系数
    corr_std_NAN = np.nanstd(data, axis=1)
    corr_std = np.where(valid_counts>=3, corr_std_NAN, np.nan)
    corr_mean = np.mean(data, axis=1)
    corr_cv = corr_std / (corr_mean+1e-9) #防止除0
    
    # 按照要求返回 DataFrame
    return pd.DataFrame({'corr_std':corr_std}), pd.DataFrame({'corr_cv':corr_cv})


def calc_MomentumTriggerStrategy(df):
    df['ma10'] = df['Close'].rolling(10).mean()
    df['ma20'] = df['Close'].rolling(20).mean()
    df['ma30'] = df['Close'].rolling(30).mean()
    
    df['corr_std'], df['corr_cv'] = calculate_average_correlation_numpy(df)
    pass

def data_to_df(data):
    """
    把tq.get_market_data返回的dict转换为DataFrame格式
    """
    combined = pd.concat(data.values(), keys=data.keys(), axis=0)
    df = combined.stack().unstack(level=0).reset_index()
    df.columns.name = None
    df.rename(columns={'level_0': 'Date', 'level_1': 'Symbol'}, inplace=True)
    df = df.sort_values(by=['Symbol', 'Date'], ascending=[True, True])  #按股票代码和日期排序
    df = df.reset_index(drop=True)  #重置索引
    return df

#初始化
tq.initialize(__file__)

#1.基础配置
batch_codes = tq.get_stock_list_in_sector('通达信88',list_type = 1) #目标板块
start_time = '20200101' #数据起始时间
target_end = '' #datetime.now().strftime('%Y%m%d') #数据结束时间
target_gain = 5.0
target_block_name = 'ZFXG'

#2.数据获取与处理
df_real = tq.get_market_data(
    field_list=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'],
    stock_list=['880501.SH'],
    start_time=start_time,
    end_time=target_end,
    dividend_type='front', #前复权
    period='1d',
    fill_data=False      #填充缺失数据
)

stockprice_df = data_to_df(df_real)

#转换为‘日期 股票代码’的收盘价宽表
close_df = tq.price_df(df_real, 'Close')
calc_MomentumTriggerStrategy(stockprice_df)
stockprice_df.to_csv(WATCHLIST_PATH, sep=',', encoding='utf-8-sig', index=False, date_format='%Y-%m-%d', float_format='%.3f')
sector_list = tq.get_sector_list()
print(sector_list)

#3.核心：计算当时相较于昨日涨幅
prev_close = close_df.shift(1) #昨日收盘价
daily_gain = (close_df - prev_close) / prev_close * 100 #当日涨幅百分比

#4.筛选符合条件的股票
latest_date = daily_gain.index[-1] #最新日期
latest_daily_gain = daily_gain.loc[latest_date] #最新日期的涨幅数据
target_stocks = latest_daily_gain[latest_daily_gain > target_gain].sort_values(ascending=False) #符合条件的股票列表
traget_stocks_list = target_stocks.index.tolist() #符合条件的股票代码列表

#5.输出结果
print(f"\n ====== 筛选结果 (当时涨幅>{target_gain}%) ======")
if not target_stocks.empty:
    # ===================== 模块1：打印筛选结果 =====================
    print("【模块1：打印筛选结果】")
    print(f"符合条件的股票共 {len(target_stocks)} 只：")
    print(f"{'股票代码':<12} {'昨日收盘价':<12} {'当日收盘价':<12} {'当日涨幅':<10}")
    print("-" * 50)
    for stock_code, gain in target_stocks.items():
        yesterday_price = prev_close.loc[latest_date, stock_code]
        today_price = close_df.loc[latest_date, stock_code]
        print(f"{stock_code:<12} {yesterday_price:<12.2f} {today_price:<12.2f} {gain:<10.2f}%")
    print("-" * 50)

    # ===================== 模块2：添加至自定义板块 =====================
    try:
        print("\n【模块2：添加至自定义板块】")
        # 创建或更新自定义板块
        tq.send_user_block(target_block_name, traget_stocks_list, show=True)
        print(f"已将符合条件的股票添加至自定义板块 '{target_block_name}'。")
    except Exception as e:
        print(f"添加至自定义板块失败: {e}")
    print("-" * 50)

else:
    # ===================== 模块1：打印空结果 =====================
    print("【模块1：打印筛选结果】")
    print(f"暂无当日涨幅＞{target_gain}%的股票")
    print("-" * 50)

    # ===================== 模块2：清空自定义选板块 =====================
    try:
        print("\n【模块2：清空自定义板块】")
        tq.send_user_block(target_block_name, [], show=True)
        print(f"已清空自定义板块 '{target_block_name}'。")
    except Exception as e:
        print(f"清空自定义板块失败: {e}")
    print("-" * 50)

