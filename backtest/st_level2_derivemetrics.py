import logging
import datetime
import psutil
import traceback
import gc
import pyarrow as pa
import pandas as pd
import numpy as np
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from backtrader import order
from collections import namedtuple
from concurrent.futures import ProcessPoolExecutor
from common import CONFIG, DATAFRAME
from dataclasses import dataclass
from enum import IntEnum
from multiprocessing import Pool
from api.timeprofile import TimeProfile
from .util.order import OrderNode, OrderBook


def calculate_daily_sweep_metrics(deals_df: pd.DataFrame, orders_df: pd.DataFrame, min_depth_threshold: int = 2):
    """
    基于逐笔委托(Order)与逐笔成交(Deal)计算全市场股票的主动性扫盘(Sweep)指标。
    
    参数:
    ----------
    deals_df : pd.DataFrame
        当日逐笔成交数据，包含列: ['SecuCode', 'TradeTime', 'Price', 'Volume', 'Turnover', 'BuyNo', 'SellNo', 'BizIndex']
    orders_df : pd.DataFrame
        当日逐笔委托数据，包含列: ['SecuCode', 'OrderID', 'OrderTime', 'OrderPrice', 'OrderVolume', 'OrderType']
    min_depth_threshold : int, default 2
        定义“扫盘”的起步档口深度（即一笔主动单至少吃掉多少个不同的价格才算作 Sweep）
        
    返回:
    -------
    tuple: (sweep_events_df, daily_stock_ratio_df)
        - sweep_events_df: 逐笔扫盘事件明细表（用于特征分析与归档）
        - daily_stock_ratio_df: 股票级别的每日总览表（直接对接 20天波段统计）
    """
    
    # --- Step 1: 判定主动方 (以深交所为例：委托号大者为新进入的主动攻击方) ---
    # 如果是上交所，可直接使用数据商提供的 TradeDir (成交方向) 或 ExecType
    deals_df['is_buy_aggressive'] = deals_df['BuyNo'] > deals_df['SellNo']
    deals_df['initiator_id'] = np.where(deals_df['is_buy_aggressive'], deals_df['BuyNo'], deals_df['SellNo'])
    deals_df['passive_id'] = np.where(deals_df['is_buy_aggressive'], deals_df['SellNo'], deals_df['BuyNo'])

    # --- Step 2: 按主动方委托号(initiator_id)聚合，还原单笔大单的吞噬现场 ---
    print("⏳ 正在聚合逐笔成交序列，还原大单攻击行为...")
    
    # 预先计算具体时间所属的盘口时段，为“开盘半小时/尾盘”做高频特征标记
    # 转换成时间对象以便提取分钟
    time_series = pd.to_datetime(deals_df['TradeTime'], format='%H%M%S%f', errors='coerce').dt.time
    
    # 向量化计算高频脉冲时段标记 (09:30-10:00 或 14:30-15:00)
    deals_df['is_open_close_pulse'] = (
        (deals_df['TradeTime'] >= 93000000) & (deals_df['TradeTime'] <= 100000000)
    ) | (
        (deals_df['TradeTime'] >= 143000000) & (deals_df['TradeTime'] <= 150000000)
    )

    # 分组聚合
    '''
    考虑到你在上一个问题中提到内存只有 8G。如果单日逐笔成交(Deal)行数达到数千万行,
    Pandas 的 .groupby(['SecuCode', 'initiator_id']) 可能会遇到严重的内存瓶颈。
    如果代码跑起来吃力，可以将该函数内部的 Step 2 和 Step 3 替换为 Polars 的惰性表达式(lazy()),
    或者利用 initiator_id 已经有序的特性，写一个 C 级别的 Numba @njit 循环。
    由于扫盘本质上是相邻几行 initiator_id 相同的 Deal 记录，用 Numba 迭代单次通行(Single Pass)只需几百毫秒即可收工。
    '''
    grouped = deals_df.groupby(['SecuCode', 'initiator_id']).agg(
        trade_time_start=('TradeTime', 'min'),
        trade_time_end=('TradeTime', 'max'),
        price_min=('Price', 'min'),
        price_max=('Price', 'max'),
        price_first=('Price', 'first'),
        price_last=('Price', 'last'),
        total_sweep_volume=('Volume', 'sum'),
        total_sweep_turnover=('Turnover', 'sum'),
        is_buy_sweep=('is_buy_aggressive', 'first'),
        swept_depth=('Price', 'nunique'),           # ⚡ 核心：吞噬的档口深度
        passive_orders_count=('passive_id', 'nunique'), # 附加：吃掉了多少张挂单
        is_pulse_window=('is_open_close_pulse', 'any'), # 附加：是否发生在敏感时段
        deal_biz_index_min=('BizIndex', 'min'),
        deal_biz_index_max=('BizIndex', 'max')
    ).reset_index()

    # --- Step 3: 筛选出符合阈值的强力 Sweep 事件 ---
    sweep_events = grouped[grouped['swept_depth'] >= min_depth_threshold].copy()
    
    # --- Step 4: 计算冲击成本与滑点 (Slippage) 增强特征 ---
    # 买入扫盘滑点 = (最高成交价 - 起始成交价) / 起始成交价
    # 卖出扫盘滑点 = (起始成交价 - 最低成交价) / 起始成交价
    sweep_events['sweep_slippage_pct'] = np.where(
        sweep_events['is_buy_sweep'],
        (sweep_events['price_max'] - sweep_events['price_first']) / sweep_events['price_first'],
        (sweep_events['price_first'] - sweep_events['price_min']) / sweep_events['price_first']
    )

    # --- Step 5: 联动反查 Order 表，补充原始委托意图 ---
    print("🔗 正在联动反查原始委托意图 (Merge Order)...")
    # 仅对发生 Sweep 的单子进行 Merge，节约海量内存
    sweep_events = sweep_events.merge(
        orders_df[['SecuCode', 'OrderID', 'OrderTime', 'OrderPrice', 'OrderVolume', 'OrderType']],
        left_on=['SecuCode', 'initiator_id'],
        right_on=['SecuCode', 'OrderID'],
        how='left'
    ).drop(columns=['OrderID'])
    
    # 计算委托执行率：扫盘成交量 / 原始报单量 (识别是否是未扫完的超大市价单)
    sweep_events['order_execution_ratio'] = sweep_events['total_sweep_volume'] / sweep_events['OrderVolume']

    # --- Step 6: 计算日线级别全市场股票的聚合 Sweep Ratio ---
    print("📊 正在生成全市场每日 Sweep Ratio 统计大表...")
    
    # 计算每只股票当日的总成交量和总金额
    stock_total = deals_df.groupby('SecuCode').agg(
        daily_total_volume=('Volume', 'sum'),
        daily_total_turnover=('Turnover', 'sum')
    ).reset_index()
    
    # 分别计算 Buy Sweep 和 Sell Sweep 的每日总和
    sweep_summary = sweep_events.groupby('SecuCode').agg(
        daily_buy_sweep_vol=('total_sweep_volume', lambda x: x[sweep_events.loc[x.index, 'is_buy_sweep'] == True].sum()),
        daily_buy_sweep_turnover=('total_sweep_turnover', lambda x: x[sweep_events.loc[x.index, 'is_buy_sweep'] == True].sum()),
        daily_sell_sweep_vol=('total_sweep_volume', lambda x: x[sweep_events.loc[x.index, 'is_buy_sweep'] == False].sum()),
        daily_sell_sweep_turnover=('total_sweep_turnover', lambda x: x[sweep_events.loc[x.index, 'is_buy_sweep'] == False].sum()),
        buy_sweep_count=('is_buy_sweep', lambda x: (x == True).sum()),
        sell_sweep_count=('is_buy_sweep', lambda x: (x == False).sum()),
        pulse_window_sweep_count=('is_pulse_window', 'sum') # 敏感时段触发频次
    ).reset_index()
    
    # 合并并计算最终比率
    daily_stock_ratio = stock_total.merge(sweep_summary, on='SecuCode', how='left').fillna(0)
    
    # 🎯 核心指标计算
    daily_stock_ratio['buy_sweep_ratio'] = daily_stock_ratio['daily_buy_sweep_vol'] / daily_stock_ratio['daily_total_volume']
    daily_stock_ratio['sell_sweep_ratio'] = daily_stock_ratio['daily_sell_sweep_vol'] / daily_stock_ratio['daily_total_volume']
    
    print("✅ 逐笔主动性吞噬指标计算完毕。")
    return sweep_events, daily_stock_ratio

if __name__ == '__main__':
    # 伪代码示例：将每日结果存入大表后的 20天 动量清洗
    # df_20d 包含了单只股票过去 20 天的 daily_stock_ratio 序列

    # 1. 计算常规阈值（3 贝塔滚动边界）
    rolling_mean = df_20d['buy_sweep_ratio'].rolling(20).mean()
    rolling_std = df_20d['buy_sweep_ratio'].rolling(20).std()
    three_beta_threshold = rolling_mean + 3 * rolling_std

    # 2. 状态机判定
    # 【完全自然流量】
    is_white_noise = (df_20d['buy_sweep_ratio'] < three_beta_threshold).all()

    # 【拉升阶段信号】
    # 过去10天内，突破3贝塔极限阈值的频次显著升高，且集中在开盘半小时
    is_accumulation_phase = (
        (df_20d['buy_sweep_ratio'].tail(10) > three_beta_threshold.tail(10)).sum() >= 3
    ) & (df_20d['pulse_window_sweep_count'].tail(10).mean() > 5)

    # 【出货阶段信号】
    is_distribution_phase = (
        df_20d['sell_sweep_ratio'].tail(5).mean() > three_beta_threshold.tail(5).mean()
    ) & (
        df_20d['buy_sweep_ratio'].tail(5).mean() < rolling_mean.tail(5).mean()
    )