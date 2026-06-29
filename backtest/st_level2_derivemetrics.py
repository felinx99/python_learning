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
from pathlib import Path


def calculate_daily_sweep_metrics_v3(args):
    """
    [单股单日极速版] 去除全局 SecuCode 聚合，专为多进程流水线设计的吞单指标计算
    """
    rday, stock_code, checkyear, checkmonth=args
    stock_str = str(stock_code).zfill(6)
    min_depth_threshold = 2

    order_file = CONFIG.inferred_path['LEVEL2_BUFFER_PATH'] / f"monthlystaging/order/{checkyear}/{checkyear}{checkmonth}/{stock_str}/{stock_str}_{rday}.parquet"
    deal_file = CONFIG.inferred_path['LEVEL2_BUFFER_PATH'] / f"monthlystaging/deal/{checkyear}/{checkyear}{checkmonth}/{stock_str}/{stock_str}_{rday}.parquet"
    assert order_file.exists(), f"Error: '{order_file}'"
    assert deal_file.exists(), f"Error: '{deal_file}'"

    order_columns = ['BizIndex', 'LastPrice', 'OrderID', 'OrderTime', 'OrderType', 'Price', 'Volume']
    df_order = pd.read_parquet(order_file, schema=CONFIG.ORDER_SCHEMA, columns=order_columns)
    deal_columns = ['BizIndex', 'Price', 'DealTime', 'Volume', 'Side', 'BuyID', 'DealID', 'SellID']
    df_deal = pd.read_parquet(deal_file, schema=CONFIG.DEAL_SCHEMA, columns=deal_columns)
    

    if df_deal.empty:
        # 如果当天停牌或无成交，返回符合 Schema 结构的空行
        return pd.DataFrame(), pd.DataFrame([{f.name: (0 if not pa.types.is_string(f.type) else str(stock_code)) for f in CONFIG.LEVEL2_METRICS_SCHEMA}]).assign(TradeDate=rday)

    # --- 1. 过滤撤单，映射主动攻击方 ---
    trade_deals = df_deal[df_deal['Side'].isin([0, 1])].copy()
    if trade_deals.empty:
        return pd.DataFrame(), pd.DataFrame([{f.name: (0 if not pa.types.is_string(f.type) else str(stock_code)) for f in CONFIG.LEVEL2_METRICS_SCHEMA}]).assign(TradeDate=rday)

    trade_deals['initiator_id'] = np.where(trade_deals['Side'] == 0, trade_deals['BuyNo'], trade_deals['SellNo'])
    trade_deals['passive_id'] = np.where(trade_deals['Side'] == 0, trade_deals['SellNo'], trade_deals['BuyNo'])

    # 预打脉冲标签
    t = trade_deals['TradeTime']
    trade_deals['is_open_close_pulse'] = ((t >= 93000000) & (t <= 100000000)) | ((t >= 143000000) & (t <= 150000000))

    # --- 2. 局部聚合：仅按委托单和方向隔离 ---
    grouped = trade_deals.groupby(['initiator_id', 'Side']).agg(
        trade_time_start=('TradeTime', 'min'),
        trade_time_end=('TradeTime', 'max'),
        price_min=('Price', 'min'),
        price_max=('Price', 'max'),
        price_first=('Price', 'first'),
        price_last=('Price', 'last'),
        total_sweep_volume=('Volume', 'sum'),
        total_sweep_turnover=('Turnover', 'sum'),
        swept_depth=('Price', 'nunique'),          
        passive_orders_count=('passive_id', 'nunique'), 
        is_pulse_window=('is_open_close_pulse', 'any')
    ).reset_index()

    # 筛选扫盘事件
    sweep_events = grouped[grouped['swept_depth'] >= min_depth_threshold].copy()
    
    # 计算滑点
    if not sweep_events.empty:
        sweep_events['sweep_slippage_pct'] = np.where(
            sweep_events['Side'] == 0,
            (sweep_events['price_max'] - sweep_events['price_first']) / sweep_events['price_first'],
            (sweep_events['price_first'] - sweep_events['price_min']) / sweep_events['price_first']
        )
        # 反查原始 Order 表 (单股单日 merge 极快)
        sweep_events = sweep_events.merge(
            df_order[['OrderID', 'OrderTime', 'OrderPrice', 'OrderVolume', 'OrderType']],
            left_on='initiator_id', right_on='OrderID', how='left'
        ).drop(columns=['OrderID'])
        sweep_events['order_execution_ratio'] = sweep_events['total_sweep_volume'] / sweep_events['OrderVolume']
        sweep_events['SecuCode'] = stock_code
        sweep_events['TradeDate'] = rday
    
    # --- 3. 标量化计算当日总指标 (无需 GroupBy) ---
    daily_total_volume = int(trade_deals['Volume'].sum())
    daily_total_turnover = float(trade_deals['Turnover'].sum())

    # 分流多空事件
    buy_events = sweep_events[sweep_events['Side'] == 0] if not sweep_events.empty else pd.DataFrame()
    sell_events = sweep_events[sweep_events['Side'] == 1] if not sweep_events.empty else pd.DataFrame()

    daily_buy_sweep_vol = int(buy_events['total_sweep_volume'].sum()) if not buy_events.empty else 0
    daily_buy_sweep_turnover = float(buy_events['total_sweep_turnover'].sum()) if not buy_events.empty else 0.0
    buy_sweep_count = int(len(buy_events))
    buy_pulse_count = int(buy_events['is_pulse_window'].sum()) if not buy_events.empty else 0

    daily_sell_sweep_vol = int(sell_events['total_sweep_volume'].sum()) if not sell_events.empty else 0
    daily_sell_sweep_turnover = float(sell_events['total_sweep_turnover'].sum()) if not sell_events.empty else 0.0
    sell_sweep_count = int(len(sell_events))
    sell_pulse_count = int(sell_events['is_pulse_window'].sum()) if not sell_events.empty else 0

    # 组装单股日线数据
    daily_stock_ratio = pd.DataFrame([{
        'TradeDate': int(rday),
        'SecuCode': stock_code,
        'daily_total_volume': daily_total_volume,
        'daily_total_turnover': daily_total_turnover,
        'daily_buy_sweep_vol': daily_buy_sweep_vol,
        'daily_buy_sweep_turnover': daily_buy_sweep_turnover,
        'buy_sweep_count': buy_sweep_count,
        'buy_pulse_count': buy_pulse_count,
        'daily_sell_sweep_vol': daily_sell_sweep_vol,
        'daily_sell_sweep_turnover': daily_sell_sweep_turnover,
        'sell_sweep_count': sell_sweep_count,
        'sell_pulse_count': sell_pulse_count,
        'buy_sweep_ratio': float(daily_buy_sweep_vol / daily_total_volume) if daily_total_volume > 0 else 0.0,
        'sell_sweep_ratio': float(daily_sell_sweep_vol / daily_total_volume) if daily_total_volume > 0 else 0.0
    }])

    return sweep_events, daily_stock_ratio

def save_daily_batch_to_monthly_file(daily_batch_df: pd.DataFrame, rday: int, output_dir: Path):
    """
    将当日全市场所有股票计算出的指标，安全地追加到月度大文件中，并按日期、股票排序。
    支持断点重跑（自动靠后生成的记录覆盖旧记录）。
    """
    if daily_batch_df.empty:
        return
        
    output_dir.mkdir(parents=True, exist_ok=True)
    yearmonth = str(rday)[:6]  # 提取年月，如 202606
    file_path = output_dir / f"alpha_l2_metrics_{yearmonth}.parquet"

    # 2. 如果月度文件已存在，读取并合并（8G内存读写几MB的文件，耗时在毫秒级）
    if file_path.exists():
        existing_df = pq.read_table(file_path).to_pandas()
        combined_df = pd.concat([existing_df, daily_batch_df], ignore_index=True)
        
        # 🎯 核心逻辑：依靠 ['TradeDate', 'SecuCode'] 唯一键去重。
        # keep='last' 意味着如果今天的数据之前跑过一部分，重新更新时会用最新的覆盖旧的
        combined_df = combined_df.drop_duplicates(subset=['TradeDate', 'SecuCode'], keep='last')
    else:
        combined_df = daily_batch_df

    # 3. 按日期、股票代码全局严格排序
    combined_df = combined_df.sort_values(by=['TradeDate', 'SecuCode']).reset_index(drop=True)

    # 4. 根据严格指定的 Schema 写入 Parquet 文件
    table = pa.Table.from_pandas(combined_df, schema=CONFIG.LEVEL2_METRICS_SCHEMA)
    pq.write_table(table, file_path, compression='SNAPPY')
    print(f"💾 成功更新月度大文件: {file_path.name} | 当前全月总记录数: {len(combined_df)} 行")

def calculate_daily_sweep_metrics_v2(deals_df: pd.DataFrame, orders_df: pd.DataFrame, min_depth_threshold: int = 2):
    """
    [精确优化版] 基于原生 Side 字段，完美隔离多头扫盘(Buy Sweep)与空头扫盘(Sell Sweep)
    
    参数:
    ----------
    deals_df : pd.DataFrame
        逐笔成交数据，必须包含列: ['SecuCode', 'TradeTime', 'Price', 'Volume', 'Turnover', 'BuyNo', 'SellNo', 'Side', 'BizIndex']
        Side 映射: 0-买单成交, 1-卖单成交, -1-撤买, -11-撤卖
    orders_df : pd.DataFrame
        逐笔委托数据，包含列: ['SecuCode', 'OrderID', 'OrderTime', 'OrderPrice', 'OrderVolume', 'OrderType']
    """
    print("🚀 启动基于原生 Side 字段的精确 Sweep Ratio 计算流水线...")
    
    # --- 优化点 1: 过滤掉撤单记录(-1, -11)，仅保留 0(主买) 和 1(主卖) 的真实交易 ---
    trade_deals = deals_df[deals_df['Side'].isin([0, 1])].copy()
    
    # 向量化映射主动攻击方的真实 OrderID (initiator_id)
    # Side == 0 (买方主动，扫卖盘): 主动方ID是 BuyNo
    # Side == 1 (卖方主动，扫买盘): 主动方ID是 SellNo
    # 不能通过Side判断买单或卖单，一个成交单里肯定有买方和卖方，谁的编号大谁主动方
    deals_df['is_buy_aggressive'] = trade_deals['BuyNo'] > trade_deals['SellNo']
    trade_deals['initiator_id'] = np.where(trade_deals['is_buy_aggressive'], trade_deals['BuyNo'], trade_deals['SellNo'])
    trade_deals['passive_id'] = np.where(trade_deals['is_buy_aggressive'], trade_deals['SellNo'], trade_deals['BuyNo'])

    # 预打高频脉冲时段标签 (09:30-10:00 或 14:30-15:00)
    trade_deals['is_open_close_pulse'] = (
        (trade_deals['DealTime'] >= 93000000) & (trade_deals['DealTime'] <= 100000000)
    ) | (
        (trade_deals['DealTime'] >= 143000000) & (trade_deals['DealTime'] <= 150000000)
    )

    # --- 优化点 2: 将 is_buy_aggressive 纳入 groupby，彻底隔离多空吞噬行为 ---
    print("⏳ 正在进行多空隔离聚合...")
    grouped = trade_deals.groupby(['SecuCode', 'initiator_id', 'is_buy_aggressive']).agg(
        trade_time_start=('DealTime', 'min'),
        trade_time_end=('DealTime', 'max'),
        price_min=('Price', 'min'),
        price_max=('Price', 'max'),
        price_first=('Price', 'first'),
        price_last=('Price', 'last'),
        total_sweep_volume=('Volume', 'sum'),
        total_sweep_turnover=('Turnover', 'sum'),
        swept_depth=('Price', 'nunique'),           # 吞噬盘口档口数
        passive_orders_count=('passive_id', 'nunique'), # 吃掉的被动挂单单数
        is_pulse_window=('is_open_close_pulse', 'any'),
        deal_biz_index_min=('BizIndex', 'min'),
        deal_biz_index_max=('BizIndex', 'max')
    ).reset_index()

    # 筛选出真正产生“扫盘行为（吃掉多档）”的强力事件
    sweep_events = grouped[grouped['swept_depth'] >= min_depth_threshold].copy()
    
    # 精确计算滑点 / 冲击成本 (Slippage)
    # Side == 0 (买入扫盘): 价格往上扫，滑点 = (最高价 - 起始价) / 起始价
    # Side == 1 (卖出扫盘): 价格往下砸，滑点 = (起始价 - 最低价) / 起始价
    sweep_events['sweep_slippage_pct'] = np.where(
        sweep_events['is_buy_aggressive'] == True,
        (sweep_events['price_max'] - sweep_events['price_first']) / sweep_events['price_first'],
        (sweep_events['price_first'] - sweep_events['price_min']) / sweep_events['price_first']
    )

    # 联动反查 Order 表，补充原始委托意图 (Merge)
    print("🔗 联动反查原始委托意图...")
    sweep_events = sweep_events.merge(
        orders_df[['SecuCode', 'OrderID', 'OrderTime', 'OrderPrice', 'OrderVolume', 'OrderType']],
        left_on=['SecuCode', 'initiator_id'],
        right_on=['SecuCode', 'OrderID'],
        how='left'
    ).drop(columns=['OrderID'])
    
    # 计算委托执行率
    sweep_events['order_execution_ratio'] = sweep_events['total_sweep_volume'] / sweep_events['OrderVolume']

    # --- 📊 日线级别全市场股票聚合统计 ---
    print("📊 正在计算全市场每日精确多空 Sweep Ratio 总表...")
    
    # 统计单股当日总成交（用于做分母）
    stock_total = trade_deals.groupby('SecuCode').agg(
        daily_total_volume=('Volume', 'sum'),
        daily_total_turnover=('Turnover', 'sum')
    ).reset_index()
    
    # 分别统计多空扫盘的聚合值
    buy_sweeps = sweep_events[sweep_events['is_buy_aggressive'] == True].groupby('SecuCode').agg(
        daily_buy_sweep_vol=('total_sweep_volume', 'sum'),
        daily_buy_sweep_turnover=('total_sweep_turnover', 'sum'),
        buy_sweep_count=('initiator_id', 'count'),
        buy_pulse_count=('is_pulse_window', 'sum')
    ).reset_index()
    
    # 🛠️ 完美的空头扫盘指标：度量“恐慌出逃 / 强力派发”
    sell_sweeps = sweep_events[sweep_events['is_buy_aggressive'] == False].groupby('SecuCode').agg(
        daily_sell_sweep_vol=('total_sweep_volume', 'sum'),
        daily_sell_sweep_turnover=('total_sweep_turnover', 'sum'),
        sell_sweep_count=('initiator_id', 'count'),
        sell_pulse_count=('is_pulse_window', 'sum')
    ).reset_index()

    # 多表左联结，组合成最终输出
    daily_stock_ratio = stock_total.merge(buy_sweeps, on='SecuCode', how='left')
    daily_stock_ratio = daily_stock_ratio.merge(sell_sweeps, on='SecuCode', how='left').fillna(0)
    
    # 🎯 核心衍生指标计算
    daily_stock_ratio['buy_sweep_ratio'] = daily_stock_ratio['daily_buy_sweep_vol'] / daily_stock_ratio['daily_total_volume']
    daily_stock_ratio['sell_sweep_ratio'] = daily_stock_ratio['daily_sell_sweep_vol'] / daily_stock_ratio['daily_total_volume']
    
    print("✅ 指标重构优化完成。")
    return sweep_events, daily_stock_ratio
    
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
    checkyear = '2026'
    checkmonth = '06'
    tasks = []
    daily_results = []
    date_list = [
        # 20260601, 20260602, 20260603, 20260604, 20260605,        
        # 20260608, 20260609, 20260610, 20260611, 20260612,
        20260615, 20260616, 20260617, 20260618,
        #20260622, 20260623, 20260624, 20260625, 20260626,
        #20260629, 20260630
    ] 
    fpath = CONFIG.inferred_path['LEVEL2_BUFFER_PATH']/f"monthlystaging/order/{checkyear}/{checkyear}{checkmonth}/tmp_split"
    stockcode_list = [d.name for d in fpath.iterdir() if d.is_dir()]
    total_stocks = len(stockcode_list)
    for rday in date_list:
        for stock_code in stockcode_list:
            tasks.append((rday, stock_code, checkyear, checkmonth))
    
    physical_cores = 1#psutil.cpu_count(logical=False)

    with Pool(physical_cores) as p:
        print(f"开始snapshot校验")        
        results = p.imap_unordered(calculate_daily_sweep_metrics_v3, tasks, chunksize=50)
        success_count = 0
        for rday, stock_code, success, single_stock_ratio in results:
            if success:
                success_count += 1
                daily_results.append(single_stock_ratio)
                print(f"进程推进中{rday}... 已成功合并 {success_count}/{len(tasks)} 只股票 [{stock_code}]", end="\r")

    # 🏁 当天所有股票子进程跑完后，主进程一气呵成：合并 -> 去重 -> 排序 -> 追加落盘
    if daily_results:
        daily_all_shares_df = pd.concat(daily_results, ignore_index=True)
        save_daily_batch_to_monthly_file(
            daily_batch_df=daily_all_shares_df, 
            rday=rday, 
            output_dir=Path("./delivery/metrics")
        )