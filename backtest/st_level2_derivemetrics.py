import json
import numpy as np
import polars as pl
import pandas as pd
from common import CONFIG, DATAFRAME
from multiprocessing import Pool
from api.timeprofile import TimeProfile
from tools.level2_metrics import (
    Level2MetricCalculator, 
    Resample, 
    TimeFrame,
    preload_vpdistribution_cache, 
    load_ordertier_baseline, 
    load_vpdistribution, 
    save_deal_metrics,
)

# ==============================================================================
# 🔌 指标注册表与插件化管理中心
# ==============================================================================
METRIC_REGISTRY = {}

def register_metric(name: str, requires_deal: bool = True, requires_cancel: bool = False):
    """
    指标注册装饰器：将指标函数注册到全局注册表中。
    未来新增指标时，只需写一个函数并用该装饰器注册，无需修改任何主循环代码！
    """
    def decorator(func):
        METRIC_REGISTRY[name] = {
            'handler': func,
            'requires_deal': requires_deal,
            'requires_cancel': requires_cancel
        }
        return func
    return decorator


# ==============================================================================
# 📊 各指标独立的计算插件函数（计算、收集自包含）
# ==============================================================================

@register_metric('min1_bar', requires_deal=True)
def calc_min1_bar_metric(trade_deals, df_cancel, rday, stock_str, free_shares, grouped_profiles, trade_dates, thresholds):
    min1 = Resample(timeframe=TimeFrame.Minutes, compression=1)
    return Level2MetricCalculator.calculate_bars(df_tradedeal=trade_deals, rday=rday, resamples=[min1])

@register_metric('sweepratio', requires_deal=True)
def calc_sweepratio_metric(trade_deals, df_cancel, rday, stock_str, free_shares, grouped_profiles, trade_dates, thresholds):
    return Level2MetricCalculator.calculate_sweepratio(df_tradedeal=trade_deals, rday=rday)

@register_metric('vpdistribution', requires_deal=True)
def calc_vpdistribution_metric(trade_deals, df_cancel, rday, stock_str, free_shares, grouped_profiles, trade_dates, thresholds):
    return Level2MetricCalculator.calculate_vpdistribution(df_tradedeal=trade_deals, rday=rday, stock_str=stock_str)

@register_metric('volumeprofile', requires_deal=True)
def calc_volumeprofile_metric(trade_deals, df_cancel, rday, stock_str, free_shares, grouped_profiles, trade_dates, thresholds):
    vp_window_df = load_vpdistribution(
        stock_str=stock_str, rday=rday, profiles=grouped_profiles, 
        trade_dates=trade_dates, lookback_type='turnover', lookback_param=1.1, free_shares=free_shares
    )
    return Level2MetricCalculator.calculate_volumeprofile(vp_window_df)

@register_metric('ordertierbaseline', requires_deal=True)
def calc_ordertierbaseline_metric(trade_deals, df_cancel, rday, stock_str, free_shares, grouped_profiles, trade_dates, thresholds):
    return Level2MetricCalculator.calculate_ordertier(df_tradedeal=trade_deals, rday=rday)

@register_metric('min1_dealtier', requires_deal=True)
def calc_min1_dealtier_metric(trade_deals, df_cancel, rday, stock_str, free_shares, grouped_profiles, trade_dates, thresholds):
    return Level2MetricCalculator.calculate_min1_deal_tier(df_tradedeal=trade_deals, stock_thresholds=thresholds)

@register_metric('min1_canceltier', requires_deal=False, requires_cancel=True)
def calc_min1_canceltier_metric(trade_deals, df_cancel, rday, stock_str, free_shares, grouped_profiles, trade_dates, thresholds):
    return Level2MetricCalculator.calculate_min1_cancel_tier(df_cancel=df_cancel, stock_thresholds=thresholds)

@register_metric('cancelorder', requires_deal=False, requires_cancel=True)
def calc_cancelorder_metric(trade_deals, df_cancel, rday, stock_str, free_shares, grouped_profiles, trade_dates, thresholds):
    return Level2MetricCalculator.calculate_cancelratio(df_cancel=df_cancel, rday=rday)

def derive_dealmetrics_daily(args):
    """
    每日计算任务（分别调用成交分层与撤单分层计算方法）
    在一个周期里计算多个指标， 这些指标均基于df_deal数据，充分共享df_deal数据。
    """
    date_list, stock_str, free_shares, active_methods = args
    s_date = str(date_list[0])
    checkyear = s_date[:4]
    checkmonth = s_date[4:6] 

    stage_deal_root = CONFIG.l2_path['L2_STAGE'] / f"deal/{checkyear}/{checkyear}{checkmonth}"

    need_vp = any('vpdistribution' in m or 'volumeprofile' in m for m in active_methods)
    need_tier = any('tier' in m for m in active_methods)
    grouped_profiles, all_trade_dates = preload_vpdistribution_cache(stock_str) if need_vp else(None, [])
    frozen_thresholds = load_ordertier_baseline(baseline_date='202511', stock_str=stock_str) if need_tier else None

    metric_lists = {m: [] for m in active_methods}
    need_deal = any(METRIC_REGISTRY[m]['requires_deal'] for m in active_methods)
    trade_deals = None
    need_cancel = any(METRIC_REGISTRY[m]['requires_cancel'] for m in active_methods)
    df_cancel = pd.DataFrame()
    ret = False

    for rday in date_list:
        if need_deal:
            deal_file = stage_deal_root / f"{stock_str}/{stock_str}_{rday}.parquet"
            if deal_file.exists(): 
                ret = True
                df_deal = pd.read_parquet(deal_file, schema=CONFIG.DEAL_SCHEMA)
                df_deal['Turnover'] = df_deal['Price'] * df_deal['Volume']
                trade_deals = df_deal[df_deal['Side'].isin([0, 1])]

        
        if need_cancel:
            cancel_file = CONFIG.l2_path['L2_STAGE'] / f"cancel/{checkyear}/{checkyear}{checkmonth}/{stock_str}/{stock_str}_{rday}.parquet"
            if cancel_file.exists():
                df_cancel = pd.read_parquet(cancel_file, schema=CONFIG.CANCEL_SCHEMA) if cancel_file.exists() else pd.DataFrame()
                if not df_cancel.empty:
                    df_cancel['CancelTime'] = df_cancel['DealTime']
                    df_cancel['TradingDay'] = rday

        for m in active_methods:
            info = METRIC_REGISTRY[m]
            if info['requires_deal'] and (trade_deals is None or trade_deals.empty):
                continue
            if info['requires_cancel'] and df_cancel.empty:
                continue

            res_df = info['handler'](
                trade_deals=trade_deals, df_cancel=df_cancel, rday=rday,
                stock_str=stock_str, free_shares=free_shares,
                grouped_profiles=grouped_profiles, trade_dates=all_trade_dates,
                thresholds=frozen_thresholds
            )
            if res_df is not None and not res_df.empty:
                metric_lists[m].append(res_df)

    # 统一落盘保存
    for m in active_methods:
        if metric_lists[m]:
            save_deal_metrics(pd.concat(metric_lists[m], ignore_index=True), stock_str, m)

    return ret, stock_str

def derive_dealmetrics_monthly(args):
    """每月缓存/历史数据批量计算任务"""
    yearmonth, stock_str, free_shares, mode, active_methods = args
    checkyear = yearmonth[:4]
    checkmonth = yearmonth[4:6] 

    metric_lists = {m: [] for m in active_methods}

    if mode == 'CACHED':
        deal_file = CONFIG.l2_path['L2_CACHED_MONTHLY'] / f"deal/{checkyear}/{checkyear}{checkmonth}/{stock_str}.parquet"
    elif mode == 'ARCHIVED':
        deal_file = CONFIG.base_path['L2_ARCHIVED_MONTHLY'] / f"deal/{checkyear}/{checkyear}{checkmonth}/{stock_str}.parquet"
    if not deal_file.exists(): 
        return False, stock_str

    need_deal = any(METRIC_REGISTRY[m]['requires_deal'] for m in active_methods)
    if need_deal:
        deal_columns = ['Price', 'DealTime', 'Volume', 'Side', 'BuyID', 'DealID', 'SellID', 'TradingDay']
        deal_monthly_pl = pl.read_parquet(deal_file, columns=deal_columns)
        deal_dict = deal_monthly_pl.partition_by("TradingDay", as_dict=True)
        empty_deal_df = pl.DataFrame(schema=deal_monthly_pl.schema)
        del deal_monthly_pl

        trade_deals = None

    need_cancel = any(METRIC_REGISTRY[m]['requires_cancel'] for m in active_methods)
    df_cancel = pd.DataFrame()

    need_vp = any('vpdistribution' in m or 'volumeprofile' in m for m in active_methods)
    need_tier = any('tier' in m for m in active_methods)
    grouped_profiles, all_trade_dates = preload_vpdistribution_cache(stock_str) if need_vp else (None, [])
    frozen_thresholds = load_ordertier_baseline(baseline_date='202511', stock_str=stock_str) if need_tier else None

    date_list = sorted(list(deal_dict.keys()))
    ret = False
    for rday in date_list:
        rday_val = rday[0]
        if need_deal:
            deal_daily_pl = deal_dict.pop(rday, empty_deal_df)
            if not deal_daily_pl.is_empty(): 

                deal_daily_df = deal_daily_pl.to_pandas()
                del deal_daily_pl
                ret = True
                deal_daily_df['Turnover'] = deal_daily_df['Price'] * deal_daily_df['Volume']
                trade_deals = deal_daily_df[deal_daily_df['Side'].isin([0, 1])]

        
        if need_cancel:
            cancel_file = CONFIG.l2_path['L2_STAGE'] / f"cancel/{checkyear}/{checkyear}{checkmonth}/{stock_str}/{stock_str}_{rday_val}.parquet"
            if cancel_file.exists():
                df_cancel = pd.read_parquet(cancel_file, schema=CONFIG.CANCEL_SCHEMA) if cancel_file.exists() else pd.DataFrame()
            if not df_cancel.empty:
                df_cancel['CancelTime'] = df_cancel['DealTime']
                df_cancel['TradingDay'] = rday_val

        for m in active_methods:
            info = METRIC_REGISTRY[m]
            if info['requires_deal'] and (trade_deals is None or trade_deals.empty):
                continue
            if info['requires_cancel'] and df_cancel.empty:
                continue
            
            res_df = info['handler'](
                trade_deals=trade_deals, df_cancel=df_cancel, rday=rday_val,
                stock_str=stock_str, free_shares=free_shares,
                grouped_profiles=grouped_profiles, trade_dates=all_trade_dates,
                thresholds=frozen_thresholds
            )
            if res_df is not None and not res_df.empty:
                metric_lists[m].append(res_df)

    # 统一按需落盘
    # for m in active_methods:
    #     if metric_lists[m]:
    #         save_deal_metrics(pd.concat(metric_lists[m], ignore_index=True), stock_str, m)
    return ret, stock_str

def derive_tasks(mode='', date_list=[], method=None):
    if method is None:
        method = list(METRIC_REGISTRY.keys()) # 默认计算所有已注册指标

    tasks = []
    totaltask = 0

    stockinfo = CONFIG.l2_path['STOCK_META']/'stockinfo_202607.csv'
    stockinfo_df = pd.read_csv(stockinfo, dtype=CONFIG.stockinfo_csvtype)
    freeshares_dict = stockinfo_df.set_index('Stock')['ActiveCapital'].to_dict()

    if mode == 'DAILY':
        s_date = str(date_list[0])
        checkyear = s_date[:4]
        checkmonth = s_date[4:6]  
        stage_deal_root = CONFIG.l2_path['L2_STAGE'] / f"deal/{checkyear}/{checkyear}{checkmonth}"
        stockstr_list = [d.name for d in stage_deal_root.iterdir() if d.is_dir() and len(d.name) == 6]    
        totaltask = len(stockstr_list)
        for stock_str in stockstr_list:
            free_shares = freeshares_dict.get(stock_str, 10000000) 
            tasks.append((date_list, stock_str, free_shares, method)) 

        subprocess = derive_dealmetrics_daily
    elif mode == 'CACHED':
        for tmp_month in date_list:
            checkyear = tmp_month[:4]
            checkmonth = tmp_month[4:6]  
            stage_deal_root = CONFIG.l2_path['L2_CACHED_MONTHLY'] / f"deal/{checkyear}/{checkyear}{checkmonth}"
            stockstr_list = [d.stem for d in stage_deal_root.glob(f"*.parquet")]
            totaltask += len(stockstr_list)
            for stock_str in stockstr_list:
                free_shares = freeshares_dict.get(stock_str, 10000000)
                tasks.append((tmp_month, stock_str, free_shares, mode, method)) 

            subprocess = derive_dealmetrics_monthly
    elif mode == 'ARCHIVED':
        for tmp_month in date_list:
            checkyear = tmp_month[:4]
            checkmonth = tmp_month[4:6]  
            stage_deal_root = CONFIG.base_path['L2_ARCHIVED_MONTHLY'] / f"deal/{checkyear}/{checkyear}{checkmonth}"
            stockstr_list = [d.stem for d in stage_deal_root.glob(f"*.parquet")]
            totaltask += len(stockstr_list)
            for stock_str in stockstr_list:
                free_shares = freeshares_dict.get(stock_str, 10000000)
                tasks.append((tmp_month, stock_str, free_shares, mode, method))
            subprocess = derive_dealmetrics_monthly

    physical_cores = 8
    with Pool(physical_cores) as p:
        print(f"开始 Level2 指标生成 ({mode})...")        
        results = p.imap_unordered(subprocess, tasks, chunksize=50)
        success_count = 0
        for success, stock in results:
            if success:
                success_count += 1
                print(f"进程推进中.. 已计算 {success_count}/{totaltask} 只股票 [{stock}]", end="\r")


def plot_metrics():
    # 使用finplot对指标进行可视化，
    # 功能描述：
    #   1. 与股票的日线或1分钟ohlc数据联动。子图1绘制ohlc数据，子图2~N用于绘制指标。 
    #   2. 可通过键盘操作根据股票列表切换上一个股票的或下一个股票，图表跟随股票联动切换显示。
    # 功能目标：用于新指标的正确性检验 或 用于多指标的联动测试
    pass

if __name__ == '__main__':
    target_methods = ['min1_bar', 'sweepratio', 'vpdistribution', 'volumeprofile', 'ordertierbaseline', 'min1_dealtier', 'min1_canceltier', 'cancelorder']

    date_list = [
        20261008, 20261009,         
    #     20261012, 20261013, 20261014, 20261015, 20261016,         
    #     20261019, 20261020, 20261021, 20261022, 20261023,     
    #     20261026, 20261027, 20261028, 20261029, 20261030, 
    ]

    cache_month_list = ['202605']


    archived_month_list = ['202607']
    

    # ------------ 每日更新任务 --------------------
    # derive_tasks(mode='DAILY', date_list=date_list)
    
    # ------------ 缓存数据处理任务 --------------------
    derive_tasks(mode='CACHED', date_list=cache_month_list)

    # ------------ 历史数据处理任务 --------------------
    # derive_tasks(mode='ARCHIVED', date_list=archived_month_list)

    # ------------ 独立任务 - 数据可视化 --------------------
    # plot_metrics()      # 非必要任务，仅在有新指标时通过可视化方法检验指标的正确性 或 多指标策略的联动观察
       