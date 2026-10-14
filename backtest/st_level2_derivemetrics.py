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

def derive_dealmetrics_daily(args):
    """
    每日计算任务（分别调用成交分层与撤单分层计算方法）
    在一个周期里计算多个指标， 这些指标均基于df_deal数据，充分共享df_deal数据。
    """
    date_list, stock_str, free_shares = args
    s_date = str(date_list[0])
    checkyear = s_date[:4]
    checkmonth = s_date[4:6] 

    stage_deal_root = CONFIG.l2_path['L2_STAGE'] / f"deal/{checkyear}/{checkyear}{checkmonth}"

    grouped_profiles, all_trade_dates = preload_vpdistribution_cache(stock_str)

    min1_bar_list = []
    sweepratio_list = []
    vpdistribution_list = []
    volumeprofile_list = []
    ordertierbaseline_list = [] 
    min1_dealtier_list = []
    min1_canceltier_list = []
    cancelorder_list = []
    
    ret = False
    for rday in date_list:
        deal_file = stage_deal_root / f"{stock_str}/{stock_str}_{rday}.parquet"
        if not deal_file.exists(): continue

        ret = True
        df_deal = pd.read_parquet(deal_file, schema=CONFIG.DEAL_SCHEMA)
        df_deal['Turnover'] = df_deal['Price'] * df_deal['Volume']
        trade_deals = df_deal[df_deal['Side'].isin([0, 1])]

        cancel_file = CONFIG.l2_path['L2_STAGE'] / f"cancel/{checkyear}/{checkyear}{checkmonth}/{stock_str}/{stock_str}_{rday}.parquet"
        df_cancel = pd.read_parquet(cancel_file, schema=CONFIG.CANCEL_SCHEMA) if cancel_file.exists() else pd.DataFrame()
        if not df_cancel.empty:
            df_cancel['CancelTime'] = df_cancel['DealTime']

        # 1. Bar 数据
        min1 = Resample(timeframe=TimeFrame.Minutes, compression=1)
        min1_bar_df = Level2MetricCalculator.calculate_bars(df_tradedeal=trade_deals, rday=rday, resamples=[min1])
        
        # 2. 吞单指标
        sweepratio_df = Level2MetricCalculator.calculate_sweepratio(df_tradedeal=trade_deals, rday=rday)
        
        # 3. 筹码峰指标
        vpdistribution_df = Level2MetricCalculator.calculate_vpdistribution(df_tradedeal=trade_deals, rday=rday, stock_str=stock_str)
        vp_window_df = load_vpdistribution(stock_str=stock_str, rday=rday, profiles=grouped_profiles, trade_dates=all_trade_dates, lookback_type='turnover', lookback_param=1.1, free_shares=free_shares)
        volumeprofile_df = Level2MetricCalculator.calculate_volumeprofile(vp_window_df)
        
        # 4. 订单分层指标与基准阈值
        ordertierbaseline_df = Level2MetricCalculator.calculate_ordertier(df_tradedeal=trade_deals, rday=rday)
        frozen_thresholds = load_ordertier_baseline(baseline_date='202511', stock_str=stock_str)
        
        # 5. 拆分后的分钟级成交分层 & 撤单分层
        min1_dealtier_df = Level2MetricCalculator.calculate_min1_deal_tier(df_tradedeal=trade_deals, stock_thresholds=frozen_thresholds)
        min1_canceltier_df = Level2MetricCalculator.calculate_min1_cancel_tier(df_cancel=df_cancel, stock_thresholds=frozen_thresholds)
        
        # 6. 撤单影响度指标
        cancelorder_df = Level2MetricCalculator.calculate_cancelratio(df_cancel=df_cancel, rday=rday)

        if not min1_bar_df.empty: min1_bar_list.append(min1_bar_df)
        if not sweepratio_df.empty: sweepratio_list.append(sweepratio_df)
        if not vpdistribution_df.empty: vpdistribution_list.append(vpdistribution_df)
        if not volumeprofile_df.empty: volumeprofile_list.append(volumeprofile_df)
        if not ordertierbaseline_df.empty: ordertierbaseline_list.append(ordertierbaseline_df)
        if not min1_dealtier_df.empty: min1_dealtier_list.append(min1_dealtier_df)
        if not min1_canceltier_df.empty: min1_canceltier_list.append(min1_canceltier_df)
        if not cancelorder_df.empty: cancelorder_list.append(cancelorder_df)

    # 统一落盘保存
    if min1_bar_list: save_deal_metrics(pd.concat(min1_bar_list, ignore_index=True), stock_str, 'min1_bar')
    if sweepratio_list: save_deal_metrics(pd.concat(sweepratio_list, ignore_index=True), stock_str, 'sweepratio')
    if vpdistribution_list: save_deal_metrics(pd.concat(vpdistribution_list, ignore_index=True), stock_str, 'vpdistribution')
    if volumeprofile_list: save_deal_metrics(pd.concat(volumeprofile_list, ignore_index=True), stock_str, 'volumeprofile')
    if ordertierbaseline_list: save_deal_metrics(pd.concat(ordertierbaseline_list, ignore_index=True), stock_str, 'ordertierbaseline')
    if min1_dealtier_list: save_deal_metrics(pd.concat(min1_dealtier_list, ignore_index=True), stock_str, 'min1_dealtier')
    if min1_canceltier_list: save_deal_metrics(pd.concat(min1_canceltier_list, ignore_index=True), stock_str, 'min1_canceltier')
    if cancelorder_list: save_deal_metrics(pd.concat(cancelorder_list, ignore_index=True), stock_str, 'cancelorder')
    
    return ret, stock_str

def derive_dealmetrics_monthly(args):
    """每月缓存/历史数据批量计算任务"""
    yearmonth, stock_str, free_shares, mode = args
    checkyear = yearmonth[:4]
    checkmonth = yearmonth[4:6] 

    min1_bar_list, sweepratio_list, vpdistribution_list = [], [], []
    volumeprofile_list, ordertierbaseline_list = [], []
    min1_dealtier_list, min1_canceltier_list = [], []
    cancelorder_list = []

    if mode == 'CACHED':
        deal_file = CONFIG.l2_path['L2_CACHED_MONTHLY'] / f"deal/{checkyear}/{checkyear}{checkmonth}/{stock_str}.parquet"
    elif mode == 'ARCHIVED':
        deal_file = CONFIG.base_path['L2_ARCHIVED_MONTHLY'] / f"deal/{checkyear}/{checkyear}{checkmonth}/{stock_str}.parquet"
    if not deal_file.exists(): 
        return False, stock_str

    deal_columns = ['Price', 'DealTime', 'Volume', 'Side', 'BuyID', 'DealID', 'SellID', 'TradingDay']
    deal_monthly_pl = pl.read_parquet(deal_file, columns=deal_columns)
    deal_dict = deal_monthly_pl.partition_by("TradingDay", as_dict=True)
    empty_deal_df = pl.DataFrame(schema=deal_monthly_pl.schema)
    del deal_monthly_pl

    grouped_profiles, all_trade_dates = preload_vpdistribution_cache(stock_str)

    date_list = sorted(list(deal_dict.keys()))
    ret = False
    for rday in date_list:
        deal_daily_pl = deal_dict.pop(rday, empty_deal_df)
        if deal_daily_pl.is_empty(): continue

        deal_daily_df = deal_daily_pl.to_pandas()
        del deal_daily_pl
        ret = True
        deal_daily_df['Turnover'] = deal_daily_df['Price'] * deal_daily_df['Volume']
        trade_deals = deal_daily_df[deal_daily_df['Side'].isin([0, 1])]

        rday_val = rday[0]
        
        cancel_file = CONFIG.l2_path['L2_STAGE'] / f"cancel/{checkyear}/{checkyear}{checkmonth}/{stock_str}/{stock_str}_{rday_val}.parquet"
        df_cancel = pd.read_parquet(cancel_file, schema=CONFIG.CANCEL_SCHEMA) if cancel_file.exists() else pd.DataFrame()
        if not df_cancel.empty:
            df_cancel['CancelTime'] = df_cancel['DealTime']
            df_cancel['TradingDay'] = rday_val

        with TimeProfile():
            print('step1: Resample')
            min1 = Resample(timeframe=TimeFrame.Minutes, compression=1)
        with TimeProfile():
            print('step2: calculate_bars')
            min1_bar_df = Level2MetricCalculator.calculate_bars(df_tradedeal=trade_deals, rday=rday_val, resamples=[min1])
        with TimeProfile():
            print('step3: calculate_sweepratio')
            sweepratio_df = Level2MetricCalculator.calculate_sweepratio(df_tradedeal=trade_deals, rday=rday_val)
        with TimeProfile():
            print('step4: calculate_vpdistribution')
            vpdistribution_df = Level2MetricCalculator.calculate_vpdistribution(df_tradedeal=trade_deals, rday=rday_val, stock_str=stock_str)
        with TimeProfile():
            print('step5: load_vpdistribution')
            vp_window_df = load_vpdistribution(stock_str=stock_str, rday=rday_val, profiles=grouped_profiles, trade_dates=all_trade_dates, lookback_type='turnover', lookback_param=1.1, free_shares=free_shares)
        with TimeProfile():
            print('step6: calculate_volumeprofile')
            volumeprofile_df = Level2MetricCalculator.calculate_volumeprofile(vp_window_df)
        with TimeProfile():
            print('step7: calculate_ordertier')
            ordertierbaseline_df = Level2MetricCalculator.calculate_ordertier(df_tradedeal=trade_deals, rday=rday_val)
        with TimeProfile():
            print('step8: load_ordertier_baseline')
            frozen_thresholds = load_ordertier_baseline(baseline_date='202511', stock_str=stock_str)
        with TimeProfile():
            print('step9: calculate_min1_deal_tier')
            min1_dealtier_df = Level2MetricCalculator.calculate_min1_deal_tier(df_tradedeal=trade_deals, stock_thresholds=frozen_thresholds)
        with TimeProfile():
            print('step10: calculate_min1_cancel_tier')
            min1_canceltier_df = Level2MetricCalculator.calculate_min1_cancel_tier(df_cancel=df_cancel, stock_thresholds=frozen_thresholds)
        with TimeProfile():
            print('step11: calculate_cancelratio')
            cancelorder_df = Level2MetricCalculator.calculate_cancelratio(df_cancel=df_cancel, rday=rday_val)

        if not min1_bar_df.empty: min1_bar_list.append(min1_bar_df)
        if not sweepratio_df.empty: sweepratio_list.append(sweepratio_df)
        if not vpdistribution_df.empty: vpdistribution_list.append(vpdistribution_df)
        if not volumeprofile_df.empty: volumeprofile_list.append(volumeprofile_df)
        if not ordertierbaseline_df.empty: ordertierbaseline_list.append(ordertierbaseline_df)
        if not min1_dealtier_df.empty: min1_dealtier_list.append(min1_dealtier_df)
        if not min1_canceltier_df.empty: min1_canceltier_list.append(min1_canceltier_df)
        if not cancelorder_df.empty: cancelorder_list.append(cancelorder_df)

        pass

    if min1_bar_list: save_deal_metrics(pd.concat(min1_bar_list, ignore_index=True), stock_str, 'min1_bar')
    if sweepratio_list: save_deal_metrics(pd.concat(sweepratio_list, ignore_index=True), stock_str, 'sweepratio')
    if vpdistribution_list: save_deal_metrics(pd.concat(vpdistribution_list, ignore_index=True), stock_str, 'vpdistribution')
    if volumeprofile_list: save_deal_metrics(pd.concat(volumeprofile_list, ignore_index=True), stock_str, 'volumeprofile')
    if ordertierbaseline_list: save_deal_metrics(pd.concat(ordertierbaseline_list, ignore_index=True), stock_str, 'ordertierbaseline')
    if min1_dealtier_list: save_deal_metrics(pd.concat(min1_dealtier_list, ignore_index=True), stock_str, 'min1_dealtier')
    if min1_canceltier_list: save_deal_metrics(pd.concat(min1_canceltier_list, ignore_index=True), stock_str, 'min1_canceltier')
    if cancelorder_list: save_deal_metrics(pd.concat(cancelorder_list, ignore_index=True), stock_str, 'cancelorder')
    
    return ret, stock_str

def derive_tasks(mode='', date_list=[]):
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
            tasks.append((date_list, stock_str, free_shares)) 

        subprocess = derive_dealmetrics_daily
    elif mode == 'CACHED':
        for tmp_month in date_list:
            checkyear = tmp_month[:4]
            checkmonth = tmp_month[4:6]  
            stage_deal_root = CONFIG.l2_path['L2_CACHED_MONTHLY'] / f"deal/{checkyear}/{checkyear}{checkmonth}"
            stockstr_list = [d.stem for d in stage_deal_root.glob(f"*.parquet")]
            stockstr_list = ['002655']

            totaltask += len(stockstr_list)
            for stock_str in stockstr_list:
                free_shares = freeshares_dict.get(stock_str, 10000000)
                tasks.append((tmp_month, stock_str, free_shares, mode)) 

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
                tasks.append((tmp_month, stock_str, free_shares, mode))
            subprocess = derive_dealmetrics_monthly

    physical_cores = 1
    with Pool(physical_cores) as p:
        print(f"开始 Level2 指标生成 ({mode})...")        
        results = p.imap_unordered(subprocess, tasks, chunksize=50)
        success_count = 0
        for success, stock in results:
            if success:
                success_count += 1
                print(f"进程推进中.. 已计算 {success_count}/{totaltask} 只股票 [{stock}]", end="\r")
        print()

def plot_metrics():
    # 使用finplot对指标进行可视化，
    # 功能描述：
    #   1. 与股票的日线或1分钟ohlc数据联动。子图1绘制ohlc数据，子图2~N用于绘制指标。 
    #   2. 可通过键盘操作根据股票列表切换上一个股票的或下一个股票，图表跟随股票联动切换显示。
    # 功能目标：用于新指标的正确性检验 或 用于多指标的联动测试
    pass

if __name__ == '__main__':
    tasks = []
    daily_results = []
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
       