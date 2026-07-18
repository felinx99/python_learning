import numpy as np
import pyarrow as pa
import pandas as pd
import pyarrow.parquet as pq
from common import CONFIG
from multiprocessing import Pool
from api.timeprofile import TimeProfile

class StratifiedFeatureEngine:
    """高频微观结构立体分层特征引擎"""
    
    @staticmethod
    def label_order_volume(vol, thres):
        """基于指定阈值划分订单层级"""
        if vol < thres[70]: return 'Small'
        elif vol < thres[95]: return 'Medium'
        elif vol < thres[99.5]: return 'Large'
        else: return 'Mega'

    def process_minute_features(self, df_tradedeals: pd.DataFrame, df_cancel: pd.DataFrame, stock_thresholds: dict) -> pd.DataFrame:
        """
        核心步骤：根据买卖方向自动切换标准化标尺 -> 分钟级压缩 -> 双流合流
        """
        # 提取时间分钟项
        stock_code = int(df_tradedeals['SecuCode'].iat[0])
        stock_str = str(stock_code).zfill(6)
        rday = int(df_tradedeals['TradingDay'].iat[0])
        yearmonth = str(rday)[:6]  # 订单分层数据按月保存
        dst_dir = CONFIG.base_path['LEVEL2_WORKSHAPCE_PATH'] / f"01_buffer/daily/metrics/ordertier"
        dst_dir.mkdir(parents=True, exist_ok=True)
        dst_file = dst_dir / f"ordertier_{stock_str}_{yearmonth}.parquet"

        df_tradedeals['Minute'] = df_tradedeals['DealTime'] // 100000
                
        # --- A. 成交流分层还原 ---
        buy_map = df_tradedeals.groupby('BuyID')['Volume'].sum()
        sell_map = df_tradedeals.groupby('SellID')['Volume'].sum()
        
        # 仅在 unique 订单级别计算 Layer 映射关系
        buy_layer_map = buy_map.apply(self.label_order_volume, args=(stock_thresholds['bid'],))
        sell_layer_map = sell_map.apply(self.label_order_volume, args=(stock_thresholds['ask'],))
        
        # 将层级标签高速映射回逐笔明细表
        df_tradedeals['Buy_Layer'] = df_tradedeals['BuyID'].map(buy_layer_map)
        df_tradedeals['Sell_Layer'] = df_tradedeals['SellID'].map(sell_layer_map)

        # --- B. 拆分买卖流量流向 (将每笔交易分裂为买/卖两端) ---
        # 买方流量
        buy_active_series = df_tradedeals.groupby('BuyID')['Side'].min() == 0
        df_tradedeals['Buy_Is_Active'] = df_tradedeals['BuyID'].map(buy_active_series)
        df_buy_flow = pd.DataFrame({
            'Minute': df_tradedeals['Minute'],
            'Category': np.where(df_tradedeals['Buy_Is_Active'], 'act_buy', 'pas_buy'),
            'Layer': df_tradedeals['Buy_Layer'],
            'Volume': df_tradedeals['Volume']
        })
        
        # 卖方流量
        sell_active_series = df_tradedeals.groupby('SellID')['Side'].max() == 1
        df_tradedeals['Sell_Is_Active'] = df_tradedeals['SellID'].map(sell_active_series)
        df_sell_flow = pd.DataFrame({
            'Minute': df_tradedeals['Minute'],
            'Category': np.where(df_tradedeals['Sell_Is_Active'], 'act_sell', 'pas_sell'),
            'Layer': df_tradedeals['Sell_Layer'],
            'Volume': df_tradedeals['Volume']
        })
        
        # 合并双向流量并统一聚合
        df_deal_flow = pd.concat([df_buy_flow, df_sell_flow], ignore_index=True)
        
        deal_pivot = df_deal_flow.pivot_table(
            index='Minute', 
            columns=['Category', 'Layer'], 
            values='Volume', 
            aggfunc='sum'
        ).fillna(0).astype(int)
        
        # 列名扁平化：例如 deal_vol_act_buy_Small, deal_vol_pas_sell_Mega
        deal_pivot.columns = [f"deal_{cat}_vol_{layer}" for cat, layer in deal_pivot.columns]

        # --- B. 撤单流分层还原 ---
        cancel_pivot = pd.DataFrame(index=deal_pivot.index)
        if not df_cancel.empty:
            df_cancel['Minute'] = df_cancel['CancelTime'] // 100000
            
            # 分流 Apply，避免不必要的计算
            df_cancel['Layer'] = 'Small'
            is_buy_cancel = df_cancel['Side'] == -1  #-1表示撤买 -11表示撤卖
            if is_buy_cancel.any():
                df_cancel.loc[is_buy_cancel, 'Layer'] = df_cancel.loc[is_buy_cancel, 'Volume'].apply(
                    self.label_order_volume, args=(stock_thresholds['bid'],)
                )
            if (~is_buy_cancel).any():
                df_cancel.loc[~is_buy_cancel, 'Layer'] = df_cancel.loc[~is_buy_cancel, 'Volume'].apply(
                    self.label_order_volume, args=(stock_thresholds['ask'],)
                )
            
            # 撤单流透视后也立刻强制转为 int
            c_pivot = df_cancel.pivot_table(
                index='Minute',
                columns=['Side', 'Layer'],
                values='Volume',
                aggfunc='sum'
            ).fillna(0).astype(int)
            
            # 撤单列名扁平化：例如 cancel_vol_buy_Large
            c_pivot.columns = [f"cancel{'buy' if s==-1 else 'sell'}_vol_{l}" for s, l in c_pivot.columns]
            
            # join 合并后由于会引入空值，fillna(0)后必须再次 astype(int)
            cancel_pivot = cancel_pivot.join(c_pivot, how='left').fillna(0).astype(int)
            
        # --- C. 双流强行对齐合流 ---
        df_features = deal_pivot.join(cancel_pivot, how='outer').fillna(0)
        df_features['TradingDay'] = rday
        df_features.reset_index(inplace=True)

        # 5. 安全增量合并（带去重防护）
        if dst_file.exists():
            existing_df = pd.read_parquet(dst_file)
            # 从历史数据中，把属于这些交易日的数据全部剔除（实现“新数据覆盖旧数据”）
            existing_df = existing_df[existing_df['TradingDay'] != rday]
            df_features = pd.concat([existing_df, df_features], ignore_index=True)
            
        df_features = df_features.sort_values(by='TradingDay').reset_index(drop=True)
        df_features.to_parquet(dst_file, compression='zstd', index=False)

        print(f"💾 特征成功保存 ➡️ {dst_file.name} | 数据量: {df_features.shape}")
                
        return df_features

# ==============================================================================
# 2. 指标合流计算函数
# ==============================================================================
def build_stratified_cancel_features(df_deal_1m: pd.DataFrame, df_cancel_1m: pd.DataFrame) -> pd.DataFrame:
    if df_deal_1m.empty and df_cancel_1m.empty:
        return pd.DataFrame()
        
    df_merged = pd.merge(
        df_deal_1m, df_cancel_1m, 
        on=['Time_1M', 'SecuCode', 'TradingDay'], 
        how='outer'
    ).fillna(0)
    
    sides = ['buy', 'sell']
    tiers = ['small', 'medium', 'large', 'mega']
    
    for side in sides:
        for tier in tiers:
            deal_vol_col = f"{side}_{tier}_vol"
            deal_amt_col = f"{side}_{tier}_amt"
            cancel_vol_col = f"{side}_{tier}_cancel_vol"
            cancel_amt_col = f"{side}_{tier}_cancel_amt"
            
            for col in [deal_vol_col, deal_amt_col, cancel_vol_col, cancel_amt_col]:
                if col not in df_merged.columns:
                    df_merged[col] = 0.0

            # 因子 1：分层撤单率
            total_vol = df_merged[deal_vol_col] + df_merged[cancel_vol_col]
            df_merged[f"{side}_{tier}_cancel_ratio"] = (df_merged[cancel_vol_col] / total_vol.replace(0, np.nan)).fillna(0).astype('float32')

    for tier in tiers:
        # 因子 2：分层多空撤单不平衡度
        buy_cancel_amt = df_merged[f"buy_{tier}_cancel_amt"]
        sell_cancel_amt = df_merged[f"sell_{tier}_cancel_amt"]
        sum_cancel_amt = buy_cancel_amt + sell_cancel_amt
        
        df_merged[f"{tier}_cancel_imbalance"] = (
            (buy_cancel_amt - sell_cancel_amt) / sum_cancel_amt.replace(0, np.nan)
        ).fillna(0).astype('float32')

    return df_merged

def load_cancelratio(stock_code=0, rday=0, window=20):
    """
    【高性能时序加载器】提取目标日前 N 天的挂撤单时间序列
    """
    stock_str = str(stock_code).zfill(6)
    src_file = CONFIG.base_path['LEVEL2_WORKSHAPCE_PATH'] / f"01_buffer/daily/metrics/cancelratio/{stock_str}.parquet"
    if not src_file.exists():
        return pd.DataFrame()
        
    df_c = pd.read_parquet(src_file)
    df_c = df_c.sort_values(by='TradingDay').reset_index(drop=True)
    
    if rday not in df_c['TradingDay'].values:
        return pd.DataFrame()
        
    target_idx = df_c[df_c['TradingDay'] == rday].index[0]
    start_idx = max(0, target_idx - window + 1)
    
    # 截取固定窗口切片
    df_window = df_c.iloc[start_idx: target_idx + 1].copy()
    return df_window

def load_ordertier_baseline(baseline_date: int, stock_str='') -> dict:
    """
    🎯 核心逻辑：提取指定测试期baseline_date的那一个月数据, 固化生成每只股票的绝对股数分层阈值
    """
    yearmonth = str(baseline_date)[:6]  # 提取年月，如 202606

    ordertier_dir = CONFIG.base_path['LEVEL2_WORKSHAPCE_PATH'] / f"01_buffer/daily/metrics/ordertierbaseline"
    ordertier_file = ordertier_dir / f"ordertier_baseline_{stock_str}_{yearmonth}.parquet"
    ordertier_df = pd.read_parquet(ordertier_file)

    print(f"🔒 [基准锁定] 正在使用 {yearmonth} 的数据计算静态标准化分层阈值...")
    locked_thresholds = {}
    default_thres = {70: 10000.0, 95: 50000.0, 99.5: 200000.0}
    
    if ordertier_df.empty:
        locked_thresholds = {'bid': default_thres, 'ask': default_thres, 'total': default_thres}
        return locked_thresholds
        
    # 初始化相对倍数池与均值序列
    bid_norm_pool, ask_norm_pool, total_norm_pool = [], [], []
    bid_means, ask_means, total_means = [], [], []
    # 遍历基准月内的每一天（Parquet中一行代表一天）
    for _, row in ordertier_df.iterrows():
        # 从 Parquet 中读取回来的 List/Array 统一强转为 float 数组
        b_vols = np.array(row['BidVolume'], dtype=int)
        a_vols = np.array(row['AskVolume'], dtype=int)
        b_mean = float(row['BidVolumeMean'])
        a_mean = float(row['AskVolumeMean'])
        
        # 1. 买端 (Bid) 动态标准化池
        if len(b_vols) > 0 and b_mean > 0:
            bid_norm_pool.extend(b_vols / b_mean)
            bid_means.append(b_mean)
            
        # 2. 卖端 (Ask) 动态标准化池
        if len(a_vols) > 0 and a_mean > 0:
            ask_norm_pool.extend(a_vols / a_mean)
            ask_means.append(a_mean)
            
        # 3. 全局合成 (Total) 动态标准化池（与原逻辑完全一致）
        t_vols = np.concatenate([b_vols, a_vols])
        if len(t_vols) > 0:
            t_mean = float(np.mean(t_vols))
            total_norm_pool.extend(t_vols / t_mean)
            total_means.append(t_mean)
    
    # 分位映射核心闭包函数
    def calc_percentiles(pool, means):
        if not pool or not means: return default_thres
        m_mean = np.mean(means)
        return {
            70: float(np.percentile(pool, 70) * m_mean),
            95: float(np.percentile(pool, 95) * m_mean),
            99.5: float(np.percentile(pool, 99.5) * m_mean)
        }
    
    # 同时固化三套标尺，供特征引擎自由选用
    locked_thresholds = {
        'bid': calc_percentiles(bid_norm_pool, bid_means),
        'ask': calc_percentiles(ask_norm_pool, ask_means),
        'total': calc_percentiles(total_norm_pool, total_means)
    }
            
    return locked_thresholds

def calculate_cancelratio(df_deal, df_cancel, df_window: pd.DataFrame) -> pd.DataFrame:
    """
    【波段特征引擎】计算 20 天时序大单撤单非对称度与欺诈性建模特征
    """
    # if df_window.empty or len(df_window) == 0:
    #     return pd.DataFrame()
        
    # 提取当前交易日和代码
    if df_deal is None or df_deal.empty:
        return
    trade_date = int(df_deal['TradingDay'].iat[0])
    stock_code = int(df_deal['SecuCode'].iat[0])
       
    # # ----------------==================================----------------
    # # 1. 计算 20 天滚动的累计撤单率 (消除单日随机流动性冲击的噪音)
    # # ----------------==================================----------------
    # total_buy_orders = df_window['large_buy_orders'].sum()
    # total_buy_cancels = df_window['large_buy_cancels'].sum()
    # total_sell_orders = df_window['large_sell_orders'].sum()
    # total_sell_cancels = df_window['large_sell_cancels'].sum()
    
    # rolling_buy_cancel_ratio = total_buy_cancels / (total_buy_orders + 1e-8)
    # rolling_sell_cancel_ratio = total_sell_cancels / (total_sell_orders + 1e-8)
    
    # # 核心指标：大单撤单非对称度 (Asymmetry of Large Cancels)
    # # 范围在 [-1, 1]。 正值代表买单假单多(出货)，负值代表卖单假单多(洗盘末期准备拉)
    # cancel_asymmetry = rolling_buy_cancel_ratio - rolling_sell_cancel_ratio
    
    # # ----------------==================================----------------
    # # 2. 计算日线级别指标的边际变化 (用于捕捉“突然飙升”的拐点)
    # # ----------------==================================----------------
    # # 计算每日独立的撤单率序列
    # df_window['daily_buy_ratio'] = df_window['large_buy_cancels'] / (df_window['large_buy_orders'] + 1e-8)
    # df_window['daily_sell_ratio'] = df_window['large_sell_cancels'] / (df_window['large_sell_orders'] + 1e-8)
    # df_window['daily_asymmetry'] = df_window['daily_buy_ratio'] - df_window['daily_sell_ratio']
    
    # # 最新一天的状态
    # today_asymmetry = df_window['daily_asymmetry'].iloc[-1]
    
    # # 动量拐点：今天非对称度相比过去19天均值的偏离度（Z-Score化）
    # hist_asymmetry_mean = df_window['daily_asymmetry'].iloc[:-1].mean() if len(df_window) > 1 else 0
    # hist_asymmetry_std = df_window['daily_asymmetry'].iloc[:-1].std() if len(df_window) > 1 else 1
    # cancel_asymmetry_zscore = (today_asymmetry - hist_asymmetry_mean) / (hist_asymmetry_std + 1e-8)
    
    # # 3. 欺诈/合力真伪综合得分 (Spoofing Score)
    # # 如果买单撤单率极高且当前20天属于非对称买单撤单状态，定义为高欺诈托单
    # spoofing_score = rolling_buy_cancel_ratio * max(0, cancel_asymmetry)

    # return pd.DataFrame([{
    #     'TradeDate': trade_date,
    #     'SecuCode': secu_code,
    #     'rolling_buy_cancel_ratio_20d': rolling_buy_cancel_ratio,
    #     'rolling_sell_cancel_ratio_20d': rolling_sell_cancel_ratio,
    #     'cancel_asymmetry_20d': cancel_asymmetry,            # 🔍 基础波段核心：非对称度
    #     'cancel_asymmetry_zscore': cancel_asymmetry_zscore,  # ⚡ 边际拐点：大单撤单突变度
    #     'spoofing_score_20d': spoofing_score,                # 🎯 欺诈度评分
    #     'actual_cancel_lookback_days': len(df_window)
    # }])

    # ------------------------------------------------------------------------------------------------------------------        
    stock_str = str(stock_code).zfill(6)
    rday = int(df_deal['TradingDay'].iat[0])
    
    dst_dir = CONFIG.base_path['LEVEL2_WORKSHAPCE_PATH'] / "01_buffer/daily/metrics/cancelorder"
    dst_dir.mkdir(parents=True, exist_ok=True)
    dst_file = dst_dir / f"{stock_str}.parquet"
    
    #----------------------------------------------------------------------------------------------------------

    """
    【精细化盘口模块】引入撤单价格档位与价格影响度权重的 L2 清洗引擎
    :param df_order: 必须包含 'MarketPrice' 列（即当前时刻的市场基准价，已放大100倍的int32）
    """
    if df_deal is None or df_deal.empty:
        return
        
    
    # 只筛选出撤单行为
    if df_cancel.empty:
        return

    # 3. 过滤出主力大单撤单，用于研判欺诈度与合力
    df_large_cancel = df_cancel[df_cancel['Layer'] == 'Large'].copy()

    if df_large_cancel.empty:
        return

    # ----------------==================================----------------
    # 维度一：按价格档位打标签并统计 (分类)
    # ----------------==================================----------------
    
    # 买方大单撤单细分
    lc_buy = df_large_cancel[df_large_cancel['Side'] == -1]
    buy_cancel_at_touch = int(lc_buy[lc_buy['OBLevel'] == 1]['Volume'].sum())         # 买一撤单（真实伤害）
    buy_cancel_spoofing = int(lc_buy[(lc_buy['OBLevel'] > 1) & (lc_buy['OBLevel'] < 6)]['Volume'].sum()) # 买二-买五（欺诈托单）
    buy_cancel_deep = int(lc_buy[lc_buy['OBLevel'] > 5]['Volume'].sum())            # 深盘撤单
    
    # 卖方大单撤单细分
    lc_sell = df_large_cancel[df_large_cancel['Side'] == -11]
    sell_cancel_at_touch = int(lc_sell[lc_sell['OBLevel'] == 1]['Volume'].sum())       # 卖一撤单（假压盘溃退）
    sell_cancel_spoofing = int(lc_sell[(lc_sell['OBLevel'] > 1) & (lc_sell['OBLevel'] < 6)]['Volume'].sum()) # 卖二-卖五（欺诈压盘）
    sell_cancel_deep = int(lc_sell[lc_sell['OBLevel'] > 5]['Volume'].sum())           # 深盘撤单

    # ----------------==================================----------------
    # 维度二：量化对价格的影响度 (权重化)
    # ----------------==================================----------------
    # 引入距离反比权重函数: Weight = 1 / (1 + abs_tick_distance)
    # 档位为0（买一卖一）权重为 1.0；档位为1（买二卖二）权重为 0.5；档位为5时权重为 0.16
    df_large_cancel['price_impact_weight'] = 1.0 / df_large_cancel['OBLevel']
    df_large_cancel['weighted_cancel_vol'] = df_large_cancel['Volume'] * df_large_cancel['price_impact_weight']
    
    total_weighted_buy_cancel = float(df_large_cancel[df_large_cancel['Side'] == -1]['weighted_cancel_vol'].sum())
    total_weighted_sell_cancel = float(df_large_cancel[df_large_cancel['Side'] == -11]['weighted_cancel_vol'].sum())

    # 4. 组装单日轻量化盘口指标行
    df_day_metric = pd.DataFrame([{
        'SecuCode': stock_code,
        'TradingDay': rday,
        # 原始基础总量
        'buy_cancel_at_touch': buy_cancel_at_touch,
        'buy_cancel_spoofing': buy_cancel_spoofing,
        'buy_cancel_deep': buy_cancel_deep,
        'sell_cancel_at_touch': sell_cancel_at_touch,
        'sell_cancel_spoofing': sell_cancel_spoofing,
        'sell_cancel_deep': sell_cancel_deep,
        # 影响度加权总量
        'total_weighted_buy_cancel': total_weighted_buy_cancel,
        'total_weighted_sell_cancel': total_weighted_sell_cancel
    }])
    
    # -----------------------------------------------------------------------------------------------------------------
    """
    【日内1分钟高频清洗层】追踪订单全生命周期的时效性、突发脉冲与坚实度特征
    :param df_order: 包含原始日内逐笔订单 DataFrame (需包含 ApplSeqNum, Time, OrderKind, Side, Volume, Price)
    """
    # if df_order is None or df_order.empty:
    #     return pd.DataFrame()
        
    
    # try:
 
    #     # 假设时间格式为 93001000 (HHMMSSmmm)
    #     time_str = df_order['OrderTime'].astype(str).str.zfill(9)
    #     df_order['dt_time'] = pd.to_datetime(time_str, format='%H%M%S%f', errors='coerce')
    
            
    #     # 计算申报金额 (Price已放大100倍)
    #     df_order['Turnover'] = df_order['Price'] * df_order['Volume']

    #     # 2. 分流：新挂单(0) vs 撤单(1)
    #     df_new = df_order[~df_order['OrderType'].isin([-1, -11])][['OrderID', 'dt_time', 'Volume', 'OrderType', 'Turnover']].copy()
    #     df_can = df_order[df_order['OrderType'].isin([-1, -11])][['OrderID', 'dt_time', 'Volume']].copy()
        
    #     df_can = df_can.rename(columns={'dt_time': 'cancel_time', 'Volume': 'cancel_vol'})

    #     # 3. 【时空缝合】基于序列号打通订单流闭环
    #     df_matched = pd.merge(df_new, df_can, on='OrderID', how='inner')
    #     if df_matched.empty:
    #         return pd.DataFrame()

    #     # 4. 微观指标定义：寿命与被动成交率
    #     df_matched['lifetime'] = (df_matched['cancel_time'] - df_matched['dt_time']).dt.total_seconds()
    #     df_matched['fill_rate'] = (df_matched['Volume'] - df_matched['cancel_vol']) / df_matched['Volume']
    #     df_matched['fill_rate'] = df_matched['fill_rate'].clip(lower=0.0, upper=1.0) # 异常防御

    #     # 5. 筛选大单行为
    #     df_large_matched = df_matched[df_matched['Turnover'] >= large_order_threshold_to_100x].copy()
    #     if df_large_matched.empty:
    #         return pd.DataFrame()

    #     # ----------------==================================----------------
    #     # 6. 【高频重采样层】以 1 分钟 (1T) 窗口进行重采样提取日内时序特征
    #     # ----------------==================================----------------
    #     df_large_matched.set_index('cancel_time', inplace=True)
        
    #     # 买卖流拆分
    #     df_buy_large = df_large_matched[df_large_matched['OrderType'].isin([0,1,2,3])]
    #     df_sell_large = df_large_matched[df_large_matched['OrderType'].isin([10,11,12,13])]

    #     # 计算日内 1 分钟级指标
    #     # A. 撤单脉冲强度 Max_Pulse (1分钟内撤单总额的最大值)
    #     # 放大100倍后用 int64 防溢出，这里计算最大分钟撤单量（股数）
    #     max_pulse_buy = float(df_buy_large.resample('1T')['cancel_vol'].sum().max()) if not df_buy_large.empty else 0.0
    #     max_pulse_sell = float(df_sell_large.resample('1T')['cancel_vol'].sum().max()) if not df_sell_large.empty else 0.0

    #     # B. 平均生命周期 Lifetime 中位数/均值 (1分钟切片内大单的平均存活时间)
    #     mean_lifetime_buy = float(df_buy_large.resample('1T')['lifetime'].mean().median()) if not df_buy_large.empty else -1.0
    #     mean_lifetime_sell = float(df_sell_large.resample('1T')['lifetime'].mean().median()) if not df_sell_large.empty else -1.0

    #     # C. 平均坚实度 Fill_Rate (1分钟切片内撤单大单的平均成交比率)
    #     mean_fill_rate_buy = float(df_buy_large.resample('1T')['fill_rate'].mean().mean()) if not df_buy_large.empty else 1.0
    #     mean_fill_rate_sell = float(df_sell_large.resample('1T')['fill_rate'].mean().mean()) if not df_sell_large.empty else 1.0

    #     # 7. 构造日落盘行特征
    #     df_day_hf_metric = pd.DataFrame([{
    #         'SecuCode': stock_code,
    #         'TradingDay': rday,
    #         'max_pulse_buy_1m': max_pulse_buy,            # 🔍 买单撤单分钟级最高脉冲
    #         'max_pulse_sell_1m': max_pulse_sell,          # 🔍 卖单撤单分钟级最高脉冲
    #         'median_lifetime_buy_1m': mean_lifetime_buy,   # ⏳ 买单撤单日内寿命基线（极短意味着高欺诈）
    #         'median_lifetime_sell_1m': mean_lifetime_sell, # ⏳ 卖单撤单日内寿命基线
    #         'mean_fill_rate_buy_1m': mean_fill_rate_buy,   # 🎯 买方被动成交率（越接近1买盘越坚实）
    #         'mean_fill_rate_sell_1m': mean_fill_rate_sell   # 🎯 卖方被动成交率（压盘被真吃掉的比例）
    #     }])
        
    #     return df_day_hf_metric

    # except Exception as e:
    #     print(f"❌ 高频重采样计算失败 {stock_str} on {rday}: {str(e)}")
    #     return pd.DataFrame()



def derive_volumeprofile(df_deal=None):
    if df_deal is None or df_deal.empty: return
    stock_code = int(df_deal['SecuCode'].iat[0])
    stock_str = str(stock_code).zfill(6)
    rday = int(df_deal['TradingDay'].iat[0]) 
    yearmonth = str(rday)[:6]  # 提取年月，如 202606

    dst_dir = CONFIG.base_path['LEVEL2_WORKSHAPCE_PATH'] / f"01_buffer/daily/metrics/volumeprofile"
    dst_dir.mkdir(parents=True, exist_ok=True)
    dst_file = dst_dir / f"volumeprofile_{stock_str}_{yearmonth}.parquet"
    # 如果已经存在缓存，直接跳过（避免重复计算历史数据）
        
    try:    
        df_deal = df_deal[df_deal['Side'].isin([0, 1])].copy()     
        # 极轻量聚合：生成单日 Profile
        df_profile = df_deal.groupby('Price', as_index=False)['Volume'].sum()
        
        # 将股票代码和日期作为元数据写入，或者直接保存在每行中方便后续合并
        df_profile['SecuCode'] = stock_code
        df_profile['TradingDay'] = rday
        
        # 强制转换紧凑类型，进一步省内存和磁盘空间
        df_profile['Price'] = df_profile['Price'].astype('int32')
        df_profile['Volume'] = df_profile['Volume'].astype('int64')
        
        if dst_file.exists():
            existing_df = pd.read_parquet(dst_file)
            # 如果今天的数据之前跑过一部分，重新更新时会用最新的覆盖旧的
            existing_df = existing_df[existing_df['TradingDay'] != rday]
            df_profile = pd.concat([existing_df, df_profile], ignore_index=True)

        
        df_profile = df_profile.sort_values(by=['TradingDay', 'Price']).reset_index(drop=True)
        
        # 写入缓存目录
        df_profile.to_parquet(dst_file, compression='zstd', index=False)
          
        
    except Exception as e:
        print(f"❌ 生成缓存失败 {dst_file.name}: {str(e)}")

def load_volumeprofile(stock_code=0, rday=0, lookback_type='fixed', window_value=20,free_float_shares=0.0):
    """
    【高性能动态加载器】
    从缓存目录中，根据动态或固定规则，倒序加载某只股票所需的历史单日 Profile DataFrame 列表。
    
    参数：
    - all_trade_dates: 已排序的全局全量交易日列表 (如 ['20260601', '20260602', ...])
    - lookback_type: 'fixed' (固定天数) 或 'turnover' (动态换手率)
    - window_value: 若为 'fixed' 代表天数(如 20)；若为 'turnover' 代表目标换手率倍数(如 1.0 代表 100% 换手)
    - free_float_shares: 该股的自由流通股本（仅在 'turnover' 模式下需要）
    """
    stock_str = str(stock_code).zfill(6)
    src_file = CONFIG.base_path['LEVEL2_WORKSHAPCE_PATH'] / f"01_buffer/daily/metrics/volumeprofile/{stock_str}.parquet"             
    if not src_file.exists(): return []

    df_p = pd.read_parquet(src_file)
    all_trade_dates = np.unique(df_p['TradingDay']).tolist()
    grouped_profiles = {date: group for date, group in df_p.groupby('TradingDay')}
    
    # 定位当前目标日期在交易日历中的位置
    if rday not in all_trade_dates:
        return []
    target_idx = all_trade_dates.index(rday)
    
    loaded_profiles = []
    cumulative_volume = 0.0
    days_counter = 0
    
    # 从目标交易日开始，向前倒序扫描历史
    for i in range(target_idx, -1, -1):
        current_date = all_trade_dates[i]
        cday_df = grouped_profiles.get(current_date, pd.DataFrame())
        if cday_df.empty:
            continue

        loaded_profiles.append(cday_df)
        days_counter += 1
        
        # --- 策略 A：如果是固定天数窗口 ---
        if lookback_type == 'fixed' and days_counter >= window_value:
            break
            
        # --- 策略 B：如果是动态换手率窗口 ---
        if lookback_type == 'turnover' and free_float_shares:
            day_vol = cday_df['Volume'].sum()
            cumulative_volume += day_vol
            current_turnover_ratio = cumulative_volume / free_float_shares
            if current_turnover_ratio >= window_value:
                break
                
    # 保持时间正序返回（最早的天数在前面，当前目标日在最后）
    loaded_profiles.reverse()
    return loaded_profiles

def calculate_volumeprofile(list_of_daily_profiles: list, today_close_price: int, va_ratio: float = 0.70) -> pd.DataFrame:
    if not list_of_daily_profiles or len(list_of_daily_profiles) == 0:
        return pd.DataFrame()
        
    df_today = list_of_daily_profiles[-1]
    trade_date = df_today['TradingDay'].iloc[0] if 'TradingDay' in df_today.columns else df_today['TradeDate'].iloc[0]
    secu_code = df_today['SecuCode'].iloc[0]
    today_total_vol = df_today['Volume'].sum()
    
    # 1. 融合多日全局筹码矩阵并严格按价格排序 (Price已经是放大100倍的整数)
    df_rolling_profile = pd.concat(list_of_daily_profiles).groupby('Price', as_index=False)['Volume'].sum()
    df_rolling_profile = df_rolling_profile.sort_values(by='Price').reset_index(drop=True)
    
    total_vol_rolling = df_rolling_profile['Volume'].sum()
    if total_vol_rolling == 0:
        return pd.DataFrame()
        
    # 计算筹码形态熵
    df_rolling_profile['p_i'] = df_rolling_profile['Volume'] / (total_vol_rolling + 1e-8)
    profile_entropy = -np.sum(df_rolling_profile['p_i'] * np.log(df_rolling_profile['p_i'] + 1e-8))
    
    # 寻找全局强锚定价格 (Anchor POC)
    poc_idx = df_rolling_profile['Volume'].idxmax()
    anchor_poc = df_rolling_profile.loc[poc_idx, 'Price']
    
    # =====================================================================
    # 核心算法：从 POC 开始双向扩展，寻找精确的 VAH 和 VAL (70% 价值区)
    # =====================================================================
    target_va_vol = total_vol_rolling * va_ratio
    current_va_vol = df_rolling_profile.loc[poc_idx, 'Volume']
    
    l_idx = poc_idx - 1
    r_idx = poc_idx + 1
    n_bins = len(df_rolling_profile)
    
    # 双向指针步进搜索
    while current_va_vol < target_va_vol and (l_idx >= 0 or r_idx < n_bins):
        vol_l = df_rolling_profile.loc[l_idx, 'Volume'] if l_idx >= 0 else 0
        vol_r = df_rolling_profile.loc[r_idx, 'Volume'] if r_idx < n_bins else 0
        
        if vol_l >= vol_r and l_idx >= 0:
            current_va_vol += vol_l
            l_idx -= 1
        elif r_idx < n_bins:
            current_va_vol += vol_r
            r_idx += 1
        else:
            if l_idx >= 0:
                current_va_vol += vol_l
                l_idx -= 1
            else:
                break

    # 还原边界索引
    val = df_rolling_profile.loc[l_idx + 1, 'Price']
    vah = df_rolling_profile.loc[r_idx - 1, 'Price']
    
    # =====================================================================
    # 策略衍生特征工程
    # =====================================================================
    # 1. 价值区相对宽度 (无量纲化，反映筹码集中度)
    va_width_pct = (vah - val) / (anchor_poc + 1e-8)
    
    # 2. 当前收盘价相对价值区的位置 (Price Location Factor)
    # > 1 代表向上突破价值区； < 0 代表向下跌破价值区； 在0~1之间代表在价值区内部震荡
    price_position_in_va = (today_close_price - val) / (vah - val + 1e-8)
    
    # 3. 突破动能确认：当天最新价是否已凌驾于 VAH 之上
    is_breakout_high = 1 if today_close_price > vah else 0
    
    # 4. 原有的历史强锚定重叠度 (AVSI)
    buffer = anchor_poc * 0.01
    today_vol_in_anchor = df_today.loc[
        (df_today['Price'] >= anchor_poc - buffer) & 
        (df_today['Price'] <= anchor_poc + buffer), 
        'Volume'
    ].sum()
    avsi = today_vol_in_anchor / (today_total_vol + 1e-8)
    
    # 单日 POC 迁移
    daily_pocs = [d.loc[d['Volume'].idxmax(), 'Price'] for d in list_of_daily_profiles if not d.empty]
    pmv = float(np.mean(np.diff(daily_pocs))) if len(daily_pocs) >= 2 else 0.0

    return pd.DataFrame([{
        'TradeDate': trade_date,
        'SecuCode': secu_code,
        'profile_entropy_rolling': profile_entropy,
        'anchor_poc_rolling': anchor_poc,
        'val_rolling': val,
        'vah_rolling': vah,
        'va_width_pct': va_width_pct,
        'price_position_in_va': price_position_in_va,
        'is_breakout_high': is_breakout_high,
        'avsi_sticky_ratio': avsi,
        'poc_migration_vector': pmv,
        'actual_lookback_days': len(list_of_daily_profiles)
    }])

def calculate_sweepratio(df_deal=None):
    if df_deal is None or df_deal.empty: return
    stock_code = int(df_deal['SecuCode'].iat[0])
    stock_str = str(stock_code).zfill(6)
    rday = int(df_deal['TradingDay'].iat[0])

    if df_deal.empty:
        # 如果当天停牌或无成交，返回符合 Schema 结构的空行
        return pd.DataFrame(), pd.DataFrame([{f.name: (0 if not pa.types.is_string(f.type) else str(stock_code)) for f in CONFIG.LEVEL2_METRICS_SCHEMA}]).assign(TradeDate=rday)

    # --- 1. 过滤撤单，映射主动攻击方 ---
    trade_deals = df_deal[df_deal['Side'].isin([0, 1])].copy()
    if trade_deals.empty:
        return pd.DataFrame(), pd.DataFrame([{f.name: (0 if not pa.types.is_string(f.type) else str(stock_code)) for f in CONFIG.LEVEL2_METRICS_SCHEMA}]).assign(TradeDate=rday)
    
    # 预打脉冲标签
    t = trade_deals['DealTime']
    trade_deals['is_call_auction_phase'] = ((t >= 92500000) & (t < 93000000)) | ((t >= 145700000) & (t < 150000000))
    trade_deals['Turnover'] = trade_deals['Price'] * trade_deals['Volume']


    # 筛选扫盘事件
    buy_deals = trade_deals.groupby('BuyID').agg(
        dealtime_start=('DealTime', 'min'),
        dealtime_end=('DealTime', 'max'),
        price_min=('Price', 'min'),
        price_max=('Price', 'max'),
        price_first=('Price', 'first'),             #需要deal是排序后的，否则first会不准确
        price_last=('Price', 'last'),               #需要deal是排序后的，否则last会不准确
        sweep_volume=('Volume', 'sum'),             #吞单吃掉的订单总量
        sweep_turnover=('Turnover', 'sum'),         #吞单吃掉的订单总金额，与订单总量是相同指标，只是带上价格因素。
        sweep_depth=('Price', 'nunique'),           #吞单吃掉的价格档位数，越大越急迫         
        sweep_count=('DealID', 'count'),            #吞单吃掉的订单量，如果depth变化说明是市价单，depth不变可能是市价单可能是挂单
        is_CAP=('is_call_auction_phase', 'any')
    )
    # --- Step 2: 过滤出属于“吞噬/多次撮合”的订单 (count > 1) ---
    buy_sweeps = buy_deals[buy_deals['sweep_count'] > 1]

    sell_deals = trade_deals.groupby('SellID').agg(
        dealtime_start=('DealTime', 'min'),
        dealtime_end=('DealTime', 'max'),
        price_min=('Price', 'min'),
        price_max=('Price', 'max'),
        price_first=('Price', 'first'),             #需要deal是排序后的，否则first会不准确
        price_last=('Price', 'last'),               #需要deal是排序后的，否则last会不准确
        sweep_volume=('Volume', 'sum'),             #吞单吃掉的订单总量
        sweep_turnover=('Turnover', 'sum'),         #吞单吃掉的订单总金额，与订单总量是相同指标，只是带上价格因素。
        sweep_depth=('Price', 'nunique'),           #吞单吃掉的价格档位数，越大越急迫         
        sweep_count=('DealID', 'count'),            #吞单吃掉的订单量，如果depth变化说明是市价单，depth不变可能是市价单可能是挂单
        is_CAP=('is_call_auction_phase', 'any')
    )
    
    # --- Step 2: 过滤出属于“吞噬/多次撮合”的订单 (count > 1) ---
    sell_sweeps = sell_deals[sell_deals['sweep_count'] > 1]
        # =====================================================================
    # 4. 提取一阶标量指标 (充分考虑当天无多头或无空头扫盘的边界条件)
    # =====================================================================
    # 多头
    if not buy_sweeps.empty:
        daily_buy_sweep_vol = int(buy_sweeps['sweep_volume'].sum())
        daily_buy_sweep_turnover = int(buy_sweeps['sweep_turnover'].sum())
        buy_sweep_count = len(buy_sweeps)
        buy_avg_depth = float(buy_sweeps['sweep_depth'].mean())
        buy_max_depth = int(buy_sweeps['sweep_depth'].max())
        buy_multi_depth_count = int((buy_sweeps['sweep_depth'] > 1).sum())
        buy_multi_depth_vol = int(buy_sweeps.loc[buy_sweeps['sweep_depth'] > 1, 'sweep_volume'].sum())
    else:
        daily_buy_sweep_vol = 0
        daily_buy_sweep_turnover = 0
        buy_sweep_count = 0
        buy_avg_depth = 1.0
        buy_max_depth = 1
        buy_multi_depth_count = 0
        buy_multi_depth_vol = 0

    # 空头
    if not sell_sweeps.empty:
        daily_sell_sweep_vol = int(sell_sweeps['sweep_volume'].sum())
        daily_sell_sweep_turnover = int(sell_sweeps['sweep_turnover'].sum())
        sell_sweep_count = len(sell_sweeps)
        sell_avg_depth = float(sell_sweeps['sweep_depth'].mean())
        sell_max_depth = int(sell_sweeps['sweep_depth'].max())
        sell_multi_depth_count = int((sell_sweeps['sweep_depth'] > 1).sum())
        sell_multi_depth_vol = int(sell_sweeps.loc[sell_sweeps['sweep_depth'] > 1, 'sweep_volume'].sum())
    else:
        daily_sell_sweep_vol = 0
        daily_sell_sweep_turnover = 0
        sell_sweep_count = 0
        sell_avg_depth = 1.0
        sell_max_depth = 1
        sell_multi_depth_count = 0
        sell_multi_depth_vol = 0
    
    # --- 3. 标量化计算当日总指标 (无需 GroupBy) ---
    daily_total_volume = int(trade_deals['Volume'].sum())
    daily_total_turnover = int(trade_deals['Turnover'].sum())

    # =====================================================================
    # 5. 高阶特征工程：多空成交力量对比 & 急迫度分析
    # =====================================================================
    # 基础占比
    buy_sweep_ratio = daily_buy_sweep_vol / daily_total_volume
    sell_sweep_ratio = daily_sell_sweep_vol / daily_total_volume

    # 📊 多空成交力量对比 (Power Contrast)
    # 1. 净扫盘成交量 (Net Sweep Volume)
    net_sweep_volume = daily_buy_sweep_vol - daily_sell_sweep_vol
    # 2. 净扫盘量占大盘偏离度 (Sweep Power Bias) -> 消除个股流动性规模差异
    sweep_power_bias = net_sweep_volume / daily_total_volume
    # 3. 扫盘资金多空金额比 (Buy-to-Sell Sweep Ratio)
    buy_sell_sweep_ratio = daily_buy_sweep_turnover / (daily_sell_sweep_turnover + 1e-8)

    # ⚡ 多空成交急迫度分析 (Urgency Analysis)
    # 1. 动态深度偏离 (Sweep Depth Bias) -> 正值代表多头抢筹更不计成本，负值代表空头砸盘更决绝
    sweep_depth_bias = buy_avg_depth - sell_avg_depth
    # 2. 跨价位扫盘（市价单特征）次数占总扫盘的比例
    buy_multi_depth_ratio = buy_multi_depth_count / (buy_sweep_count + 1e-8)
    sell_multi_depth_ratio = sell_multi_depth_count / (sell_sweep_count + 1e-8)
    # 3. 多价位吞单成交量占自身扫盘总量的比率 (急迫纯度)
    buy_multi_depth_vol_ratio = buy_multi_depth_vol / (daily_buy_sweep_vol + 1e-8)
    sell_multi_depth_vol_ratio = sell_multi_depth_vol / (daily_sell_sweep_vol + 1e-8)
    
    # 4. 新增：多价位吞单成交量占当天全市场总成交量的比率 (绝对冲击力)
    buy_multi_depth_market_ratio = buy_multi_depth_vol / daily_total_volume
    sell_multi_depth_market_ratio = sell_multi_depth_vol / daily_total_volume

    # =====================================================================
    # 6. 组装单行结果 DataFrame (无缝对接后续的覆写或追加流程)
    # =====================================================================
    result_dict = {
        'TradeDate': rday,
        'SecuCode': stock_code,
        'daily_total_volume': daily_total_volume,
        'daily_total_turnover': daily_total_turnover,
        
        'daily_buy_sweep_vol': daily_buy_sweep_vol,
        'daily_buy_sweep_turnover': daily_buy_sweep_turnover,
        'buy_sweep_count': buy_sweep_count,
        'buy_max_sweepdepth': buy_max_depth,
        
        'daily_sell_sweep_vol': daily_sell_sweep_vol,
        'daily_sell_sweep_turnover': daily_sell_sweep_turnover,
        'sell_sweep_count': sell_sweep_count,
        'sell_max_sweepdepth': sell_max_depth,
        
        'buy_sweep_ratio': buy_sweep_ratio,
        'sell_sweep_ratio': sell_sweep_ratio,
        
        # 衍生力量指标
        'net_sweep_volume': net_sweep_volume,
        'sweep_power_bias': sweep_power_bias,
        'buy_sell_sweep_ratio': buy_sell_sweep_ratio,
        
        # 衍生急迫度指标
        'sweep_depth_bias': sweep_depth_bias,
        'buy_multi_depth_ratio': buy_multi_depth_ratio,
        'sell_multi_depth_ratio': sell_multi_depth_ratio,
        'buy_multi_depth_vol_ratio': buy_multi_depth_vol_ratio,  
        'sell_multi_depth_vol_ratio': sell_multi_depth_vol_ratio, 
        'buy_multi_depth_market_ratio': buy_multi_depth_market_ratio, 
        'sell_multi_depth_market_ratio': sell_multi_depth_market_ratio, 
        # 预留占位符 (配合您的脉冲时段指标，若不需要可删去)
        'buy_pulse_count': 0,
        'sell_pulse_count': 0
    }
    
    return pd.DataFrame([result_dict])

def save_daily_batch_to_monthly_file(daily_batch_df: pd.DataFrame, rday: int):
    """
    将当日全市场所有股票计算出的指标，安全地追加到月度大文件中，并按日期、股票排序。
    支持断点重跑（自动靠后生成的记录覆盖旧记录）。
    """
    if daily_batch_df.empty:
        return
    output_dir = CONFIG.base_path['LEVEL2_WORKSHAPCE_PATH'] / f"01_buffer/daily/metrics/sweepratio"   
    output_dir.mkdir(parents=True, exist_ok=True)
    yearmonth = str(rday)[:6]  # 提取年月，如 202606
    file_path = output_dir / f"sweepratio_metrics_{yearmonth}.parquet"

    # 2. 如果月度文件已存在，读取并合并（8G内存读写几MB的文件，耗时在毫秒级）
    if file_path.exists():
        existing_df = pq.read_table(file_path).to_pandas()
        # 如果今天的数据之前跑过一部分，重新更新时会用最新的覆盖旧的
        existing_df = existing_df[existing_df['TradingDay'] != rday]
        combined_df = pd.concat([existing_df, daily_batch_df], ignore_index=True)
    else:
        combined_df = daily_batch_df

    # 3. 按日期、股票代码全局严格排序
    combined_df = combined_df.sort_values(by=['TradeDate', 'SecuCode']).reset_index(drop=True)

    # 4. 根据严格指定的 Schema 写入 Parquet 文件
    table = pa.Table.from_pandas(combined_df, schema=CONFIG.LEVEL2_METRICS_SCHEMA)
    pq.write_table(table, file_path, compression='zstd')
    print(f"💾 成功更新月度大文件: {file_path.name} | 当前全月总记录数: {len(combined_df)} 行")

# ==============================================================================
# 状态读写辅助函数（支持断点续跑、增量运行）
# ==============================================================================
def load_rolling_state() -> dict:
    filepath = CONFIG.base_path['LEVEL2_WORKSHAPCE_PATH'] / "01_buffer/daily/metrics/cancel_order/rolling_state.pkl"

    """从本地磁盘载入历史滚动状态"""
    if filepath.exists():
        try:
            with open(filepath, 'rb') as f:
                state = pickle.load(f)
            print(f"📂 成功载入历史滚动状态，共包含 {len(state)} 只股票的历史数据。")
            return state
        except Exception as e:
            print(f"❌ 载入状态文件失败: {e}，将初始化全新状态。")
    else:
        print("ℹ️ 未检测到历史状态文件，将进行冷启动初始化。")
    return {}

def save_rolling_state(state_data: dict):
    filepath = CONFIG.base_path['LEVEL2_WORKSHAPCE_PATH'] / "01_buffer/daily/metrics/cancel_order/rolling_state.pkl"
    """将当前滚动状态保存到本地磁盘"""
    try:
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, 'wb') as f:
            pickle.dump(state_data, f)
        print(f"💾 滚动状态已成功保存至: {filepath}")
    except Exception as e:
        print(f"❌ 保存状态文件失败: {e}")

def calculate_OrderTier(trade_deals):
    if trade_deals is None or trade_deals.empty: return
    stock_code = int(trade_deals['SecuCode'].iat[0])
    stock_str = str(stock_code).zfill(6)
    rday = int(trade_deals['TradingDay'].iat[0])
    yearmonth = str(rday)[:6]  # 订单分层门限数据按月保存

    dst_dir = CONFIG.base_path['LEVEL2_WORKSHAPCE_PATH'] / f"01_buffer/daily/metrics/ordertierbaseline"
    dst_dir.mkdir(parents=True, exist_ok=True)
    dst_file = dst_dir / f"ordertier_baseline_{stock_str}_{yearmonth}.parquet"
      
    try:    
        buy_volume_df = trade_deals.groupby('BuyID').agg(Volume=('Volume', 'sum'))
        sell_volume_df = trade_deals.groupby('SellID').agg(Volume=('Volume', 'sum'))

        buy_volume = buy_volume_df.values.ravel()
        sell_volume = sell_volume_df.values.ravel()

        buy_volume_mean = float(np.mean(buy_volume))
        sell_volume_mean = float(np.mean(sell_volume))

        daily_ordertier_df = pd.DataFrame([[rday, buy_volume_mean, sell_volume_mean, buy_volume, sell_volume]], columns=['TradingDay', 'BidVolumeMean', 'AskVolumeMean', 'BidVolume', 'AskVolume'])

        
        if dst_file.exists():
            existing_df = pd.read_parquet(dst_file)   
            # 从历史数据中，把属于这些交易日的数据全部剔除（实现“新数据覆盖旧数据”）
            existing_df = existing_df[existing_df['TradingDay'] != rday]
            daily_ordertier_df = pd.concat([existing_df, daily_ordertier_df], ignore_index=True).reset_index(drop=True)
        
        daily_ordertier_df.to_parquet(dst_file, compression='zstd', index=False)   
    except Exception as e:
        print(f"❌ 生成缓存失败 {dst_file.name}: {str(e)}")



def derive_deal_metrics(date_list=[], checkmonth='', checkyear=''):    
    stage_deal_root = CONFIG.base_path['LEVEL2_BUFFER_PATH'] / f"monthlystaging/deal/{checkyear}/{checkyear}{checkmonth}"
    for rday in date_list:
        # 3.1 读取每日全市场大文件（全天只读一次）
        stockstr_list = [d.name for d in stage_deal_root.iterdir() if d.is_dir()]
        stockstr_list = ['000001']
        total_stocks = len(stockstr_list)

        for idx, stock_str in enumerate(stockstr_list):
            stock_code = int(stock_str)
            deal_file = stage_deal_root / f"{stock_str}/{stock_str}_{rday}.parquet"
            assert deal_file.exists(), f"Error: '{deal_file}'"
            df_deal = pd.read_parquet(deal_file, schema=CONFIG.DEAL_SCHEMA)
            trade_deals = df_deal[df_deal['Side'].isin([0, 1])]
            cancel_file = CONFIG.base_path['LEVEL2_BUFFER_PATH'] / f"monthlystaging/cancel/{checkyear}/{checkyear}{checkmonth}/{stock_str}/{stock_str}_{rday}.parquet"
            assert cancel_file.exists(), f"Error: '{cancel_file}'"
            df_cancel = pd.read_parquet(cancel_file, schema=CONFIG.CANCEL_SCHEMA)           
            df_cancel['CancelTime'] = df_cancel['DealTime']
            # 计算吞单指标
            # sweepratio_df = calculate_sweepratio(df_deal)
            # save_daily_batch_to_monthly_file(daily_batch_df=sweepratio_df, rday=rday)
            # 计算筹码峰指标
            # calculate_volumeprofile 真正需要的代码，但未改造参数，还需要与下面的代码进行比较，进一步的结合
            # derive_volumeprofile(df_deal)
            # 计算订单分层指标
            calculate_OrderTier(trade_deals) 
            frozen_thresholds = load_ordertier_baseline(baseline_date=rday, stock_str=stock_str)
            # 计算撤单指标
            engine = StratifiedFeatureEngine()
            # 进行特征压缩计算
            df_minute_features = engine.process_minute_features(trade_deals, df_cancel, frozen_thresholds)
            
            calculate_cancelratio(trade_deals, df_cancel, df_window=pd.DataFrame())



if __name__ == '__main__':
    tasks = []
    daily_results = []
    date_list = [
        # 20260701, 
        20260702, 20260703,         
        # 20260706, 20260707, 20260708, 20260709, 20260710, 
        # 20260713, 20260714, 
        # 20260715, 20260716, 20260717,
        # 20260720, 20260721, 20260722, 20260723, 20260724,
        # 20260727, 20260728, 20260729, 20260730, 20260731
    ]
    s_date = str(date_list[0])
    cur_year = s_date[:4]
    cur_month = s_date[4:6]

    derive_deal_metrics(date_list=date_list, checkmonth=cur_month, checkyear=cur_year)
   
        

    # date_list = [ 20260601 ] 
    # fpath = CONFIG.inferred_path['LEVEL2_BUFFER_PATH']/f"monthlystaging/order/{cur_year}/{cur_year}{cur_month}/tmp_split"
    # stockcode_list = [d.name for d in fpath.iterdir() if d.is_dir()]
    # stockcode_list = [564, 725]

    # total_stocks = len(stockcode_list)
    # for rday in date_list:
    #     for stock_code in stockcode_list:
    #         tasks.append((rday, stock_code, cur_year, cur_month))
    
    # physical_cores = 1#psutil.cpu_count(logical=False)

    # with Pool(physical_cores) as p:
    #     print(f"开始level2指标生成")        
    #     results = p.imap_unordered(calculate_sweepratio, tasks, chunksize=50)
    #     success_count = 0
    #     for ret_df in results:
    #         if not ret_df.empty:
    #             success_count += 1
    #             daily_results.append(ret_df)
    #             print(f"进程推进中{rday}... 已成功计算指标 {success_count}/{len(tasks)} 只股票", end="\r")

    # # 🏁 当天所有股票子进程跑完后，主进程一气呵成：合并 -> 去重 -> 排序 -> 追加落盘
    # if daily_results:
    #     daily_all_shares_df = pd.concat(daily_results, ignore_index=True)
    #     save_daily_batch_to_monthly_file(
    #         daily_batch_df=daily_all_shares_df, 
    #         rday=rday
    #     )