import numpy as np
import pandas as pd
from common import CONFIG, DATAFRAME
from dataclasses import dataclass
from enum import IntEnum

# ==============================================================================
# 1. 基础配置与枚举
# ==============================================================================
class TimeFrame(IntEnum):
    Ticks = 0
    MicroSeconds = 1
    Seconds = 2
    Minutes = 3
    Days = 4
    Weeks = 5
    Months = 6
    Years = 7
    NoTimeFrame = 8

@dataclass
class Resample:
    timeframe: TimeFrame = TimeFrame.Seconds
    compression: int = 1

FREQ_MAP = {
    TimeFrame.Seconds: "s",
    TimeFrame.Minutes: "min", 
    TimeFrame.Days: "D",
    TimeFrame.Weeks: "W",
    TimeFrame.Months: "ME",
    TimeFrame.Years: "YE"
}

DATE_COL_MAP = {
    'min1_bar': 'date',
    'sweepratio': 'TradingDay',
    'vpdistribution': 'TradingDay',
    'volumeprofile': 'TradingDay',
    'ordertierbaseline': 'TradingDay',
    'min1_ordertier': 'TradingDay',
    'min1_dealtier': 'TradingDay',
    'min1_canceltier': 'TradingDay',
    'ordertier': 'TradingDay',
    'cancelorder': 'TradingDay',
}

SORT_COLS_MAP = {
    'min1_bar': ['date'],
    'sweepratio': ['TradingDay'],
    'vpdistribution': ['TradingDay', 'Price'],
    'volumeprofile': ['TradingDay'],
    'ordertierbaseline': ['TradingDay'],
    'min1_ordertier': ['TradingDay'],
    'min1_dealtier': ['TradingDay'],
    'min1_canceltier': ['TradingDay'],
    'ordertier': ['TradingDay'],
    'cancelorder': ['TradingDay'],
}


# ==============================================================================
# 2. L2 高频指标综合计算引擎 (内聚所有指标与微观分层逻辑)
# ==============================================================================
class Level2MetricCalculator:
    """L2 业务指标与微观特征计算统一命名空间"""

    @staticmethod
    def _parse_deal_time(date_str, time_series):
        base_dt = pd.to_datetime(str(date_str), format='%Y%m%d')
        t = time_series.astype(np.int64)
        hh = t // 10000000
        mm = (t // 100000) % 100
        ss = (t // 1000) % 100
        ms = t % 1000
        total_seconds = hh * 3600 + mm * 60 + ss + ms / 1000.0
        return base_dt + pd.to_timedelta(total_seconds, unit='s')

    @staticmethod
    def _label_order_volume(vol: np.ndarray, thres: dict) -> np.ndarray:
        """基于指定阈值划分订单层级,使用 NumPy 向量化"""
        bins = np.array([thres[70], thres[95], thres[99.5]], dtype=np.float64)
        indices = np.digitize(vol, bins)
        mapping = np.array(['Small', 'Medium', 'Large', 'Mega'])
        return mapping[indices]

    @staticmethod
    def calculate_bars(df_tradedeal: pd.DataFrame, rday: int, resamples=[]) -> pd.DataFrame:
        """
        功能 1：从逐笔成交 Tick 合成多周期 Bar 并保存为 Parquet
        :param deal_df: 包含 Price, Volume, Side, DealTime 的原始 DataFrame
        :param stock_str: 股票代码, 如 '000001'
        :param rday: 日期,整数 如 20260529
        """

        deal_df = df_tradedeal.copy()
        deal_df.index = pd.DatetimeIndex(Level2MetricCalculator._parse_deal_time(str(rday), deal_df['DealTime']), name='date')
        
        # 3. 循环 Resample 合成各周期 K 线
        for r in resamples:
            freq = f"{r.compression}{FREQ_MAP[r.timeframe]}"
            resampler = deal_df.resample(freq) if r.timeframe == TimeFrame.Days else deal_df.resample(freq, closed='right', label='right')
            
            bar_df = resampler.agg({
                'Price': ['first', 'max', 'min', 'last'],
                'Volume': 'sum',
                'Turnover': 'sum'
            })
            bar_df.columns = ['open', 'high', 'low', 'close', 'volume', 'turnover']
            bar_df = bar_df[bar_df['volume'] > 0].reset_index()
            return bar_df.astype({
                'open': 'int32', 'high': 'int32', 'low': 'int32', 'close': 'int32',
                'volume': 'int64', 'turnover': 'int64', 'date': 'datetime64[s]'
            })
        return pd.DataFrame()

    @staticmethod
    def calculate_sweepratio(df_tradedeal: pd.DataFrame, rday: int) -> pd.DataFrame:
        """2. 计算吞单及多空急迫度指标[cite: 2]"""
        trade_deals = df_tradedeal.copy()
        # 预打脉冲标签
        t = trade_deals['DealTime']
        trade_deals['is_call_auction_phase'] = ((t >= 92500000) & (t < 93000000)) | ((t >= 145700000) & (t < 150000000))

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
        return pd.DataFrame([{
            'TradingDay': rday,
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
        }])

    @staticmethod
    def calculate_vpdistribution(df_tradedeal: pd.DataFrame, rday: int, stock_str: str) -> pd.DataFrame:
        """3. #计算成交量价格分布，统计每个价格点上有多少成交量"""

        # 极轻量聚合：生成单日 Profile
        df_profile = df_tradedeal.groupby('Price', as_index=False)['Volume'].sum()
        # 将股票代码和日期作为元数据写入，或者直接保存在每行中方便后续合并
        df_profile['SecuCode'] = int(stock_str)
        df_profile['TradingDay'] = rday
        # 强制转换紧凑类型，进一步省内存和磁盘空间
        df_profile['Price'] = df_profile['Price'].astype('int32')
        df_profile['Volume'] = df_profile['Volume'].astype('int64')
        return df_profile

    @staticmethod
    def calculate_volumeprofile(list_of_daily_profiles=[], today_close_price=0, va_ratio=0.70) -> pd.DataFrame:
        """计算筹码峰滚动特征指标"""
        if not list_of_daily_profiles: return pd.DataFrame()

        df_today = list_of_daily_profiles[-1]
        trade_date = df_today['TradingDay'].iloc[0]
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
                current_va_vol += vol_l; l_idx -= 1
            elif r_idx < n_bins:
                current_va_vol += vol_r; r_idx += 1
            else:
                if l_idx >= 0: current_va_vol += vol_l; l_idx -= 1
                else: break

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
            'TradingDay': trade_date,
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

    @staticmethod
    def calculate_ordertier(df_tradedeal: pd.DataFrame, rday: int) -> pd.DataFrame:
        """5. 计算订单分层基础均值与序列[cite: 2]"""
        buy_volume = df_tradedeal.groupby('BuyID')['Volume'].sum().values.ravel()
        sell_volume = df_tradedeal.groupby('SellID')['Volume'].sum().values.ravel()

        buy_volume_mean = float(np.mean(buy_volume)) if len(buy_volume) > 0 else 0.0
        sell_volume_mean = float(np.mean(sell_volume)) if len(sell_volume) > 0 else 0.0

        return pd.DataFrame([[
            rday, 
            buy_volume_mean, 
            sell_volume_mean, 
            buy_volume, 
            sell_volume
        ]], columns=['TradingDay', 'BidVolumeMean', 'AskVolumeMean', 'BidVolume', 'AskVolume'])

    @staticmethod
    def calculate_min1_deal_tier(df_tradedeal: pd.DataFrame, stock_thresholds: dict) -> pd.DataFrame:
        """
        核心步骤：根据买卖方向自动切换标准化标尺 -> 分钟级压缩 -> 双流合流
        """            
        # 提取时间分钟项
        rday = int(df_tradedeal['TradingDay'].iat[0])
        df_tradedeals = df_tradedeal.copy()
        df_tradedeals['Minute'] = df_tradedeals['DealTime'] // 100000
                
        # --- A. 成交流分层还原 ---
        buy_agg = df_tradedeals.groupby('BuyID').agg({'Volume': 'sum', 'Side': 'min'})
        sell_agg = df_tradedeals.groupby('SellID').agg({'Volume': 'sum', 'Side': 'max'})
        
        # 仅在 unique 订单级别计算 Layer 映射关系
        buy_layers = Level2MetricCalculator._label_order_volume(buy_agg['Volume'].values, stock_thresholds['bid'])
        sell_layers = Level2MetricCalculator._label_order_volume(sell_agg['Volume'].values, stock_thresholds['ask'])
        
        buy_layer_map = pd.Series(buy_layers, index=buy_agg.index)
        sell_layer_map = pd.Series(sell_layers, index=sell_agg.index)

        df_tradedeals['Buy_Layer'] = df_tradedeals['BuyID'].map(buy_layer_map)
        df_tradedeals['Sell_Layer'] = df_tradedeals['SellID'].map(sell_layer_map)

        buy_is_active = pd.Series(buy_agg['Side'].values == 0, index=buy_agg.index)
        sell_is_active = pd.Series(sell_agg['Side'].values == 1, index=sell_agg.index)
        
        df_tradedeals['Buy_Is_Active'] = df_tradedeals['BuyID'].map(buy_is_active)
        df_tradedeals['Sell_Is_Active'] = df_tradedeals['SellID'].map(sell_is_active)
        
        # --- B. 拆分买卖流量流向 (将每笔交易分裂为买/卖两端) ---
        # 买方流量
        df_buy_flow = pd.DataFrame({
            'Minute': df_tradedeals['Minute'],
            'Category': np.where(df_tradedeals['Buy_Is_Active'], 'act_buy', 'pas_buy'),
            'Layer': df_tradedeals['Buy_Layer'],
            'Volume': df_tradedeals['Volume']
        })
        
        # 卖方流量
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
        deal_pivot['TradingDay'] = rday
        deal_pivot.reset_index(inplace=True)
        return deal_pivot

    @staticmethod
    def calculate_min1_cancel_tier(df_cancel: pd.DataFrame, stock_thresholds: dict) -> pd.DataFrame:
        """6B. 计算 1 分钟撤单分层与撤单流还原"""
        assert df_cancel is not None and not df_cancel.empty, "df_cancel为空, 无法继续处理"
            
        rday = int(df_cancel['TradingDay'].iat[0])
        df_cancel = df_cancel.copy()
        df_cancel['Minute'] = df_cancel['CancelTime'] // 100000
        df_cancel['Layer'] = 'Small'

        volumes = df_cancel['Volume'].values
        sides = df_cancel['Side'].values
        # 向量化分配买卖盘阈值
        layers = np.empty(len(df_cancel), dtype=object)
        is_buy = sides == -1
        is_sell = ~is_buy
        
        if is_buy.any():
            layers[is_buy] = Level2MetricCalculator._label_order_volume(volumes[is_buy], stock_thresholds['bid'])
        if is_sell.any():
            layers[is_sell] = Level2MetricCalculator._label_order_volume(volumes[is_sell], stock_thresholds['ask'])
        
        df_cancel['Layer'] = layers
        cancel_pivot = df_cancel.pivot_table(
            index='Minute', 
            columns=['Side', 'Layer'], 
            values='Volume', 
            aggfunc='sum'
        ).fillna(0).astype(int)

        cancel_pivot.columns = [f"cancel{'buy' if s==-1 else 'sell'}_vol_{l}" for s, l in cancel_pivot.columns]
        cancel_pivot['TradingDay'] = rday
        cancel_pivot.reset_index(inplace=True)
        return cancel_pivot

    @staticmethod
    def calculate_cancelratio(df_cancel: pd.DataFrame, rday: int) -> pd.DataFrame:
        """
        【波段特征引擎】计算 20 天时序大单撤单非对称度与欺诈性建模特征
        """
        assert df_cancel is not None and not df_cancel.empty, "df_cancel为空, 无法继续处理"
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
        
        
        df_large_cancel = df_cancel[df_cancel['Layer'] == 'Large'].copy() if 'Layer' in df_cancel.columns else df_cancel.copy()
        if df_large_cancel.empty:
            return pd.DataFrame()


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

        return pd.DataFrame([{
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

# ==============================================================================
# 3. 数据存取与环境辅助函数 (I/O & Baselines)
# ==============================================================================
def load_ordertier_baseline(baseline_date: int, stock_str='') -> dict:
    """
    🎯 核心逻辑：提取指定测试期baseline_date的那一个月数据, 固化生成每只股票的绝对股数分层阈值
    """
    default_thres = {70: 1000, 95: 8000, 99.5: 50000}
    yearmonth = str(baseline_date)[:6]
    ordertier_file = CONFIG.l2_path['L2_MATRICS'] / f"ordertierbaseline/{yearmonth}/ordertierbaseline_{stock_str}.parquet"
    if not ordertier_file.exists():
        print(f"WARNING: {stock_str} 无法读取订单分层文件 {ordertier_file}")
        return {'bid': default_thres, 'ask': default_thres, 'total': default_thres}

    ordertier_df = pd.read_parquet(ordertier_file)
    if ordertier_df.empty:
        return {'bid': default_thres, 'ask': default_thres, 'total': default_thres}
        
    # 初始化相对倍数池与均值序列
    bid_norm_pool, ask_norm_pool, total_norm_pool = [], [], []
    bid_means, ask_means, total_means = [], [], []
    # 遍历基准月内的每一天（Parquet中一行代表一天）
    for _, row in ordertier_df.iterrows():
        # 从 Parquet 中读取回来的 List/Array 统一强转为 float 数组
        b_vols = np.array(row['BidVolume'], dtype=int)
        a_vols = np.array(row['AskVolume'], dtype=int)
        b_mean, a_mean = float(row['BidVolumeMean']), float(row['AskVolumeMean'])
        
        # 1. 买端 (Bid) 动态标准化池
        if len(b_vols) > 0 and b_mean > 0:
            bid_norm_pool.extend(b_vols / b_mean); bid_means.append(b_mean)
        # 2. 卖端 (Ask) 动态标准化池
        if len(a_vols) > 0 and a_mean > 0:
            ask_norm_pool.extend(a_vols / a_mean); ask_means.append(a_mean)
        # 3. 全局合成 (Total) 动态标准化池（与原逻辑完全一致）
        t_vols = np.concatenate([b_vols, a_vols])
        if len(t_vols) > 0:
            total_norm_pool.extend(t_vols / np.mean(t_vols))
            total_means.append(float(np.mean(t_vols)))
    
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
    return {
        'bid': calc_percentiles(bid_norm_pool, bid_means), 
        'ask': calc_percentiles(ask_norm_pool, ask_means), 
        'total': calc_percentiles(total_norm_pool, total_means)
    }

def load_cancelratio(stock_code=0, rday=0, window=20):
    """
    【高性能时序加载器】提取目标日前 N 天的挂撤单时间序列
    """
    stock_str = str(stock_code).zfill(6)
    src_file = CONFIG.l2_path['L2_MATRICS']/f"cancelratio/{stock_str}.parquet"
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

def preload_vpdistribution_cache(stock_str=''):
    """
    ⚡ 核心优化：单股票整月只读取一次历史 vpdistribution 文件，并构建内存查找缓存
    """
    src_file = CONFIG.l2_path['L2_MATRICS']/f"vpdistribution/{stock_str}.parquet"             
    if not src_file.exists(): return None, []

    df_p = pd.read_parquet(src_file)
    if df_p.empty: return None, []
    
    all_trade_dates = np.unique(df_p['TradingDay']).tolist()
    grouped_profiles = {date: group for date, group in df_p.groupby('TradingDay')}
    return grouped_profiles, all_trade_dates

def load_vpdistribution(stock_str='', rday=0, profiles=None, trade_dates=[], lookback_type='fixed', lookback_param=20, free_shares=0.0):
    """
    【高性能动态加载器】
    从缓存目录中，根据动态或固定规则，倒序加载某只股票所需的历史单日 Profile DataFrame 列表。
    
    参数：
    - trade_dates: 已排序的全局全量交易日列表 (如 ['20260601', '20260602', ...])
    - lookback_type: 'fixed' (固定天数) 或 'turnover' (动态换手率)
    - lookback_param: 若为 'fixed' 代表天数(如 20)；若为 'turnover' 代表目标换手率倍数(如 1.0 代表 100% 换手)
    - free_float_shares: 该股的自由流通股本（仅在 'turnover' 模式下需要）
    """
    if not profiles or rday not in trade_dates: return []
    
    # 定位当前目标日期在交易日历中的位置
    target_idx = trade_dates.index(rday) 
    loaded_profiles, cumulative_volume, days_counter = [], 0.0, 0

    # 从目标交易日开始，向前倒序扫描历史
    for i in range(target_idx, -1, -1):
        cday_df = profiles.get(trade_dates[i], pd.DataFrame())
        if cday_df.empty: continue
        loaded_profiles.append(cday_df)
        days_counter += 1
        # --- 策略 A：如果是固定天数窗口 ---
        if lookback_type == 'fixed' and days_counter >= lookback_param: break

        # --- 策略 B：如果是动态换手率窗口 ---
        if lookback_type == 'turnover' and free_shares:
            cumulative_volume += cday_df['Volume'].sum()
            if (cumulative_volume / free_shares) >= lookback_param: break

    # 保持时间正序返回（最早的天数在前面，当前目标日在最后）            
    loaded_profiles.reverse()
    return loaded_profiles

def _get_metric_save_dir(metric_type: str, year: str, yearmonth: str):
    """
    内部辅助函数：根据类型及时间动态生成保存路径
    #ohlc 1分钟数据，单股票按年存储
    #sweepratio 日数据，单股票全时间段存储
    #vp_distribution 日数据，单股票全时间段存储
    #volumeprofile 日数据，单股票全时间段存储
    #ordertier_baseline 日数据，单股票按月存储，基准以月为单位                   
    #ordertier 1分钟数据，单股票按年存储               
    #ordertier 日数据，单股票全时间段存储  
    """
 
    if metric_type == 'min1_bar':
        return CONFIG.base_path['CN_DATA_PATH'] / f"stock/1m/{year}"
    elif metric_type == 'ordertierbaseline':
        return CONFIG.l2_path['L2_MATRICS'] / f"ordertierbaseline/{yearmonth}"
    elif metric_type in ('min1_dealtier', 'min1_canceltier'):
        return CONFIG.l2_path['L2_MATRICS'] / metric_type /f"{year}"
    elif metric_type in ('sweepratio', 'vpdistribution', 'volumeprofile', 'ordertier', 'cancelratio', 'cancelorder'):
        return CONFIG.l2_path['L2_MATRICS'] / metric_type
    else:
        raise ValueError(f"❌ 未知的指标类型: {metric_type}")


# ==============================================================================
# 🟢 核心落盘保存函数
# ==============================================================================
def save_deal_metrics(save_df: pd.DataFrame, stock_str: str, type: str):
    """
    通用量化指标落盘保存函数（自动处理历史数据增量覆盖与 Schema 校验）
    :param save_df: 需要保存的 DataFrame 数据
    :param stock_str: 股票代码，如 '000001'
    :param type: 指标类型，如 'min1_bar', 'sweepratio' 等
    """
    # 1. 获取当前指标对应的正确日期列名 ('TradingDay')
    date_col = DATE_COL_MAP[type]

    sample_date = str(save_df[date_col].iloc[0]).replace('-', '').replace('/', '')
    year = sample_date[:4]
    yearmonth = sample_date[:6]

    save_dir = _get_metric_save_dir(type, year, yearmonth)
    save_dir.mkdir(parents=True, exist_ok=True)
    save_file = save_dir / f"{type}_{stock_str}.parquet"

    if save_file.exists():
        existing_df = pd.read_parquet(save_file)
        new_dates = save_df[date_col].unique()
        existing_df = existing_df[~existing_df[date_col].isin(new_dates)]
        save_df = pd.concat([existing_df, save_df], ignore_index=True)

    save_df.sort_values(by=SORT_COLS_MAP[type]).reset_index(drop=True).to_parquet(save_file, engine='pyarrow', compression='zstd', index=False)




def predict_exchange_code(stock_str):       
    # 60或68开头 -> 上海 (SH)
    if stock_str.startswith(('60', '68')):
        return "SH"
    # 00或30开头 -> 深圳 (SZ)
    elif stock_str.startswith(('00', '30')):
        return "SZ"
    # 83、87、43开头 -> 北京北交所/新三板 (BJ)
    elif stock_str.startswith(('83', '87', '43')):
        return "BJ"
    
    return "UNKNOWN"

def verify_bars_against_csv(date_list):    
    strdate_list = [str(d) for d in date_list]
    formatted_dates = [f"{d[:4]}-{d[4:6]}-{d[6:]}" for d in strdate_list]
    
    src_root = CONFIG.base_path['CN_DATA_PATH']/ "stock/1day"
    stockstr_list = [p.stem for p in src_root.glob('*.csv')]
    

    dst_root = CONFIG.tdx_data_path[DATAFRAME['DAY']]

    for stock_str in stockstr_list:
        exchange = predict_exchange_code(stock_str)
        src_file = src_root/f"{stock_str}.csv"
        dst_file = dst_root/f"{stock_str}.{exchange}.csv"

        if src_file.exists() and not dst_file.exists():
            print(f"❌ [文件缺失] 股票: {stock_str} | 目标文件 {dst_file.name} 不存在")
            pass

        if not src_file.exists() or not dst_file.exists():
            continue

        src_df = pd.read_csv(src_file)
        dst_df = pd.read_csv(dst_file)
        
        src_df = src_df[src_df['date'].isin(formatted_dates)]
        dst_df = dst_df[dst_df['date'].isin(formatted_dates)]

        src_dates = set(src_df['date'])
        dst_dates = set(dst_df['date'])
        
        for d in (src_dates - dst_dates):
            print(f"⚠️ [日期缺失] 股票: {stock_str} | 日期: {d} | 原因: dst_file 中缺失该交易日")
        for d in (dst_dates - src_dates):
            print(f"⚠️ [日期缺失] 股票: {stock_str} | 日期: {d} | 原因: src_file 中缺失该交易日")

        # 🟢 5. 基于 date 取交集对齐，准备进行价格校验
        merged_df = pd.merge(src_df, dst_df, on='date', suffixes=('_src', '_dst'))
        if merged_df.empty:
            continue

        # 🟢 6. 逐列对比 open, high, low, close
        compare_cols = ['open', 'high', 'low', 'close']
        for col in compare_cols:
            src_val = merged_df[f'{col}_src']
            dst_val_int = np.round(merged_df[f'{col}_dst'] * 100).astype(np.int32)

            # 纯整数的不等判定 (!=)，CPU 执行效率极高
            mismatch_mask = src_val != dst_val_int

            # 如果存在不一致的行，精确打印定位
            if mismatch_mask.any():
                mismatch_rows = merged_df[mismatch_mask]
                for _, row in mismatch_rows.iterrows():
                    err_date = row['date']            
                    print(f"🚨 [数据不一致] 股票: {stock_str} | 日期: {err_date}")
 
  
  