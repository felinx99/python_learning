import ast
import pandas as pd
import numpy as np
import itertools
import re
import seaborn as sns
import talib as ta
import matplotlib.pyplot as plt
import gc
import vectorbt as vbt
from pathlib import Path
from dateutil.relativedelta import relativedelta
from . import sectorpick
from .util.breakout import breakout_strategy
from api.timeprofile import TimeProfile

STOCKLIST_PATH = 'E:\\output\\Astock\\stockpicking\\stocklist.csv'
RESULT_PATH = 'E:\\output\\Astock\\stockpicking\\analysis\\'
DATA_PATH = 'E:\\datas\\tdx\\day_2018_2025'
STARTDATE = '2018-01-02'
ENDDATE = '2025-12-26'

stock_csvtype = {
    'open': 'float32',
    'high': 'float32',
    'low': 'float32',
    'close': 'float32',
    'volume': 'float64',
}

def trendup_logic():
    sectorslist_df = sectorpick.get_up_sector(sectorlist=['concept', 'l3'], ret='daily')
    stocklist_df = sectorpick.get_daily_list_in_sector(sectorslist_df)
    return stocklist_df

def check_broadcast_compatibility(dfs_dict):
    """
    检查所有输入矩阵是否具备 100% 的广播兼容性
    """
    first_key = list(dfs_dict.keys())[0]
    ref = dfs_dict[first_key]
    
    print(f"--- 🛠️ 深度兼容性自查 (基准: {first_key}) ---")
    results = []
    for name, df in dfs_dict.items():
        # 基本一致性检查
        idx_match = ref.index.equals(df.index)
        col_match = ref.columns.equals(df.columns)
        idx_name_match = ref.index.name == df.index.name
        col_name_match = ref.columns.name == df.columns.name
        dtype_match = ref.values.dtype == df.values.dtype
        
        # 内存对象检查 (vectorbt 的隐藏要求)
        idx_is_same = ref.index is df.index
        col_is_same = ref.columns is df.columns
        
        status = "✅" if (idx_match and col_match and idx_name_match and col_name_match) else "❌"
        
        print(f"{status} [{name:8}] Shape: {df.shape} | index name:{df.index.name} | colname:{df.columns.name} | Dtype: {df.values.dtype}")
        if status == "❌":
            print(f"   ∟ Index一致: {idx_match} | Col一致: {col_match}")
            print(f"   ∟ Index名: '{df.index.name}' | Col名: '{df.columns.name}'")
        
        results.append(status == "✅")
    return all(results)


def custom_strategy_logic(close, high, low, k_window=14, d_window=3, atr_window=14, atr_threshold=0.02):
    """
    策略逻辑：
    - 入场：%K线 在超卖区(20)向上穿过 %D线，且当前波动率(ATR/Price) > 阈值（避开横盘僵尸股）
    - 出场：%K线 进入超买区(80) 或 价格跌破快线均线
    """
    # 计算 STOCH (%K, %D)
    stoch = vbt.STOCH.run(high, low, close, k_window=k_window, d_window=d_window)
    fast_k = stoch.k.values.astype(np.float32)
    slow_d = stoch.d.values.astype(np.float32)
    
    # 计算 ATR 波动率过滤（相对于价格的比例）
    atr = vbt.ATR.run(high, low, close, window=atr_window).atr.values.astype(np.float32)
    volatility_filter = (atr / close.values) > atr_threshold
    
    # 定义信号矩阵
    # 入场：K线在20以下上穿D线，且有足够波动率
    entries = (fast_k < 20) & (fast_k > slow_d) & volatility_filter
    
    # 出场：K线超过80，或者简单的均线离场
    exits = (fast_k > 80)
    
    return entries, exits

def prepare_stocks(src_df=None, values=[]):
    #对数据进行预处理，src_df可以是股票，也可以是板块，可以是单只也可以是股票组合
    ret_list = {}
    for col in values:
        col_df = f'{col}_df'
        col_df = src_df.pivot(index='date', columns='symbol', values=col)

        col_df.index = pd.to_datetime(col_df.index)
        col_df = col_df.sort_index().ffill().astype(np.float32).dropna(how='all')  #剔除全为空的行dropna(how='all')
        ret_list[col] = col_df

    return ret_list

def load_all_stocks(stocklist):
    close_dict = {}

    target_tickers = [str(ticker) for ticker, founddate in stocklist]
    srcpath = Path(DATA_PATH)/'all_stock_daily.parquet'

    try:
        full_df = pd.read_parquet(srcpath, engine='pyarrow')
    except Exception as e:
        print(f'日线数据读取失败：{e}')

    full_df = full_df[full_df['symbol'].isin(target_tickers)]
    return full_df

def get_stock_inlist(df=None, stocklist=None):
    target_tickers = [str(ticker) for ticker, founddate in stocklist]
    partstocks = df[df['symbol'].isin(target_tickers)]
    return partstocks

def get_batch_trade_details(pf, symbol_names, combo_params, window_id, start_str, end_str, date_index):
    """
    针对当前 vbt 版本的字段名提取交易细节
    """
    # 1. 获取 readable 记录
    trades_df = pf.trades.records_readable.copy()
    if trades_df.empty:
        return pd.DataFrame()

    num_symbols = len(symbol_names)
    
    # 2. 映射 Symbol 和 参数 (针对 'Column' 字段)
    def map_info(col_idx):
        p_idx = int(col_idx // num_symbols)
        s_idx = int(col_idx % num_symbols)
        s_name = symbol_names[s_idx] if s_idx < len(symbol_names) else "Unknown"
        p_val = combo_params[p_idx] if p_idx < len(combo_params) else "Unknown"
        return s_name, p_val

    # 这里的 'Column' 对应你列表里的第二个字段
    mapped_data = trades_df['Column'].apply(map_info)
    trades_df['Symbol'] = [x[0] for x in mapped_data]
    trades_df['Params'] = [str(x[1]) for x in mapped_data]
    
    # 3. 时间映射：将索引 (131, 141) 转换为真实日期
    # 注意：使用 pf.wrapper.index 进行定位
    trades_df['Entry Date'] = date_index[trades_df['Entry Timestamp']]
    trades_df['Exit Date'] = date_index[trades_df['Exit Timestamp']]
    
    # 4. 添加窗口信息
    trades_df['Window_ID'] = window_id
    trades_df['Window_Range'] = f"{start_str}_{end_str}"

    # 5. 统一列名映射，对标你的字段名
    final_cols = {
        'Avg Entry Price': 'Entry Price',
        'Avg Exit Price': 'Exit Price',
        'PnL': 'PnL',
        'Return': 'Return',
        'Size': 'Size'
    }
    trades_df = trades_df.rename(columns=final_cols)

    # 6. 返回核心列
    return trades_df[[
        'Symbol', 'Params', 
        'Entry Date', 'Exit Date', 'Entry Price', 'Exit Price', 
        'Size', 'PnL', 'Return'
    ]]

def get_score_single(pf):
    """
    专为单参数测试设计：处理标量数据，逻辑简单透明
    """
    # 1. 提取标量指标 (Numpy float64)
    ann_ret_raw = pf.annualized_return()
    # 标量处理：使用原生 min/max 或 np.clip
    ann_ret = np.clip(ann_ret_raw if np.isfinite(ann_ret_raw) else -0.99, -0.99, 2.0)

    # 2. 最大回撤
    max_dd_raw = abs(pf.max_drawdown())
    max_dd = max_dd_raw if max_dd_raw > 0 else 0.01

    # 3. 夏普率 (处理可能的 inf 或 nan)
    sharpe_raw = pf.sharpe_ratio()
    if not np.isfinite(sharpe_raw):
        sharpe = 0.0
    else:
        sharpe = np.clip(sharpe_raw, -5.0, 5.0)

    # 4. 胜率
    win_rate = pf.trades.win_rate()
    if np.isnan(win_rate): win_rate = 0.0

    # 5. 回撤持续时间 (Timedelta 处理)
    max_dur = pf.drawdowns.max_duration()
    # 判断是否为 Timedelta 类型
    if hasattr(max_dur, 'days'):
        dd_dur_days = float(max_dur.days)
    else:
        dd_dur_days = float(max_dur)
    
    dd_score = np.clip(1.0 - (dd_dur_days / 252.0), 0.0, 1.0)

    # 6. 计算最终得分
    calmar = ann_ret / max_dd
    final_value = pf.final_value()
    
    score = (0.4 * calmar) + (0.2 * dd_score) + (0.2 * sharpe) + (0.2 * win_rate)

    # 7. 返回一个简单的字典或单行 DataFrame 方便查看
    return {
        'score': float(score),
        'ann_ret': float(ann_ret),
        'max_dd': float(max_dd),
        'sharpe': float(sharpe),
        'win_rate': float(win_rate),
        'dd_dur': dd_dur_days,
        'final_value': float(final_value)
    }

# 4. 定义评分函数 (复用你之前的多维度逻辑)
def get_score(pf):
    # 1. 基础指标提取
    ann_ret = pf.annualized_return()
    ann_ret = ann_ret.replace([np.inf, -np.inf], 2.0).fillna(-0.99).clip(-0.99, 2.0)

    max_dd = pf.max_drawdown().abs().replace(0, 0.01).fillna(1.0)
    
    # 2. 核心修正：夏普率防爆处理
    sharpe = pf.sharpe_ratio().replace([np.inf, -np.inf], np.nan).fillna(0).clip(-5.0, 5.0)
    
    # 3. 其他指标

    win_rate = pf.trades.win_rate().fillna(0)

    
    duration_series = pf.drawdowns.max_duration()
    dd_dur_days = duration_series.dt.days.fillna(0).astype(np.float32)
    dd_score = (1 - (dd_dur_days / 252.0)).clip(0, 1)

    calmar = ann_ret / max_dd
    # 获取期末总资产（100万变成了多少）
    final_value = pf.final_value()
    
    # 获取平均持仓比例（诊断资金是否一直在睡觉）
    # 4. 计算得分
    score = (0.4 * calmar) + (0.2 * dd_score) + (0.2 * sharpe) + (0.2 * win_rate)


    return pd.DataFrame({
        'score': score.astype(np.float32),
        'ann_ret': ann_ret.astype(np.float32),
        'max_dd': max_dd.astype(np.float32),
        'sharpe': sharpe.astype(np.float32),
        'win_rate': win_rate.astype(np.float32),
        'dd_dur': dd_dur_days.astype(np.float32),
        'final_value': final_value.astype(np.float32)
    })
    
def generate_time_chunks(start_date, end_date, split_window, roll_step):
    current_start = start_date
    chunks = []
    
    while current_start < end_date:
        # 记录切片（确保不超出总数据的边界）
        actual_end = min(current_start + split_window, end_date)
        chunks.append((current_start, actual_end))
        
        # 按照滚动步长移动
        current_start += roll_step
        
        # 如果步进后已经没有意义，提前退出
        if current_start + relativedelta(days=1) >= end_date:
            break
            
    return chunks

def chunked_iterable(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]

#参数热力图
def plot_parameter_heatmap(df, index_param='short', column_param='long', score_col='score'):
    # 聚合所有维度，只保留我们关心的两个参数轴
    plot_df = df.reset_index().groupby([index_param, column_param])[score_col].mean().unstack()
    
    plt.figure(figsize=(10, 8))
    sns.heatmap(plot_df, annot=True, fmt=".3f", cmap='RdYlGn', center=0)
    plt.title(f'Parameter Heatmap: {index_param} vs {column_param}')
    plt.xlabel(column_param)
    plt.ylabel(index_param)
    plt.show()

#参数敏感性分析
def plot_sensitivity(df, param_name):
    plt.figure(figsize=(8, 6))
    # 将 MultiIndex 重置，方便绘图
    temp_df = df.reset_index()
    sns.boxplot(x=param_name, y='score', data=temp_df)
    plt.title(f'Sensitivity Analysis: {param_name}')
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.show()

#参数稳定性评分函数
def get_robustness_report(all_window_data, param_names):
    """
    计算每个参数组合在所有时间切片上的稳定性表现
    """
    # 1. 按参数分组，计算得分的均值和标准差
    grouped = all_window_data.groupby(level=param_names)['score']
    
    mean_score = grouped.mean()
    std_score = grouped.std().fillna(1.0) # 防止只有一两个切片时 std 为 0
    
    # 2. 计算“参数收益夏普”：均值 / 标准差
    # 这代表了该参数每承担一单位的波动，能换取多少稳定的得分
    robustness_sharpe = mean_score / (std_score + 1e-6)
    
    # 3. 计算“胜率切片占比”：在多少比例的切片中得分大于 0
    win_slices_ratio = grouped.apply(lambda x: (x > 0).mean())
    
    # 4. 汇总
    robust_df = pd.DataFrame({
        'avg_score': mean_score,
        'score_std': std_score,
        'stability_sharpe': robustness_sharpe,
        'profitable_slice_pct': win_slices_ratio
    })
    
    # 5. 综合评价指标：稳定性夏普 * 胜率占比 (惩罚那些偶尔大赚但经常小亏的参数)
    robust_df['final_robust_rank'] = robust_df['stability_sharpe'] * robust_df['profitable_slice_pct']
    
    return robust_df.sort_values(by='final_robust_rank', ascending=False)

def to_f32_np(df):
    return np.ascontiguousarray(df.values, dtype=np.float32)

def compute_cv_adaptive(ma_list):
    """
    ma_list: 一个包含多个 Numpy 数组的列表 [ma1, ma2, ma3, ...]
    每个数组形状均为 (n_time, n_symbols)
    """
    # 1. 沿新轴堆叠：形状变为 (n_samples, n_time, n_symbols)
    # 例如 3 条均线，n_samples 就是 3
    stacked_ma = np.stack(ma_list, axis=0)
    
    # 2. 沿 axis=0 计算均值
    ma_mean = np.mean(stacked_ma, axis=0)
    
    # 3. 沿 axis=0 计算标准差，显式指定 ddof=1 (即分母使用 n-1)
    # 这是自适应样本数的关键，NumPy 会根据 stacked_ma 的第一维自动处理 n
    ma_std = np.std(stacked_ma, axis=0, ddof=1)
    
    # 4. 计算变异系数
    cv = (ma_std / (ma_mean + 1e-8)) * 100
    return cv.astype(np.float32)

if __name__ == '__main__':
    # 1. 准备数据 (建议替换为你的 TDX 数据)
    TICKERS_DF = pd.read_csv(STOCKLIST_PATH, usecols=[0,5], skiprows=1, header=None) #read_csv返回的DF数据格式
    stocklist = list(TICKERS_DF.to_records(index=False))
    
    allstocks_data_df = load_all_stocks(stocklist)
    print("1.加载数据完成")
    volume_short_window = 3
    volume_long_window = 7
    volume_short_column = f"volume{volume_short_window}"
    volume_long_column = f"volume{volume_long_window}"
    allstocks_data_df[volume_short_column] = allstocks_data_df['volume'].rolling(window=volume_short_window).mean()
    allstocks_data_df[volume_long_column] = allstocks_data_df['volume'].rolling(window=volume_long_window).mean().shift(volume_short_window)
    allstocks_data_df['pct_chg'] = allstocks_data_df['ohlc'].pct_change()
    allstocks_data_df['atr'] = ta.ATR(allstocks_data_df.high, allstocks_data_df.low, allstocks_data_df.close, timeperiod=14)
    need_cols = ['close', 'high', 'atr', 'ohlc', volume_short_column, volume_long_column, 'pct_chg']
    stocks_part_dict = prepare_stocks(src_df=allstocks_data_df, values=need_cols)
    print("2.生成average volume, pct_chg")
    
    # 2. 数据切片，按指定的split,roll参数进行切片和滚动
    split_config = relativedelta(months=24)
    roll_config = relativedelta(months=6)

    # 1. 转换日期格式
    start_dt = pd.to_datetime(STARTDATE, format='%Y-%m-%d')
    end_dt = pd.to_datetime(ENDDATE, format='%Y-%m-%d')

    time_chunks = generate_time_chunks(start_dt, end_dt, split_config, roll_config)
    
    # 3. 参数空间:测试版本和完整版本
    #converged_window_space = [6]
    #converged_threshold_space = [3]
    #vol_gain_space = [2.2]
    #k_atr_space = [3.0]
    #short_space = [6]
    #mid_space = [14]
    #long_space = [20,25]

    converged_window_space = [4,5,6]
    converged_threshold_space = [1.8, 2.1, 2.4, 2.7]
    vol_gain_space = [1.3, 1.5, 1.8, 2.1]
    k_atr_space = [2.5, 3.0, 3.5, 4.0]
    short_space = (3,4,5)
    mid_space = (8, 10, 12, 14)
    long_space = (20,25,30,35)



    #预处理sma部分
    all_sma_windows = sorted(short_space + mid_space + long_space)
    ma_cache = {}
    for w in all_sma_windows:
        sma_period = f"sma_{w}"
        ma_cache[w] = vbt.MA.run(stocks_part_dict['ohlc'], window=w).ma.values.astype(np.float32)
		
    sma_combos = list(itertools.product(short_space, mid_space, long_space))
    ma_convg_cache = {}
    for combo in sma_combos:
        ma_arrays = [ma_cache[w] for w in combo]
        # 存入字典，Key 是组合元组
        ma_convg_cache[(combo[0], combo[1], combo[2])] = compute_cv_adaptive(ma_arrays)

    
    all_combos = list(itertools.product(converged_window_space,converged_threshold_space,vol_gain_space, k_atr_space, short_space, mid_space, long_space))
    print(f"3. 均值计算完成,参数组合{len(all_combos)}")
    # 5. 循环窗口执行寻优
    window_results = []
    all_trade_details = []
    param_names = ['cv_w', 'ct', 'vg', 'k_atr', 'short', 'mid', 'long']
    global_dates = stocks_part_dict['close'].index
    idx = 0
    for i, (s_dt, e_dt) in enumerate(time_chunks):
        with TimeProfile():
            s_str, e_str = s_dt.strftime('%Y-%m-%d'), e_dt.strftime('%Y-%m-%d')
            #if s_str != '2024-01-02':
            #    continue
            
            # 局部时间切片 (宽表)
            w_close = stocks_part_dict['close'].loc[s_str:e_str]
            w_high = stocks_part_dict['high'].loc[s_str:e_str]
            w_atr = stocks_part_dict['atr'].loc[s_str:e_str]
            #w_ohlc = stocks_part_dict['ohlc'].loc[s_str:e_str]
            w_vol3 = stocks_part_dict[volume_short_column].loc[s_str:e_str]
            w_vol10 = stocks_part_dict[volume_long_column].loc[s_str:e_str]
            w_pct = stocks_part_dict['pct_chg'].loc[s_str:e_str]

            if len(w_close) < 100:
                continue
            
            print(f"🔄 正在处理切片 {i+1}: {s_dt.strftime('%Y%m%d')} -> {e_dt.strftime('%Y%m%d')} ({len(w_close)} 行)")
            window_scores = []
            #batch_size保持在10~15之间，可以在内存和运行速度之间平衡
            batch_size = len(short_space)*len(converged_window_space)*len(long_space)
            #assert batch_size > 1, print("batch_size should greater than 1")
            for combo_batch in chunked_iterable(all_combos, batch_size):
                cw_w = [c[0] for c in combo_batch]
                ct_w = [c[1] for c in combo_batch]
                vg_w = [c[2] for c in combo_batch]
                k_atr_w = [c[3] for c in combo_batch]
                idx += 1
                window_slice = global_dates.slice_indexer(s_str, e_str)
                # 假设 s_str, e_str 对应的位置是 start_idx, end_idx
                start_idx = window_slice.start
                end_idx = window_slice.stop  # 必须 +1 包含结束日
                combo_batch_size = len(combo_batch)
                num_symbols = w_close.shape[1]
                n_time = end_idx - start_idx

                # 预分配全量平铺矩阵
                s_ma_tile = np.empty((n_time, combo_batch_size * num_symbols), dtype=np.float32)
                #m_ma_tile = np.empty((n_time, combo_batch_size * num_symbols), dtype=np.float32)
                l_ma_tile = np.empty((n_time, combo_batch_size * num_symbols), dtype=np.float32)
                ma_conv_tile = np.empty((n_time, combo_batch_size * num_symbols), dtype=np.float32)
                
                for i, (cw, ct, vg, k_atr, s_w, m_w, l_w) in enumerate(combo_batch):
                    col_start = i * num_symbols
                    col_end = (i + 1) * num_symbols
                    conv_comb_column=f"conv_{s_w}_{m_w}_{l_w}"
                    
                    # 直接进行 Numpy 切片拷贝，速度极快
                    # 这里的索引是 [时间切片, 股票切片]
                    # 如果你的 combo_batch 对应的股票顺序和 ma_np_cache 一致：
                    s_ma_tile[:, col_start:col_end] = ma_cache[s_w][start_idx:end_idx, :]
                    #m_ma_tile[:, col_start:col_end] = ma_cache[m_w][start_idx:end_idx, :]
                    l_ma_tile[:, col_start:col_end] = ma_cache[l_w][start_idx:end_idx, :]
                    ma_conv_tile[:, col_start:col_end] = ma_convg_cache[(s_w, m_w, l_w)][start_idx:end_idx, :]



                # np.tile 将矩阵按行 1 倍、按列 n_params 倍进行复制
                #ohlc_tile = to_f32_np(w_ohlc)
                close_tile = to_f32_np(w_close)
                high_tile = to_f32_np(w_high)
                atr_tile = to_f32_np(w_atr)
                vol3_tile = to_f32_np(w_vol3)
                vol10_tile = to_f32_np(w_vol10)
                pct_tile = to_f32_np(w_pct)
                # 运行自定义指标
                entries_np, exits_np = breakout_strategy(
                    close_tile, high_tile, atr_tile, vol3_tile,vol10_tile,pct_tile,ma_conv_tile,
                    s_ma_tile, l_ma_tile,
                    cw_w, ct_w, vg_w, k_atr_w,
                    n_3d = (w_close.shape[0],combo_batch_size*num_symbols,num_symbols)
                )
               
                # 信号诊断 (仅对每片首个 Batch 采样)
                if len(window_scores) == 0:
                    first_combo_entries = entries_np[:, :num_symbols]
                    daily_hits = first_combo_entries.sum(axis=1) # 提取第一组参数 (id=0) 的所有股票信号并求和
                    print(f" [信号诊断] 首组参数日均信号: {daily_hits.mean():.2f} | 峰值: {daily_hits.max()}")

                group_by_ids = np.repeat(np.arange(len(combo_batch)), num_symbols)

                # 运行组合回测
                pf = vbt.Portfolio.from_signals(
                    close = np.tile(close_tile, (1, len(combo_batch))),
                    entries=entries_np,
                    exits=exits_np, 
                    size=0.01, # 单次开仓2%
                    size_type='percent',
                    cash_sharing=True,
                    #call_seq='random',
                    fees=0.002,
                    slippage=0.001,
                    freq='D',
                    init_cash=10000000,
                    sl_stop=0.15,          # 15% 固定止损
                    group_by=group_by_ids, # 按参数组合分组计算
                )
                
                batch_trades = get_batch_trade_details(pf, w_close.columns.tolist(), combo_batch, i+1, s_str, e_str, w_close.index)
                if (s_str == '2024-01-02') and (not batch_trades.empty):
                    f_path = Path(RESULT_PATH)/'vb'/f"vb_test_out_{idx}.csv"
                    batch_trades.to_csv(f_path, index=False, encoding='utf-8-sig', float_format='%.2f')
                    del batch_trades
        
                # 收集多维度指标 
                batch_score = get_score(pf)
                #batch_score = get_score_single(pf)
                batch_score.index = pd.MultiIndex.from_tuples(
                    [combo_batch[i] for i in batch_score.index], 
                    names=param_names
                )
                window_scores.append(batch_score)
                
                
                #显示清理内存
                del pf, entries_np,exits_np, ma_conv_tile, pct_tile
                del close_tile, high_tile, atr_tile, vol3_tile, vol10_tile, s_ma_tile, l_ma_tile
                gc.collect()

            if (end_dt - e_dt).days < 30: break
            
            # 汇总当前窗口所有批次，并存入总列表
            if window_scores:
                window_df = pd.concat(window_scores)
                print(f"   ✅ 切片处理完成。当前切片最高分: {window_df['score'].max():.4f}, 平均夏普: {window_df['sharpe'].mean():.4f}")
                window_results.append(window_df)
            
            


    # 6. 最终聚合：选出在所有时间窗口表现最稳的参数
    if window_results:
        # 将所有时间窗的数据纵向堆叠
        all_window_data = pd.concat(window_results)
        robust_report = get_robustness_report(all_window_data, param_names)

        print("\n" + "🛡️" * 10 + " 参数稳定性排行榜 (跨切片表现) " + "🛡️" * 10)
        print(robust_report.head(5))
        # 1. 计算所有窗口的平均值
        final_df = all_window_data.groupby(level=param_names).mean()
        final_df['calmar_avg'] = final_df['ann_ret'] / final_df['max_dd'].replace(0, 0.01)
        # 2. 计算得分的标准差 (稳定性指标)
        score_std = all_window_data.groupby(level=param_names)['score'].std().fillna(1.0)
        final_df['stability_score'] = final_df['score'] / (1 + score_std)
        
        # 热力图
        #plot_parameter_heatmap(robust_report, index_param='short', column_param='long', score_col='stability_sharpe')
        #plot_parameter_heatmap(final_df, 'cv_w', 'ct')

        # 敏感性分析
        #plot_sensitivity(all_window_data, 'ct')

        # 按综合得分排序
        top_3 = final_df.sort_values(by='stability_score', ascending=False).head(3)
        
        print("\n" + "🏆" * 10 + " 寻优结果前三名 (全时段平均) " + "🏆" * 10)
        
        for i, (params, row) in enumerate(top_3.iterrows()):
            cv, ct, vg, k_atr, s, m, l = params
            print(f"【Top {i+1}】 参数: CV_{cv} | CT_{ct} | VG_{vg} | MA({s},{m},{l})")
            print(f" 综合得分: {row['score']:.4f} (稳定性参考: {row['stability_score']:.4f})")
            print(f" 收益表现: 年化收益={row['ann_ret']*100:.2f}% | 卡玛比率={row['calmar_avg']:.2f} | 夏普={row['sharpe']:.2f}")
            print(f" 交易质量: 胜率={row['win_rate']*100:.2f}%")
            print(f" 风险控制: 最大回撤={row['max_dd']*100:.2f}% | 平均最大回撤持续={int(row['dd_dur'])}天")
            #print(f" 资金占用: 期末资产={row['final_value']:.2f} | 持仓占比={row['exposure']:.2f}")
            print(f" 资金占用: 期末资产={row['final_value']:.2f}")
            print("-" * 60)

        best_p = top_3.index[0]
        print(f"\n🌟 推荐最优组合: CV={best_p[0]}, CT={best_p[1]}, VG={best_p[2]}, MA_Set=({best_p[3]},{best_p[4]},{best_p[5]})")

   # if all_trade_details:
    #    final_trade_report = pd.concat(all_trade_details, ignore_index=True)
   #     final_trade_report.to_csv(RESULT_PATH, index=False, encoding='utf-8-sig', float_format='%.2f')
   #     print(f"🚀 全时段交易报告已生成！总计 {len(final_trade_report)} 笔交易。")
   