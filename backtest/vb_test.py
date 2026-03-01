import vectorbt as vbt
import pandas as pd
import numpy as np
from pathlib import Path

STOCKLIST_PATH = 'E:\\output\\Astock\\stockpicking\\stocklist.csv'
DATA_PATH = 'E:\\datas\\tdx\\day_2018_2025'

stock_csvtype = {
    'open': 'float32',
    'high': 'float32',
    'low': 'float32',
    'close': 'float32',
    'volume': 'float32',
}

def load_all_stocks(stocklist):
    close_dict = {}

    for ticker, founddate in stocklist:
        tickerpath = Path(DATA_PATH)/f'{ticker}.csv'
        df = pd.read_csv(tickerpath, dtype=stock_csvtype, parse_dates=['date'])
        close_dict[ticker] = df['close']
        return pd.DataFrame(close_dict).sort_index().ffill().dropna()

# 4. 定义评分函数 (复用你之前的多维度逻辑)
def get_score(pf):
    # 基础指标
    ann_ret = pf.annualized_return()
    max_dd = pf.max_drawdown().abs()
    sharpe = pf.sharpe_ratio()
    win_rate = pf.trades.win_rate()
    dd_dur = pf.max_drawdown_duration().dt.days
    
    # 归一化函数
    def norm(s):
        return (s - s.min()) / (s.max() - s.min() + 1e-6)

    # 计算 Calmar
    calmar = ann_ret / max_dd.replace(0, np.nan)
    calmar = calmar.fillna(0)
    
    # 综合得分 (40%卡玛 + 20%时间 + 20%夏普 + 20%胜率)
    score = (0.4 * norm(calmar)) + \
            (0.2 * (1 - norm(dd_dur))) + \
            (0.2 * norm(sharpe)) + \
            (0.2 * norm(win_rate))
    
    # 门槛过滤 (不符合条件的设为负分)
    mask = (ann_ret > 0.08) & (max_dd < 0.25)
    return score.where(mask, -1)

if __name__ == '__main__':
    # 1. 准备数据 (建议替换为你的 TDX 数据)
    TICKERS_DF = pd.read_csv(STOCKLIST_PATH, usecols=[0,5], skiprows=1, header=None) #read_csv返回的DF数据格式
    stocklist = list(TICKERS_DF.to_records(index=False))
    price_df = load_all_stocks(stocklist)

    # 2. 定义时间滚动窗口 (2年维度，1年步进)# 2018-2020, 2019-2021, 2020-2022...
    splitter = vbt.Splitter.from_rolling(
        price_df.index, 
        window_len=252 * 2, # 假设每年252个交易日
        every=252           # 每年滑动一次
    )

    # 3. 参数空间
    rsi_windows = [10, 14, 20]
    fast_windows = np.arange(10, 21)
    slow_windows = [30, 40, 50, 60]

    # 5. 循环窗口执行寻优
    window_results = []

    for i, (train_indices, test_indices) in enumerate(splitter.split()):
        window_price = price_df.iloc[train_indices]
        start_date = window_price.index[0].strftime('%Y')
        end_date = window_price.index[-1].strftime('%Y')
        print(f"正在处理窗口 {i+1}: {start_date} - {end_date}")
        
        # 向量化计算指标
        rsi = vbt.RSI.run(window_price, window=rsi_windows, param_product=True)
        sma_f = vbt.MA.run(window_price, window=fast_windows, param_product=True)
        sma_s = vbt.MA.run(window_price, window=slow_windows, param_product=True)
        
        # 信号生成
        entries = (rsi.rsi < 30) & (sma_f.ma > sma_s.ma)
        exits = (rsi.rsi > 70) | (window_price < sma_f.ma)
        
        # 回测
        pf = vbt.Portfolio.from_signals(window_price, entries, exits, fees=0.0003, freq='B')
        
        # 获取该窗口下所有股票、所有参数的得分
        # 注意：此时 score 的 Index 是 (rsi_w, fast_w, slow_w, stock_code)
        score = get_score(pf)
        
        # 将股票维度的得分平均化，只保留参数维度
        param_score = score.groupby(level=['rsi_window', 'ma_window', 'ma_window_1']).mean()
        window_results.append(param_score)

    # 6. 最终聚合：选出在所有时间窗口表现最稳的参数
    final_agg_score = pd.concat(window_results, axis=1).mean(axis=1)
    best_params = final_agg_score.idxmax()

    print("\n" + "="*40)
    print(f"🚀 全周期/全市场 最优鲁棒参数: {best_params}")
    print(f"🌟 综合稳定性评分: {final_agg_score.max():.4f}")
    print("="*40)