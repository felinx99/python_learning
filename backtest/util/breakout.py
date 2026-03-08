import numpy as np
import pandas as pd
import vectorbt as vbt
import itertools
import gc
from numba import njit, prange

@njit(parallel=True, fastmath=True)
def breakout_strategy(ohlc, close, volume, s_ma, m_ma, l_ma, cv_w, ct, vg, n_symbols):
    n_time, n_total_cols = s_ma.shape
    entries = np.zeros((n_time, n_total_cols), dtype=np.bool_)
    exits = np.zeros((n_time, n_total_cols), dtype=np.bool_)

    # 1. 预计算成交量 Sum (针对 n_symbols)
    # 增加一个保护：如果 i < 5，则计算能拿到的所有均值
    vol_sum_all = np.zeros((n_time, n_symbols), dtype=np.float32)
    for s in prange(n_symbols):
        running_vol = 0.0
        for t in range(n_time):
            running_vol += volume[t, s]
            if t >= 5:
                running_vol -= volume[t-5, s]
            vol_sum_all[t, s] = running_vol

    for col in prange(n_total_cols):
        base_col = col % n_symbols
        c_w = int(cv_w[col])
        c_t = ct[col]
        c_v_g = vg[col]
        
        # --- 核心改进：起始索引最小化 ---
        # 只需要确保 i 能够往回数 c_w 天即可 (用于计算收敛窗口)
        # 即使 c_w=6，我们也只跳过前 6 天，而不是 40 天
        start_idx = c_w 

        for i in range(start_idx, n_time):
            # A. 平仓逻辑 (由于 MA 已预热，第 0 天起即可判断)
            if close[i, base_col] < l_ma[i, col]:
                exits[i, col] = True

            # B. 入场逻辑
            # 1. 趋势过滤 (MA 在切片第一天就是准的)
            if not (s_ma[i, col] > m_ma[i, col] > l_ma[i, col]):
                continue

            # 2. 爆量过滤 (需要 5 日数据支撑)
            # 如果 i < 5，则跳过或改用更短的均值，这里建议 i >= 5 开启
            if i < 5 or not (volume[i, base_col] > (vol_sum_all[i-1, base_col] / 5.0 * c_v_g)):
                continue

            # 3. 收敛压缩判断
            max_p = -1.0
            min_p = 1e10
            for k in range(i - c_w, i):
                p = ohlc[k, base_col]
                if p > max_p: max_p = p
                if p < min_p: min_p = p
            
            if ohlc[i, base_col] > max_p:
                if (max_p - min_p) / min_p <= c_t:
                    entries[i, col] = True
                
    return entries, exits