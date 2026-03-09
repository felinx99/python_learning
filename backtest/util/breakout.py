import numpy as np
import pandas as pd
import vectorbt as vbt
import itertools
import gc
from numba import njit, prange

@njit(parallel=True, fastmath=True)
def breakout_strategy(close, volume3, volume10, pct_chg, ma_conv, s_ma, l_ma, cv_w, ct, vg, n_3d):
    n_time, n_total_cols, n_symbols = n_3d
    entries = np.zeros((n_time, n_total_cols), dtype=np.bool_)
    exits = np.zeros((n_time, n_total_cols), dtype=np.bool_)
    

    for col in prange(n_total_cols):
        symbol_col = col % n_symbols
        params_col = col // n_symbols
        c_w = int(cv_w[params_col])
        c_t = ct[params_col]
        c_v_g = vg[params_col]

        # 内部循环会被 Numba 的 fastmath 和 SIMD 优化得极快
        for t in range(0, n_time):
            # 判定条件1：3日平均交易量 > 10日平均交易量的vg倍
            vol_ok = volume3[t, symbol_col] > (volume10[t, symbol_col] * c_v_g)
            # 判定条件2：收盘价大于long_sma，且long_sma方向向上(今日long_sma大于昨日long_sma) 
            tmp_s_idx = max(0, t-1)
            ma_up = l_ma[t, col] > l_ma[tmp_s_idx, col]
            price_ok = close[t, symbol_col] > l_ma[t, col]
            
            # Numba 会将这三个判断融合成一条逻辑指令
            if vol_ok and ma_up and price_ok:  
                is_convergence = False              
                #判定条件3： --- 均线收敛 ---
                #近3日价格突破，取突破前均线收敛度[-(2+cv_w)~ -2]的均值，与门限比较
                tmp_e_idx = max(0, t - 2)
                tmp_s_idx = max(0, tmp_e_idx - c_w + 1)
                
                sum_conv = 0.0
                for i in range(tmp_s_idx, tmp_e_idx + 1):
                    # 每一天的收敛度计算 (基于当时那一天的过去 c_w 天)
                    sum_conv += ma_conv[i, col]
                
                avg_convergence = sum_conv / c_w
                if avg_convergence < c_t:
                    is_convergence = True   

                if is_convergence:
                    #判定条件4：近三日都上涨，至少1天涨幅超过4%，3天涨幅超过10%
                    day1 = max(0, t-1)
                    day2 = max(0, t-2)
                    pct_min = min(pct_chg[t, symbol_col], pct_chg[day1, symbol_col], pct_chg[day2, symbol_col])
                    pct_max = max(pct_chg[t, symbol_col], pct_chg[day1, symbol_col], pct_chg[day2, symbol_col])
                    pct_sum = pct_chg[t, symbol_col] + pct_chg[day1, symbol_col] + pct_chg[day2, symbol_col]
                    
                    entries[t, col] = (pct_min>1e-6) and (pct_max>0.04) and (pct_sum>0.1)

            #短期均线下穿长期均线，则退出
            tmp_d = max(0, t-1)
            exits[t, col] = s_ma[t, col] < l_ma[t, col] and (s_ma[tmp_d, col] >= l_ma[tmp_d, col])
                
    return entries, exits