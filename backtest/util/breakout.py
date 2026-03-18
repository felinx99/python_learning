import numpy as np
import pandas as pd
import vectorbt as vbt
import itertools
import gc
from numba import njit, prange
YEAR_DAYS = 244.0

@njit(parallel=True, fastmath=True)
def fast_score_kernel(value_arr, init_cash_arr):
    n_time = value_arr.shape[0]
    n_groups = value_arr.shape[1]
    
    ann_rets = np.zeros(n_groups, dtype=np.float32)
    max_dds = np.zeros(n_groups, dtype=np.float32)
    dd_dur_days = np.zeros(n_groups, dtype=np.int32)
    sharpe_ratios = np.zeros(n_groups, dtype=np.float32)
    final_values = np.zeros(n_groups, dtype=np.float32)
    
    ann_factor = YEAR_DAYS
    years = n_time / ann_factor
    
    for j in range(n_groups):
        group_value = value_arr[:, j]
        init_cash = init_cash_arr[j] # 从数组中获取对应组的初始资金
        
        last_val = group_value[-1]
        final_values[j] = last_val
        
        # 年化收益率
        if init_cash > 0:
            ratio = last_val / init_cash
            if ratio > 0:
                ann_rets[j] = (ratio ** (1.0 / years)) - 1
            else:
                ann_rets[j] = -1.0
        
        # 最大回撤
        peak = 0
        mdd = 0.0
        max_dur = 0
        curr_dd_start_idx = 0
        for i in range(n_time):
            val = group_value[i]
            if val > peak: 
                peak = val
                curr_dd_start_idx = i
            else:
                if peak > 0:
                    dd = (peak - val) / peak
                    if dd > mdd: mdd = dd
                    curr_dur = i - curr_dd_start_idx
                    if curr_dur > max_dur:
                        max_dur = curr_dur
        max_dds[j] = mdd
        dd_dur_days[j] = max_dur
        
        # 夏普率
        sum_r = 0.0
        sum_r2 = 0.0
        count = n_time - 1
        if count > 1:
            for i in range(count):
                r = (group_value[i+1] / group_value[i]) - 1
                sum_r += r
                sum_r2 += r * r
            mean_r = sum_r / count
            var_r = (sum_r2 / count) - (mean_r ** 2)
            std_r = np.sqrt(max(0.0, var_r))
            if std_r > 1e-9:
                sharpe_ratios[j] = (mean_r / std_r) * np.sqrt(ann_factor)

    return ann_rets, max_dds, dd_dur_days, sharpe_ratios, final_values

@njit(parallel=True, fastmath=True)
def breakout_strategy(close, high, atr, volume_s, volume_l, pct_chg, ma_conv, s_ma, l_ma, cv_w, ct, vg, k_atr, n_3d):
    n_time, n_total_cols, n_symbols = n_3d
    entries = np.zeros((n_time, n_total_cols), dtype=np.bool_)
    exits = np.zeros((n_time, n_total_cols), dtype=np.bool_)
    

    for col in prange(n_total_cols):
        symbol_col = col % n_symbols
        params_col = col // n_symbols
        c_w = int(cv_w[params_col])
        c_t = ct[params_col]
        c_v_g = vg[params_col]
        c_k_atr = k_atr[params_col]
        in_position = False
        highest_price = 0.0
        trailing_stop = 0.0
        PnL = 0.0
        entry_price = 0.0

        # 内部循环会被 Numba 的 fastmath 和 SIMD 优化得极快
        for t in range(0, n_time):
            # 判定条件1：短平均交易量 > 长平均交易量的vg倍
            vol_ok = volume_s[t, symbol_col] > (volume_l[t, symbol_col] * c_v_g)
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
                    
                    entries[t, col] = (pct_min>1e-6) and (pct_max>0.02) and (pct_sum>0.05)
                    if entries[t, col]:
                        in_position = True
                        highest_price = high[t, symbol_col]
                        #entry_price = close[t, symbol_col]

            #动态止损(吊灯止损法，以ATR为核心指标)，则退出
            if in_position:
                # 更新持有期间的最高价
                highest_price = max(highest_price, high[t, symbol_col])
                # 动态计算止损位
                trailing_stop = highest_price - (c_k_atr * atr[t, symbol_col])
                #trailing_stop_2 = highest_price * 0.75
                #PnL = min(0, trailing_stop-entry_price)
                #if PnL > 0:
                #    trailing_stop = min(trailing_stop, trailing_stop_2)
                # 只有在收盘价跌破动态线，且长期趋势走弱时卖出
                if close[t, symbol_col] < trailing_stop:
                    exits[t, col] = True
                    in_position = False
                
                
    return entries, exits