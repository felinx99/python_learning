import numpy as np
import pandas as pd
import vectorbt as vbt
import itertools
import gc

# 定义支持全参数寻优的工厂类
BreakoutMonitor = vbt.IndicatorFactory(
    class_name="BreakoutMonitor",
    short_name="bkout",
    input_names=["ohlc", "close", "volume", "s_ma", "m_ma", "l_ma"], 
    param_names=["cv_w", "ct", "vg"], # cv_w:收敛窗口, ct:收敛阈值, vg:量能倍数
    output_names=["entry", "exit"]
).from_apply_func(
    lambda ohlc, close, volume, s_ma, m_ma, l_ma, cv_w, ct, vg: 
    apply_breakout_logic_full(ohlc, close, volume, s_ma, m_ma, l_ma, cv_w, ct, vg),
    cv_w=5, ct=0.03, vg=1.5
)

def apply_breakout_logic_full(ohlc, close, volume, s_ma, m_ma, l_ma, cv_w, ct, vg):
    # 1. 均线收敛度 CV
    ma_stack = np.stack([s_ma, m_ma, l_ma], axis=0)
    cv = np.std(ma_stack, axis=0) / (np.mean(ma_stack, axis=0) + 1e-6)
    
    # 使用动态窗口 cv_w 计算滚动均线
    # 注意：在 vbt.from_apply_func 中，如果 cv_w 是参数序列，需确保 nb 函数处理正确
    # 这里我们使用高性能的 rolling_mean_nb
    avg_conv = vbt.generic.nb.rolling_mean_nb(cv, int(cv_w)) 
    # 条件 1：3天前的收敛度符合阈值 (使用 np.roll 模拟 shift(3))
    is_converged = np.roll(avg_conv < ct, 3, axis=0) 

    # 2. 价格突破逻辑 (基于 close/ohlc 价格，二选一)
    oc = ohlc
    # 3天整体涨幅判定
    total_3d_gain = (oc / (np.roll(oc, 3, axis=0) + 1e-6)) - 1
    
    # 3日连续走势判定
    pct_chg = (oc - np.roll(oc, 1, axis=0)) / (np.roll(oc, 1, axis=0) + 1e-6)
    all_pos = vbt.generic.nb.rolling_min_nb(pct_chg, 3) > 0
    has_big_win = vbt.generic.nb.rolling_max_nb(pct_chg, 3) > 0.04
       
    
    # 3. 量能点火
    v_ma10 = vbt.generic.nb.rolling_mean_nb(volume, 10)
    v_avg3 = vbt.generic.nb.rolling_mean_nb(volume, 3)
    volume_ignited = v_avg3 > (v_ma10 * vg)

    # 4. 趋势确认 (价格在慢线之上)
    buy_signal = is_converged & (total_3d_gain > 0.10) & all_pos & \
                 has_big_win & volume_ignited & (close > l_ma)
    
    # 填充开头的 NaN 滚动产生的 False Positive
    buy_signal[:20, :] = False 
    sell_signal = close < l_ma
    
    return buy_signal, sell_signal