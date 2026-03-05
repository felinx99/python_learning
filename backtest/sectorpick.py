import vectorbt as vbt
import pandas as pd
import numpy as np

# 1. 准备数据 (建议替换为你的 TDX 数据)
price = pd.Series(
    np.cumsum(np.random.randn(1000)) + 100, 
    index=pd.date_range("2022-01-01", periods=1000, freq="B"),
    name="Close"
)

# 2. 定义参数空间
rsi_windows = [10, 14, 20]
fast_windows = np.arange(10, 21) # range(10, 21) 即 10 到 20
slow_windows = [30, 40, 50, 60]

# 3. 计算所有参数组合下的指标 (vbt 会自动进行笛卡尔积广播)
# 使用 param_product=True 强制生成所有可能的组合
rsi = vbt.RSI.run(price, window=rsi_windows, param_product=True)
sma_fast = vbt.MA.run(price, window=fast_windows, param_product=True)
sma_slow = vbt.MA.run(price, window=slow_windows, param_product=True)

# 4. 生成信号矩阵
# 注意：vbt 会确保不同指标之间的列对齐
entries = (rsi.rsi < 30) & (sma_fast.ma > sma_slow.ma)
exits = (rsi.rsi > 70) | (price < sma_fast.ma)

# 5. 执行回测 (一次性跑完 132 组策略)
pf = vbt.Portfolio.from_signals(
    price, 
    entries, 
    exits, 
    fees=0.0003, 
    slippage=0.001,
    freq='B'
)

# 2. 提取关键指标
annual_return = pf.annualized_return()
max_dd = pf.max_drawdown().abs()
sharpe = pf.sharpe_ratio()
win_rate = pf.trades.win_rate()

# 3. 计算卡玛比率 (处理分母为 0 的情况)
calmar = annual_return / max_dd.replace(0, np.nan)
calmar = calmar.fillna(0)

# 4. 归一化处理 (Min-Max Scaling) 
# 这是为了让不同量级的指标（如 0.6 的胜率和 2.0 的夏普）能在同一维度比较
def normalize(s):
    return (s - s.min()) / (s.max() - s.min())

norm_calmar = normalize(calmar)
norm_sharpe = normalize(sharpe)
norm_win_rate = normalize(win_rate)

# 5. 构建你的“甜点区”综合得分
# 50% 卡玛 + 30% 夏普 + 20% 胜率
final_score = (0.5 * norm_calmar) + (0.3 * norm_sharpe) + (0.2 * norm_win_rate)

# 6. 排除掉你讨厌的低收益策略 (设置硬性门槛)
# 比如：年化收益必须大于 10%，最大回撤必须小于 25%
mask = (annual_return > 0.10) & (max_dd < 0.25)
filtered_score = final_score.where(mask, 0) # 不满足条件的得分为 0

# 7. 选出最优
best_params = filtered_score.idxmax()
print(f"最优参数组合: {best_params}")
print(f"年化收益: {annual_return[best_params]:.2%}")
print(f"最大回撤: {max_dd[best_params]:.2%}")
print(f"卡玛比率: {calmar[best_params]:.2f}")