import vectorbt as vbt
import pandas as pd
import numpy as np
import itertools
from pathlib import Path
from dateutil.relativedelta import relativedelta
import gc

STOCKLIST_PATH = 'E:\\output\\Astock\\stockpicking\\stocklist.csv'
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

def load_all_stocks(stocklist):
    close_dict = {}

    target_tickers = [str(ticker) for ticker, founddate in stocklist]
    srcpath = Path(DATA_PATH)/'all_stock_daily.parquet'

    try:
        full_df = pd.read_parquet(srcpath, engine='pyarrow')
    except Exception as e:
        print(f'宽表读取失败：{e}')

    full_df = full_df[full_df['symbol'].isin(target_tickers)]
    return full_df

def prepare_all_stocks(src_df):
    close_df = src_df.pivot(index='date', columns='symbol', values='close')

    close_df.index = pd.to_datetime(close_df.index)
    close_df = close_df.sort_index().ffill().astype(np.float32).dropna(how='all')  #剔除全为空的行dropna(how='all')
    return close_df

# 4. 定义评分函数 (复用你之前的多维度逻辑)
def get_score(pf):
    # 1. 基础指标提取
    ann_ret = pf.annualized_return().fillna(-0.99)
    # 彻底清理年化收益中的 inf
    ann_ret = ann_ret.replace([np.inf, -np.inf], 2.0).clip(lower=-0.99, upper=2.0)

    max_dd = pf.max_drawdown().abs().replace(0, 0.01).fillna(1.0)
    
    # 2. 核心修正：夏普率防爆处理
    sharpe = pf.sharpe_ratio()
    # 先处理 NaN，再处理 inf，最后限制在一个合理区间（如 -5 到 5）
    sharpe = sharpe.replace([np.inf, -np.inf], np.nan).fillna(0)
    sharpe = sharpe.clip(lower=-5.0, upper=5.0)
    
    # 3. 其他指标
    counts = pf.trades.count()
    win_rate = pf.trades.win_rate().fillna(0)

    
    duration_series = pf.drawdowns.max_duration()
    dd_dur_days = duration_series.dt.days.fillna(0).astype(np.float32)
    dd_score = (1 - (dd_dur_days / 252.0)).clip(lower=0, upper=1)

    calmar = ann_ret / max_dd
    # 获取期末总资产（100万变成了多少）
    final_value = pf.value().iloc[-1]
    
    # 获取平均持仓比例（诊断资金是否一直在睡觉）
    exposure = pf.asset_value().sum(axis=1).mean() / pf.value().mean()
    # 4. 计算得分
    score = (0.4 * calmar) + (0.2 * dd_score) + (0.2 * sharpe) + (0.2 * win_rate)
    
    # 5. 诊断：如果依然出现 inf，打印出来
    if np.isinf(score).any():
        print(f"⚠️ 警告：检测到 inf 得分！交易笔数均值: {counts.mean():.2f}")

    return pd.DataFrame({
        'score': score,
        'ann_ret': ann_ret,
        'max_dd': max_dd,
        'sharpe': sharpe,
        'win_rate': win_rate,
        'dd_dur': dd_dur_days,
        'final_value': final_value,
        'exposure': exposure
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

if __name__ == '__main__':
    # 1. 准备数据 (建议替换为你的 TDX 数据)
    TICKERS_DF = pd.read_csv(STOCKLIST_PATH, usecols=[0,5], skiprows=1, header=None) #read_csv返回的DF数据格式
    stocklist = list(TICKERS_DF.to_records(index=False))
    src_df = load_all_stocks(stocklist)
    price_df = prepare_all_stocks(src_df)

    # 2. 数据切片，按指定的split,roll参数进行切片和滚动
    split_config = relativedelta(months=24)
    roll_config = relativedelta(months=6)

    # 1. 转换日期格式
    start_dt = pd.to_datetime(STARTDATE, format='%Y-%m-%d')
    end_dt = pd.to_datetime(ENDDATE, format='%Y-%m-%d')

    time_chunks = generate_time_chunks(start_dt, end_dt, split_config, roll_config)
    
    # 3. 参数空间
    rsi_space = [10, 14, 20]
    fast_space = np.arange(10, 21)
    slow_space = [30, 40, 50, 60]

    all_combos = list(itertools.product(rsi_space, fast_space, slow_space))


    # 5. 循环窗口执行寻优
    window_results = []
    
    for i, (s_dt, e_dt) in enumerate(time_chunks):
        s_str, e_str = s_dt.strftime('%Y-%m-%d'), e_dt.strftime('%Y-%m-%d')
        window_price = price_df.loc[s_str:e_str]

        if len(window_price) < 100:
            continue
        
        print(f"🔄 正在处理切片 {i+1}: {s_dt.strftime('%Y%m%d')} -> {e_dt.strftime('%Y%m%d')} ({len(window_price)} 行)")
        window_scores = []
        for combo_batch in chunked_iterable(all_combos, len(fast_space)):
            rsi_w = [c[0] for c in combo_batch]
            fast_w = [c[1] for c in combo_batch]
            slow_w = [c[2] for c in combo_batch]
            # --- 向量化计算 (保持不变) ---
            rsi = vbt.RSI.run(window_price, window=rsi_w)
            sma_f = vbt.MA.run(window_price, window=fast_w)
            sma_s = vbt.MA.run(window_price, window=slow_w)
            
            # 提取矩阵数值
            f_vals = sma_f.ma.values.astype(np.float32)
            s_vals = sma_s.ma.values.astype(np.float32)
            r_vals = rsi.rsi.values.astype(np.float32)
            p_vals = np.tile(window_price.values.astype(np.float32), (1, len(combo_batch)))

            entries_np = ((r_vals < 30) & (f_vals > s_vals)).astype(np.bool_)
            exits_np = ((r_vals > 70) | (p_vals < f_vals)).astype(np.bool_)
            
            group_indices = np.repeat(np.arange(len(combo_batch)), len(window_price.columns))
            pf = vbt.Portfolio.from_signals(close=p_vals, entries=entries_np, exits=exits_np,size=0.02, size_type='Percent', call_seq='random',
                accumulate=False, slippage=0.001, init_cash=1000000,cash_sharing=True, group_by=group_indices, fees=0.002, freq='D', direction='longonly'
            )
            '''
            # --- 深度透视：解剖第一组参数的第一笔交易 ---
            try:
                # 选取第一个参数组合下的所有交易
                # pf.iloc[:, 0] 代表第一个参数组合对应的一组股票
                # 我们取出该组中所有已平仓的交易记录
                trades_df = pf.trades.records_readable
                
                if not trades_df.empty:
                    # 挑选出关键字段进行打印
                    # Column 0 通常是第一组参数中的第一只股票
                    print("\n" + "📑" * 5 + " 交易明细透视 (前 10 笔) " + "📑" * 5)
                    # 获取基础数据并手动构建 DataFrame
                    # entries 和 exits 的索引对应价格矩阵中的位置
                    
                    # 打印列名，让我们看看你的版本里到底叫什么
                    #print(f"DEBUG: 可用列名: {records.columns.tolist()}")
                    display_cols = [
                        'Column', 'Entry Timestamp', 'Exit Timestamp', 
                        'Direction','Size', 'Avg Entry Price', 'Avg Exit Price', 
                        'PnL', 'Return', 'Status'
                    ]
                    # 按照 Return 排序，看看那个导致 inf 的交易到底长什么样
                    print(trades_df[display_cols].sort_values(by='Return', ascending=False).head(10))
                    
                    # 重点检查：价格是否为 0？入场和出场是否在同一天？
                    sample = trades_df.iloc[0]
                    if sample['Avg Entry Price'] == 0 or sample['Avg Exit Price'] == 0:
                        print("⚠️ 警告：检测到价格为 0 的交易，请检查 window_price 是否包含空值或 0")
                    if sample['Entry Timestamp'] == sample['Exit Timestamp']:
                        print("⚠️ 警告：检测到日内交易（Entry=Exit），在 freq=252 模式下可能导致年化计算溢出")
                else:
                    print("❌ 警告：pf.trades 为空，当前批次未产生任何有效交易")
            except Exception as e:
                print(f"❌ 诊断执行失败: {e}")
            '''
            # 诊断：看看这一批次到底产生了多少笔交易
            total_trades = pf.trades.count().sum()
            print(f"DEBUG: 当前批次总交易笔数: {total_trades}")
            '''
            # --- 维度与层级诊断诊断开始 ---
            print("\n" + "📊" * 5 + " MultiIndex 层级深度分析 " + "📊" * 5)
            
            # 1. 检查原始价格索引
            print(f"1. 原始价格列数: {len(window_price.columns)} | 示例: {window_price.columns[:2].tolist()}")
            
            # 2. 检查生成的信号/回测对象索引 (如果是裸数组传入，这里可能没索引)
            # 但我们可以通过 entries (DataFrame) 来观察 vectorbt 期望的结构
            print(f"2. 当前批次参数组数: {len(combo_batch)}")
            
            # 3. 核心：观察 get_score 后的 Series 结构
            s_raw = get_score(pf)
            print(f"3. 评分结果(s_raw) 长度: {len(s_raw)} (应等于 股票数 * 参数组数)")
            print(f"   当前索引类型: {type(s_raw.index)}")
            
            # 4. 模拟手动注入索引过程
            p_idx = pd.MultiIndex.from_tuples(combo_batch, names=['rsi', 'fast', 'slow'])
            s_idx = window_price.columns
            # from_product 会产生笛卡尔积，这正是 vectorbt 平铺数据的逻辑
            full_idx = pd.MultiIndex.from_product([p_idx, s_idx])
            
            print(f"4. 构建的 Full_Index 层级数: {full_idx.nlevels}")
            print(f"   层级名称: {full_idx.names}")
            print(f"   前 3 行索引索引展示:")
            for i in range(min(3, len(full_idx))):
                print(f"      {full_idx[i]}")

            # 5. 验证对齐
            if len(s_raw) == len(full_idx):
                print("✅ 验证通过：评分结果长度与构建的索引完全匹配！")
            else:
                print(f"❌ 验证失败：评分结果({len(s_raw)}) 与 索引({len(full_idx)}) 长度不符！")
            
            print("📊" * 15 + "\n")
            # --- 维度与层级诊断结束 ---
            '''

            # 收集多维度指标 
            batch_score = get_score(pf)
            batch_score.index = pd.MultiIndex.from_tuples(combo_batch, names=['rsi', 'fast', 'slow'])
            batch_score.index = pd.MultiIndex.from_tuples(
                combo_batch, 
                names=['rsi', 'fast', 'slow']
            )
            #聚合参数：level=[0,1,2] 对应 (rsi, fast, slow)
            window_scores.append(batch_score)
            
            #显示清理内存
            del pf, entries_np, exits_np, rsi, sma_f, sma_s, f_vals, s_vals, r_vals, p_vals
            gc.collect()

        if (end_dt - e_dt).days < 30: break
        
        # 汇总当前窗口所有批次，并存入总列表
        if window_scores:
            window_df = pd.concat(window_scores)
            print(f"   ✅ 切片处理完成。当前切片最高分: {window_df['score'].max():.4f}, 平均夏普: {window_df['sharpe'].mean():.4f}")
            window_results.append(window_df)
            window_results.append(window_df)


    # 6. 最终聚合：选出在所有时间窗口表现最稳的参数
    if window_results:
        # 将所有时间窗的数据纵向堆叠
        all_window_data = pd.concat(window_results)
        
        # 1. 计算所有窗口的平均值
        final_df = all_window_data.groupby(level=['rsi', 'fast', 'slow']).mean()
        
        # 2. 计算得分的标准差 (稳定性指标)
        score_std = all_window_data.groupby(level=['rsi', 'fast', 'slow'])['score'].std().fillna(1.0)
        final_df['stability_score'] = final_df['score'] / (1 + score_std)
    
        # 3. 重新计算卡玛比率
        final_df['calmar_avg'] = (final_df['ann_ret'] / final_df['max_dd'].replace(0, 0.01)).fillna(0)
        
        # 按综合得分排序
        top_3 = final_df.sort_values(by='score', ascending=False).head(3)
        
        print("\n" + "🏆" * 10 + " 寻优结果前三名 (全时段平均) " + "🏆" * 10)
        
        for i, (params, row) in enumerate(top_3.iterrows()):
            rsi_val, fast_val, slow_val = params
            print(f"【Top {i+1}】 参数组合: RSI_{rsi_val} | 快线_{fast_val} | 慢线_{slow_val}")
            print(f" 综合得分: {row['score']:.4f} (稳定性参考: {row['stability_score']:.4f})")
            print(f" 收益表现: 年化收益={row['ann_ret']*100:.2f}% | 卡玛比率={row['calmar_avg']:.2f} | 夏普={row['sharpe']:.2f}")
            print(f" 交易质量: 胜率={row['win_rate']*100:.2f}%")
            print(f" 风险控制: 最大回撤={row['max_dd']*100:.2f}% | 平均最大回撤持续={int(row['dd_dur'])}天")
            print(f" 资金占用: 期末资产={row['final_value']:.2f} | 持仓占比={row['exposure']:.2f}")
            print("-" * 60)

        best_p = top_3.index[0]
        print(f"\n🌟 推荐最优组合: RSI={best_p[0]}, Fast={best_p[1]}, Slow={best_p[2]}")