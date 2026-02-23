import pandas as pd
import numpy as np
import os
import psutil

from multiprocessing import Pool
from functools import partial
from dataclasses import dataclass, fields, asdict


# 设置数据路径
STOCKLIST_PATH = 'E:\\output\\Astock\\stockpicking\\'
STOCKDATA_PATH = 'E:\\datas\\tdx\\day_2018_2025'
TARGET_YEAR = 2025

@dataclass
class StockResult:
    # 定义字段及默认值，顺序即为 CSV 列顺序
    stock_name: str = ""
    max_gain: float = 0.0
    start_date: str = ""
    end_date: str = ""
    duration: int = 0
    mdd: float = 1.0
    mdd_duration: int = 0  # 新增指标只需在这里插一列
    calmar: float = 0.0
    sharpe: float = 0.0
    mean_volume: float = 0.0
    median_volume: float = 0.0

    @classmethod
    def get_column_names(cls):
        """静态方法：一键获取所有字段名作为 CSV 列名"""
        return [f.name for f in fields(cls)]

    @classmethod
    def get_dict(cls, **kwargs):
        """静态方法：获取一个带有默认值的字典"""
        return asdict(cls(**kwargs))

def filter_top_stocks(df_results, top_n=300):
    # 1. 数据清洗：确保数值类型正确
    df = df_results.copy()
    numeric_cols = ['max_gain', 'mdd', 'calmar', 'sharpe']
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
    
    # 2. 硬性门槛筛选
    # 限制最大回撤不能超过 35% (mdd是负数，所以是 > -0.35)
    # 限制持续时间至少 50 个自然日
    mask = (df['mdd'] > -0.35) & (df['duration'] >= 50) & (df['max_gain'] > 1)
    df_filtered = df[mask].copy()
   
    # 3. 综合评分计算 (Score)
    # 权重配置：涨幅 0.6，卡玛 0.25，夏普 0.15
    # 这个权重组合保证了：即使卡玛和夏普很高，但涨幅排在末尾的票，综合得分也不会高
    df_filtered['gain_score'] = df_filtered['max_gain'].rank(pct=True)
    df_filtered['calmar_score'] = df_filtered['calmar'].rank(pct=True)
    df_filtered['sharpe_score'] = df_filtered['sharpe'].rank(pct=True)

    df_filtered['final_score'] = (
        (df_filtered['gain_score']**2) * 0.60 + 
        df_filtered['calmar_score'] * 0.25 + 
        df_filtered['sharpe_score'] * 0.15
    )

    # 4. 排序并取 Top 300
    # 优先按综合得分排，如果得分相同，按收益率排
    top_300 = df_filtered.sort_values(
        by=['final_score', 'max_gain'], 
        ascending=[False, False]
    ).head(top_n)
    
    return top_300

# ==========================================
# 核心计算函数 (保持不变)
# ==========================================
def calculate_metrics(df_year, stock_name): 
    closes = df_year['close'].values
    dates = df_year['date'].values
    volumes = df_year['volume'].values
    
    if len(closes) < 2:
        return StockResult.get_dict(stock_name=stock_name)


    # --- 寻找最大涨幅区间 ---
    accum_min = np.minimum.accumulate(closes)
    with np.errstate(divide='ignore', invalid='ignore'):
        gains = (closes - accum_min) / accum_min
    
    gains[np.isnan(gains)] = -np.inf
    gains[np.isinf(gains)] = -np.inf

    end_idx = np.argmax(gains)
    max_gain = gains[end_idx]

    if max_gain <= 0:
        return StockResult.get_dict(stock_name=stock_name)

    start_idx = np.argmin(closes[:end_idx+1]) 
    
    # 截取该上涨区间内的价格序列用于后续指标计算
    period_prices = closes[start_idx : end_idx+1]
    
    # --- 新增指标 1: 最大回撤 (Maximum Drawdown)及最大回撤持续时间 ---
    # 计算在该上涨波段中，价格从高点回调的最大幅度
    period_accum_max = np.maximum.accumulate(period_prices)
    # 避免价格全相等导致除以0
    with np.errstate(divide='ignore', invalid='ignore'):
        dd_series = (period_prices - period_accum_max) / period_accum_max
    dd_series = np.nan_to_num(dd_series, nan=0.0)
    #找到最大回撤发生的索引 (Trough)
    trough_idx = np.argmin(dd_series) 
    mdd = dd_series[trough_idx]
    # 处理可能的异常值
    mdd = 1 if np.isinf(mdd) or np.isnan(mdd) else mdd
    # 找到该回撤发生前，最近的最高点索引 (Peak)
    # 逻辑：在 trough_idx 之前，价格第一次等于 period_accum_max[trough_idx] 的位置
    # 或者直接找从 0 到 trough_idx 之间，价格最大的那个索引
    peak_idx = np.argmax(period_prices[:trough_idx + 1])

    # 5. 计算最大回撤的持续天数（交易日数量）
    mdd_duration = int(trough_idx - peak_idx)

    # --- 卡玛比率 (Calmar Ratio) ---
    # 公式：区间收益率 / |最大回撤|。对 MDD 进行“平滑处理”给分母加上一个基数0.1（类似拉普拉斯平滑），防止分母太小导致结果爆炸。
    abs_mdd = abs(mdd) 
    calmar = max_gain / (abs_mdd+0.10)

    # --- 夏普比率 (Sharpe Ratio) ---
    # 计算日收益率
    if len(period_prices) > 1:
        daily_returns = np.diff(period_prices) / period_prices[:-1]
        avg_ret = np.mean(daily_returns)
        std_ret = np.std(daily_returns)
        
        # 假设无风险年化利率为 2%，转换为日均：0.02 / 244
        # 年化因子为 sqrt(244)
        if std_ret > 0:
            sharpe = (avg_ret - (0.02 / 244)) / std_ret * np.sqrt(244)
        else:
            sharpe = 0.0
    else:
        sharpe = 0.0

    # --- 数据格式化 ---
    start_date = dates[start_idx]
    end_date = dates[end_idx]
    duration = (end_date - start_date).astype('timedelta64[D]').astype(int)
    period_volumes = volumes[start_idx : end_idx+1]
    
    return StockResult.get_dict(
        stock_name=stock_name,
        max_gain=max_gain,
        start_date=start_date,
        end_date=end_date,
        duration=duration,
        mdd=mdd,
        mdd_duration=mdd_duration,
        calmar=calmar,
        sharpe=sharpe,
        mean_volume=np.mean(period_volumes),
        median_volume=np.median(period_volumes)
        )


def process_stock(stock_name, stockdata_path, target_year):
    """
    单个股票的处理函数，供进程池调用
    """   
    try:
        file_path = os.path.join(stockdata_path, f'{stock_name}.csv')
        # 1. 检查文件是否存在
        if not os.path.exists(file_path):
            return None

        # 2. 读取数据 (指定列名以防万一，优化类型)
        # 假设文件没有 header，如果有 header 请去掉 header=None
        # 根据题目描述：日期,开盘,最高,最低,收盘,交易量
        df = pd.read_csv(file_path, names=['date', 'open', 'high', 'low', 'close', 'volume'], 
                         dtype={'open': 'float32', 'high': 'float32', 'low': 'float32', 'close': 'float32', 'volume': 'float32'},
                         header=None, skiprows=1) # 假设第一行是标题，如果不是请调整

        # 3. 日期处理
        df['date'] = pd.to_datetime(df['date']).astype('datetime64[s]')
        
        # 4. 筛选年份
        df_year = df[df['date'].dt.year == target_year].sort_values('date').reset_index(drop=True)
        
        if df_year.empty:
            return None

        # 5. 计算指标
        result = calculate_metrics(df_year, stock_name)
        
        if result:
            result['stock_name'] = stock_name
            return result
        return None

    except Exception as e:
        # 打印错误但不中断程序
        print(f"Error processing {stock_name}: {e}") 
        return None

def analyze_stocks(stock_list=[], stockdata_path='', target_year=0, batch_size=1000):
    write_buffer = []
    all_results_buffer = []
    header_written = False

    output_filename = os.path.join(os.path.dirname(STOCKLIST_PATH), f"{target_year}_analysis.csv")
    top300_filename = os.path.join(os.path.dirname(STOCKLIST_PATH), f"{target_year}_top300_analysis.csv")

    process_stockpartial = partial(process_stock, stockdata_path=stockdata_path, target_year=target_year)
    physical_cores = psutil.cpu_count(logical=False)
    
    #for p in stock_list:
        #process_stock(stock_name=p, stockdata_path=stockdata_path, target_year=target_year)  # 测试单个股票处理函数
   
    
    with Pool(physical_cores) as p:
        iterator = p.imap_unordered(process_stockpartial, stock_list, chunksize=100)
        for res in iterator:
            if res is None:
                continue
            
            write_buffer.append(res)
            all_results_buffer.append(res) # 用于最后计算 Top300

            if(len(write_buffer) >= batch_size):
                _flush_to_csv(write_buffer, output_filename, header_written)
                header_written = True # 标记头已写
                write_buffer = [] # 清空缓冲区，释放内存
                print(f"处理进度: {len(all_results_buffer)}/{len(stock_list)} 股票数据已写入...")
                

        #循环结束，写入剩余的残余数据
        if write_buffer:
            _flush_to_csv(write_buffer, output_filename, header_written)
        
    
    if all_results_buffer:
        # 按涨幅降序排列
        df_all = pd.DataFrame(all_results_buffer)
        df_top300 = filter_top_stocks(df_all)
        cols = StockResult.get_column_names()
        # 6. 提取 TOP 300 并保存
        df_top300['start_date'] = pd.to_datetime(df_top300['start_date'], errors='coerce').astype('datetime64[s]')
        df_top300['end_date'] = pd.to_datetime(df_top300['end_date'], errors='coerce').astype('datetime64[s]')
        df_top300.to_csv(top300_filename, index=False, header=True, columns=cols, encoding='utf-8-sig', date_format='%Y-%m-%d', float_format='%.2f')
        print(f"TOP300 已保存至: {top300_filename}")
    
def _flush_to_csv(data_list, filename, has_header):
    """
    辅助函数：将列表数据追加写入 CSV
    """
    if not data_list:
        print("no data to write.")
        return
        
    df_chunk = pd.DataFrame(data_list)
       
    # 定义列顺序
    cols = StockResult.get_column_names()
    
    # --- 核心简化检查：一票否决制 ---
    if set(df_chunk.columns) != set(cols):
        # 计算差异详情，方便报错时一眼看出问题
        raise ValueError(f"字段不一致！\n")
    
    # 写入模式：如果是第一次写，用 'w'；之后都用 'a' (append)
    mode = 'a' if has_header else 'w'
    # 是否写表头：只有第一次需要写
    header = not has_header
    df_chunk['start_date'] = pd.to_datetime(df_chunk['start_date'], errors='coerce').astype('datetime64[s]')
    df_chunk['end_date'] = pd.to_datetime(df_chunk['end_date'], errors='coerce').astype('datetime64[s]')
    try:
        df_chunk.to_csv(filename, mode=mode, header=header, index=False, columns=cols, encoding='utf-8-sig', date_format='%Y-%m-%d', float_format='%.2f')
    except Exception as e:
        print(f"file write error:{filename}, {e}")

# ==========================================
# 使用示例
# ==========================================
if __name__ == "__main__":
    STOCKLIST_FILE = os.path.join(os.path.dirname(STOCKLIST_PATH), 'stocklist.csv')
    assert os.path.exists(STOCKLIST_FILE), f"Error: '{STOCKLIST_FILE}'"
    df_stocklist = pd.read_csv(STOCKLIST_FILE, usecols=[0], skiprows=1, header=None, dtype={0: str}) #read_csv返回的DF数据格式
    stocklist = df_stocklist[0].tolist()  # 转为 list 格式
    
    # 由于没有真实文件，运行下面这行会直接结束。
    # 请在实际环境中取消注释并传入真实的 stocklist
    analyze_stocks(stocklist, STOCKDATA_PATH, TARGET_YEAR)

