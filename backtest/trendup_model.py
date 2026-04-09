import pandas as pd
import numpy as np
from pathlib import Path
import sys
print(sys.path)
from common.config_loader import CONFIG

def get_allstock():
    srcpath = Path(CONFIG['paths']['DATA_PATH'])/'all_stock_daily.parquet'

    try:
        full_df = pd.read_parquet(srcpath, engine='pyarrow')
    except Exception as e:
        print(f'宽表读取失败：{e}')
    return full_df

def find_growth_segments(df, threshold=0.6, window_size=250):
    """
    df: 必须包含 'close', 'date' 字段
    threshold: 涨幅阈值 (0.6 代表 60%)
    """
    segments = []
    i = 0
    n = len(df)
    
    while i < n:
        # 寻找局部的低点作为起点
        start_price = df['close'].iloc[i]
        start_date = df.index[i]
        
        # 在随后的 window_size 天内寻找最高点
        look_ahead = df.iloc[i : i + window_size]
        if look_ahead.empty: break
            
        max_idx = look_ahead['close'].idxmax()
        max_price = look_ahead['close'].max()
        max_date = max_idx
        
        growth = (max_price - start_price) / start_price
        
        if growth >= threshold:
            # 找到了一个符合要求的段，记录它
            # 为了获取更精确的“建仓期”，我们记录该段开始前的 60 天
            segments.append({
                'start_date': start_date,
                'peak_date': max_date,
                'growth': growth,
                'duration': (max_date - start_date).days
            })
            # 跳过这一段，寻找下一个可能的段
            i = df.index.get_loc(max_date) + 1
        else:
            i += 1
            
    return pd.DataFrame(segments)


if __name__ == '__main__':
    stock_df = get_allstock()
    pass