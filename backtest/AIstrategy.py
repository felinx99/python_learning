import pandas as pd
import talib as ta
import numpy as np
import time
from pathlib import Path
from datetime import datetime, timedelta
from scipy.stats import linregress
from zmq import IntEnum
from .util import datafeed
from . import sectorpick

RESULT_PATH = 'E:\\output\\Astock\\stockpicking\\analysis\\tmp'
DATA_PATH = 'E:\\datas\\tdx\\day_2018_2025'
START_DATE = '20251024' 
END_DATE = '20260106' 
TARGET_BLOCK_NAME = 'ZFXG'  

date_fmt = {
    'DAY': '%Y%m%d',
}

# ================= 每周任务 =================
#配置参数
WATCHLIST_PATH = "E:\\output\\Astock\\stockpicking\\my_watchlist.csv"  # 周日扫描出的潜力股池
LOOKBACK_PERIOD = 60       # 考察 120 个交易日的强度
MIN_RPS = 80    #最低RPS评分
TOP_N_SECTORS = 10      #强势行业数量10
TOP_N_CONCEPTS = 15 #热闹板块数量50
STOCK_R2_LIMIT = 0.70       # 个股趋势平稳度下限
CHANGE_WINDOW = 10          # 计算 RPS 变动的时间窗口 (10个交易日)

class DfIndicators:
    def __init__(self, period=400):
        self.today = datetime.now().strftime(date_fmt['DAY'])
        self.start_date = pd.to_datetime(START_DATE, format=date_fmt['DAY'])

    def cal_r2(self, series):
        """计算价格对数回归的 R2 (平稳度)"""
        if len(series) < 60: return 0
        y = np.log(series.tail(60).values)
        x = np.arange(len(y))
        slope, intercept, r_val, p_val, std_err = linregress(x, y)
        return r_val**2
    
    def cal_rps(self, df, price_col='close', period=LOOKBACK_PERIOD):
        """
        #简单RPS计算
        if df.empty or len(df) < period:
            return 0
        return (df[price_col].iloc[-1] / df[price_col].iloc[-period] - 1) * 100
        """
        """计算RPS评分:基于线性回归斜率法"""   
        if len(df) < period: return 0
        
        # 1. 计算不同周期的涨幅
        ret_10 = (df[price_col].iloc[-1] / df[price_col].iloc[-10] - 1)
        ret_20 = (df[price_col].iloc[-1] / df[price_col].iloc[-20] - 1)
        ret_period = (df[price_col].iloc[-1] / df[price_col].iloc[-period] - 1)
        
        # 2. 计算加权动量, (权重侧重近期)
        weighted_momentum = ret_10 * 0.5 + ret_20 * 0.3 + ret_period * 0.2
        
        # 3. 计算趋势平稳度 (R2)
        y = np.log(df[price_col].tail(period).values)
        x = np.arange(len(y))
        slope, intercept, r_value, p_value, std_err = linregress(x, y)
        r_squared = r_value**2
        
        # 4. 最终得分：动量 * 平稳度
        # 只有趋势平稳且向上的股票得分才会极高
        final_score = weighted_momentum * r_squared
        return final_score
    
    def cal_rps_change(self, df, price_col='close', period=LOOKBACK_PERIOD, lookback_days=CHANGE_WINDOW):
        """
        计算行业/概念的当前RPS及其在过去 lookback_days 里的变化量
        """
        print(f"正在获取板块数据并计算RPS变动率（回测窗口：{lookback_days}天）...")
        
        # 1. 获取申万一级行业列表
        sw_index_list = self.data.get_sector_list(sector_type='SECTOR_L1')
        
        end_date = datetime.now().strftime(date_fmt['DAY'])
        start_date = (datetime.now() - timedelta(days=250)).strftime(date_fmt['DAY'])
        
        sector_data = []
        
        for _, row in sw_index_list.iterrows():
            try:
                hist = self.data.get_daily(symbol=row['sector_code'], start_date=start_date, end_date=end_date)
                if len(hist) < period + lookback_days: continue
                
                # 计算当前和 N 天前的涨幅
                # 当前60日涨幅
                ret_now = (hist['close'].iloc[-1] / hist['close'].iloc[-period] - 1) * 100
                # N天前的60日涨幅
                ret_prev = (hist['close'].iloc[-(1+lookback_days)] / hist['close'].iloc[-(period+lookback_days)] - 1) * 100
                
                sector_data.append({
                    '名称': row['sector_name'],
                    '代码': row['sector_code'],
                    'ret_now': ret_now,
                    'ret_prev': ret_prev
                })
            except: continue
            
        df = pd.DataFrame(sector_data)
        
        # 2. 计算两个时间点的RPS排名
        df['RPS_当前'] = df['ret_now'].rank(pct=True) * 100
        df['RPS_前值'] = df['ret_prev'].rank(pct=True) * 100
        
        # 3. 计算RPS变动率 (RPS Change)
        df['RPS变动'] = df['RPS_当前'] - df['RPS_前值']
        
        # 4. 排序：按变动率排序，寻找黑马
        df = df.sort_values(by='RPS变动', ascending=False)
        
        return df[['名称', 'RPS_前值', 'RPS_当前', 'RPS变动']]
    
    def cal_relative_strength(self, stock_df, sector_df):
        """计算个股相对于板块的RS评分"""
        # 确保日期对齐
        combined = pd.merge(stock_df[['date', 'close']], sector_df[['date', 'close']], on='date', suffixes=('_stock', '_sector'))
        
        # 1. 计算基础RS比率
        combined['rs_ratio'] = combined['close_stock'] / combined['close_sector']
        
        # 2. 计算超额收益率 (过去20天)
        stock_ret = combined['close_stock'].pct_change(20).iloc[-1]
        sector_ret = combined['close_sector'].pct_change(20).iloc[-1]
        excess_ret_20 = stock_ret - sector_ret
        
        # 3. RS斜率：RS比率的5日均线方向
        combined['rs_ma5'] = combined['rs_ratio'].rolling(5).mean()
        rs_slope = combined['rs_ma5'].iloc[-1] > combined['rs_ma5'].iloc[-5]
        
        # 4. 综合评分：超额收益 * 斜率系数
        # 如果斜率向上，给予奖励分
        final_score = excess_ret_20 * 100 * (1.2 if rs_slope else 0.8)
        
        return final_score


class WeeklyScanner:
    def __init__(self, datafeed=None, indicator=None):
        self.today = datetime.now().strftime(date_fmt['DAY'])
        self.start_date = pd.to_datetime(START_DATE, format=date_fmt['DAY'])
        self.data = datafeed
        self.calc = indicator

    def get_rps_with_change(self, period=60, lookback_days=5):
        """
        计算行业/概念的当前RPS及其在过去 lookback_days 里的变化量
        """
        print(f"正在获取板块数据并计算RPS变动率（回测窗口：{lookback_days}天）...")
        
        # 1. 获取申万一级行业列表
        sw_index_list = self.data.get_sector_list(sector_type='SECTOR_L1')
                
        end_date = datetime.now().strftime(date_fmt['DAY'])
        start_date = (datetime.now() - timedelta(days=250)).strftime(date_fmt['DAY'])
        
        sector_data = []
        
        for _, row in sw_index_list.iterrows():
            try:
                hist = self.data.get_daily(symbol=row['sector_code'], start_date=start_date, end_date=end_date)
                if len(hist) < period + lookback_days: continue
                
                # 计算当前和 N 天前的涨幅
                # 当前60日涨幅
                ret_now = (hist['close'].iloc[-1] / hist['close'].iloc[-period] - 1) * 100
                # N天前的60日涨幅
                ret_prev = (hist['close'].iloc[-(1+lookback_days)] / hist['close'].iloc[-(period+lookback_days)] - 1) * 100
                
                sector_data.append({
                    '名称': row['sector_name'],
                    '代码': row['sector_code'],
                    'ret_now': ret_now,
                    'ret_prev': ret_prev
                })
            except: continue
            
        df = pd.DataFrame(sector_data)
        
        # 2. 计算两个时间点的RPS排名
        df['RPS_当前'] = df['ret_now'].rank(pct=True) * 100
        df['RPS_前值'] = df['ret_prev'].rank(pct=True) * 100
        
        # 3. 计算RPS变动率 (RPS Change)
        df['RPS变动'] = df['RPS_当前'] - df['RPS_前值']
        
        # 4. 排序：按变动率排序，寻找黑马
        df = df.sort_values(by='RPS变动', ascending=False)
        
        return df[['名称', 'RPS_前值', 'RPS_当前', 'RPS变动']]
    
    def calculate_r2(self, series):
        """计算价格对数回归的 R2 (平稳度)"""
        if len(series) < 60: return 0
        y = np.log(series.tail(60).values)
        x = np.arange(len(y))
        slope, intercept, r_val, p_val, std_err = linregress(x, y)
        return r_val**2
    
    def get_top_sectors(self):
        """获取当前市场中表现最强的行业板块 sw代表申万数据 l1代表一级行业"""
        sw_index_list = self.data.get_sector_list(sector_type='SECTOR_L1')
         
        sector_results = []
        end_date = datetime.now().strftime(date_fmt['DAY'])
        start_date = (datetime.now() - timedelta(days=120)).strftime(date_fmt['DAY'])

        for _, row in sw_index_list.iterrows():
            try:
                hist = self.data.get_daily(symbol=row['sector_code'], start_date=start_date, end_date=end_date)
                #计算简易RPS评分
                rps_60 = self.calc.cal_rps(hist, price_col='close', period=LOOKBACK_PERIOD)
                row_name = row['sector_name'] if 'sector_name' in row else rps_60
                sector_results.append({'name': row_name, 'code': row['sector_code'], 'type':'SW', 'score': rps_60})
            except Exception as e:
                print(f"获取数据{row['sector_name']}失败: {e}")
                continue

        df = pd.DataFrame(sector_results).sort_values(by='score', ascending=False)
        return df.head(TOP_N_SECTORS)
    
    def get_top_concepts(self):
        """获取当前市场中表现最强的概念板块"""
        concept_board_df = self.data.get_sector_list(sector_type='SECTOR_CONCEPT')

        sector_results = []
        end_date = datetime.now().strftime(date_fmt['DAY'])
        start_date = (datetime.now() - timedelta(days=120)).strftime(date_fmt['DAY'])
        
        for _, row in concept_board_df.iterrows():
            try:
                hist = self.data.get_daily(symbol=row['sector_code'], start_date=start_date, end_date=end_date)
                #计算简易RPS评分
                rps_60 = self.calc.cal_rps(hist, price_col='close', period=LOOKBACK_PERIOD)
                row_name = row['sector_name'] if 'sector_name' in row else rps_60
                sector_results.append({'name': row_name, 'code': row['sector_code'], 'type':'Concept', 'score': rps_60})
            except Exception as e:
                print(f"获取数据{row['sector_name']}失败: {e}")
                continue
        
        df = pd.DataFrame(sector_results).sort_values(by='score', ascending=False)
        
        return df.head(TOP_N_CONCEPTS)
    
    #每周股票筛选条件
    #1. RS得分高于80分，说明该股在过去120天内表现优于80%的股票。
    #2. RS变动率>20且RS>70
    #3. R2平稳度高于0.7，说明该股的价格趋势较为稳定，适合趋势跟踪策略。
    #4. 收盘价站稳60日均线之上，说明该股处于多头排列中，趋势较强。
    def get_weekly_stocks(self, symbol):
        hist = self.data.get_daily(symbol=symbol, start_date=self.start_date, end_date=self.end_date)

        if len(hist) < 120: 
            return False, None, None
        
        r2 = self.calculate_r2(hist['close'])
        momentum = self.calc.cal_rps(hist, 'close')

        # 过滤：高质量趋势 + 多头排列 (收盘 > MA120)
        ma120 = hist['close'].rolling(120).mean().iloc[-1]
        if r2 >= STOCK_R2_LIMIT and hist['close'].iloc[-1] > ma120:
            return True, round(r2, 3), round(momentum, 2)
        else:
            return False, None, None

    def weekly_scan(self):
        """每周日执行：行业共振 + 潜力池更新"""
        # 1. 筛选强势行业和概念
        strong_sw = self.get_top_sectors()
        strong_concept = self.get_top_concepts()

        resonance_list = []

        # 2. 获取行业与概念的交集（共振个股）
        sw_stock_total = set()
        for _, sw_row in strong_sw.iterrows():
            try:
                sw_stocks = self.data.get_stocklist_in_index(sector=sw_row['code'])
                sw_stock_codes = set(sw_stocks['stock_code'].tolist())
                sw_stock_total.update(sw_stock_codes)
            except Exception as e:
                print(f"获取申万行业 {sw_row['code']} 成分股失败: {e}")
                continue
        
        # 3. 遍历强概念，获取成分股
        for _, cp_row in strong_concept.iterrows():
            try:
                cp_stocks = self.data.get_stocklist_in_index(sector=cp_row['code'])
                cp_stock_codes = set(cp_stocks['stock_code'].tolist())           
                    
                # 4. 取交集：这就是共振股
                common_stocks = sw_stock_total.intersection(cp_stock_codes)

                if common_stocks:
                    for code in common_stocks:
                        # 进一步筛选：多头 (收盘 > MA120) + 趋势平稳度 (R2)
                        is_valid, r2, momentum = self.get_weekly_stocks(code)
                        
                        if is_valid:
                            #name = cp_stocks[cp_stocks['stock_code'] == code]['name'].values[0]
                            resonance_list.append({                          
                                'sw_code': sw_row['code'],
                                'concept_code': cp_row['code'],
                                'stock_code': code,
                                #'stock_name': name,
                                'R2': round(r2, 3),
                                'momentum': round(momentum, 2)
                        })
                        time.sleep(0.1) # 避免频率过快

            except Exception as e:
                print(f"获取概念 {cp_row['code']} 成分股失败: {e}")
                continue

        # 4. 保存结果
        if resonance_list:
            final_df = pd.DataFrame(resonance_list).drop_duplicates(subset=['stock_code'])
            # 按照 R2 和动能综合排序
            final_df = final_df.sort_values(by=['R2', 'momentum'], ascending=False)
            final_df.to_csv(WATCHLIST_PATH, index=False, encoding='utf-8-sig')
            stock_list = final_df['stock_code'].astype(str).tolist()
            
            self.data.update_block(block_name=TARGET_BLOCK_NAME, stock_list=stock_list)
            print(f"--- 扫描完成！共锁定 {len(final_df)} 只高质量种子股，已更新 my_watchlist.csv ---")
        else:
            print("--- 扫描结束，未找到符合条件的个股 ---")


# ================= 每日任务 =================
#配置参数
PORTFOLIO_PATH = "E:\\output\\Astock\\stockpicking\\my_portfolio.csv"  # 当前持仓股（用于监测止损）
R2_THRESHOLD = 0.7                  # 趋势平稳度阈值
ATR_MULT = 2.5                      # ATR止损倍数
COMPACT_THRESHOLD = 0.08    #VCP紧凑度阈值：平均振幅在8%以内
VCP_WINDOW = 10          # VCP计算窗口：最近10天
ATR_WINDOW = 10         # ATR计算窗口：14天

class TrendStrategyTerm:
    def __init__(self, datafeed=None, indicator=None):
        self.today = datetime.now().strftime(date_fmt['DAY'])
        self.start_date = pd.to_datetime(START_DATE, format=date_fmt['DAY'])
        self.output_file = f"{self.today}_report.txt"
        self.data = datafeed
        self.calc = indicator
        self.output_csv = ''

    #筛选板块内的强势个股，个股与板块指数的趋势，计算RS评分
    def calculate_relative_strength(self, stock_df, sector_df):
        """
        计算个股相对于板块的RS评分
        stock_df: 个股历史数据
        sector_df: 板块指数历史数据
        """
        # 确保日期对齐
        combined = pd.merge(stock_df[['date', 'close']], sector_df[['date', 'close']], on='date', suffixes=('_stock', '_sector'))
        
        # 1. 计算基础RS比率
        combined['rs_ratio'] = combined['close_stock'] / combined['close_sector']
        
        # 2. 计算超额收益率 (过去20天)
        stock_ret = combined['close_stock'].pct_change(20).iloc[-1]
        sector_ret = combined['close_sector'].pct_change(20).iloc[-1]
        excess_ret_20 = stock_ret - sector_ret
        
        # 3. RS斜率：RS比率的5日均线方向
        combined['rs_ma5'] = combined['rs_ratio'].rolling(5).mean()
        rs_slope = combined['rs_ma5'].iloc[-1] > combined['rs_ma5'].iloc[-5]
        
        # 4. 综合评分：超额收益 * 斜率系数
        # 如果斜率向上，给予奖励分
        final_score = excess_ret_20 * 100 * (1.2 if rs_slope else 0.8)
        
        return final_score
    
    # --- 核心算法库 ---
    def get_quality_score(self, df):
        """计算加权动量和R2平稳度"""
        if len(df) < 60: return 0, 0
        close = df['close'].tail(60).values
        # 加权动量 (近20日权重70%)
        m_20 = (close[-1] / close[-20]) - 1
        m_60 = (close[-1] / close[0]) - 1
        score = m_20 * 0.7 + m_60 * 0.3
        # R2 平稳度
        x = np.arange(60)
        slope, intercept, r_val, p_val, std_err = linregress(x, np.log(close))
        return score * (r_val**2), r_val**2

    def get_atr(self, df, period=ATR_WINDOW):
        """计算ATR指标"""
        #tr = np.maximum(df['high'] - df['low'], 
        #                     np.maximum(abs(df['high'] - df['close'].shift(1)), 
        #                                abs(df['low'] - df['close'].shift(1))))
        #df['atr'] = tr.rolling(period).mean()
        df['atr'] = ta.ATR(df['high'], df['low'], df['close'], timeperiod=period)
        atr = df['atr'].iloc[-1].item()
        return atr 

    def get_trailingstop(self, df):
        """计算基于ATR的动态止损位（Chandelier Exit）"""
        # 1. 计算过去10天的最高价
        # 2. 用最高价减去 ATR 的倍数作为止损位
        df['max_price'] = df['ohlc'].rolling(window=ATR_WINDOW).max().iloc[-1].item()
        df['atr'] = ta.ATR(df['high'], df['low'], df['close'], timeperiod=ATR_WINDOW)  
        df['trailing_stop'] = df['max_price'] - (df['atr'] * ATR_MULT)

        trailing_stop_now = df['trailing_stop'].iloc[-1].item()
        atr_now = df['atr'].iloc[-1].item()
        return trailing_stop_now, atr_now
    

    # 替换下面的check_stock_vcp_style为多维度筛选函数，
    # 结合RPS质量评分、VCP紧凑度、价格趋势EMA，成交量MFI等多维度条件，综合评估个股是否具备入场潜力。
    # --- 5. 综合策略信号 ---
    # 入场条件：
    # 1. 箱体突破：收盘价创20日新高
    # 2. VCP特征：处于波动收缩后的放量期
    # 3. 均线斜率：EMA10 向上
    # 4. 强弱过滤：动量在板块前列 (此处用Momentum > 0 简单示意)
    # 5. 特大单净流入
    def advanced_trend_strategy(self, df, universe_df=None):
        """
        df: 包含 ['open', 'high', 'low', 'close', 'volume'] 的日线数据
        universe_df: 用于计算RPS的全市场数据（可选）
        """
        
        # --- 1. 基础指标计算 ---
        # 使用EMA提高敏感度，减少滞后
        df['EMA10'] = ta.ema(df['close'], length=10)
        df['EMA20'] = ta.ema(df['close'], length=20)
        
        # ATR 用于风险控制和止损
        df['ATR'] = ta.atr(df['high'], df['low'], df['close'], length=14)
        
        # --- 2. 解决滞后：VCP 波动收缩特征 ---
        # 计算过去20天的最高价和最低价的百分比区间
        df['Price_Range'] = (df['high'].rolling(20).max() - df['low'].rolling(20).min()) / df['low'].rolling(20).min()
        # 波动收缩定义：当前波幅小于过去20天波幅均值的 0.6 倍
        df['VCP_Contraction'] = df['Price_Range'] < (df['Price_Range'].rolling(60).mean() * 0.6)

        # --- 3. 识别一波流：RPS (相对强度) ---
        # 简化版RPS：个股过去250天涨幅在全市场的排名（需跨个股数据，此处演示逻辑）
        if universe_df is not None:
            df['RPS_250'] = self.calc.cal_rps(df, universe_df, window=250)
        else:
            # 替代方案：个股动量
            df['Momentum'] = ta.roc(df['close'], length=125) 
        
        # --- 4. 成交量多维验证 ---
        # 成交量倍率：当前成交量是过去5天均量的多少倍
        df['Vol_Ratio'] = df['volume'] / ta.sma(df['volume'], length=5).shift(1)

        # --- 5. 综合策略信号 ---
        # 入场条件：
        # 1. 箱体突破：收盘价创20日新高
        # 2. VCP特征：处于波动收缩后的放量期
        # 3. 均线斜率：EMA10 向上
        # 4. 强弱过滤：动量在板块前列 (此处用Momentum > 0 简单示意)
        # 5. 特大单净流入：此处暂缺数据，示意逻辑为：特大单净流入 > 0
        
        condition1 = df['close'] > df['high'].shift(1).rolling(20).max() # 突破
        condition2 = df['VCP_Contraction'].shift(1) # 突破前有收缩
        condition3 = df['EMA10'] > df['EMA10'].shift(1) # 斜率向上
        condition4 = df['Vol_Ratio'] > 1.5 # 放量验证 1.5-3 倍的“温和放量”作为阈值，鲁棒性更强。
        condition5 = df['Large_order_Net_Inflow'] > 0 # 特大单净流入（示意）
        
        df['Signal'] = np.where(condition1 & condition2 & condition3 & condition4 & condition5, 1, 0)

        # --- 6. 动态止损 (ATR Chandelier Exit) ---
        # 解决利润回撤问题
        df['Max_Price'] = df['close'].rolling(window=10).max()
        df['Trailing_Stop'] = df['Max_Price'] - (df['ATR'] * 2.5)
        
        return df
    # 示例调用
    # df = pd.read_csv('your_stock_data.csv')
    # strategy_df = advanced_trend_strategy(df)


    def check_vcp(self, df, period=VCP_WINDOW, threshold=COMPACT_THRESHOLD):
        # VCP 波动收缩形态
        # 1.寻找 Tightness（紧凑度）： 在趋势回撤或横盘期，寻找连续 5-10 天波动率萎缩（振幅 $< 8\%$）的区域。
        # 2.买入信号： 突破紧凑区的最高点（Pivot Point）。
        #计算窗口内vcp平均波幅
        # 1. 计算当前价格区间 (high-low) 的百分比
        df['vcp'] = (df['high'] - df['low']) / df['low']
        # 2. 计算过去 20 天的平均波幅
        df['vcp_range'] = df['vcp'].rolling(period).mean()
        # 3. 计算波幅的稳定性（标准差）
        df['vcp_std'] = df['vcp'].rolling(period).std()
        
        #VCP特征1：判断是否满足vcp特征:当前波幅均值在下降，且标准差也在缩小，说明进入收缩期
        vcp_signal1 = (df['vcp_range'] < df['vcp_range'].shift(period) * threshold) & \
                    (df['vcp_std'] < df['vcp_std'].shift(period) * threshold)
        
        #VCP特征2：当前价格突破收缩区的最高点（Pivot Point）
        df['pivot'] = df['high'].rolling(period).max().shift(1) # 收缩区最高点
        vcp_signal2 = df['close'] > df['pivot']

        df['vcp_signal'] = vcp_signal1 & vcp_signal2
        vcp_signal_now = df['vcp_signal'].iloc[-1].item()
        amp_now = df['vcp'].iloc[-1].item()
        pivot_now = df['pivot'].iloc[-1].item()
        return vcp_signal_now, amp_now, pivot_now
    
    def check_ema(self, df):
        ema_signal = False
        try:                       
            # 1. 计算均线
            close = df['close']
            ema10 = ta.ema(df['close'], length=10)
            ema20 = ta.ema(df['close'], length=20)
            ema60 = ta.ema(df['close'], length=60)
            ema120 = ta.ema(df['close'], length=120)   
            
            # 2. 条件一：多头排列
            is_bull_market = (close.iloc[-1] > ema20.iloc[-1]) and (ema20.iloc[-1] > ema60.iloc[-1]) and (ema60.iloc[-1] > ema120.iloc[-1])
                        
            # 3. 条件二：当前股价站稳均线（离MA20不远，防止追高）
            is_near_ma = close.iloc[-1] < ema20.iloc[-1] * 1.15
            
            ema_signal = is_bull_market and is_near_ma
            return ema_signal
        except:
            return False, None
        
    def check_rps(self, df, sector):
        rps_signal = False
        # 计算个股相对于板块的RPS评分
        # 1. 获取板块指数数据（示意，实际应缓存或批量获取）
        start_date = pd.to_datetime(df['date'].iloc[0]).strftime(date_fmt['DAY'])
        end_date = pd.to_datetime(df['date'].iloc[-1]).strftime(date_fmt['DAY'])
        sector_df = self.data.get_daily(symbol=sector, start_date=start_date, end_date=end_date)
        # 2. 计算RPS评分
        rps_score = self.calculate_relative_strength(df, sector_df)
        rps_signal = rps_score > 80 # 示例阈值
        return rps_signal
    
    def check_mfi(self, df):
        mfi_signal = False
        # 计算MFI指标，验证资金流入
        mfi = ta.MFI(
            df['high'].values, 
            df['low'].values, 
            df['close'].values, 
            df['volume'].values, 
            timeperiod=14)
        mfi_signal = mfi[-1] > 50 # 示例条件：资金流入强劲
        return mfi_signal
    
    def check_volratio(self, df):
        #股价放量（Vol > 5日均量 1.5倍）
        vol_signal = False
        # 成交量倍率：当前成交量是过去5天均量的多少倍
        vol_sma5 = ta.SMA(df['volume'].values.astype(float), timeperiod=5)
        #vol_sma5[-1]是今天，【-2】是昨天，避免当日数据异常导致误判
        vol_ratio = df['volume'].iloc[-1] / vol_sma5[-2]
        vol_signal = vol_ratio > 1.5 # 示例条件：放量验证
        return vol_signal
    
    def check_large_order(self, df):
        large_order_signal = True
        # 特大单净流入：此处暂缺数据，示意逻辑为：特大单净流入 > 0
        # 实际生产中需要接入特大单数据接口进行验证
        return large_order_signal

    def detect_breakout_pattern(self, df):
        """
        识别均线点火突破模式
        条件1：均线收敛蓄势（3天前的一周内）
        条件2：近3天爆发突破（价格、涨幅、量能）
        条件3：今日趋势确认（MA20斜率）
        """
        if len(df) < 130:  # 确保有足够数据计算MA120和回测窗口
            return False, {}

        # --- 基础数据准备 ---
        df = df.copy()
        df['ma20'] = df['收盘'].rolling(20).mean()
        df['ma60'] = df['收盘'].rolling(60).mean()
        df['ma120'] = df['收盘'].rolling(120).mean()
        
        # 计算收敛度 (变异系数 CV)
        ma_cols = ['ma20', 'ma60', 'ma120']
        df['converged'] = df[ma_cols].std(axis=1) / df[ma_cols].mean(axis=1)

        # --- 条件 1：3天前的一周均线靠近收敛 ---
        # 取从第-8天到第-4天（即3天前的一周）的收敛度均值
        conv_window = df['converged'].iloc[-8:-3] 
        avg_convergence = conv_window.mean()
        is_converged = avg_convergence < 0.03  # 阈值可调，0.03代表间距在3%以内

        # --- 条件 2：最新3天向上突破 ---
        recent_3 = df.tail(3).copy()
        # 计算每日涨幅 (基于前一收盘)
        recent_3['pct_chg'] = df['收盘'].pct_change().tail(3)
        
        # 2.1 价格特征
        # 最近三天都收涨 (涨幅 > 0)
        all_positive = (recent_3['pct_chg'] > 0).all()
        # 至少1天涨幅 > 5%
        has_big_win = (recent_3['pct_chg'] > 0.05).any()
        # 3天整体涨幅 > 10%
        total_3d_gain = (df['收盘'].iloc[-1] / df['收盘'].iloc[-4]) - 1
        price_breakout = all_positive and has_big_win and (total_3d_gain > 0.10)

        # 2.2 量能特征
        avg_vol_3d = df['成交量'].tail(3).mean()
        ma_vol_10 = df['成交量'].rolling(10).mean().iloc[-1]
        volume_ignited = avg_vol_3d > (ma_vol_10 * 1.5)

        # --- 条件 3：今天状态确认 ---
        # MA20斜率向上 (今日MA20 > 昨日MA20)
        ma20_up = df['ma20'].iloc[-1] > df['ma20'].iloc[-2]
        # 价格在MA20之上 (确保在通道上方)
        above_ma20 = df['收盘'].iloc[-1] > df['ma20'].iloc[-1]

        # --- 最终判定 ---
        is_triggered = is_converged and price_breakout and volume_ignited and ma20_up and above_ma20

        # 返回结果及关键指标（便于存入CSV复盘）
        result_data = {
            "触发状态": is_triggered,
            "前期收敛度": round(avg_convergence, 4),
            "3天总涨幅": f"{total_3d_gain:.2%}",
            "成交量倍数": round(avg_vol_3d / ma_vol_10, 2),
            "MA20斜率": "向上" if ma20_up else "向下"
        }

        return is_triggered, result_data

    # --- 每日任务：个股进出场监测 ---
    def daily_monitor(self, stocklist_df=None):
        print(f"\n【{self.today} 日线交易监测：机会与风险】")
        
        # A. 监测潜力池 (入场信号)
        try:
            watchlist = pd.read_csv(WATCHLIST_PATH)
            signal_list = []
            end_date = datetime.now().strftime(date_fmt['DAY'])
            start_date = (datetime.now() - timedelta(days=350)).strftime(date_fmt['DAY'])

            for _, row in watchlist.iterrows():
                code = str(row['stock_code'].zfill(6))
                #name = row['Name']
                
                df = self.data.get_daily(symbol=str(code), start_date=start_date, end_date=end_date)

                if len(df) < 120: 
                    return
                
                # --- 入场逻辑：综合策略信号 ---
                # 1. 箱体突破：以VCP为特征，处于波动收缩后的放量期，收盘价创20日新高
                # 2. 均线斜率：EMA10 向上
                # 3. 强弱过滤：动量在板块前列 (此处用Momentum > 0 简单示意)
                # 4. 特大单净流入：此处暂缺数据，示意逻辑为：特大单净流入 > 0
                #condition_vcp  = df['vcp_signal'].iloc[-1].item(), 
                #amp = df['vcp'].iloc[-1].item(), 
                #pivot = df['pivot'].iloc[-1].item()
                condition_vcp, amp, pivot = self.check_vcp(df)
                condition_ema = self.check_ema(df)
                condition_rps = self.check_rps(df, row['concept_code']) # 示例：用概念指数作为板块代表
                condition_mfi = self.check_mfi(df)
                condition_volration = self.check_volratio(df)
                condition_largeorder = self.check_large_order(df)

                buy_signal = condition_vcp and condition_ema and condition_rps and condition_mfi and condition_volration and condition_largeorder
                
                if buy_signal:
                    # 计算基于 ATR 的吊灯止损位
                    stop_loss, atr = self.get_trailingstop(df)
                    curr_price = df['close'].iloc[-1]
                    
                    # 汇总所有关键信息
                    signal_data = {
                        "日期": self.today,
                        "代码": code,
                        #"名称": name,
                        "现价": round(curr_price, 2),
                        "止损位": round(stop_loss, 2),
                        "风险空间(%)": round(((curr_price - stop_loss) / curr_price) * 100, 2),
                        "10日振幅": f"{amp:.2%}",
                        "10日高点(Pivot)": round(pivot, 2),
                        "ATR(10)": round(atr, 3),
                        "所属行业": row.get('行业', '未知'),
                        "所属概念": row.get('概念', '未知')
                    }
                    signal_list.append(signal_data)
                    print(f"✅ 信号触发: {code} {'name'} (突破 {pivot:.2f})")
                time.sleep(0.1) # 避免频率过快
        except Exception as e:
            print(f"读取潜力池失败: {e}")
        
        
        # B. 监测持仓池 (离场信号) 逻辑：最高价回落 2.5*ATR 则卖出
        print("持仓风险监测完成。")

        # --- 保存为 CSV ---
        if signal_list:
            result_df = pd.DataFrame(signal_list)
            result_df.to_csv(self.output_csv, index=False, encoding='utf-8-sig') # utf-8-sig 保证 Excel 打开不乱码
            print(f"\n📂 监测完成！已生成信号报表: {self.output_csv}")
        else:
            print("\n🏁 监测完成，今日无符合条件的突破信号。")
    def daily_monitor_breakout(self, stocklist_df=None, converged_windwos=5, converged_threshold=0.03, vol_gain=1.5):        
        # A. 监测潜力池 (入场信号)
        try:
            signal_list = []
            srcpath = Path(DATA_PATH)/'all_stock_daily.parquet'

            try:
                full_df = pd.read_parquet(srcpath, engine='pyarrow')
            except Exception as e:
                print(f'宽表读取失败：{e}')

            for index, row in stocklist_df.iterrows():              
                df = full_df[full_df['symbol']==row['Code']]

                if len(df) < 120: 
                    continue
                #condition_vcp  = df['vcp_signal'].iloc[-1].item(), 
                #amp = df['vcp'].iloc[-1].item(), 
                #pivot = df['pivot'].iloc[-1].item()
                condition_vcp, amp, pivot = self.check_vcp(df)


                # 计算收敛度 (变异系数 CV)
                ma_cols = ['sma5', 'sma10', 'sma20']
                df['converged'] = df[ma_cols].std(axis=1) / df[ma_cols].mean(axis=1)

                # --- 条件 1：3天前的一周均线靠近收敛 ---
                # 取从第-8天到第-4天（即3天前的一周）的收敛度均值
                df['avg_convergence'] = df['converged'].rolling(window=converged_windwos).mean().shift(3)
                #print(f'{index} avg_convergence:{avg_convergence}')
                df['is_converged'] = (df['avg_convergence'] < converged_threshold).fillna(False)  # 阈值可调，0.03代表间距在3%以内

                # --- 条件 2：最新3天向上突破 ---             
                # 1. 基础计算：计算每日涨幅 (全量计分隔夜涨幅和日内涨幅两种,隔夜涨幅可以用ohlc替代close)
                df['oc'] = (df['open']+df['close'])/2
                df['pct_chg'] = df['oc'].pct_change()
                # 3.1 最近三天都收涨 (最小值 > 0)
                df['all_positive'] = df['pct_chg'].rolling(window=3).min() > 0
                # 3.2 至少1天涨幅 > 5% (最大值 > 0.05)
                df['has_big_win'] = df['pct_chg'].rolling(window=3).max() > 0.04
                # 3.3 3天整体涨幅 > 10% (今日价格 / 3天前价格 - 1)
                # 注意：iloc[-1]/iloc[-4] 对应的是 3 天的跨度，所以用 shift(3)
                df['total_3d_gain'] = (df['oc'] / df['oc'].shift(3)) - 1
                df['gain_large_enough'] = df['total_3d_gain'] > 0.10

                # 4. 综合价格突破条件
                df['price_breakout'] = df['all_positive'] & df['has_big_win'] & df['gain_large_enough']
                cols_to_fix = ['all_positive', 'has_big_win', 'gain_large_enough', 'price_breakout']
                df[cols_to_fix] = df[cols_to_fix].fillna(False)
                '''
                # --- 基础数据计算 ---
                # 1. 计算每日的“日内涨幅” 
                df['intraday_chg'] = (df['close'] - df['open']) / df['open']
                # 2.1 最近三天都收红盘 (all_positive)
                df['all_positive'] = df['intraday_chg'].rolling(window=3).min() > 0
                # 2.2 至少有 1 天涨幅超过 5% (has_big_win)
                df['has_big_win'] = df['intraday_chg'].rolling(window=3).max() > 0.05
                # 2.3 3天整体涨幅 > 10% (gain_large_enough)
                df['total_3d_gain'] = (df['close'] / df['open'].shift(2)) - 1
                df['gain_large_enough'] = df['total_3d_gain'] > 0.10
                '''
                
                # 2.2 量能特征
                df['avg_vol_3d'] = df['volume'].rolling(window=3).mean()
                df['ma_vol_10'] = df['volume'].rolling(window=10).mean()
                df['volume_ignited'] = df['avg_vol_3d'] > (df['ma_vol_10'] * vol_gain)
                df['volume_ignited'] = df['volume_ignited'].fillna(False)

                # --- 条件 3：今天状态确认 ---
                # MA20斜率向上 (今日MA20 > 昨日MA20)# .diff() 会自动计算 df['sma20'] - df['sma20'].shift(1)
                df['ma20_up'] = df['sma20'].diff() > 0
                # 价格在MA20之上 (确保在通道上方),此处也可以设置为ohlc>sma20
                df['above_ma20_line'] = df['close'] > df['sma20']
                df['is_above20'] = df['ma20_up'] & df['above_ma20_line']

                # --- 最终判定 ---
                df['buy_signal'] = df['is_converged'] & df['price_breakout'] & df['volume_ignited'] & df['is_above20']
                buy_signal = df['buy_signal'] .iloc[-1].item()
                avg_convergence = df['avg_convergence'].iloc[-1].item()
                total_3d_gain = df['total_3d_gain'].iloc[-1].item()
                avg_vol_3d = df['avg_vol_3d'].iloc[-1].item()
                ma_vol_10 = df['ma_vol_10'].iloc[-1].item()
                ma20_up = df['ma20_up'].iloc[-1].item()

                if buy_signal:
                    # 计算基于 ATR 的吊灯止损位
                    stop_loss, atr = self.get_trailingstop(df)
                    curr_price = df['close'].iloc[-1].item()

                    # 返回结果及关键指标（便于存入CSV复盘）
                    result_data = {
                        "触发状态": buy_signal,
                        "前期收敛度": round(avg_convergence, 4),
                        "3天总涨幅": f"{total_3d_gain:.2%}",
                        "成交量倍数": round(avg_vol_3d / ma_vol_10, 2),
                        "MA20斜率": "向上" if ma20_up else "向下"
                    }
                    
                    # 汇总所有关键信息
                    signal_data = {
                        "日期": self.today,
                        "代码": row['Code'],
                        "名称": row['Name'],
                        "现价": round(curr_price, 2),
                        "止损位": round(stop_loss, 2),
                        "风险空间(%)": round(((curr_price - stop_loss) / curr_price) * 100, 2),
                        "10日振幅": f"{amp:.2%}",
                        "10日高点(Pivot)": round(pivot, 2),
                        "ATR(10)": round(atr, 3),
                    }
                    signal_list.append(signal_data)
                    print(f"✅ 信号触发: {row['Code']} {row['Name']} (突破 {pivot:.2f})")
        except Exception as e:
            print(f"读取潜力池失败: {e}")
        
        
        # B. 监测持仓池 (离场信号) 逻辑：最高价回落 2.5*ATR 则卖出
        print("持仓风险监测完成。")

        # --- 保存为 CSV ---
        if signal_list:
            result_df = pd.DataFrame(signal_list)
            self.output_csv = Path(RESULT_PATH)/f"breakout_{self.today}.csv"
            result_df.to_csv(self.output_csv, encoding='utf-8-sig', index=False, date_format=date_fmt['DAY'], float_format='%.2f') 
            print(f"\n📂 监测完成！已生成信号报表: {self.output_csv}")
        else:
            print("\n🏁 监测完成，今日无符合条件的突破信号。")

# --- 执行入口 ---
if __name__ == "__main__":
    feed = datafeed.FeedManager.register('tdx')
    feed.init_feed()
    indicator = DfIndicators()
    ts = TrendStrategyTerm(datafeed=feed, indicator=indicator)
    #ws = WeeklyScanner(datafeed=feed, indicator=indicator)

    #weekday = datetime.now().weekday()
    #ws.weekly_scan()
    #if weekday == 5: # 周六执行周度扫描
    #    ws.weekly_scan()



    sectors_list = sectorpick.get_up_sector(sectorlist=['concept', 'l3'], ret='today')
    feed.update_block(block_code='ZFBK', stock_list=sectors_list)

    stock_list = []
    for sector in sectors_list:
        templist = feed.get_stocklist_in_index(sector=sector)
        stock_list.append(templist)

    stocklist_df = pd.concat(stock_list, ignore_index=True).drop_duplicates()
    ts.daily_monitor_breakout(stocklist_df)
    
    
    