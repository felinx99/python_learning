import logging
import os
from datetime import datetime
import pandas as pd

# 导入你的模块
from backtest.stocklist import StockPoolManager
from backtest import st_breakout, st_boxtheory, annual_stock_analysis # 假设你放在 strategies 文件夹下
from tools import download_tdx
# from data_module import update_daily_data # 假设你的数据下载模块

# --- 配置区 ---
OUTPUT_PATH = "./data/stock_pools/"
TDX_BLOCK_PATH = "C:/new_tdx/T0002/blocknew/"
LOG_FILE = f"./logs/pipeline_{datetime.now().strftime('%Y%m%d')}.log"

# 配置日志
os.makedirs("./logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.FileHandler(LOG_FILE, encoding='utf-8'), logging.StreamHandler()]
)

def price_loader(code, start_date):
    """
    这里对接你第一步下载的本地数据文件
    """
    file_path = f"./data/daily_kline/{code}.csv"
    if os.path.exists(file_path):
        df = pd.read_csv(file_path, index_col='date', parse_dates=True)
        return df.loc[start_date:]
    return None

def run_pipeline():
    logging.info("=== 每日流水线开始执行 ===")
    
    try:
        # 第一步：数据更新
        logging.info("步骤 1: 正在更新本地行情数据...")
        download_tdx.download_stock()
        download_tdx.download_sector(sectorlist=['concept', 'l1', 'l2', 'l3'])
        logging.info("数据更新完成。")

        # 初始化股票池管理器
        manager = StockPoolManager()

        # 第二步：策略并行筛选
        # 策略 A: 点火突破
        logging.info("步骤 2.1: 运行 [点火突破] 策略...")
        df_breakout = st_breakout.TrendStrategyTerm().run_strategy() # 假设返回含有 code, name 的 DF
        manager.add_to_pool(df_breakout, "点火突破")

        # 策略 B: 箱体理论
        #logging.info("步骤 2.2: 运行 [箱体突破] 策略...")
        #df_box = st_boxtheory.run_strategy()
        #manager.add_to_pool(df_box, "箱体突破")

        # 策略 C: 年度涨幅 TOP300
        #logging.info("步骤 2.3: 运行 [年度涨幅TOP300] 策略...")
        #df_top300 = annual_stock_analysis.run_strategy()
        #manager.add_to_pool(df_top300, "年度涨幅TOP300")

        # 第三步：同步与指标更新 (核心维护)
        logging.info("步骤 3: 正在进行股票池同步与价格指标更新...")
        # 该函数会处理：通达信手动同步 -> 价格指标补全 -> 自动出池判定 -> 写回文件
        manager.sync_and_update(price_loader)
        
        logging.info("=== 每日流水线执行成功 ===")

    except Exception as e:
        logging.error(f"流水线执行过程中发生错误: {str(e)}", exc_info=True)

if __name__ == "__main__":
    run_pipeline()