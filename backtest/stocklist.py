import pandas as pd
from pathlib import Path
from datetime import datetime
from common import CONFIG

# 配置路径
OUTPUT_PATH = CONFIG.base_path['STOCK_OUTPUT_PATH']
TDX_BLOCK_PATH = "C:/new_tdx/T0002/blocknew/"  # 请根据实际路径修改
POOL_FILE = OUTPUT_PATH/"selection_pool.csv"
DELETED_FILE = OUTPUT_PATH/"deleted_pool.csv"

class StockPoolManager:
    def __init__(self):
        assert POOL_FILE.exists(), f"Error: '{POOL_FILE}'"
        assert DELETED_FILE.exists(), f"Error: '{DELETED_FILE}'"
        self.selection_df = self._load_csv(POOL_FILE)
        self.deleted_df = self._load_csv(DELETED_FILE)

    def _load_csv(self, path):
        return pd.read_csv(path, dtype={'股票代码': str})

    def _save_csv(self):
        self.selection_df.to_csv(POOL_FILE, index=False, encoding='utf_8_sig')
        self.deleted_df.to_csv(DELETED_FILE, index=False, encoding='utf_8_sig')

    # --- 通达信同步功能 ---
    def _read_tdx_blk(self, filename):
        """读取通达信.blk文件内容"""
        path = os.path.join(TDX_BLOCK_PATH, filename)
        if not os.path.exists(path): return []
        with open(path, 'r') as f:
            # 通达信代码格式通常为: 市场代码+股票代码 (如 0000001, 1600000)
            return [line.strip()[-6:] for line in f.readlines() if line.strip()]

    def _write_tdx_blk(self, filename, code_list):
        """将代码列表写入通达信.blk"""
        path = os.path.join(TDX_BLOCK_PATH, filename)
        with open(path, 'w') as f:
            for code in code_list:
                # 简单市场判定逻辑 (深市0, 沪市1)
                prefix = '1' if code.startswith(('6', '9')) else '0'
                f.write(f"{prefix}{code}\n")

    # --- 核心逻辑：入池 ---
    def add_to_pool(self, code, name, reason):
        """
        满足入池条件逻辑
        """
        now = datetime.now().strftime('%Y-%m-%d')
        
        # 如果已在待选池，则合并（更新原因或跳过）
        if not self.selection_df.empty and code in self.selection_df['股票代码'].values:
            idx = self.selection_df[self.selection_df['股票代码'] == code].index
            self.selection_df.loc[idx, '入池原因'] += f" | {reason}"
            return

        # 如果在删除池，则先从删除池移出
        if not self.deleted_df.empty and code in self.deleted_df['股票代码'].values:
            self.deleted_df = self.deleted_df[self.deleted_df['股票代码'] != code]

        # 初始化数据行
        new_row = {
            '股票代码': code, '股票名称': name, '入池时间': now, '入池原因': reason,
            '最低价': None, '最低价日期': None, '最高价': None, '最高价日期': None,
            '最大回撤': 0.0, '最大回撤日期': None, '最高点至今间隔': 0
        }
        self.selection_df = pd.concat([self.selection_df, pd.DataFrame([new_row])], ignore_index=True)

    # --- 核心逻辑：出池 ---
    def remove_from_pool(self, code, reason):
        """
        满足出池条件逻辑
        """
        if self.selection_df.empty or code not in self.selection_df['股票代码'].values:
            return

        # 提取数据并转入删除池
        row = self.selection_df[self.selection_df['股票代码'] == code].iloc[0].to_dict()
        row['出池原因'] = reason
        row.pop('最高点至今间隔', None) # 删除不需要的列
        
        self.deleted_df = pd.concat([self.deleted_df, pd.DataFrame([row])], ignore_index=True)
        self.selection_df = self.selection_df[self.selection_df['股票代码'] != code]

    # --- 自动化更新逻辑 ---
    def sync_and_update(self, price_provider_func):
        """
        1. 更新池内股票价格特征指标
        2. 处理出池条件判断
        3. 处理通达信手动增删同步
        """
        today = datetime.now()

        # A. 更新价格特征指标 (需要外部传入获取历史数据的函数)
        for idx, row in self.selection_df.iterrows():
            code = row['股票代码']
            entry_date = row['入池时间']
            # 获取从入池日期至今的K线数据
            hist_data = price_provider_func(code, entry_date) 
            
            if not hist_data.empty:
                max_p = hist_data['close'].max()
                max_d = hist_data['close'].idxmax()
                min_p = hist_data['close'].min()
                min_d = hist_data['close'].idxmin()
                
                # 计算最大回撤 (从入池后的最高点开始算)
                post_max_data = hist_data.loc[max_d:]
                current_drawdown = (post_max_data['close'].min() - max_p) / max_p
                
                self.selection_df.at[idx, '最高价'] = max_p
                self.selection_df.at[idx, '最高价日期'] = max_d
                self.selection_df.at[idx, '最低价'] = min_p
                self.selection_df.at[idx, '最低价日期'] = min_d
                self.selection_df.at[idx, '最大回撤'] = current_drawdown
                self.selection_df.at[idx, '最高点至今间隔'] = (today - pd.to_datetime(max_d)).days

        # B. 出池条件判定
        # 条件1 & 2：时间超时或回撤过大
        to_exit_1 = self.selection_df[self.selection_df['最高点至今间隔'] > 60]
        for code in to_exit_1['股票代码']: self.remove_from_pool(code, "最高点间隔超过60天")
        
        to_exit_2 = self.selection_df[self.selection_df['最大回撤'] < -0.4] # 注意回撤是负数
        for code in to_exit_2['股票代码']: self.remove_from_pool(code, "回撤超过40%")

        # C. 通达信手动同步逻辑
        tdx_codes = self._read_tdx_blk("股票待选池.blk")
        csv_codes = self.selection_df['股票代码'].tolist() if not self.selection_df.empty else []

        # 手动添加：通达信有，CSV没有
        for c in tdx_codes:
            if c not in csv_codes:
                self.add_to_pool(c, "未知(手动)", "手工添加")

        # 手动删除：CSV有，通达信没有
        for c in csv_codes:
            if c not in tdx_codes:
                self.remove_from_pool(c, "手工删除")

        # 最后同步写回通达信
        self._write_tdx_blk("股票待选池.blk", self.selection_df['股票代码'].tolist())
        self._write_tdx_blk("删除板块池.blk", self.deleted_df['股票代码'].tolist())
        
        self._save_csv()