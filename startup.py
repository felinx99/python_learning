import builtins
import sys
import os
import traceback
# 添加模块所在目录的绝对路径,然后导入模块正常了。否则报错(未安装backtrader) 
import sys
sys.path.append(r'E:\gitcode\backtrader')

from backtest import run

class PrintOverride:
    def __init__(self):
        self.original_print = builtins.print
        
    def custom_print(self, *args, **kwargs):
        stack = traceback.extract_stack()
        caller = stack[-2]
        
        # 构建调用信息
        filename = os.path.basename(caller.filename)
        caller_info = f"[{filename}:{caller.lineno}]"
        
        # 保存原始输出目标
        original_stdout = kwargs.get('file', sys.stdout)
        
        try:
            # 添加调用信息并打印
            if 'file' in kwargs:
                del kwargs['file']
            self.original_print(caller_info, *args, file=original_stdout, **kwargs)
            
        except Exception as e:
            self.original_print(f"Print error: {e}", file=sys.stderr)
            
    def __enter__(self):
        builtins.print = self.custom_print
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        builtins.print = self.original_print

# args list:
    # strategy: CrossOver, BuyAndHold, WeightedHold, NCAV
    # universe: 'sp500', 'faang', 'sp500_tech', 'sp100', 'forex_m7'
    # tickers: ['AAPL', 'BABA', 'IBKR']
    # start: '2020-01-01'
    # end: '2020-01-01'
    # kwargs: Invalid, Additional arguments to pass through to the strategy

strategy_args = dict(
    cheat_on_open = True,
    onlinemode = True,
)
runkwargs = dict(
    strategy='CrossOver',
    tickers= ['EURUSD',],
    start='2020-09-01',
    end='2021-12-31',
    plot=False,
    verbose = True,
    kwargs=strategy_args,
)

# 使用示例
if __name__ == '__main__':
    # 方法1：全局替换
    # builtins.print = PrintOverride().custom_print
    run.run_realtime(**runkwargs) 