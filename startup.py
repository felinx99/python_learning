# 添加模块所在目录的绝对路径,然后导入模块正常了。否则报错(未安装backtrader) 
import sys
sys.path.append(r'E:\gitcode\backtrader')

from backtest import run

# args list:
    # strategy: CrossOver, BuyAndHold, WeightedHold, NCAV
    # universe: 'sp500', 'faang', 'sp500_tech', 'sp100', 'forex_m7'
    # tickers: ['AAPL', 'BABA', 'IBKR']
    # start: '2020-01-01'
    # end: '2020-01-01'
    # kwargs: Invalid, Additional arguments to pass through to the strategy
runkwargs = dict(
    strategy='CrossOver',
    universe='forex_m7',
    start='2015-01-01',
    end='2023-12-31',
    plot=True,
    
)

run.run_backtest_offline(**runkwargs)