import datetime
import threading
import queue
import time
import multiprocessing
import sys
import itertools
import matplotlib.pyplot as plt
import os
from jinja2 import Environment, FileSystemLoader

import backtrader as bt

def timestamp2str(ts):
    """ Converts Timestamp object to str containing date and time
    """
    date = ts.date().strftime("%Y-%m-%d")
    time = ts.time().strftime("%H:%M:%S")
    return ' '.join([date, time])


def get_now():
    """ Return current datetime as str
    """
    return timestamp2str(datetime.now())


def dir_exists(foldername):
    """ Return True if folder exists, else False
    """
    return os.path.isdir(foldername)

class PerformanceReport:
    """ Report with performce stats for given backtest run
    """

    def __init__(self, stratbt, infilename,
                 outputdir, user, memo):
        self.stratbt = stratbt  # works for only 1 stategy
        self.infilename = infilename
        self.outputdir = outputdir
        self.user = user
        self.memo = memo
        self.check_and_assign_defaults()

    def check_and_assign_defaults(self):
        """ Check initialization parameters or assign defaults
        """
        if not self.infilename:
            self.infilename = 'Not given'
        if not dir_exists(self.outputdir):
            msg = "*** ERROR: outputdir {} does not exist."
            print(msg.format(self.outputdir))
            sys.exit(0)
        if not self.user:
            self.user = 'Happy Canary'
        if not self.memo:
            self.memo = 'No comments'

    def get_performance_stats(self):
        """ Return dict with performace stats for given strategy withing backtest
        """
        st = self.stratbt
        dt = st.data._dataname['open'].index
        trade_analysis = st.analyzers.myTradeAnalysis.get_analysis()
        rpl = trade_analysis.pnl.net.total
        total_return = rpl / self.get_startcash()
        total_number_trades = trade_analysis.total.total
        trades_closed = trade_analysis.total.closed
        bt_period = dt[-1] - dt[0]
        bt_period_days = bt_period.days
        drawdown = st.analyzers.myDrawDown.get_analysis()
        sharpe_ratio = st.analyzers.mySharpe.get_analysis()['sharperatio']
        sqn_score = st.analyzers.mySqn.get_analysis()['sqn']
        kpi = {# PnL
               'start_cash': self.get_startcash(),
               'rpl': rpl,
               'result_won_trades': trade_analysis.won.pnl.total,
               'result_lost_trades': trade_analysis.lost.pnl.total,
               'profit_factor': (-1 * trade_analysis.won.pnl.total / trade_analysis.lost.pnl.total),
               'rpl_per_trade': rpl / trades_closed,
               'total_return': 100 * total_return,
               'annual_return': (100 * (1 + total_return)**(365.25 / bt_period_days) - 100),
               'max_money_drawdown': drawdown['max']['moneydown'],
               'max_pct_drawdown': drawdown['max']['drawdown'],
               # trades
               'total_number_trades': total_number_trades,
               'trades_closed': trades_closed,
               'pct_winning': 100 * trade_analysis.won.total / trades_closed,
               'pct_losing': 100 * trade_analysis.lost.total / trades_closed,
               'avg_money_winning': trade_analysis.won.pnl.average,
               'avg_money_losing':  trade_analysis.lost.pnl.average,
               'best_winning_trade': trade_analysis.won.pnl.max,
               'worst_losing_trade': trade_analysis.lost.pnl.max,
               #  performance
               'sharpe_ratio': sharpe_ratio,
               'sqn_score': sqn_score,
               'sqn_human': self._sqn2rating(sqn_score)
               }
        return kpi

    def get_equity_curve(self):
        """ Return series containing equity curve
        """
        st = self.stratbt
        dt = st.data._dataname['open'].index
        value = st.observers.broker.lines[1].array[:len(dt)]
        curve = pd.Series(data=value, index=dt)
        return 100 * curve / curve.iloc[0]

    def _sqn2rating(self, sqn_score):
        """ Converts sqn_score score to human readable rating
        See: http://www.vantharp.com/tharp-concepts/sqn.asp
        """
        if sqn_score < 1.6:
            return "Poor"
        elif sqn_score < 1.9:
            return "Below average"
        elif sqn_score < 2.4:
            return "Average"
        elif sqn_score < 2.9:
            return "Good"
        elif sqn_score < 5.0:
            return "Excellent"
        elif sqn_score < 6.9:
            return "Superb"
        else:
            return "Holy Grail"

    def __str__(self):
        msg = ("*** PnL: ***\n"
               "Start capital         : {start_cash:4.2f}\n"
               "Total net profit      : {rpl:4.2f}\n"
               "Result winning trades : {result_won_trades:4.2f}\n"
               "Result lost trades    : {result_lost_trades:4.2f}\n"
               "Profit factor         : {profit_factor:4.2f}\n"
               "Total return          : {total_return:4.2f}%\n"
               "Annual return         : {annual_return:4.2f}%\n"
               "Max. money drawdown   : {max_money_drawdown:4.2f}\n"
               "Max. percent drawdown : {max_pct_drawdown:4.2f}%\n\n"
               "*** Trades ***\n"
               "Number of trades      : {total_number_trades:d}\n"
               "    %winning          : {pct_winning:4.2f}%\n"
               "    %losing           : {pct_losing:4.2f}%\n"
               "    avg money winning : {avg_money_winning:4.2f}\n"
               "    avg money losing  : {avg_money_losing:4.2f}\n"
               "    best winning trade: {best_winning_trade:4.2f}\n"
               "    worst losing trade: {worst_losing_trade:4.2f}\n\n"
               "*** Performance ***\n"
               "Sharpe ratio          : {sharpe_ratio:4.2f}\n"
               "SQN score             : {sqn_score:4.2f}\n"
               "SQN human             : {sqn_human:s}"
               )
        kpis = self.get_performance_stats()
        # see: https://stackoverflow.com/questions/24170519/
        # python-# typeerror-non-empty-format-string-passed-to-object-format
        kpis = {k: -999 if v is None else v for k, v in kpis.items()}
        return msg.format(**kpis)

    def plot_equity_curve(self, fname='equity_curve.png'):
        """ Plots equity curve to png file
        """
        curve = self.get_equity_curve()
        buynhold = self.get_buynhold_curve()
        xrnge = [curve.index[0], curve.index[-1]]
        dotted = pd.Series(data=[100, 100], index=xrnge)
        fig, ax = plt.subplots(1, 1)
        ax.set_ylabel('Net Asset Value (start=100)')
        ax.set_title('Equity curve')
        _ = curve.plot(kind='line', ax=ax)
        _ = buynhold.plot(kind='line', ax=ax, color='grey')
        _ = dotted.plot(kind='line', ax=ax, color='grey', linestyle=':')
        return fig

    def _get_periodicity(self):
        """ Maps length backtesting interval to appropriate periodiciy for return plot
        """
        curve = self.get_equity_curve()
        startdate = curve.index[0]
        enddate = curve.index[-1]
        time_interval = enddate - startdate
        time_interval_days = time_interval.days
        if time_interval_days > 5 * 365.25:
            periodicity = ('Yearly', 'Y')
        elif time_interval_days > 365.25:
            periodicity = ('Monthly', 'M')
        elif time_interval_days > 50:
            periodicity = ('Weekly', '168H')
        elif time_interval_days > 5:
            periodicity = ('Daily', '24H')
        elif time_interval_days > 0.5:
            periodicity = ('Hourly', 'H')
        elif time_interval_days > 0.05:
            periodicity = ('Per 15 Min', '15M')
        else: periodicity = ('Per minute', '1M')
        return periodicity

    def plot_return_curve(self, fname='return_curve.png'):
        """ Plots return curve to png file
        """
        curve = self.get_equity_curve()
        period = self._get_periodicity()
        values = curve.resample(period[1]).ohlc()['close']
        # returns = 100 * values.diff().shift(-1) / values
        returns = 100 * values.diff() / values
        returns.index = returns.index.date
        is_positive = returns > 0
        fig, ax = plt.subplots(1, 1)
        ax.set_title("{} returns".format(period[0]))
        ax.set_xlabel("date")
        ax.set_ylabel("return (%)")
        _ = returns.plot.bar(color=is_positive.map({True: 'green', False: 'red'}), ax=ax)
        return fig

    def generate_html(self):
        """ Returns parsed HTML text string for report
        """
        basedir = os.path.abspath(os.path.dirname(__file__))
        images = os.path.join(basedir, 'templates')
        eq_curve = os.path.join(images, 'equity_curve.png')
        rt_curve = os.path.join(images, 'return_curve.png')
        fig_equity = self.plot_equity_curve()
        fig_equity.savefig(eq_curve)
        fig_return = self.plot_return_curve()
        fig_return.savefig(rt_curve)
        env = Environment(loader=FileSystemLoader('.'))
        template = env.get_template("templates/template.html")
        header = self.get_header_data()
        kpis = self.get_performance_stats()
        graphics = {'url_equity_curve': 'file://' + eq_curve,
                    'url_return_curve': 'file://' + rt_curve
                    }
        all_numbers = {**header, **kpis, **graphics}
        html_out = template.render(all_numbers)
        return html_out

    def generate_pdf_report(self):
        """ Returns PDF report with backtest results
        """
        html = self.generate_html()
        outfile = os.path.join(self.outputdir, 'report.pdf')
        HTML(string=html).write_pdf(outfile)
        msg = "See {} for report with backtest results."
        print(msg.format(outfile))

    def get_strategy_name(self):
        return self.stratbt.__class__.__name__

    def get_strategy_params(self):
        return self.stratbt.cerebro.strats[0][0][-1]

    def get_start_date(self):
        """ Return first datafeed datetime
        """
        dt = self.stratbt.data._dataname['open'].index
        return timestamp2str(dt[0])

    def get_end_date(self):
        """ Return first datafeed datetime
        """
        dt = self.stratbt.data._dataname['open'].index
        return timestamp2str(dt[-1])

    def get_header_data(self):
        """ Return dict with data for report header
        """
        header = {'strategy_name': self.get_strategy_name(),
                  'params': self.get_strategy_params(),
                  'file_name': self.infilename,
                  'start_date': self.get_start_date(),
                  'end_date': self.get_end_date(),
                  'name_user': self.user,
                  'processing_date': get_now(),
                  'memo_field': self.memo
                  }
        return header

    def get_series(self, column='close'):
        """ Return data series
        """
        return self.stratbt.data._dataname[column]

    def get_buynhold_curve(self):
        """ Returns Buy & Hold equity curve starting at 100
        """
        s = self.get_series(column='open')
        return 100 * s / s[0]

    def get_startcash(self):
        return self.stratbt.broker.startingcash



class NewCerebro(bt.Cerebro):
    
    params = (
        ('run_mode', 'backtest'),
    )

    def __init__(self, **kwds):
        super().__init__(**kwds)
        self.cerebordatetime = datetime.datetime.now()  #用于判断定时器的时间

        self.dailytimer = self.add_timer(when=datetime.time(12,00), repeat=datetime.timedelta(minutes=5),)
        self.weeklytimer = self.add_timer(bt.timer.SESSION_END, weekday=[1,3], weekcarry=True)
        self.monthlytimer = self.add_timer(bt.timer.SESSION_END, monthdays=[1], monthcarry=True)

        self._stopThreadRunOnlineData_event = threading.Event()
        
        self._stopThreadOnTimer_event = threading.Event()
        self._startThreadOnTimer_event = threading.Event()

        self._stopThreadRunOnlineData_semaphore = threading.Semaphore(0) 

    

    def add_report_analyzers(self, riskfree=0.01):
            """ Adds performance stats, required for report
            """
            self.addanalyzer(bt.analyzers.SharpeRatio,
                             _name="mySharpe",
                             riskfreerate=riskfree,
                             timeframe=bt.TimeFrame.Months)
            self.addanalyzer(bt.analyzers.DrawDown,
                             _name="myDrawDown")
            self.addanalyzer(bt.analyzers.AnnualReturn,
                             _name="myReturn")
            self.addanalyzer(bt.analyzers.TradeAnalyzer,
                             _name="myTradeAnalysis")
            self.addanalyzer(bt.analyzers.SQN,
                             _name="mySqn")

    def get_strategy_backtest(self):
        return self.runstrats[0][0]

    def report(self, outputdir,
               infilename=None, user=None, memo=None):
        bt = self.get_strategy_backtest()
        rpt =PerformanceReport(bt, infilename=infilename,
                               outputdir=outputdir, user=user,
                               memo=memo)
        rpt.generate_pdf_report()

    def notify_timer(self, timer, when, *args, **kwargs):
        #print('strategy notify_timer with tid {}, when {} cheat {}'.
        #      format(timer.p.tid, when, timer.p.cheat))
        _, isowk, isowkday = self.cerebordatetime.date().isocalendar()

        if timer == self.dailytimer:
            txt = 'dailytimer {}, Week {}, Day {}'.format(
            self.cerebordatetime, isowk, isowkday,)
            #print(txt)
        elif timer == self.weeklytimer:
            txt = 'weeklytimer{}, Week {}, Day {}'.format(
            self.cerebordatetime, isowk, isowkday,)
            #print(txt)
        elif timer == self.monthlytimer:
            txt = 'monthlytimer{}, Week {}, Day {}'.format(
            self.cerebordatetime,isowk, isowkday,)
            #print(txt)
        else:
            pass
            #print("run in notify_timer, no timer match")
    
    def stop_barupdate(self):
        self._stopThreadRunOnlineData_event.set()

    def start_barupdate(self, item=None, timeout=1):   
        self._stopThreadRunOnlineData_semaphore.release()

    def stop_onTimer(self):
        self._stopThreadOnTimer_event.set()

    def start_onTimer(self):
        self._startThreadOnTimer_event.set()
    
    def _RunOnlineData(self):
        while not self._stopThreadRunOnlineData_event.is_set():
            # 等待信号
            try:
                self._stopThreadRunOnlineData_semaphore.acquire()
            
                # 信号触发，对齐数据
                datas = sorted(self.datas,
                       key=lambda x: len(x), reverse=True)
                data0 = datas[0]
                d0ret = sum(len(d) == len(data0) for d in datas)

                hasDataAlready = not d0ret or d0ret == len(datas)

                #等待数据集齐
                if hasDataAlready:
                    print(f"数据集齐，开始处理数据")
                    self.runstrategieskenel()
       
            except queue.Empty: # 重置信号标志，准备下次等待
                print("队列为空，消费者等待...")
                time.sleep(1)
            
       
        return self.runstrats

    def _DealOnTimer(self):
        print("run in DealOnTimer")
        while not self._stopThreadOnTimer_event.is_set():
            # 等待信号
            self._startThreadOnTimer_event.wait()
            
            # 信号触发，执行代码
            print("Signal received, executing code...")
       
            # 重置信号标志，准备下次等待
            self._startThreadOnTimer_event.clear() 
            
    def _run_online_start(self):
        # 创建新线程1，用于处理新数据
        run_onlineData_thread = threading.Thread(target=self._RunOnlineData, name='RunOnlineDataThread')
        run_onlineData_thread.start()
        # 创建新线程2， 用于处理定时器响应on_timer
        onTimer_thread = threading.Thread(target=self._DealOnTimer, name='OnTimerThread')
        onTimer_thread.start() 

    def run_online(self):
        #kwargs['predata'] = True    #在线方式，默认提前加载数据
        #kwargs['preload'] = True    #在线方式，默认提前加载数据
        self._run_online_start() 
        return None 
    

    def runstrategies(self, iterstrat, predata=False):
        self.prerunstrategies(iterstrat=iterstrat, predata=predata)
        self.runstrategieskenel()
        #self.finishrunstrategies(predata=predata)
        return self.runningstrats
    
    def run(self, **kwargs):
        '''The core method to perform backtesting. Any ``kwargs`` passed to it
        will affect the value of the standard parameters ``Cerebro`` was
        instantiated with.

        If ``cerebro`` has not datas the method will immediately bail out.

        It has two different execution modes:
            Offline Mode: The data length is fixed. For all data, the strategy
                completes all calculations in a single run and returns the 
                result.
                Explanation: In offline mode, the system processes all 
                            available data at once without waiting for new data
                            to arrive. This is typically used when the dataset
                            is complete and there's no need for real-time updates.
            Online Mode: The data length is infinite. New data is acquired in
                real time. The strategy initially completes calculations on the
                existing data and then suspends until new data arrives. When new
                data is generated, the strategy performs calculations on it. The
                calculation results are output periodically using a timer function.
                Explanation: In online mode, the system continuously processes
                            data as it becomes available. The strategy starts
                            with the initial dataset and then updates its
                            calculations whenever new data is received. This
                            mode is suitable for applications that require
                            real-time analysis or continuous monitoring.
        '''
        
        
        run_mode = kwargs.get('run_mode')          
        if run_mode != 'backtest':
            self.prerun(**kwargs)
            self.startrun()
            #self.finishrun()
            self.run_online() 
            if not self._dooptimize:
                # avoid a list of list for regular cases
                rets = self.runstrats[0]
            rets = self.runstrats
        else:
            rets = super().run(**kwargs)
        
        return rets