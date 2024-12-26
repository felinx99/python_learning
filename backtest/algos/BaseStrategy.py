import itertools
import backtrader as bt

class Strategy(bt.Strategy):
    """
    Wrapper for `bt.Strategy` to log orders and perform other generic tasks.
    """

    params = {
        'riskfreerate': 0.046,
        'cheat_on_open': False,
        'verbose': True
    }

    def __init__(self, kwargs=None):
        bt.Strategy.__init__(self)
        self.order = None
        self.buyprice = None
        self.buycomm = None
        self.order_rejected = False
        self.verbose = self.params.verbose

    def log(self, txt, date=None):
        if self.verbose:
            date = date or self.data.datetime.date(0)
            print('{}, {}'.format(date.isoformat(), txt))

    def _stop(self):
        self.stop()

        for analyzer in itertools.chain(self.analyzers, self._slave_analyzers):
            analyzer._stop()

    
    def _updateOrderParams(self, kwargs):
        '''
        本函数完成从ib_incync的初始化参数到backtrader的order类的参数的转换
        ibbroker使用的是ib_insync的order类，需要兼容backtrader的order类,因此将
        ib_incync的初始化参数转化为backtrader的order类的参数,以完成后面的类初始化
        本函数需要在对象生成前调用，不能放在对象的__init__函数中
        ('owner', None), 
        ('data', None),
        ('size', None), 
        ('price', None), 
        ('pricelimit', None),
        ('exectype', None), 
        ('valid', None), 
        ('tradeid', 0), 
        ('oco', None),
        ('trailamount', None), 
        ('trailpercent', None),
        ('parent', None), 
        ('transmit', True),
        ('simulated', False),
        # To support historical order evaluation
        ('histnotify', False),
        ('orderId', 0),
        '''
        kwargs['totalQuantity'] = kwargs.get('size', 0)
        kwargs['price'] = kwargs.get('lmtPrice', 0)
        kwargs['exectype'] = \
                bt.brokers.ibbroker.IBOrder._IBOrdTypes.get(kwargs.get('orderType'), None)
        kwargs['pricelimit'] = kwargs.get('lmtPrice', 0)
 
    def buy(self, data=None, size=0, **kwargs):
        '''
        buy订单提交前需要进行资金检查, 如果资金不足, 则需要取消订单
        '''
        if size == 0:
            return None
        data = data if data is not None else self.datas[0]
        size = size if size != 0 else self.getposition(data).position
        self._updateOrderParams(kwargs)	
        return self.broker.buy(self, data, size=abs(size), **kwargs)


    def sell(self, data=None, size=0, **kwargs):
        '''
        sell订单提交前需要进行资金检查, 如果资金不足, 则需要取消订单
        '''
        if size == 0:
            return None
        data = data if data is not None else self.datas[0]
        size = size if size != 0 else self.getposition(data).position
        self._updateOrderParams(kwargs)	
        return self.broker.sell(self, data, size=abs(size), **kwargs)

    def close(self, data=None, size=0, **kwargs):
        '''
        close时不需要在submit订单前进行资金检查
        '''
        if size == 0:
            return None
        data = data if data is not None else self.datas[0]
        possize = self.getposition(data).position
        assert abs(size) <= abs(possize)
        size = size if size != 0 else possize
        self._updateOrderParams(kwargs)	
        
        if possize > 0:
            return self.sell(data=data, size=size, **kwargs)
        elif possize < 0:
            return self.buy(data=data, size=size, **kwargs)

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            return

        # Check if an order has been completed
        # Attention: broker could reject order if not enought cash
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log('BUY {}\tprice {:.2f}\tsize {:.2f}\tCost: {:.2f}\tComm: {:.2f}'.format(
                    order.data._name,
                    order.executed.price,
                    order.executed.size,
                    order.executed.value,
                    order.executed.comm))
                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            if order.issell():
                self.log('SELL {}\tprice {:.2f}\tsize{:.2f}\t Cost: {:.2f}\tComm: {:.2f}'.format(
                    order.data._name,
                    order.executed.price,
                    order.executed.size,
                    order.executed.value,
                    order.executed.comm))

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            status_reason = {
                order.Canceled: 'Canceled',
                order.Margin: 'Margin Called',
                order.Rejected: 'Rejected'
            }
            self.log('Order {}: {} {}'.format(
                status_reason[order.status],
                'BUY' if order.isbuy() else 'SELL',
                order.data._name
            ))
            self.log('Cash: {:.2f}, Order: {:.2f}'.format(self.broker.get_cash(),
                                                          (order.price or 0) * (order.size or 0)))
            self.order_rejected = True

        # Write down: no pending order
        self.order = None
