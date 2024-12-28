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
 
    def buy(self, data=None, size=0, **kwargs):
        '''
        buy订单提交前需要进行资金检查, 如果资金不足, 则需要取消订单
        '''
        ordtype = kwargs.get('ordertype', None)
        if ordtype == 'BKT':
            lmtPrice=kwargs.get('price')
            takeProfitPrice=kwargs.pop('takeProfitPrice')
            stopLossPrice=kwargs.pop('stopLossPrice')

            newkwargs = dict(lmtPrice=lmtPrice, 
                             action='BUY',
                             orderType='LMT', 
                             transmit=False)
            newkwargs.update(kwargs)
            parent = self.broker.buy(self,data,size,**newkwargs)

            newkwargs = dict(lmtPrice=takeProfitPrice, 
                             action='SELL',
                             orderType='LMT', 
                             parentId=parent.orderId,
                             parent=parent,
                             transmit=False)
            newkwargs.update(kwargs)
            takeprofit = self.broker.sell(self,data,size,**newkwargs)

            newkwargs = dict(auxPrice=stopLossPrice, 
                             action='SELL',
                             orderType='STP', 
                             parentId=parent.orderId,
                             parent=parent,
                             transmit=True)
            newkwargs.update(kwargs)
            stoploss = self.broker.sell(self,data,size, **kwargs)

            return [parent, takeprofit, stoploss]
        else:
            if ordtype == 'LMT':
                kwargs['lmtPrice'] = kwargs.get('price') 
            elif ordtype == 'STP':
                kwargs['auxPrice'] = kwargs.get('price')
            return self.broker.buy(self, data, size=size, **kwargs)


    def sell(self, data=None, size=0, **kwargs):
        '''
        sell订单提交前需要进行资金检查, 如果资金不足, 则需要取消订单
        '''
        ordtype = kwargs.get('ordertype', None)
        if ordtype == 'BKT':
            lmtPrice=kwargs.pop('lmtPrice')
            takeProfitPrice=kwargs.pop('takeProfitPrice')
            stopLossPrice=kwargs.pop('stopLossPrice')

            newkwargs = dict(lmtPrice=lmtPrice, 
                             action='SELL',
                             orderType='LMT', 
                             transmit=False)
            newkwargs.update(kwargs)
            parent = self.broker.sell(self,data,size,**newkwargs)

            newkwargs = dict(lmtPrice=takeProfitPrice, 
                             action='BUY',
                             orderType='LMT', 
                             parentId=parent.orderId,
                             transmit=False)
            newkwargs.update(kwargs)
            takeprofit = self.broker.buy(self,data,size,**newkwargs)

            newkwargs = dict(lmtPrice=stopLossPrice, 
                             action='BUY',
                             orderType='STP', 
                             transmit=True)
            newkwargs.update(kwargs)
            stoploss = self.broker.buy(self,data,size, **kwargs)

            return [parent, takeprofit, stoploss]
        else:
            if ordtype == 'LMT':
                kwargs['lmtPrice'] = kwargs.get('price') 
            elif ordtype == 'STP':
                kwargs['auxPrice'] = kwargs.get('price')
            return self.broker.sell(self, data, size=size, **kwargs)

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
        
        if possize > 0:
            return self.sell(data=data, size=size, **kwargs)
        elif possize < 0:
            return self.buy(data=data, size=size, **kwargs)

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            if order.status == order.Submitted:
                self.log('ORDER ACCEPTED/SUBMITTED')
            order.data.order = order
            return
        
        if order.status in [order.Expired]:
            self.log('ORDER EXPIRED')

        # Check if an order has been completed
        # Attention: broker could reject order if not enought cash
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log('ORDER BUY EXECUTED {}\tprice {:.2f}\tsize {:.2f}\tCost: {:.2f}\tComm: {:.2f}'.format(
                    order.data._name,
                    order.executed.price,
                    order.executed.size,
                    order.executed.value,
                    order.executed.comm))

            if order.issell():
                self.log('ORDER SELL EXECUTED {}\tprice {:.2f}\tsize{:.2f}\t Cost: {:.2f}\tComm: {:.2f}'.format(
                    order.data._name,
                    order.executed.price,
                    order.executed.size,
                    order.executed.value,
                    order.executed.comm))
                
            pass

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

        # Write down: no pending order
        order.data.order = None
