from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import itertools
import backtrader as bt
from backtrader.order import Order

class Strategy(bt.Strategy):
    """
    Wrapper for `bt.Strategy` to log orders and perform other generic tasks.
    """

    params = {
        'riskfreerate': 0.046,
        'cheat_on_open': False,
        'verbose': True,
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

    def _convert_orderparams(self, orderType=None, price=0, kwargs=None):
        '''
        完成两两个功能：
        1.将IB Order 的ordertype转换为backtrader的exectype
        2.将backtrader的price转换为IB order的price类别 
        '''

        if orderType in['LMT', 'LIT']:
            kwargs['exectype'] = Order.Limit
            kwargs['lmtPrice'] = price
        elif orderType == 'STP LMT':
            kwargs['exectype'] = Order.StopLimit
            kwargs['lmtPrice'] = price
            kwargs['pricelimit'] = kwargs.get('auxPrice')
        elif orderType == 'STP':
            kwargs['exectype'] = Order.Stop
            kwargs['auxPrice'] = price
        elif orderType == 'MKT':
            kwargs['exectype'] = Order.Market
        elif orderType == 'TRAIL':
            kwargs['exectype'] = Order.StopTrail
            kwargs['trailStopPrice'] = price
        elif orderType == 'TRAIL LIMIT':
            kwargs['exectype'] = Order.StopTrailLimit
            kwargs['trailStopPrice'] = price
            kwargs['pricelimit'] = price - kwargs.get('lmtPriceOffset')

    def buy(self, data=None, size=0, price=0, orderType=None, **kwargs):
        '''
        buy订单提交前需要进行资金检查, 如果资金不足, 则需要取消订单
        '''
        assert orderType in ['MKT','LMT','LIT','STP','STP LMT','TRAIL','TRAIL LIMIT', 'BKT']
        if orderType == 'BKT':
            takeProfitPrice=kwargs.pop('takeProfitPrice')
            stopLossPrice=kwargs.pop('stopLossPrice')

            newkwargs = dict(action='BUY',
                             transmit=False)
            newkwargs.update(kwargs)
            self._convert_orderparams(orderType='LMT', price=price, kwargs=newkwargs)
            parent = self.broker.buy(self,data,size, price=price,orderType='LMT', **newkwargs)

            newkwargs = dict(action='SELL',
                             parentId=parent.orderId,
                             parent=parent,
                             transmit=False)
            newkwargs.update(kwargs)
            self._convert_orderparams(orderType='LMT', price=takeProfitPrice, kwargs=newkwargs)
            takeprofit = self.broker.sell(self,data,size,price=takeProfitPrice, orderType='LMT', **newkwargs)

            newkwargs = dict(action='SELL',       
                             parentId=parent.orderId,
                             parent=parent,
                             transmit=True)
            newkwargs.update(kwargs)
            self._convert_orderparams(orderType='STP', price=stopLossPrice, kwargs=newkwargs)
            stoploss = self.broker.sell(self,data,size, price=stopLossPrice, orderType='STP', **newkwargs)

            return [parent, takeprofit, stoploss]
        else:
            self._convert_orderparams(orderType, price, kwargs)
            return self.broker.buy(self, data, size=size, price=price, orderType=orderType,**kwargs)


    def sell(self, data=None, size=0, price=0, orderType=None, **kwargs):
        '''
        sell订单提交前需要进行资金检查, 如果资金不足, 则需要取消订单
        '''
        assert orderType in ['MKT','LMT','LIT','STP','STP LMT','TRAIL','TRAIL LIMIT', 'BKT']
        if orderType == 'BKT':
            takeProfitPrice=kwargs.pop('takeProfitPrice')
            stopLossPrice=kwargs.pop('stopLossPrice')

            newkwargs = dict(action='SELL',
                             transmit=False)
            newkwargs.update(kwargs)
            self._convert_orderparams(orderType='LMT', price=price, kwargs=newkwargs)
            parent = self.broker.sell(self,data,size, price=price, orderType='LMT',**newkwargs)

            newkwargs = dict(action='BUY',   
                             parentId=parent.orderId,
                             parent=parent,
                             transmit=False)
            newkwargs.update(kwargs)
            self._convert_orderparams(orderType='LMT', price=takeProfitPrice, kwargs=newkwargs)
            takeprofit = self.broker.buy(self,data,size, price=takeProfitPrice, orderType='LMT', **newkwargs)

            newkwargs = dict(action='BUY',
                             parentId=parent.orderId,
                             parent=parent,
                             transmit=True)
            newkwargs.update(kwargs)
            self._convert_orderparams(orderType='STP', price=stopLossPrice, kwargs=newkwargs)
            stoploss = self.broker.buy(self,data,size, price=stopLossPrice,orderType='STP', **kwargs)

            return [parent, takeprofit, stoploss]
        else:
            self._convert_orderparams(orderType, price, kwargs)
            return self.broker.sell(self, data, size=size, price=price, orderType=orderType, **kwargs)

    def close(self, data=None, size=0, price=0, orderType=None, **kwargs):
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
            return self.sell(data=data, size=size, price=price, orderType=orderType, **kwargs)
        elif possize < 0:
            return self.buy(data=data, size=abs(size), price=price, orderType=orderType, **kwargs)

    def notify_order(self, order):
        BUY_SELL = 'Buy' if order.isbuy() else 'Sell'

        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            self.log('{}: Order ref: {} / Type {} / Status {}'.format(
                order.data._name,
                order.ref, 
                BUY_SELL,
                order.getstatusname()))
            order.data.order = order
            return
        
        if order.status in [order.Expired]:
            self.log(f"{order.data._name}: " \
                     f"Order ref: {order.ref} / Type {BUY_SELL} / " \
                     f"Status {order.getstatusname()} / "\
                     f"Reason: Expired / ")

        # Check if an order has been completed
        # Attention: broker could reject order if not enought cash
        if order.status in [order.Completed]:
            self.log(f"{order.data._name}: " \
                     f"Order ref: {order.ref} / Type {BUY_SELL} / " \
                     f"Status {order.getstatusname()} / " \
                     f"Price: {order.executed.price:.6f} / " \
                     f"Size: {order.executed.size} / " )                

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            status_reason = {
                order.Canceled: 'Canceled',
                order.Margin: 'Margin Called',
                order.Rejected: 'Rejected'
            }
            self.log(f"{order.data._name}: " \
                     f"Order ref: {order.ref} / Type {BUY_SELL} / " \
                     f"Status {order.getstatusname()} / "\
                     f"Reason: {status_reason[order.status]} / ")

        # Write down: no pending order
        order.data.order = None
