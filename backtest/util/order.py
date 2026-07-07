from sortedcontainers import SortedDict


class DealStatus:
    # 💡 优化 1：使用 __slots__ 彻底消灭每个实例的 __dict__ 字典，内存暴减 70%，实例化加速数倍
    __slots__ = ('deal_count', 'deal_volume', 'deal_turnover')
    def __init__(self, deal_count=0, deal_volume=0, deal_turnover=0):
        self.deal_count = deal_count
        self.deal_volume = deal_volume
        self.deal_turnover = deal_turnover

    def update(self, volume=0, price=0):
        self.deal_count += 1
        self.deal_volume += volume
        self.deal_turnover += volume * price

    def get(self):
        return self.deal_count, self.deal_volume, self.deal_turnover

class OrderNode:
    """订单节点：存储单个订单的明细，双向链表节点"""
    __slots__ = ('ref_id', 'price', 'qty', 'side', 'next', 'prev', 'price_list')
    def __init__(self, ref_id, price, qty, side):
        self.ref_id = ref_id        
        self.price = price
        self.qty = qty              
        self.side = side            
        self.next = None
        self.prev = None
        self.price_list = None

class PriceLevelList:
    """价格级队列：维护相同价格下订单的双向链表，保证时间优先 (FIFO)"""
    __slots__ = ('price', 'head', 'tail', 'total_volume', 'order_count')
    def __init__(self, price):
        self.price = price
        self.head = None
        self.tail = None
        self.total_volume = 0   # 该档位总挂单量,通过接口维护，不可直接操作      
        self.order_count = 0    # 该档位总订单数,通过接口维护，不可直接操作       

    def append(self, node):
        """O(1) 尾部插入新订单"""
        if not self.head:
            self.head = node
            self.tail = node
        else:
            self.tail.next = node
            node.prev = self.tail
            self.tail = node
        self.total_volume += node.qty
        self.order_count += 1

    def remove(self, node):
        """O(1) 任意位置断开指针移除订单"""
        if node.prev:
            node.prev.next = node.next
        else:
            self.head = node.next
        if node.next:
            node.next.prev = node.prev
        else:
            self.tail = node.prev
        self.total_volume -= node.qty
        self.order_count -= 1

    def reduce(self, node, cancel_qty):
        """O(1) 部分撤单（减仓）"""
        node.qty -= cancel_qty
        self.total_volume -= cancel_qty

    def clear(self):
        self.head = None
        self.tail = None
        self.total_volume = 0
        self.order_count = 0

class OrderBook:
    """基于 Method 3 专门对接 L2 逐笔数据的订单薄状态机"""
    def __init__(self):
        self.bids = SortedDict()    
        self.asks = SortedDict()                
        self.ref_id_map = {}                    
        self.deals = DealStatus() 

        self.total_bid_vol = 0
        self.total_ask_vol = 0
        self.bid_sum_pv = 0
        self.ask_sum_pv = 0

    def insert_order(self, ref_id, price, qty, side):
        """处理委托：插入新订单"""
        if ref_id in self.ref_id_map:
            return  

        node = OrderNode(ref_id, price, qty, side)
        self.ref_id_map[ref_id] = node
        
        # 💡 优化 5：增量累加总委托统计
        if side == 'B':
            self.total_bid_vol += qty
            self.bid_sum_pv += price * qty
            price_list = self.bids.get(price)
            if price_list is None:
                price_list = PriceLevelList(price)
                self.bids[price] = price_list
        else:
            self.total_ask_vol += qty
            self.ask_sum_pv += price * qty
            price_list = self.asks.get(price)
            if price_list is None:
                price_list = PriceLevelList(price)
                self.asks[price] = price_list

        price_list.append(node)
        node.price_list = price_list

    def cancel_order(self, ref_id, cancel_qty=None):
            """处理撤单"""
            node = self.ref_id_map.get(ref_id)
            if node is None:
                return False  
            
            side = node.side
            price = node.price   
            price_list = node.price_list

            if cancel_qty is None or cancel_qty >= node.qty:
                actual_cancel_qty = node.qty
                price_list.remove(node)
                if price_list.order_count == 0:
                    if side == 'B':
                        del self.bids[price]
                    else:
                        del self.asks[price]
                del self.ref_id_map[ref_id]
            else:
                actual_cancel_qty = cancel_qty
                price_list.reduce(node, cancel_qty)

            # 💡 增量扣减全盘统计
            if side == 'B':
                self.total_bid_vol -= actual_cancel_qty
                self.bid_sum_pv -= price * actual_cancel_qty
            else:
                self.total_ask_vol -= actual_cancel_qty
                self.ask_sum_pv -= price * actual_cancel_qty

            return True

    def execute_trade(self, ref_id, exec_qty, exec_price):
            """处理成交"""
            node = self.ref_id_map.get(ref_id)
            if node is None:
                return False
            
            side = node.side
            price = node.price
            price_list = node.price_list

            if side == 'B':
                self.deals.update(exec_qty, exec_price)

            # 无论全额还是部分成交，实际从挂单中扣除的量
            actual_exec_qty = node.qty if exec_qty >= node.qty else exec_qty

            if exec_qty >= node.qty:
                price_list.remove(node)
                if price_list.order_count == 0:
                    if side == 'B':
                        del self.bids[price]
                    else:
                        del self.asks[price]
                del self.ref_id_map[ref_id]
            else:
                price_list.reduce(node, exec_qty)
                
            # 💡 增量扣减全盘挂单统计（注意：挂单削减必须用挂单价格 price，而非成交价 exec_price）
            if side == 'B':
                self.total_bid_vol -= actual_exec_qty
                self.bid_sum_pv -= price * actual_exec_qty
            else:
                self.total_ask_vol -= actual_exec_qty
                self.ask_sum_pv -= price * actual_exec_qty

            return True

    def get_topN_snapshot(self, side, n_levels=5):
        """获取当前五档盘口"""
        if side == 'B':
            book = self.bids
            keys = book.keys()
            n = len(keys)
            if n == 0: return []
            sorted_prices = keys[max(0, n - n_levels):][::-1]
        else:
            book = self.asks
            sorted_prices = book.keys()[:n_levels]
            
        return [(p, book[p].total_volume, book[p].order_count) for p in sorted_prices]

    def get_orderbook_stats(self):
        weight_bid_price = round((self.bid_sum_pv / self.total_bid_vol) if self.total_bid_vol > 0 else 0.0, 4)
        weight_ask_price = round((self.ask_sum_pv / self.total_ask_vol) if self.total_ask_vol > 0 else 0.0, 4)
        return self.total_bid_vol, self.total_ask_vol, weight_bid_price, weight_ask_price

    def get_deal_status(self):
        return self.deals.get()


    def calibrate_level(self, side, price, true_vol, true_count):
        """快照对账与基础裁剪"""
        book = self.bids if side == 'B' else self.asks
    
        if price not in book and true_vol <= 0:
            return

        if price in book:
            p_list = book[price]
            # 💡 修正增量指标
            if side == 'B':
                self.total_bid_vol -= p_list.total_volume
                self.bid_sum_pv -= price * p_list.total_volume
            else:
                self.total_ask_vol -= p_list.total_volume
                self.ask_sum_pv -= price * p_list.total_volume

            curr = p_list.head
            while curr:
                if curr.ref_id in self.ref_id_map:
                    del self.ref_id_map[curr.ref_id]
                curr = curr.next
            p_list.clear()
        else:
            p_list = PriceLevelList(price)
            book[price] = p_list

        if true_vol > 0:
            syn_id = f"syn_{side}_{int(price)}"
            syn_node = OrderNode(syn_id, price, true_vol, side)
            self.ref_id_map[syn_id] = syn_node
            p_list.head = syn_node
            p_list.tail = syn_node
            p_list.total_volume = true_vol
            p_list.order_count = true_count

            if side == 'B':
                self.total_bid_vol += true_vol
                self.bid_sum_pv += price * true_vol
            else:
                self.total_ask_vol += true_vol
                self.ask_sum_pv += price * true_vol
        else:
            del book[price]

    def get_bbo_bid(self):
        return self.bids.peekitem(-1)[0] if self.bids else 0

    def get_bbo_ask(self):        
        return self.asks.peekitem(0)[0] if self.asks else 0
