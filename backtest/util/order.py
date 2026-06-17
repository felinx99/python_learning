from sortedcontainers import SortedDict
from dataclasses import dataclass


@dataclass
class DealStatus:
   """Class for keeping track of an item in inventory."""
   deal_count: int = 0
   deal_volume: int = 0
   deal_turnover: int = 0

   def update(self, volume=0, price=0):
       self.deal_count += 1
       self.deal_volume += volume
       self.deal_turnover += volume*price

class OrderNode:
    """订单节点：存储单个订单的明细，双向链表节点"""
    def __init__(self, ref_id, price, qty, side, timestamp):
        self.ref_id = ref_id        # 订单ID (对应 OrderID / BuyID / SellID)
        self.price = price
        self.qty = qty              # 当前剩余未成交数量
        self.side = side            # 'B' 或 'S'
        self.timestamp = timestamp
        self.next = None
        self.prev = None

class PriceLevelList:
    """价格级队列：维护相同价格下订单的双向链表，保证时间优先 (FIFO)"""
    def __init__(self, price):
        self.price = price
        self.head = None
        self.tail = None
        self.total_volume = 0       # 该档位总挂单量,通过接口维护，不可直接操作
        self.order_count = 0        # 该档位总订单数,通过接口维护，不可直接操作

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
        """O(1) 任意位置断开指针移除订单 (撤单或全额成交的关键)"""
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
        """
        O(1) 部分撤单（减仓）：仅减少订单数量。
        """
        node.qty -= cancel_qty
        self.total_volume -= cancel_qty

    def clear(self):
        """清空当前价格档位的所有底层节点指针"""
        self.head = None
        self.tail = None
        self.total_volume = 0
        self.order_count = 0

class OrderBook:
    """基于 Method 3 专门对接 L2 逐笔数据的订单薄状态机"""
    def __init__(self):
        self.bids = SortedDict(lambda x: -x)    # 买盘盘口 {price: PriceLevelList}
        self.asks = SortedDict()                # 卖盘盘口 {price: PriceLevelList}# 默认升序排列
        self.ref_id_map = {}                    # 全局 ID 哈希映射 {ref_id: OrderNode}
        self.deals = SortedDict() 

    def insert_order(self, ref_id, price, qty, side, timestamp):
        """处理委托：插入新订单"""
        if ref_id in self.ref_id_map:
            print(f"⚠️ [Insert Warning] ref_id {ref_id} exists")
            return  # 异常防重处理

        node = OrderNode(ref_id, price, qty, side, timestamp)
        self.ref_id_map[ref_id] = node
        
        book = self.bids if side == 'B' else self.asks
        if price not in book:
            book[price] = PriceLevelList(price)
        book[price].append(node)

    def cancel_order(self, ref_id, cancel_qty=None):
        """处理撤单：依靠哈希表实现平均 O(1) 精确销单"""
        if ref_id not in self.ref_id_map:
            return False  # 找不到说明之前可能已经完全成交了
        
        node = self.ref_id_map[ref_id]
        book = self.bids if node.side == 'B' else self.asks    
        price_list = book[node.price]

        if cancel_qty is None or cancel_qty >= node.qty:
            price_list.remove(node)

            if price_list.order_count == 0:
                del book[node.price]

            del self.ref_id_map[ref_id]
        else:
            price_list.reduce(node, cancel_qty)

        return True

    def execute_trade(self, ref_id, exec_qty, exec_price):
        """处理成交：根据成交明细里的明确 ID 扣减对应的挂单量"""
        if ref_id not in self.ref_id_map:
            print(f"⚠️ execute_trade ref_id {ref_id} not exists")
            return False
        
        node = self.ref_id_map[ref_id]
        book = self.bids if node.side == 'B' else self.asks
        price_list = book[node.price]

        #必须在订单薄操作前执行dealbook,有可能node.price不存在
        #不可用node.price计算成交额，因竞价阶段执行价格与node.price不一致
        if node.side == 'B':
            dealbook = self.deals
            if exec_price not in dealbook:
                dealbook[exec_price] = DealStatus()
            dealbook[exec_price].update(exec_qty, exec_price)

        if exec_qty > node.qty:
            print(f"⚠️ execute_trade ref_id {ref_id} qty not match. exec_qty:{exec_qty} node.qty:{node.qty}")
        
        if exec_qty >= node.qty:
            # 挂单被完全吃掉，直接从链表和哈希表中移除
            price_list.remove(node)
            if price_list.order_count == 0:
                del book[node.price]
            del self.ref_id_map[ref_id]
        else:
            # 部分成交，原地减少挂单剩余数量
            price_list.reduce(node, exec_qty)
            
        return True

    def get_topN_snapshot(self, side, n_levels=5):
        """获取当前五档盘口"""
        assert side in ['S', 'B'], f"Error: side type '{side}' Not Support"
        book = self.bids if side == 'B' else self.asks
        #sorted_prices = sorted(book.keys(), reverse=(side == 'B'))[:n_levels]

        sorted_prices = book.keys()[:n_levels]
            
        return [(p, book[p].total_volume, book[p].order_count) for p in sorted_prices]

    def get_orderbook_stats(self):
        """
        计算当前全盘的总委托量及成交量加权平均委托价
        """
        total_bid_vol = sum(level.total_volume for level in self.bids.values())
        total_ask_vol = sum(level.total_volume for level in self.asks.values())
        
        bid_sum_pv = sum(level.price * level.total_volume for level in self.bids.values())
        ask_sum_pv = sum(level.price * level.total_volume for level in self.asks.values())
        
        weight_bid_price = round((bid_sum_pv / total_bid_vol) if total_bid_vol > 0 else 0.0, 4)
        weight_ask_price = round((ask_sum_pv / total_ask_vol) if total_ask_vol > 0 else 0.0, 4)

        return total_bid_vol, total_ask_vol, weight_bid_price, weight_ask_price

    def get_deal_status(self):
        total_dealnum = sum(level.deal_count for level in self.deals.values())
        total_dealvolume = sum(level.deal_volume for level in self.deals.values())
        total_dealturnover = sum(level.deal_turnover for level in self.deals.values())

        return total_dealnum, total_dealvolume, total_dealturnover

    def calibrate_level(self, side, price, true_vol, true_count, timestamp):
        book = self.bids if side == 'B' else self.asks
    
        # 💡 核心优化 1：短路保护 SortedDict 频繁无效平衡
        if price not in book and true_vol <= 0:
            return

        # 如果价位存在，才清理旧的 ref_id 映射
        if price in book:
            p_list = book[price]
            curr = p_list.head
            while curr:
                if curr.ref_id in self.ref_id_map:
                    del self.ref_id_map[curr.ref_id]
                curr = curr.next
            p_list.clear()
        else:
            book[price] = PriceLevelList(price)

        # 💡 核心优化 2：按真实量决定剪枝还是保留
        if true_vol > 0:
            p_list = book[price]
            syn_id = f"syn_{side}_{int(price)}_{timestamp}"
            syn_node = OrderNode(syn_id, price, true_vol, side, timestamp)
            self.ref_id_map[syn_id] = syn_node
            p_list.head = syn_node
            p_list.tail = syn_node
            p_list.total_volume = true_vol
            p_list.order_count = true_count
        else:
            del book[price]

    def get_bbo_bid(self):
        return self.bids.peekitem(0)[0] if self.bids else 0

    def get_bbo_ask(self):        
        return self.asks.peekitem(0)[0] if self.asks else 0