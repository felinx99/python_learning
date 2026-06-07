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
        self.total_volume = 0       # 该档位总挂单量
        self.order_count = 0        # 该档位总订单数

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

    def clear(self):
        """清空当前价格档位的所有底层节点指针"""
        self.head = None
        self.tail = None
        self.total_volume = 0
        self.order_count = 0

class OrderBook:
    """基于 Method 3 专门对接 L2 逐笔数据的订单薄状态机"""
    def __init__(self):
        self.bids = {}          # 买盘盘口 {price: PriceLevelList}
        self.asks = {}          # 卖盘盘口 {price: PriceLevelList}
        self.ref_id_map = {}    # 全局 ID 哈希映射 {ref_id: OrderNode}

    def insert_order(self, ref_id, price, qty, side, timestamp):
        """处理委托：插入新订单"""
        if ref_id in self.ref_id_map:
            print("ref_id exists")
            return  # 异常防重处理
        
        node = OrderNode(ref_id, price, qty, side, timestamp)
        self.ref_id_map[ref_id] = node
        
        book = self.bids if side == 'B' else self.asks
        if price not in book:
            book[price] = PriceLevelList(price)
        book[price].append(node)

    def cancel_order(self, ref_id):
        """处理撤单：依靠哈希表实现平均 O(1) 精确销单"""
        if ref_id not in self.ref_id_map:
            return False  # 找不到说明之前可能已经完全成交了
        
        node = self.ref_id_map[ref_id]
        book = self.bids if node.side == 'B' else self.asks
        
        price_list = book[node.price]
        price_list.remove(node)
        
        if price_list.order_count == 0:
            del book[node.price]
            
        del self.ref_id_map[ref_id]
        return True

    def execute_trade(self, ref_id, exec_qty):
        """处理成交：根据成交明细里的明确 ID 扣减对应的挂单量"""
        if ref_id not in self.ref_id_map:
            return False
        
        node = self.ref_id_map[ref_id]
        book = self.bids if node.side == 'B' else self.asks
        price_list = book[node.price]
        
        if exec_qty >= node.qty:
            # 挂单被完全吃掉，直接从链表和哈希表中移除
            price_list.remove(node)
            if price_list.order_count == 0:
                del book[node.price]
            del self.ref_id_map[ref_id]
        else:
            # 部分成交，原地减少挂单剩余数量
            node.qty -= exec_qty
            price_list.total_volume -= exec_qty
            
        return True

    def get_topN_snapshot(self, side, n_levels=5):
        """获取当前五档盘口"""
        assert side in ['S', 'B'], f"Error: side type '{side}' Not Support"
        book = self.bids if side == 'B' else self.asks
        sorted_prices = sorted(book.keys(), reverse=(side == 'B'))[:n_levels]
        # 返回格式：[(价格, 挂单量, 单数), ...]
        return [(p, book[p].total_volume, book[p].order_count) for p in sorted_prices]

    def get_orderbook_stats(self):
        """
        计算当前全盘的总委托量及成交量加权平均委托价
        """
        total_bid_vol = sum(level.total_volume for level in self.bids.values())
        total_ask_vol = sum(level.total_volume for level in self.asks.values())
        
        bid_sum_pv = sum(level.price * level.total_volume for level in self.bids.values())
        ask_sum_pv = sum(level.price * level.total_volume for level in self.asks.values())
        
        weight_bid_price = round((bid_sum_pv / total_bid_vol) if total_bid_vol > 0 else 0.0)
        weight_ask_price = round((ask_sum_pv / total_ask_vol) if total_ask_vol > 0 else 0.0)
        
        return total_bid_vol, total_ask_vol, weight_bid_price, weight_ask_price

    def calibrate_level(self, side, price, true_vol, true_count, timestamp):
        book = self.bids if side == 'B' else self.asks
        if price not in book:
            book[price] = PriceLevelList(price)
        
        p_list = book[price]
        curr = p_list.head
        while curr:
            if curr.ref_id in self.ref_id_map:
                del self.ref_id_map[curr.ref_id]
            curr = curr.next
        p_list.clear()

        if true_vol > 0:
            syn_id = f"syn_{side}_{int(price)}_{timestamp}"
            syn_node = OrderNode(syn_id, price, true_vol, side, timestamp)
            self.ref_id_map[syn_id] = syn_node
            p_list.head = syn_node
            p_list.tail = syn_node
            p_list.total_volume = true_vol
            p_list.order_count = true_count
        else:
            del book[price]