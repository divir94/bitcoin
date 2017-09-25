from collections import namedtuple

import bitcoin.storage.api as st

Order = namedtuple('Order', ['id', 'type', 'quote', 'base', 'side', 'price', 'size', 'time'])
Cancel = namedtuple('Cancel', ['id'])
LimitOrder = namedtuple('LimitOrder', ['price', 'size', 'side'])
BUY = 'buy'
SELL = 'sell'


class BackTester(object):
    def __init__(self, exchange, product_id):
        self.exchange = exchange
        self.product_id = product_id
        self.base = self.product_id.split('-')[0]
        self.quote = self.product_id.split('-')[1]
        self.outstanding_orders = {}
        self.trades = {}
        self.balance = {}

    def place_orders(self, orders):
        # update orders and balance
        for order in orders:
            if type(order).__name__ == 'LimitOrder':
                if order.side == BUY:
                    value = order.price * order.size
                    current_balance = self.balance[order.base]
                    field = order.base
                elif order.side == SELL:
                    value = order.size
                    field = order.currency
                    current_balance = self.balance[order.quote]
                else:
                    raise ValueError
            elif type(order).__name__ == 'Cancel':
                self.balance[field] += value
                del self.outstanding_orders[order.id]
            else:
                raise ValueError('Invalid order type {}'.format(order.type))

            assert current_balance >= value, 'Insufficient balance {} for order {}'.format(current_balance, order)
            self.balance[field] -= value
            self.outstanding_orders[order.id] = order
        return

    def get_fills(self, msg, book, orders):
        assert len(orders) <= 2, 'More than a single buy/sell order is not currently supported'
        fills = {}
        # fills is dict[order id, fill qty]
        if msg['type'] == 'match':
            if msg['side'] == BUY:
                for order in orders:
                    if order.size != BUY:
                        continue
                    if order.price > msg['side'] or (order.price == float(msg['price']) and order.time <= msg['time']):
                        fills[order.id] = min(float(msg['size']), order.size)
                return fills
            else:
                assert msg['side'] == SELL
                for order in orders:
                    if order.size != SELL:
                        continue
                    if order.price < msg['side'] or (order.price == float(msg['price']) and order.time <= msg['time']):
                        fills[order.id] = min(float(msg['size']), order.size)
                return fills
        else:
            return fills

    def handle_fills(self, fills):
        for order_id, fill_qty in fills.iteritems():
            order = self.outstanding_orders[order_id]

            if order.side == 'buy':
                value = order.size
                field = order.currency
            elif order.side == 'sell':
                value = order.price * order.size
                field = order.base
            else:
                raise ValueError
            self.balance[field] += value

            if fill_qty == 0:
                del self.outstanding_orders[order.id]
            else:
                self.outstanding_orders[order.id].size -= fill_qty
        return

    def run(self, strategy, start, end):
        book = st.get_book(self.exchange, self.product_id, timestamp=start)
        msgs = st.get_messages(self.exchange, self.product_id, start=start, end=end)

        for msg in msgs:
            book.process_message(msg)
            fills = self.get_fills(msg, book, self.outstanding_orders)
            self.handle_fills(fills)
            new_orders = strategy.compute_orders(message=msg,
                                                 book=book,
                                                 outstanding_orders=self.outstanding_orders,
                                                 balance=self.balance)
            self.place_orders(new_orders)


if __name__ == '__main__':
    BackTester('gdax', 'BTC-USD')