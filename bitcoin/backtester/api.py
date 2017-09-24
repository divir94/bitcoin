from collections import namedtuple

import bitcoin.storage.api as st


Order = namedtuple('Order', ['id', 'type', 'currency', 'base', 'side', 'price', 'size', 'cancel'])


class Backtester(object):
    def __init__(self, exchange, product_id):
        self.exchange = exchange
        self.product_id = product_id
        self.outstanding_orders = {}
        self.trades = {}
        self.balance = {}

    def place_orders(self, orders):
        # update orders and balance
        for order in orders:
            if order.type == 'limit':
                if order.side == 'buy':
                    value = order.price * order.size
                    current_balance = self.balance[order.base]
                    field = order.base
                elif order.side == 'sell':
                    value = order.size
                    field = order.currency
                    current_balance = self.balance[order.currency]
                else:
                    raise ValueError
            else:
                raise ValueError('Invalid order type {}'.format(order.type))

            if order.cancel:
                self.balance[field] += value
                del self.outstanding_orders[order.id]
            else:
                assert current_balance >= value, 'Insufficient balance {} for order {}'.format(current_balance, order)
                self.balance[field] -= value
                self.outstanding_orders[order.id] = order
        return

    def get_fills(self, msg, book, orders):
        # fills is dict[order id, fill qty]
        fills = {}
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
            new_orders = strategy.rebalance(message=msg,
                                            book=book,
                                            outstanding_orders=self.outstanding_orders,
                                            balance=self.balance)
            self.place_orders(new_orders)
            fills = self.get_fills(msg, book, self.outstanding_orders)
            self.handle_fills(fills)
            book.process_message(msg)
