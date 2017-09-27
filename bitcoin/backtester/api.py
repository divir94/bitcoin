import pandas as pd

import bitcoin.backtester.strategy as strat
import bitcoin.storage.api as st
import bitcoin.logs.logger as lc


logger = lc.config_logger('backtester', level='DEBUG', file_handler=False)


class Backtester(object):
    def __init__(self, exchange, product_id, balance={}):
        self.exchange = exchange
        self.product_id = product_id
        self.balance = balance
        self.outstanding_orders = {}
        self.trades = {}

    @staticmethod
    def get_fills(msg, orders):
        # fills is dict[order id, fill qty]
        assert len(orders) <= 2, 'More than a single buy/sell order is not currently supported'
        fills = {}
        if not msg['type'] == 'match':
            return fills

        # the message side field indicates the maker order side
        maker_side = msg['side']
        match_price = float(msg['price'])
        match_size = float(msg['size'])
        match_time = pd.to_datetime(msg['time'])

        for _, order in orders.iteritems():
            # our order is competitive if it has a better price than the match price
            competitive_price = order.price > match_price if order.side == 'buy' else order.price < match_price
            # match at same price if our order came before
            early_at_same_price = (order.price == match_price) and (order.time < match_time)
            # order has to be same side as maker i.e. opposite side of taker
            same_side_as_maker = (maker_side == order.side)
            if same_side_as_maker and (competitive_price or early_at_same_price):
                fills[order.id] = min(match_size, order.size)
                logger.info('Filled ({}) of order: {}'.format(fills[order.id], order))
        return fills

    def handle_fills(self, fills):
        """
        Called after orders have been filled. Updates balance and outstanding orders.
        """
        for order_id, fill_qty in fills.iteritems():
            order = self.outstanding_orders[order_id]

            # update balance
            if order.side == 'buy':
                value = order.size
                field = order.quote
            elif order.side == 'sell':
                value = order.price * order.size
                field = order.base
            else:
                raise ValueError
            self.balance[field] += value

            # update orders
            self.outstanding_orders[order.id].size -= fill_qty
            # remove if filled
            if self.outstanding_orders[order.id].size == 0:
                del self.outstanding_orders[order.id]
        return

    def place_orders(self, orders):
        """
        Called to place orders. Updates balance and outstanding orders.
        """
        for order in orders:
            if order.type == 'limit':
                if order.side == 'buy':
                    value = order.price * order.size
                    current_balance = self.balance[order.base]
                    field = order.base
                elif order.side == 'sell':
                    value = order.size
                    field = order.quote
                    current_balance = self.balance[order.quote]
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

    def run(self, strategy, start, end=None):
        book = st.get_book(self.exchange, self.product_id, timestamp=start)
        msgs = st.get_messages_by_time(self.exchange, self.product_id, start=start, end=end)

        for msg in msgs:
            book.process_message(msg)
            fills = self.get_fills(msg, self.outstanding_orders)
            self.handle_fills(fills)
            new_orders = strategy.rebalance(msg=msg,
                                            book=book,
                                            outstanding_orders=self.outstanding_orders,
                                            balance=self.balance)
            self.place_orders(new_orders)
        return book


if __name__ == '__main__':
    exchange = 'GDAX'
    product_id = 'BTC-USD'
    start = pd.datetime(2017, 9, 26, 4, 31)
    end = pd.datetime(2017, 9, 26, 4, 32)
    strategy = strat.Strategy()

    backtester = Backtester(exchange, product_id)
    backtester.run(strategy, start, end)
