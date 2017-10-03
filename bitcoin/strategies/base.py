from sortedcontainers import SortedListWithKey

import bitcoin.util as util
from bitcoin.backtester.orders import *


CumPriceLevel = namedtuple('CumPriceLevel', ['price', 'cum_size'])
CumPriceLevel.__new__.__defaults__ = (None,)


class BaseStrategy(util.BaseObject):
    def __init__(self, quote='USD', base='BTC', skip_msgs=1, order_size=1, sig_price_change=0.5):
        self.quote = quote
        self.base = base
        self.skip_msgs = skip_msgs  # skip some messages
        self.order_size = order_size
        self.sig_price_change = sig_price_change  # significant price change to update order
        self.msg_num = -1

    def get_target_prices(self, book):
        """
        Get price at which to place orders on both sides before a wall.
        """
        raise NotImplementedError

    @staticmethod
    def get_cumulative_volume(book, max_volume=None):
        """
        {
            'asks': list of (price, cumulative volume) is ascending order,
            'bids': list of (price, cumulative volume) is descending order,
        }
        """
        volume_so_far = 0
        all_levels = {
            'bids': book.bids,
            'asks': book.asks
        }
        cumulative_volume = {
            'bids': SortedListWithKey(key=lambda x: -x.price),
            'asks': SortedListWithKey(key=lambda x: x.price)
        }

        for side, levels in all_levels.iteritems():
            for level in levels:
                volume_so_far += level.size
                value = CumPriceLevel(price=level.price, cum_size=volume_so_far)
                cumulative_volume[side].add(value)
                if max_volume and (volume_so_far > max_volume):
                    break
        return cumulative_volume

    def _get_cancel_orders(self, outstanding_orders, side, target_price):
        orders = []
        num_orders = len(outstanding_orders.get(side, []))
        if num_orders == 0 or target_price is None:
            return orders

        current_order = outstanding_orders[side][0]
        sig_change = abs(target_price - current_order.price) > self.sig_price_change
        if sig_change:
            cancel_order = CancelOrder(current_order.id)
            orders.append(cancel_order)
        return orders

    def _get_new_orders(self, outstanding_orders, side, target_price, balance):
        orders = []
        num_orders = len(outstanding_orders.get(side, []))
        if num_orders != 0 or target_price is None:
            return orders

        currency = self.quote if side == OrderSide.BUY else self.base
        amount = target_price * self.order_size if side == OrderSide.BUY else self.order_size

        if balance[currency] >= amount and target_price is not None:
            limit_order = LimitOrder(quote=self.quote,
                                     base=self.base,
                                     side=side,
                                     price=target_price,
                                     size=self.order_size)
            orders.append(limit_order)
        return orders

    def rebalance(self, msg, book, outstanding_orders, balance):
        """
        If we don't have an order on each side, place a new order. Otherwise, update existing order if needed.
        """
        # skip msgs. defaults to no skip
        self.msg_num += 1
        if self.msg_num % self.skip_msgs != 0:
            return

        new_orders = []
        target_buy, target_ask = self.get_target_prices(book)
        target_prices = {OrderSide.BUY: target_buy,
                         OrderSide.SELL: target_ask}

        # update orders for each side
        for side, target_price in target_prices.iteritems():
            # new orders
            new_orders += self._get_new_orders(outstanding_orders=outstanding_orders,
                                               side=side,
                                               target_price=target_price,
                                               balance=balance)
            # cancel orders
            new_orders += self._get_cancel_orders(outstanding_orders=outstanding_orders,
                                                  side=side,
                                                  target_price=target_price)
        return new_orders
