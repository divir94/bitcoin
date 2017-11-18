import bitcoin.util as util
import bitcoin.strategies.util as autil


class BaseStrategy(util.BaseObject):
    def __init__(self, order_size=1):
        self.order_size = order_size

    def get_view(self, book):
        raise NotImplementedError

    def rebalance(self, msg, book, current_orders, balance, exposure):
        """
        Generate orders to trade to desired view.
        """
        if msg['type'] == 'received':
            return [], None

        view = self.get_view(book)
        best_bid, best_ask = book.get_best_bid_ask()
        desired_orders = autil.generate_orders(view, exposure, current_orders, best_bid, best_ask)
        actual_orders = autil.filter_orders(desired_orders, balance)
        return actual_orders, view
