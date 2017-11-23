import pandas as pd

from sortedcontainers import SortedListWithKey
import bitcoin.util as util
from price_level import PriceLevel


class OrderBook(util.BaseObject):
    def __init__(self, sequence, bids=None, asks=None, timestamp=None):
        """
        Bids and asks are sorted lists of PriceLevel objects. Each PriceLevel corresponds to a price and contains
        all the orders for that price. The class also maintains a mapping of order_id to price. The can be used to get
        the corresponding PriceLevel for a given order_id.

        The add, remove and update methods return (price, new_size, order_id).

        Parameters
        ----------
        sequence: int
        bids: list[list]
        asks: list[list]
        timestamp: pd.datetime
        """
        self.sequence = int(sequence)
        self.timestamp = timestamp
        self.bids = SortedListWithKey(key=lambda x: -x.price)
        self.asks = SortedListWithKey(key=lambda x: x.price)
        self.orders = {}  # dict[order_id, price]

        # initialize bids and asks
        sides = {'buy': bids, 'sell': asks}
        for side, orders in sides.iteritems():
            if orders is None:
                continue
            for price, size, order_id in orders:
                price, size = float(price), float(size)
                self.add(side=side, price=price, size=size, order_id=order_id)

    def _get_levels_from_side(self, side):
        """
        Get either bids or asks based on the side
        """
        return self.bids if side == 'buy' else self.asks

    def _get_levels_from_price(self, price):
        """
        Get either bids or asks based on the price
        """
        if not self.asks:
            assert self.bids, 'Both bids and asks cannot be empty to get levels'
            return self.bids
        best_ask = self.asks[0].price
        levels = self.asks if price >= best_ask else self.bids
        return levels

    @staticmethod
    def _get_levels_idx(price, levels):
        """
        Gets the index of price in levels or None
        """
        dummy_price_level = PriceLevel(price, {})
        idx = levels.bisect_left(dummy_price_level)
        price_seen = idx < len(levels) and price == levels[idx].price
        return idx if price_seen else None

    def _get_level(self, order_id):
        """
        Get PriceLevel from the order_id
        """
        price = self.orders[order_id]
        levels = self._get_levels_from_price(price)
        idx = self._get_levels_idx(price, levels)
        assert idx is not None
        level = levels[idx]
        assert order_id in level.orders
        return level

    def get(self, order_id):
        """
        Get the (price, size, order_id)
        """
        assert order_id in self.orders
        level = self._get_level(order_id)
        price = level.price
        size = level.orders[order_id]
        return price, size, order_id

    def add(self, side, price, size, order_id):
        """
        Add an order to an existing price level or create a new price level
        """
        assert order_id not in self.orders

        # get index
        levels = self._get_levels_from_side(side)
        idx = self._get_levels_idx(price, levels)
        if idx is None:
            # new price level
            level = PriceLevel(price, orders={order_id: size})
            # TODO(vidurj) there should be some way to make this more efficient since we are already computing the
            # location to place this element
            levels.add(level)  # this is SortedListWithKey.add
        else:
            # add to existing price level
            level = levels[idx]
            level.add(size, order_id)  # this is PriceLevel.add
        self.orders[order_id] = price
        return price, size, order_id

    def update(self, order_id, new_size):
        """
        Update or remove an order from a price level
        """
        assert order_id in self.orders

        level = self._get_level(order_id)
        price, old_size, order_id = level.update(order_id, new_size)

        # remove level
        if util.is_close(level.size, 0):
            levels = self._get_levels_from_price(price)
            levels.remove(level)  # this is SortedListWithKey.remove

        # remove order
        if util.is_close(new_size, 0):
            del self.orders[order_id]
        return price, old_size, order_id

    def get_best_bid_ask(self):
        best_bid = self.bids[0].price
        best_ask = self.asks[0].price
        return best_bid, best_ask

    def to_set(self):
        """
        Set of (price, size, order_id) for all orders
        """
        bids = [level.to_set() for level in self.bids]
        asks = [level.to_set() for level in self.asks]
        orders = bids + asks
        if not orders:
            return set()
        result = set.union(*orders)
        return result

    def to_df(self, level_type, depth=None):
        """
        Get level 2 or level 3 book as a DataFrame

        Parameters
        ----------
        level_type: int
        depth: int
            depth to truncate the book

        Returns
        -------
        pd.DataFrame
            if level_type is 2:
                columns: [bid, bid_size, ask, ask_size]
            if level_type is 3:
                columns: [sequence, received_time, side, price, size, order_id]
        """
        def _get_data(levels, side):
            if level_type == 2:
                data = [(level.price, level.size) for level in levels[:depth]]
                columns = [side, '{}_size'.format(side)]
                data = pd.DataFrame(data, columns=columns)
            else:
                data = [(level.price, order_size, order_id)
                        for level in levels[:depth]
                        for order_id, order_size in level.orders.iteritems()]
                data = pd.DataFrame(data, columns=['price', 'size', 'order_id'])
            return data

        assert level_type in [2, 3]
        bids = _get_data(self.bids, 'bid')
        asks = _get_data(self.asks, 'ask')

        if level_type == 2:
            df = pd.concat([bids, asks], axis=1)
        else:
            bids['side'] = 'bid'
            asks['side'] = 'ask'
            df = pd.concat([bids, asks])
            df['sequence'] = self.sequence
            df['received_time'] = util.time_to_str(self.timestamp)
        return df
