from decimal import Decimal
from sortedcontainers import SortedListWithKey

import bitcoin.util as util


class OrderBook(util.BaseObject):
    def __init__(self, sequence, bids=None, asks=None):
        """
        Bids and asks are sorted lists of PriceLevel objects. Each PriceLevel corresponds to a price and contains
        all the orders for that price. The class also maintains a mapping of order_id to price. The can be used to get
        the corresponding PriceLevel for a given order_id.

        The add, remove and update methods return (price, new_size, order_id).

        Parameters
        ----------
        sequence: int
        bids: SortedListWithKey[PriceLevel]
        asks: SortedListWithKey[PriceLevel]
        """
        self.sequence = int(sequence)
        self.bids = bids or SortedListWithKey(key=lambda x: x.price)
        self.asks = asks or SortedListWithKey(key=lambda x: x.price)
        self.orders = {}  # dict[order_id, price]

    def _get_levels_from_side(self, side):
        """get either bids or asks based on the side"""
        return self.bids if side == 'buy' else self.asks

    def _get_levels_from_price(self, price):
        """get either bids or asks based on the price"""
        if not self.asks:
            assert self.bids, 'Both bids and asks canot be empty to get levels'
            return self.bids
        best_ask = self.asks[0].price
        levels = self.asks if price >= best_ask else self.bids
        return levels

    def _get_levels_idx(self, price, levels):
        """gets the index of price in levels or -1"""
        idx = levels.bisect_key_left(price)
        price_seen = idx < len(levels) and price == levels[idx].price
        return idx if price_seen else -1

    def _get_level(self, order_id):
        """get PriceLevel from the order_id"""
        price = self.orders[order_id]
        levels = self._get_levels_from_price(price)
        idx = self._get_levels_idx(price, levels)
        assert idx != -1
        level = levels[idx]
        assert order_id in level.orders
        return level

    def get(self, order_id):
        """get the (price, size, order_id)"""
        assert order_id in self.orders
        level = self._get_level(order_id)
        price = level.price
        size = level.orders[order_id]
        return price, size, order_id

    def add(self, side, price, size, order_id):
        """add an order to an exsiting price level or create a new price level"""
        assert order_id not in self.orders
        # get index
        levels = self._get_levels_from_side(side)
        idx = self._get_levels_idx(price, levels)
        if idx == -1:
            # new price level
            level = PriceLevel(price, orders={order_id: size})
            levels.add(level)  # this is SortedListWithKey.add
        else:
            # add to existing price level
            level = levels[idx]
            level.add(size, order_id)  # this is PriceLevel.add
        self.orders[order_id] = price
        return price, size, order_id

    def update(self, order_id, new_size):
        """update or remove from an order from a price level"""
        assert order_id in self.orders
        level = self._get_level(order_id)
        level.update(order_id, new_size)
        price = level.price

        # remove level
        if level.size == 0:
            levels = self._get_levels_from_price(price)
            levels.update(level, 0)  # this is SortedListWithKey.remove

        # remove order
        if new_size == 0:
            del self.orders[order_id]
        return price, new_size, order_id


class PriceLevel(util.BaseObject):
    def __init__(self, price, orders):
        """
        Contains sll the bid or ask orders for a corresponding price.
        The add, remove and update methods return (price, new_size, order_id).

        Parameters
        ----------
        price: float
        orders: dict[order_id, size]
        """
        self.price = Decimal(price)
        self.size = Decimal(sum(orders.values()))
        self.orders = {k: Decimal(v) for k, v in orders.iteritems()}

    def add(self, size, order_id):
        size = Decimal(size)
        # add size and order_id
        self.size += size
        self.orders[order_id] = size
        return self.price, size, order_id

    def update(self, order_id, new_size):
        new_size = Decimal(new_size)
        assert order_id in self.orders
        # subtract the size change
        old_size = self.orders[order_id]
        self.size += (new_size - old_size)
        # update order
        self.orders[order_id] = new_size
        # remove order if needed
        if new_size == 0:
            del self.orders[order_id]
        return self.price, new_size, order_id


def json_to_book(data, level, sequence='sequence'):
    if level == 2:
        func = get_price_levels_from_level_2
    elif level == 3:
        func = get_price_levels_from_level_3
    else:
        raise ValueError('Invalid level: {}'.format(level))

    bids = func(data['bids'])
    asks = func(data['asks'])
    sequence = data.get(sequence)
    book = OrderBook(bids, asks, sequence)
    return book


def level_to_set(level):
    result = {
        (str(level.price), str(size), order_id)
        for level in level
        for order_id, size in level.orders.iteritems()
    }
    return result


def book_to_set(book):
    result = {
        'sequence': book.sequence,
        'bids': level_to_set(book.bids),
        'asks': level_to_set(book.asks)
    }
    return result


def json_to_set(data):
    result = {
        'sequence': data['sequence'],
        'bids': {(price, size, order_id) for price, size, order_id in data['bids']},
        'asks': {(price, size, order_id) for price, size, order_id in data['asks']}
    }
    return result


def get_price_levels_from_level_2(lst):
    levels = (PriceLevel(price, size) for price, size in lst)
    result = SortedListWithKey(levels, key=lambda level: level.price)
    return result


def get_price_levels_from_level_3(lst):
    levels = SortedListWithKey(key=lambda level: level.price)
    for price, size, order_id in lst:
        price = Decimal(price)
        size = Decimal(size)
        add_order(levels, price, size, order_id)
    return levels
