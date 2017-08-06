from decimal import Decimal
from sortedcontainers import SortedListWithKey

import bitcoin.util as util


class OrderBook(util.BaseObject):
    def __init__(self, sequence, bids=None, asks=None):
        """
        Bids and asks are sorted lists of PriceLevel objects. Each PriceLevel corresponds to a price and contains
        all the orders for that price. The class also maintains a mapping of order_id to price. The can be used to get
        the corresponding PriceLevel for a given order_id.

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

    def _get_levels(self, side):
        return self.bids if side == 'buy' else self.asks

    def _get_levels_idx(self, price, levels):
        idx = levels.bisect_key_left(price)
        price_seen = idx < len(levels) and price == levels.price
        return idx if price_seen else -1

    def add(self, side, price, size, order_id):
        assert order_id not in self.orders
        levels = self._get_levels(side)
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

    def get(self, order_id):
        """get the (price, size, order_id)"""
        return

    def remove(self, order_id):
        return

    def update(self, order_id, new_size):
        return


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

    def remove(self, order_id):
        assert order_id in self.orders
        # subtract original size
        size = self.orders[order_id]
        self.size -= size
        # remove order
        del self.orders[order_id]
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


if __name__ == '__main__':
    from bitcoin.websocket.gdax import get_gdax_book
    data = get_gdax_book()
    print get_price_levels_from_level_3(data['bids'])
