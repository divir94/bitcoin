from sortedcontainers import SortedListWithKey

from bitcoin.bot.util import JsonObject


class OrderBook(JsonObject):
    def __init__(self, bids=None, asks=None, seq=None):
        """

        Parameters
        ----------
        seq: int
        bids: SortedListWithKey[PriceLevel]
        asks: SortedListWithKey[PriceLevel]
        """
        self.seq = seq
        self.bids = bids
        self.asks = asks


class PriceLevel(JsonObject):
    def __init__(self, price, size, orders={}):
        """
        All the bid or ask orders at a particular price

        Parameters
        ----------
        price: float
        size: float
        orders: dict[order_id, size]
        """
        self.price = float(price)
        self.size = float(size)
        self.orders = orders


def get_price_levels_from_level_2(lst):
    levels = (PriceLevel(price, size) for price, size in lst)
    result = SortedListWithKey(levels, key=lambda level: level.price)
    return result


def add_order(levels, price, size, order_id):
    idx = levels.bisect_key_left(price)
    seen_price = idx < len(levels) and levels[idx].price == price
    if seen_price:
        price_level = levels[idx]
        price_level.size += size
        price_level.orders[order_id] = size
    else:
        price_level = PriceLevel(price, size, {order_id: size})
        levels.add(price_level)
    return


def remove_order(levels, price, new_size, order_id):
    """remove (partially) filled or cancelled orders"""
    idx = levels.bisect_key_left(price)
    price_level = levels[idx]
    assert order_id in price_level.orders, 'order not in order book!'

    # only order at this price level
    if len(price_level.orders) == 1:
        levels.remove(price_level)
    # just remove this order from the price level
    else:
        old_size = price_level.orders[order_id]
        price_level.size -= old_size
        price_level.orders.pop(order_id)
        assert new_size <= old_size, 'new size has to be less than old size!'
    return


def update_order(levels, price, new_size, order_id):
    idx = levels.bisect_key_left(price)
    price_level = levels[idx]
    if order_id not in price_level.orders:
        return

    old_size = price_level.orders[order_id]
    price_level.size -= (new_size - old_size)
    price_level.orders[order_id] = new_size
    assert new_size <= old_size, 'new size has to be less than old size!'
    return


def get_price_levels_from_level_3(lst):
    levels = SortedListWithKey(key=lambda level: level.price)
    for price, size, order_id in lst:
        price, size = float(price), float(size)
        add_order(levels, price, size, order_id)
    return levels


def get_order_book(data, level, seq=None):
    """bitstamp list of lists to order book object"""
    if level == 2:
        func = get_price_levels_from_level_2
    elif level == 3:
        func = get_price_levels_from_level_3
    else:
        raise ValueError('Invalid level: {}'.format(level))

    bids = func(data['bids'])
    asks = func(data['asks'])
    seq = data.get(seq)
    book = OrderBook(bids, asks, seq)
    return book
