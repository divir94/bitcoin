from decimal import Decimal
from sortedcontainers import SortedListWithKey

import bitcoin.util as util


class OrderBook(util.JsonObject):
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


class PriceLevel(util.JsonObject):
    def __init__(self, price, size, orders={}):
        """
        All the bid or ask orders at a particular price

        Parameters
        ----------
        price: float
        size: float
        orders: dict[order_id, size]
        """
        self.price = Decimal(price)
        self.size = Decimal(size)
        self.orders = orders


def convert_data_to_book(data, level, seq=None):
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


def add_order(levels, price, size, order_id):
    idx = levels.bisect_key_left(price)
    seen_price = idx < len(levels) and levels[idx].price == price
    if seen_price:
        # add order to level
        price_level = levels[idx]
        price_level.size += size
        price_level.orders[order_id] = size
    else:
        # add new price level
        price_level = PriceLevel(price, size, {order_id: size})
        levels.add(price_level)
    return


def remove_order(levels, price, size, order_id):
    """remove (partially) filled or cancelled orders"""
    if not price:
        # ignore market order
        return

    idx = levels.bisect_key_left(price)
    seen_order = idx < len(levels) and order_id in levels[idx].orders
    if not seen_order:
        # ignore done message which are not on the book. this could be because of fully-filled orders or
        # self-trade prevention
        return

    price_level = levels[idx]
    assert price == price_level.price, 'Removing order price does not match!'

    # only order at this price level
    if len(price_level.orders) == 1:
        del levels[idx]
    # just remove this order from the price level
    else:
        price_level.size -= size
        price_level.orders.pop(order_id)
    return


def update_order(levels, price, old_size, new_size, order_id):
    if not price:
        # ignore market order
        return

    idx = levels.bisect_key_left(price)
    price_level = levels[idx]

    if order_id not in price_level.orders:
        # ignore change orders for orders not in book
        return

    assert old_size == price_level.orders[order_id], 'Change order old_size != original size!'
    assert new_size <= old_size, 'New size has to be less than old size!'

    price_level.size -= (new_size - old_size)
    price_level.orders[order_id] = new_size
    return


def compare_books(book1, book2, logger=None):
    if book1.seq != book2.seq:
        print 'Sequence numbers do not match: {}, {}'.format(book1.seq, book2.seq)
    bids_same = compare_levels('bid', book1.bids, book2.bids)
    asks_same = compare_levels('ask', book1.asks, book2.asks)
    return bids_same and asks_same


def compare_levels(side, levels1, levels2):
    is_same = True
    size = min(len(levels1), len(levels2))
    for i in range(size):
        level1 = levels1[i]
        level2 = levels2[i]
        if level1 != level2:
            import pdb;
            pdb.set_trace()
            is_same = False
            print '\n{}: levels different'.format(side)
            print level1
            print '=' * 20
            print level2
    return is_same
