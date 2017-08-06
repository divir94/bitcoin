from decimal import Decimal
from sortedcontainers import SortedListWithKey

import bitcoin.util as util


class OrderBook(util.JsonObject):
    def __init__(self, bids=None, asks=None, sequence=None):
        """

        Parameters
        ----------
        sequence: int
        bids: SortedListWithKey[PriceLevel]
        asks: SortedListWithKey[PriceLevel]
        """
        self.sequence = sequence
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
    try:
        seen_order = idx < len(levels) and order_id in levels[idx].orders
    except Exception as e:
        import pdb
        pdb.set_trace()

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


def compare_books(actual, expected):
    if actual['sequence'] != expected['sequence']:
        print 'Sequence numbers do not match: {}, {}'.format(actual.sequence, expected.sequence)
    bids_same = compare_levels('bid', actual['bids'], expected['bids'])
    #asks_same = compare_levels('ask', actual.asks, expected.asks)
    return actual == expected


def compare_levels(side, actual, expected):
    is_same = True
    size = min(len(actual), len(expected))

    if len(actual) != len(expected):
        print 'book size mismatch'
        is_same = False

    for i in range(size):
        actual_level = actual[i]
        expected_level = expected[i]

        if actual_level.price != expected_level.price:
            print 'price mismatch'
            is_same = False

        if actual_level.size != expected_level.size:
            print 'size mismatch'
            is_same = False

        actual_orders = set(actual_level.orders.keys())
        expected_orders = set(expected_level.orders.keys())
        extra_orders = actual_orders.difference(expected_orders)
        missing_orders = expected_orders.difference(actual_orders)

        if extra_orders:
            print 'extra orders'
            print extra_orders

        if missing_orders:
            print 'missing orders'
            print missing_orders

        # if actual_level != expected_level:
        #     # import pdb;
        #     # pdb.set_trace()
        #     #is_same = False
        #     print '\n{}: levels different'.format(side)
        #     print actual_level
        #     print '=' * 20
        #     print expected_level
    return is_same


if __name__ == '__main__':
    from bitcoin.websocket.gdax import get_gdax_book
    data = get_gdax_book()
    print get_price_levels_from_level_3(data['bids'])
