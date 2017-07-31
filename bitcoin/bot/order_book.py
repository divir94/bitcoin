from sortedcontainers import SortedListWithKey

from bitcoin.bot.util import JsonObject


class OrderBook(JsonObject):
    def __init__(self, bids, asks, _id=None):
        """

        Parameters
        ----------
        _id: int
        bids: SortedListWithKey[PriceLevel]
        asks: SortedListWithKey[PriceLevel]
        """
        self.id = _id
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
        self.price = price
        self.size = size
        self.orders = orders


def to_price_level(lst):
    return (PriceLevel(price, size) for price, size in lst)


def to_order_book(data):
    """bitstamp list of lists to order book object"""
    bids_lst = to_price_level(data['bids'])
    bids = SortedListWithKey(bids_lst, key=lambda x: x.price)

    asks_lst = to_price_level(data['asks'])
    asks = SortedListWithKey(asks_lst, key=lambda x: x.price)

    book = OrderBook(bids, asks)
    return book

