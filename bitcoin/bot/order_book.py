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
        self.price = float(price)
        self.size = float(size)
        self.orders = orders


def get_price_levels_from_level_2(lst):
    levels = (PriceLevel(price, size) for price, size in lst)
    result = SortedListWithKey(levels, key=lambda level: level.price)
    return result


def get_price_levels_from_level_3(lst):
    levels = SortedListWithKey(key=lambda level: level.price)
    for price, size, order_id in lst:
        price, size = float(price), float(size)
        idx = levels.bisect_key_left(price)
        seen_price = idx < len(levels) and levels[idx].price == price
        if seen_price:
            price_level = levels[idx]
            price_level.size += size
            price_level.orders[order_id] = size
        else:
            price_level = PriceLevel(price, size, {order_id: size})
            levels.add(price_level)
    return levels


def get_order_book(data, level, _id=None):
    """bitstamp list of lists to order book object"""
    if level == 2:
        func = get_price_levels_from_level_2
    elif level == 3:
        func = get_price_levels_from_level_3
    else:
        raise ValueError('Invalid level: {}'.format(level))

    bids = func(data['bids'])
    asks = func(data['asks'])
    _id = data.get(_id)
    book = OrderBook(bids, asks, _id)
    return book


if __name__ == '__main__':
    lst = [
        [
            "2845.18",
            "0.20265626",
            "cc6d1639-fa53-4968-abc5-716f2748ee85"
        ],
        [
            "2845.18",
            "0.0140202",
            "c9ffaeb2-3c87-40ec-ada7-e242e9192bdc"
        ],
        [
            "2845.15",
            "0.82691468",
            "54276351-d0ed-4c8f-9a6b-84141e9751a2"
        ],
        [
            "2845.1",
            "0.01",
            "a2f3d6d5-75da-486e-af74-bb14bbbde29f"
        ]
    ]
    import requests
    import json
    import time

    url = 'https://www.bitstamp.net/api/v2/order_book/btcusd'
    #url = 'https://api.gdax.com/products/BTC-USD/book?level=3'
    response = requests.get(url)
    data = json.loads(response.content)
    start = time.time()
    book = get_order_book(data, 2, 'sequence')['bids']
    end = time.time() - start
    print book
    print end
