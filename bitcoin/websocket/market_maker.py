from bitcoin.websocket import GdaxOrderBook


class OutstandingOrders(object):
    def __init__(self):
        self._bids = set()
        self._asks = set()

    def contains(self, order_id):
        return self.is_ask(order_id) or self.is_bid(order_id)

    def is_bid(self, order_id):
        return order_id in self._bids

    def is_ask(self, order_id):
        return order_id in self._asks

    def add_bid(self, order_id):
        self._bids.add(order_id)

    def add_ask(self, order_id):
        self._asks.add(order_id)


class OrderSupport(object):
    def __init__(self,
                 gdax_client,
                 order_id,
                 supporting_vol,
                 supporting_order_to_vol,
                 required_supporting_vol=5):
        self.order_id = order_id
        self.supporting_vol = supporting_vol
        self._supporting_order_to_vol = supporting_order_to_vol
        self._required_supporting_vol = required_supporting_vol
        self.gdax_client = gdax_client

    def cancelled_order(self, order_id):
        if order_id in self._supporting_order_to_vol:
            self.supporting_vol -= self._supporting_order_to_vol[order_id]
            if self.supporting_vol < self._required_supporting_vol:
                self.gdax_client.async_cancel_order(order_id)


class MarketMaker(object):
    def __init__(self):
        self.balances = None
        self.outstanding_orders = OutstandingOrders()
        # watched_orders is a map from my outstanding orders, to the volume, order_id's and order volumes
        # behind it. This way, every incoming cancellation can be checked against this list and the volume
        # updated. If the volume falls behind a threshold, my order is cancelled.
        self.watched_orders = {}


if __name__ == "__main__":
    ws = GdaxOrderBook()
    ws.run()
