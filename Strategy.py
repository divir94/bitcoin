from collections import namedtuple
import time
import tensorflow as tf

BUY = 'buy'
SELL = 'sell'

config_named_tuple = namedtuple('Config',
                                [
                                    'product_id',
                                    'significant_volume',
                                    'min_tick',
                                    'max_vol',
                                    'max_vol2',
                                    'monitored_prices',
                                    'order_size',
                                    'min_price_change'
                                ])

cancel_named_tuple = namedtuple('Cancel', ['id'])
limit_buy_order_named_tuple = namedtuple('LimitBuyOrder', ['price', 'size'])
limit_sell_order_named_tuple = namedtuple('LimitSellOrder', ['price', 'size'])

btc_usd_config = config_named_tuple(
    product_id='BTC-USD',
    significant_volume=5,
    min_tick=0.01,
    max_vol=30,
    max_vol2=50,
    monitored_prices=[0.1, 1, 10],
    order_size=0.01,
    min_price_change=1
)


# class Features(object):
#     def __init__(self, product_id, size_for_avg=5):
#         self.product_id = product_id
#         self.size_for_avg = size_for_avg
#         # TODO(vidurj) we probably just want to maintain the volume within x% of the price
#         self.avg_bid = None
#         self.avg_ask = None
#
#     def update_on_msg(self, msg):
#         pass
#
#     def get_features(self):
#         return [
#             self.avg_bid,
#             self.avg_ask
#         ]


def get_volume_behind(side, cumulative_vol, index, x):
    new_index = side.bisect_left(x)
    return cumulative_vol[new_index] - cumulative_vol[index]


class Scorer(object):
    def __init__(self, num_features):
        pass


class Decisions(object):
    def __init__(self):
        self.orders = []

    def buy_limit_order(self, price, size):
        self.orders.append(limit_buy_order_named_tuple(price, size))

    def sell_limit_order(self, price, size):
        self.orders.append(limit_sell_order_named_tuple(price, size))

    def cancel_all(self, outstanding_orders):
        cancellations = [cancel_named_tuple(order['id']) for order in outstanding_orders.all_orders]
        self.orders.extend(cancellations)

    def cancel_buy_orders(self, outstanding_orders):
        cancellations = [cancel_named_tuple(order['id']) for order in outstanding_orders.buy_orders]
        self.orders.extend(cancellations)

    def cancel_sell_orders(self, outstanding_orders):
        cancellations = [cancel_named_tuple(order['id']) for order in outstanding_orders.sell_orders]
        self.orders.extend(cancellations)


class Strategy(object):
    def __init__(self, config):
        self.last_updated = time.time()
        self.asks_model = None
        self.bids_model = None
        self.config = config

    def pick_best_order(self, orders, best_opp_side, side):
        assert side == 'bid' or side == 'ask'
        if side == 'bid':
            scorer = self.bids_model.score
        else:
            scorer = self.asks_model.score
        cumulative_volume_bids = self.book_to_cumulative_volume(orders)
        best_price = None
        best_score = - 1
        for vol, level in zip(cumulative_volume_bids, orders):
            if vol > self.config.max_vol:
                break
            if side == 'bid':
                price = level.price + self.config.min_tick
            else:
                price = level.price - self.config.min_tick
            if price == best_opp_side:
                continue
            features = [vol]
            for price_delta in self.config.monitored_prices:
                if side == 'bid':
                    new_index = orders.bisect_left(price - price_delta)
                else:
                    new_index = orders.bisect_left(price + price_delta)
                volume = cumulative_volume_bids[new_index] - vol
                features.append(volume)
            score = scorer(features)
            if score > 1 and score > best_score:
                best_score = score
                best_price = price
        return best_price

    def calculate_orders(self, msg, book, balances, outstanding_orders):
        # Wait for 1 second between recalculating orders.
        decisions = Decisions()
        significant_change = msg['type'] == 'match' and float(msg['size']) > self.config.significant_volume
        if not significant_change and time.time() - self.last_updated < 1:
            return decisions.orders
        bid_price = self.pick_best_order(book.bids, book.asks[0].price, 'bids')
        ask_price = self.pick_best_order(book.asks, book.bids[0].price, 'asks')

        if BUY in outstanding_orders:
            if (abs(outstanding_orders[BUY]["price"] - bid_price) > self.config.min_price_change) or \
                    (outstanding_orders[BUY]["filled_size"] > 0):
                decisions.cancel_buy_orders(outstanding_orders)
                if bid_price:
                    limit_buy_order_named_tuple(price=bid_price, size=self.config.order_size)
        elif bid_price:
            limit_buy_order_named_tuple(price=bid_price, size=self.config.order_size)

        if SELL in outstanding_orders:
            if (abs(outstanding_orders[SELL]["price"] - ask_price) > self.config.min_price_change) or \
                    (outstanding_orders[SELL]["filled_size"] > 0):
                decisions.cancel_sell_orders(outstanding_orders)
                if ask_price:
                    decisions.sell_limit_order(price=ask_price, size=self.config.order_size)
        elif ask_price:
            decisions.sell_limit_order(price=ask_price, size=self.config.order_size)

        return decisions.orders

    def book_to_cumulative_volume(self, side):
        total_volume = 0
        cumulative_volume = []
        for level in side:
            cumulative_volume.append(total_volume)
            total_volume += level.size
            if total_volume > self.config.max_vol2:
                return cumulative_volume
        return cumulative_volume
