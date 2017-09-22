from collections import namedtuple
import time
import tensorflow as tf
import numpy as np

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


def compute_bucket_features(orders, side):
    """
    This function featurizes a limit order book by recording the order volume present between
    0.05%, 0.1%, 0.15%, ... 0.25% of the best bid and asks respectively.
    This gives us 10 buckets, and so 10 real values features.
    """
    assert side == 'bid' or side == 'ask'
    best_price = orders[0].price
    buckets = [-1 for _ in range(10)]
    volume = 0
    for point in orders:
        if side == 'bid':
            distance = best_price / point.price - 1
        else:
            distance = point.price / best_price - 1
        volume += point.size
        bucket = int(distance * 10000) / 5
        if bucket >= len(buckets):
            break
        buckets[bucket] = volume
    return buckets


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
        self.order_features, self.estimated_profit = self.create_embedding()

    def create_embedding(self):
        features = tf.placeholder('float', [None, self.config.feature_dim])
        with_log_features = tf.concat([features, tf.log(features)], 1)
        input_dim = 2 * self.config.feature_dim
        input_vec = with_log_features
        for layer_n, output_dim in enumerate(self.config.layer_sizes):
            w = tf.Variable(tf.random_normal([input_dim, output_dim]), name='w_' + str(layer_n))
            b = tf.Variable(tf.zeros([output_dim]), name='b_' + str(layer_n))
            input_vec = tf.nn.relu(tf.matmul(w, input_vec) + b)
            input_dim = output_dim
        w = tf.Variable(tf.random_normal([input_dim, 1]), name='w_final')
        b = tf.Variable(tf.zeros([1]), name='b_final')
        prediction = tf.matmul(w, input_vec) + b
        return features, prediction

    def pick_best_order(self, orders, best_opp_side, side):
        assert side == 'bid' or side == 'ask'
        if side == 'bid':
            scorer = self.bids_model.score
        else:
            scorer = self.asks_model.score
        cumulative_volume_bids = self._book_to_cumulative_volume(orders)
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
        # TODO(vidurj) it would be good to have this 'bids' 'asks' indicator be part of the side of the
        # order book
        order_book_features = compute_bucket_features(book.bids, 'bids') + \
                          compute_bucket_features(book.asks, 'asks')
        order_book_embedding =
        bid_price = self.pick_best_order(book.bids, book.asks[0].price, 'bids')
        ask_price = self.pick_best_order(book.asks, book.bids[0].price, 'asks')

        if BUY in outstanding_orders:
            if (abs(outstanding_orders[BUY]['price'] - bid_price) > self.config.min_price_change) or \
                    (outstanding_orders[BUY]['filled_size'] > 0):
                decisions.cancel_buy_orders(outstanding_orders)
                if bid_price:
                    limit_buy_order_named_tuple(price=bid_price, size=self.config.order_size)
        elif bid_price:
            limit_buy_order_named_tuple(price=bid_price, size=self.config.order_size)

        if SELL in outstanding_orders:
            if (abs(outstanding_orders[SELL]['price'] - ask_price) > self.config.min_price_change) or \
                    (outstanding_orders[SELL]['filled_size'] > 0):
                decisions.cancel_sell_orders(outstanding_orders)
                if ask_price:
                    decisions.sell_limit_order(price=ask_price, size=self.config.order_size)
        elif ask_price:
            decisions.sell_limit_order(price=ask_price, size=self.config.order_size)

        return decisions.orders

    def _book_to_cumulative_volume(self, side):
        total_volume = 0
        cumulative_volume = []
        for level in side:
            cumulative_volume.append(total_volume)
            total_volume += level.size
            if total_volume > self.config.max_vol2:
                return cumulative_volume
        return cumulative_volume
