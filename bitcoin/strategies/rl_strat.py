from collections import namedtuple
import time
import tensorflow as tf
import numpy as np
from bitcoin.backtester.api import CancelOrder, LimitOrder, OutstandingOrder, OrderSide

config_named_tuple = namedtuple('Config',
                                [
                                    'product_id',
                                    'base',
                                    'quote',
                                    'significant_volume',
                                    'min_tick',
                                    'max_vol',
                                    'max_vol2',
                                    'monitored_prices',
                                    'order_size',
                                    'min_price_change',
                                    'layer_sizes',
                                    'feature_dim'
                                ])

btc_usd_config = config_named_tuple(
    product_id='BTC-USD',
    base='BTC',
    quote='USD',
    significant_volume=5,
    min_tick=0.01,
    max_vol=30,
    max_vol2=50,
    monitored_prices=[0.1, 1, 10],
    order_size=0.01,
    min_price_change=1,
    layer_sizes=[50, 25, 10],
    feature_dim=22
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
    assert side == OrderSide.BUY or side == OrderSide.SELL
    if side == OrderSide.BUY:
        best_price = orders[-1].price
    else:
        best_price = orders[0].price
    buckets = [-1 for _ in range(10)]
    volume = 0
    for point in orders:
        if side == OrderSide.BUY:
            distance = best_price / point.price - 1
        else:
            distance = point.price / best_price - 1
        volume += point.size
        bucket_index = int(distance * 10000) / 5
        if bucket_index < len(buckets):
            buckets[bucket_index] = volume
        else:
            break
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

    def limit_order(self, price, size, side):
        assert side == OrderSide.BUY or side == OrderSide.SELL
        self.orders.append(LimitOrder(price, size, side))

    def cancel_order(self, order):
        self.orders.append(CancelOrder(order.id))

    # def cancel_all(self, outstanding_orders, side=None):
    #     if side == OrderSide.BUY:
    #         cancellations = [CancelOrder(order.id) for order in outstanding_orders if side == OrderSide.BUY]
    #     elif side == OrderSide.SELL:
    #         cancellations = [CancelOrder(order.id) for order in outstanding_orders if side == OrderSide.SELL]
    #     else:
    #         assert side is None
    #         cancellations = [CancelOrder(order.id) for order in outstanding_orders]
    #     self.orders.extend(cancellations)


class Strategy(object):
    def __init__(self, config=btc_usd_config):
        self.last_updated = time.time()
        self.config = config
        self.order_features, self.estimated_profit = self._create_embedding()
        self.side_to_currency = {OrderSide.BUY: self.config.quote, OrderSide.SELL: self.config.base}
        self.sess = tf.Session()

    def _scorer(self, features):
        pass

    def _create_embedding(self):
        features = tf.placeholder('float', [None, self.config.feature_dim])
        with_log_features = tf.concat([features, tf.log(features)], 1)
        input_dim = 2 * self.config.feature_dim
        input_vec = with_log_features
        for layer_n, output_dim in enumerate(self.config.layer_sizes):
            w = tf.Variable(tf.random_normal([input_dim, output_dim]), name='w_' + str(layer_n))
            b = tf.Variable(tf.zeros([output_dim]), name='b_' + str(layer_n))
            input_vec = tf.nn.relu(tf.matmul(input_vec, w) + b)
            input_dim = output_dim
        w = tf.Variable(tf.random_normal([input_dim, 1]), name='w_final')
        b = tf.Variable(tf.zeros([1]), name='b_final')
        prediction = tf.matmul(w, input_vec) + b
        return features, prediction

    def _pick_best_order(self, orders, best_opp_side, side, common_features):
        assert side == OrderSide.BUY or side == OrderSide.SELL
        cumulative_volume_bids = self._book_to_cumulative_volume(orders)
        num_order_specific_features = 1 + len(self.config.monitored_prices)
        order_features = [None for _ in range(num_order_specific_features)] + common_features + [side == OrderSide.BUY]
        best_price = None
        best_score = - 1
        for vol, level in zip(cumulative_volume_bids, orders):
            if vol > self.config.max_vol:
                break
            if side == OrderSide.BUY:
                price = level.price + self.config.min_tick
            else:
                price = level.price - self.config.min_tick
            if price == best_opp_side:
                continue
            order_features[0] = vol
            index = 1
            for price_delta in self.config.monitored_prices:
                if side == OrderSide.BUY:
                    new_index = orders.bisect_left(price - price_delta)
                else:
                    new_index = orders.bisect_left(price + price_delta)
                volume = cumulative_volume_bids[new_index] - vol
                order_features[index] = volume
                index += 1
            score = self._scorer(order_features)
            if score > 1 and score > best_score:
                best_score = score
                best_price = price
        return best_price

    def rebalance(self, msg, book, balance, outstanding_orders):
        # Wait for 1 second between recalculating orders.
        decisions = Decisions()
        significant_change = msg['type'] == 'match' and float(msg['size']) > self.config.significant_volume
        if not significant_change and time.time() - self.last_updated < 1:
            return decisions.orders
        # TODO(vidurj) it would be good to have this 'bids' 'asks' indicator be part of the side of the
        # order book
        order_book_features = compute_bucket_features(book.bids, OrderSide.BUY) + \
                              compute_bucket_features(book.asks, OrderSide.SELL)
        bid_price = self._pick_best_order(book.bids, book.asks[0].price, OrderSide.BUY, order_book_features)
        self._update_orders(OrderSide.BUY, bid_price, outstanding_orders.values(), decisions, balance)
        ask_price = self._pick_best_order(book.asks, book.bids[0].price, OrderSide.SELL, order_book_features)
        self._update_orders(OrderSide.SELL, ask_price, outstanding_orders.values(), decisions, balance)
        return decisions.orders

    def _update_orders(self, side, new_price, outstanding_orders, decisions, balance):
        assert side == OrderSide.BUY or side == OrderSide.SELL
        relevant_orders = [order.side == side for order in outstanding_orders]
        if len(relevant_orders) > 0:
            assert len(relevant_orders) == 1
            relevant_order = relevant_orders[0]
            if (abs(relevant_order.price - new_price) > self.config.min_price_change) or \
                    (relevant_order.filled_size > 0):
                decisions.cancel_order(relevant_order)
                if new_price:
                    decisions.limit_order(price=new_price, size=self.config.order_size, side=side)
        elif new_price and balance[self.side_to_currency[side]] >= self.config.order_size:
            decisions.limit_order(price=new_price, size=self.config.order_size, side=side)
        return decisions

    def _book_to_cumulative_volume(self, side):
        total_volume = 0
        cumulative_volume = []
        for level in side:
            cumulative_volume.append(total_volume)
            total_volume += level.size
            if total_volume > self.config.max_vol2:
                return cumulative_volume
        return cumulative_volume
