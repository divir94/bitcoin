import numpy as np
from collections import namedtuple

import bitcoin.backtester.util as butil


vwap = namedtuple('item', ['time', 'price'])


class TickerMomStrategy(object):
    """
    Time series momentum strategy that trades based on median price change last 50 trades.
    """
    def __init__(self):
        # go long/short if past price change is more/less than this amount
        self.long_thresh = 5
        self.short_thresh = -5

        self.num_past_trades = 50
        self.past_prices = []
        self.prev_view = None

    def rebalance(self, context, book, msg):
        """
        Record view and other variables in the context.

        Parameters
        ----------
        context: Context object
            implements `record` method
        msg: dict
            match message

        Returns
        -------
        view: namedtuple
            size: is +1, 0 or -1
            price: mid vwap
        """
        # update past prices
        current_price = msg['price']

        if len(self.past_prices) < self.num_past_trades * 2:
            self.past_prices += [current_price]
            view_size = np.nan
            past_price_change = np.nan
        else:
            self.past_prices = self.past_prices[1:] + [current_price]

            past_median = np.median(self.past_prices[:self.num_past_trades])
            current_median = np.median(self.past_prices[self.num_past_trades:])
            past_price_change = current_median - past_median

            if past_price_change >= self.long_thresh:
                view_size = 1
            elif past_price_change <= self.short_thresh:
                view_size = -1
            else:
                view_size = 0

        # record
        view = butil.View(size=view_size, price=current_price)
        view_diff = view.size - self.prev_view.size if self.prev_view else None

        if self.prev_view and abs(view_diff) > 0:
            context.record(time=msg['time'],
                           view=view_size,
                           view_diff=view_diff,
                           price=current_price,
                           past_price_change=past_price_change)

        self.prev_view = view
        return view
