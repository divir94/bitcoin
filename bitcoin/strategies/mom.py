import numpy as np
from collections import deque, namedtuple

import bitcoin.strategies.util as sutil


vwap = namedtuple('item', ['time', 'price'])


class MomStrategy(object):
    """
    Time series momentum strategy that goes long/short based on the price change over the past minute.
    The idea is to long the top decile and short the bottom decile of past price changes.
    The decile bounds are updated in an expanding window.
    """
    def __init__(self):
        # go long/short if past price change is more/less than this amount
        self.long_thresh = 5
        self.short_thresh = -5
        # num seconds in the past to look at to calculate past price change
        self.lookback = 60
        # deque is double sided queue of dict(time, vwap)
        self.past_vwaps = deque()

    def rebalance(self, context, book):
        """
        Record view and other variables in the context.

        Parameters
        ----------
        context: Context object
            implements `record` method
        book: OrderBook
            has `timestamp` attribute used to index recorded variables

        Returns
        -------
        None
        """
        # get current vwap
        price = sutil.get_vwap(book, size=1)
        current_time = book.timestamp
        current_vwap = vwap(time=current_time, price=price)
        self.past_vwaps.append(current_vwap)

        # update past 1 min vwap
        update = True

        # remove old values greater than the lookback period
        while update:
            last_time = self.past_vwaps[0].time
            time_delta = (current_time - last_time).total_seconds()
            update = time_delta > self.lookback
            # remove old value
            if update:
                self.past_vwaps.popleft()

        # get view
        last_vwap = self.past_vwaps[0]
        time_delta = (current_time - last_vwap.time).total_seconds()
        past_price_change = price - last_vwap.price if time_delta > self.long_thresh else np.nan
        if past_price_change >= self.long_thresh:
            view = 1
        elif past_price_change <= self.short_thresh:
            view = -1
        else:
            view = 0

        # record
        context.record(time=book.timestamp,
                       view=view,
                       price=price,
                       num_msgs=len(self.past_vwaps),
                       past_price=last_vwap.price,
                       past_price_change=past_price_change)
