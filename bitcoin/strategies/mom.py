import pandas as pd
import numpy as np
from collections import deque, namedtuple
from datetime import datetime

import bitcoin.strategies.util as sutil
import bitcoin.params as params


vwap = namedtuple('item', ['time', 'price'])


class MomStrategy(object):
    """
    Time series momentum strategy that goes long/short based on the price change over the past minute.
    The idea is to long the top decile and short the bottom decile of past price changes.
    The decile bounds are updated in an expanding window.
    """
    def __init__(self):
        # go long/short if past price change is more/less than this amount in %
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
        book: object
            has `time_str` attribute used to index recorded variables

        Returns
        -------
        None
        """
        # get current vwap
        price = sutil.get_vwap(book, size=1)
        current_time = datetime.strptime(book.time_str, params.DATE_FORMAT['GDAX'])
        current_vwap = vwap(time=current_time, price=price)
        self.past_vwaps.append(current_vwap)

        # update past 1 min vwap
        update = True

        # remove old values greater than the lookback period
        while update:
            last_time = self.past_vwaps[-1].time
            time_delta = (current_time - last_time).total_seconds()
            update = time_delta > self.lookback
            # remove old value
            if update:
                self.past_vwaps.popleft()

        # get view
        last_vwap = self.past_vwaps[-1]
        past_price_change = price - last_vwap.price
        bins = [-np.inf, -self.short_thresh, self.long_thresh, np.inf]
        view = pd.cut([past_price_change], bins=bins, labels=[-1, 0, 1])[0]

        # record
        context.record(view=view, price=price, past_price_change=past_price_change)
