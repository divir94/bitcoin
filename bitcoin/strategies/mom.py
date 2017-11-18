from collections import deque
from datetime import datetime

import bitcoin.strategies.util as autil
import bitcoin.params as params
from bitcoin.strategies.base import BaseStrategy


class TSMomStrategy(BaseStrategy):
    def __init__(self):
        super(TSMomStrategy, self).__init__()
        # go long/short if past price change is more than this amount in USD
        self.long_thresh = 30
        self.short_thresh = -30
        self.vwaps = deque()
        self.tracking = {}

    def get_view(self, book):
        """
        View is an integer (e.g. +1, 0, -1) for desired exposure
        """
        # get current vwap
        vwap = autil.get_vwap(book, size=10)
        current_time = datetime.strptime(book.time_str, params.DATE_FORMAT['GDAX'])
        vwap['time'] = current_time
        self.vwaps.append(vwap)

        # update past 1 min vwap
        current_vwap = self.vwaps[-1]
        prev_vwap = self.vwaps[0]
        last_time = prev_vwap['time']

        update_deque = True
        while update_deque:
            time_delta = (current_vwap['time'] - prev_vwap['time']).total_seconds()
            last_minute = time_delta > 60
            if last_minute:
                self.vwaps.popleft()
            else:
                update_deque = False
            prev_vwap = self.vwaps[0]

        # get past return
        prev_mid_vwap = (prev_vwap['asks'] + prev_vwap['bids']) / 2
        current_mid_vwap = (current_vwap['asks'] + current_vwap['bids']) / 2
        price_change = current_mid_vwap - prev_mid_vwap

        # view
        if price_change > self.long_thresh:
            view = 1
        elif price_change < self.short_thresh:
            view = -1
        else:
            view = 0

        # track
        self.tracking[current_time] = [view, price_change, current_mid_vwap, last_time]
        return view
