import pandas as pd
from collections import namedtuple


FillOrder = namedtuple('FillOrder', ['order_id', 'fill_time', 'fill_size'])
DoneOrder = namedtuple('DoneOrder', ['order_id', 'time', 'status'])
OpenOrder = namedtuple('OpenOrder', ['time', 'price', 'size'])
View = namedtuple('View', ['size', 'price'])


SIDE_DICT = {
    'buy': 1.0,
    'sell': -1.0
}


class Context(object):
    """
    Records local variables from strategy at every rebalance.
    """
    def __init__(self):
        self._recorded_vars = []

    @property
    def result(self):
        df = pd.DataFrame(self._recorded_vars).drop_duplicates()
        if 'time' in df.columns:
            df.set_index('time', inplace=True)
        return df

    def record(self, **kwargs):
        self._recorded_vars.append(kwargs)


def get_competitive_prices(book):
    """
    Get target bid and ask prices that are 1 cent above the best bid/ask or at the best bid/ask.

    Parameters
    ----------
    book: GdaxOrderBook

    Returns
    -------
    tuple(float, float)
    """
    best_bid, best_ask = book.get_best_bid_ask()
    spread = best_ask - best_bid
    if spread == 0.01:
        target_bid, target_ask = best_bid, best_ask
    else:
        target_bid, target_ask = best_bid + 0.01, best_ask - 0.01
    return target_bid, target_ask
