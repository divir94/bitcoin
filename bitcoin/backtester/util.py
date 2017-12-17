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
