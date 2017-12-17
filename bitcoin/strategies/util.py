import numpy as np
import pandas as pd

import bitcoin.logs.logger as lc


logger = lc.config_logger('strategy_util', level='INFO', file_handler=False)


def get_vwap(book, size):
    """
    Get volume weighted average price of the top `size` coins. It's a better approximation of the price than
    using the mid price of the best bid and ask.

    Parameters
    ----------
    book: object
    size: int

    Returns
    -------
    float
    """
    all_levels = dict(bids=book.bids, asks=book.asks)
    vwap = dict(bids=0, asks=0)

    for side, levels in all_levels.iteritems():
        volume_so_far = 0
        for level in levels:
            # size is min(level size, max size)
            vwap[side] += level.price * min(level.size, size - volume_so_far)
            volume_so_far += level.size
            if volume_so_far > size:
                # reached max size
                break
    # midpoint
    avg_bid = vwap['bids'] / size
    avg_ask = vwap['asks'] / size
    mid_vwap = (avg_bid + avg_ask) / 2
    return mid_vwap


def get_mom_deciles(prices, bins=10):
    """
    Get time series momentum deciles i.e. mean forward price difference when sorted by mean past price difference.

    Parameters
    ----------
    prices: pd.Series
    bins: int

    Returns
    -------
    pd.DataFrame
        index: bin numbers
        columns: [fwd, past]
    """
    groups = prices.groupby(pd.TimeGrouper(freq='1T'))
    # past return is the difference b/w the mean prices of the current and past minute
    past = groups.mean().diff()
    fwd = past.shift(-1)
    past_fwd = pd.DataFrame(dict(past=past, fwd=fwd))
    past_fwd = past_fwd[~past_fwd.past.isnull()]

    # split into equal sized bins and take the mean
    chunks = np.array_split(past_fwd.sort_values('past'), bins)
    deciles = pd.DataFrame([chunk.mean() for chunk in chunks])
    deciles.iplot(kind='bar', title='TS Mom Price Differences')
    return deciles
