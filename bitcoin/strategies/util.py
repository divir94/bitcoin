import uuid
import numpy as np
import pandas as pd

import bitcoin.logs.logger as lc
from bitcoin.backtester.orders import *


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


def get_competitive_prices(best_bid, best_ask):
    spread = best_ask - best_bid
    if spread == 0.01:
        target_bid, target_ask = best_bid, best_ask
    else:
        target_bid, target_ask = best_bid + 0.01, best_ask - 0.01
    return target_bid, target_ask


def generate_orders(view, exposure, current_orders, best_bid, best_ask, sig_price_change=1):
    """
    Get new orders from view and current orders. Places orders at the most competitive price.
    """
    trade_size = abs(view - exposure)
    eps = 1e-4
    cancel_orders = []
    new_orders = []

    # current orders
    buy_order = current_orders.get(OrderSide.BUY)
    sell_order = current_orders.get(OrderSide.SELL)

    # get target price and side
    target_bid, target_ask = get_competitive_prices(best_bid, best_ask)
    side = OrderSide.BUY if view > exposure else OrderSide.SELL
    target_price = target_bid if side == OrderSide.BUY else target_ask

    # cancel opposite side
    opp_side_order = sell_order if side == OrderSide.BUY else buy_order
    if opp_side_order:
        cancel_orders.append(CancelOrder(opp_side_order.id))

    # cancel same side ONLY if bigger or there is significant price change
    same_side_order = buy_order if side == OrderSide.BUY else sell_order
    if same_side_order:
        is_bigger = same_side_order.size > trade_size
        sig_change = abs(same_side_order.price - target_price) > sig_price_change
        if is_bigger or sig_change:
            cancel_orders.append(CancelOrder(same_side_order.id))

    # place new order ONLY if there is no order on the same side
    elif trade_size > eps:
        new_order = LimitOrder(id=uuid.uuid4(),
                               side=side,
                               price=target_price,
                               size=trade_size)
        new_orders.append(new_order)

    all_orders = new_orders + cancel_orders
    return all_orders


# TODO(divir): finish this
def filter_orders(orders, balance):
    """
    Remove orders for which we don't have enough balance.
    """
    new_orders = orders
    return new_orders


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
