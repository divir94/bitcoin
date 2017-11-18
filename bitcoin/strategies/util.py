import uuid
from sortedcontainers import SortedListWithKey

import bitcoin.logs.logger as lc
from bitcoin.backtester.orders import *


logger = lc.config_logger('strategy_util', level='INFO', file_handler=False)


def get_vwap(book, size):
    all_levels = dict(bids=book.bids, asks=book.asks)
    vwap = dict(bids=0, asks=0)

    for side, levels in all_levels.iteritems():
        volume_so_far = 0
        for level in levels:
            vwap[side] += level.price * min(level.size, size - volume_so_far)
            volume_so_far += level.size
            if volume_so_far > size:
                break
    return vwap


def get_cumulative_volume(book, max_volume=None):
    """
    {
        'asks': list of (price, cumulative volume) is ascending order,
        'bids': list of (price, cumulative volume) is descending order,
    }
    """
    CumPriceLevel = namedtuple('CumPriceLevel', ['price', 'cum_size'])
    CumPriceLevel.__new__.__defaults__ = (None,)

    all_levels = {
        'bids': book.bids,
        'asks': book.asks
    }
    cumulative_volume = {
        'bids': SortedListWithKey(key=lambda x: -x.price),
        'asks': SortedListWithKey(key=lambda x: x.price)
    }

    for side, levels in all_levels.iteritems():
        volume_so_far = 0
        for level in levels:
            volume_so_far += level.size
            value = CumPriceLevel(price=level.price, cum_size=volume_so_far)
            cumulative_volume[side].add(value)
            if max_volume and (volume_so_far > max_volume):
                break
    return cumulative_volume


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
    # if all_orders:
    #     logger.debug('View: {}, exposure: {}'.format(view, exposure))
    #     logger.debug('Current orders: {}'.format(current_orders))
    #     logger.debug('New orders: {}'.format(all_orders))
    return all_orders


# TODO(divir): finish this
def filter_orders(orders, balance):
    """
    Remove orders for which we don't have enough balance.
    """
    new_orders = orders
    return new_orders
