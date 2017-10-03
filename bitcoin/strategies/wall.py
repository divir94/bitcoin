"""
This strategy places an order just before a "wall" e.g. if there are 10 coins within 1 USD of each other.
"""
from bitcoin.strategies.base import BaseStrategy, CumPriceLevel


class WallStrategy(BaseStrategy):
    def __init__(self):
        super(WallStrategy, self).__init__()
        self.max_volume = 100
        self.min_tick = 0.1
        self.price_delta = 1
        self.required_volume = 10

    def get_target_prices(self, book):
        cumulative_volume = self.get_cumulative_volume(book, self.max_volume)
        best_bid, best_ask = book.get_best_bid_ask()
        target_buy, target_ask = None, None

        for side, levels in cumulative_volume.iteritems():
            for level in levels:
                # reached max volume
                if level.cum_size > self.max_volume:
                    break

                # crossed bid-ask spread
                min_tick = -self.min_tick if side == 'bids' else self.min_tick
                price = level.price + min_tick
                if price == best_ask:
                    continue

                # get volume behind current level
                price_delta = -self.price_delta if side == 'bids' else self.price_delta
                search_price = CumPriceLevel(price + price_delta)
                new_index = levels.bisect_key_left(search_price)
                if new_index < len(levels):
                    volume_behind = levels[new_index] - level.cum_size
                else:
                    break

                # update target price if there is enough volume behind
                if volume_behind >= self.required_volume:
                    if side == 'bids':
                        target_buy = price
                    else:
                        target_ask = price

        return target_buy, target_ask
