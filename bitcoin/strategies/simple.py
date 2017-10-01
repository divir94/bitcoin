"""
This strategy always has an order at the best bid and ask.
"""
from bitcoin.strategies.base import BaseStrategy


class SimpleStrategy(BaseStrategy):
    @staticmethod
    def get_target_prices(book):
        target_buy, target_ask = book.get_best_bid_ask()
        return target_buy, target_ask
