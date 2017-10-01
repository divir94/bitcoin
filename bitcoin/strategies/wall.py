"""
This strategy places an order just before a "wall" e.g. if there are 10 coins within 1 USD of each other.
"""
from bitcoin.strategies.base import BaseStrategy


class WallStrategy(BaseStrategy):
    @staticmethod
    def get_target_prices(book):
        return
