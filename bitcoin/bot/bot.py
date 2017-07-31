import logging
import time

from bitcoin.websocket.bitstamp import BitstampOrderBook

# logging
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

fh = logging.FileHandler('../logs/bot.log', 'w')
fh.setFormatter(formatter)
logger.addHandler(fh)

ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)

# exchanges
BS_CHANNEL = 'order_book'


class Bot(object):
    def __init__(self):
        self.balances = None
        self.gx_book = {}
        self.bs_book = {}
        self.current_limit_orders = []

    def run(self):
        bs_ws = BitstampOrderBook(self.handle_book_change, BS_CHANNEL)
        bs_ws.run()
        time.sleep(10)
        bs_ws.close()

    def handle_book_change(self, new_book, exchange):
        logger.debug('Handling {} book change: {}'.format(exchange, new_book))

        # get latest order books
        if exchange == 'GDAX':
            old_book = self.gx_book
            self.gx_book = new_book
        elif exchange == 'BITSTAMP':
            old_book = self.bs_book
            self.bs_book = new_book
        else:
            raise ValueError('Unknown exchange'.format(exchange))

        # update orders if book changed substantially
        is_different = is_book_different(new_book, old_book)

        if is_different:
            # get new orders
            orders = get_limit_orders(self.gx_book, self.bs_book, self.balances)

            # update orders
            update_limit_orders(orders)


def is_book_different(new_book, old_book):
    if not old_book:
        return True

    best_bid_different = new_book['bids'][0] != old_book['bids'][0]
    best_ask_different = new_book['asks'][0] != old_book['asks'][0]
    is_different = best_bid_different or best_ask_different

    if is_different:
        logger.debug('\nNew book: {}\nOld book: {}'.format(new_book, old_book))
    return is_different


def get_limit_orders(gx_book, bs_book, balances):
    """
    Calculates the price and size for a buy and sell limit order on both exchanges. 4 orders in total.
    """
    pass


def update_limit_orders(new_orders):
    # cancel exisiting orders

    # think about fund availability

    # when to place new orders? after callback from cancel?
    pass


if __name__ == '__main__':
    bot = Bot()
    bot.run()
