from threading import Thread

import bitcoin.logs.logger as lc
import bitcoin.params as params
from bitcoin.websocket.core_ws import WebSocket


logger = lc.config_logger('bitstamp_ws', 'DEBUG')


class BitstampOrderBook(WebSocket):
    def __init__(self, product_id, channel):
        self.exchange = 'BITSTAMP'
        super(BitstampOrderBook, self).__init__(url=params.WS_URL[self.exchange],
                                                channel=channel,
                                                heartbeat=False)
        self.book = None

    def on_message(self, msg):
        logger.debug(msg)


if __name__ == '__main__':
    product_id = 'BTC-USD'
    live_orders = {'event': 'pusher:subscribe', 'data': {'channel': 'live_orders'}}
    live_trades = {'event': 'pusher:subscribe', 'data': {'channel': 'live_trades'}}

    ws1 = BitstampOrderBook(product_id=product_id, channel=live_orders)
    ws2 = BitstampOrderBook(product_id=product_id, channel=live_trades)

    t1 = Thread(target=ws1.start).start()
    t2 = Thread(target=ws2.start).start()
