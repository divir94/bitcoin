import bitcoin.logs.logger as lc
import bitcoin.order_book as ob
import bitcoin.params as params
from bitcoin.websocket.core_ws import WebSocket


logger = lc.config_logger('bitstamp_ws', 'DEBUG')


class BitstampOrderBook(WebSocket):
    def __init__(self, product_id, on_change=None):
        super(BitstampOrderBook, self).__init__(url=params.BS_WS_URL,
                                                channel=params.BS_CHANNELS[product_id],
                                                heartbeat=False)
        self.book = None
        self.on_change = on_change

    def on_message(self, msg):
        logger.debug(msg)


if __name__ == '__main__':
    ws = BitstampOrderBook('btcusd')
    ws.start()
