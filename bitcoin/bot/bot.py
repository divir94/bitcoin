from threading import Thread

import bitcoin.logs.logger as lc
import bitcoin.websocket.gdax_ws as gws


logger = lc.config_logger('bot')


class Bot(object):
    def __init__(self):
        self.ws = gws.GdaxWebSocket()

    def run(self):
        t = Thread(name='websocket', target=self.ws.start)
        t.start()


if __name__ == '__main__':
    bot = Bot()
    bot.run()
