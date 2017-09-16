from bitcoin.websocket.gdax import GdaxOrderBook
import gdax

"""
    Plan:
        Look at the order book imbalance, and maybe momentum to place orders

    Necessary Infrastructure:
        Need to be able to place orders quickly in response to market events too. For instance, if there is a
        lot of volume at the best bid but now an opportunity opens up to place a bid 1 cent above.
        Know our balance and outstanding orders at all times? Or can we just query them every second
        when placing new orders.
        We can cancel orders at any time. For this purpose we need to maintain the average price of x coins
        in each book, so we can cancel our order if it doesn't look favorable.

"""


class Features(object):
    def __init__(self, product_id, size_for_avg=5):
        self.product_id = product_id
        self.size_for_avg = size_for_avg
        # TODO(vidurj) we probably just want to maintain the volume within x% of the price
        self.avg_bid = None
        self.avg_ask = None

    def update_on_msg(self, msg):
        pass

    def get_features(self):
        return [
            self.avg_bid,
            self.avg_ask
        ]


class Trader(object):
    def __init__(self):
        self.client = gdax.AuthenticatedClient(
            key="8e944b216e435691b071dd3f0f62caa9",
            b64secret="uSysPzywwj3LUlPuuNkKEHRpxhd4A+K/JGwbEfrc28nkd4c6qMLwr1IbRsw1GB1BKQzVXoj88n/WjMvIAvv/MA==",
            passphrase="ltg10b8unuo"
        )

        # self.balances = {
        #     'usd': None,
        #     'btc': None,
        #     'eth': None
        # }
        # self.outstanding_orders = {
        #     'btc_usd_bid': None,
        #     'btc_usd_ask': None,
        #     'eth_usd_bid': None,
        #     'eth_usd_ask': None,
        #     'eth_btc_bid': None,
        #     'eth_btc_ask': None
        # }
        # self.product_id_to_features = {
        #     'BTC-USD': Features('BTC-USD'),
        #     'ETH-BTC': Features('ETH-BTC'),
        #     'ETH-USD': Features('ETH-USD')
        # }

        def cancellation_logic_btc_usd(msg):
            if msg['type'] == 'match' and msg['size'] > 5:
                self.client.cancel_all()

        self.btc_usd_ws = GdaxOrderBook('BTC-USD')
