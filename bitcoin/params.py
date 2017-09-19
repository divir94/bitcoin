#######
# Gdax
#######

GX_WS_URL = 'wss://ws-feed.gdax.com'

GX_CHANNELS = {
    'BTC-USD': {'type': 'subscribe', 'product_ids': ['BTC-USD']},
    'ETH-USD': {'type': 'subscribe', 'product_ids': ['ETH-USD']},
    'ETH-BTC': {'type': 'subscribe', 'product_ids': ['ETH-BTC']}
}

GX_SNAPSHOT_TBLS = {
    'BTC-USD': 'GdaxBtcUsdSnapshot',
    'ETH-USD': 'GdaxEthUsdSnapshot',
    'ETH-BTC': 'GdaxEthBtcSnapshot',
}

GX_MSG_TBLS = {
    'BTC-USD': 'GdaxBtcUsdMessage',
    'ETH-USD': 'GdaxEthUsdMessage',
    'ETH-BTC': 'GdaxEthBtcMessage',
}

GX_MSG_COLS_TBL = ['sequence', 'time', 'received_time', 'type', 'price', 'size', 'order_id', 'side', 'order_type',
                   'funds', 'remaining_size', 'reason', 'trade_id', 'maker_order_id', 'taker_order_id', 'new_size',
                   'old_size', 'new_funds', 'old_funds']

GDAX_DATE_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'

###########
# Bitstamp
###########

BS_WS_URL = 'ws://ws.pusherapp.com/app/de504dc5763aeef9ff52?protocol=7'

BS_CHANNELS = {
    'btcusd': {'event': 'pusher:subscribe', 'data': {'channel': 'live_orders'}},
}
