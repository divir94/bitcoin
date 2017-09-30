WS_URL = {
    'GDAX': 'wss://ws-feed.gdax.com',
    'BITSTAMP': 'ws://ws.pusherapp.com/app/de504dc5763aeef9ff52?protocol=7',

}

CHANNEL = {
    'GDAX': {
        'BTC-USD': {'type': 'subscribe', 'product_ids': ['BTC-USD']},
        'ETH-USD': {'type': 'subscribe', 'product_ids': ['ETH-USD']},
        'ETH-BTC': {'type': 'subscribe', 'product_ids': ['ETH-BTC']}
    },
    'BITSTAMP': {
        'btcusd': {'event': 'pusher:subscribe', 'data': {'channel': 'live_orders'}},
    }
}

SNAPSHOT_TBL = {
    'GDAX': {
        'BTC-USD': 'GdaxBtcUsdSnapshot',
        'ETH-USD': 'GdaxEthUsdSnapshot',
        'ETH-BTC': 'GdaxEthBtcSnapshot',
    }
}

MSG_TBL = {
    'GDAX': {
        'BTC-USD': 'GdaxBtcUsdMessage',
        'ETH-USD': 'GdaxEthUsdMessage',
        'ETH-BTC': 'GdaxEthBtcMessage',
    }
}

MSG_COL_NAME = {
    'GDAX':
        ['sequence', 'time', 'received_time', 'type', 'price', 'size', 'order_id', 'side', 'order_type',
         'funds', 'remaining_size', 'reason', 'trade_id', 'maker_order_id', 'taker_order_id', 'new_size',
         'old_size', 'new_funds', 'old_funds']
}

MSG_NUMERIC_FIELD = {
    'GDAX':
        {'price', 'size', 'funds', 'remaining_size', 'new_size', 'old_size', 'new_funds', 'old_funds'}
}

DATE_FORMAT = {
    'GDAX': '%Y-%m-%dT%H:%M:%S.%fZ'
}
