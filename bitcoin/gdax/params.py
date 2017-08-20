WS_URL = 'wss://ws-feed.gdax.com'

SNAPSHOT_TBLS = {
    'BTC-USD': 'GdaxBtcUsdSnapshot',
    'ETH-USD': 'GdaxEthUsdSnapshot',
    'ETH-BTC': 'GdaxEthBtcSnapshot',
}

MSG_TBLS = {
    'BTC-USD': 'GdaxBtcUsdMessage',
    'ETH-USD': 'GdaxEthUsdMessage',
    'ETH-BTC': 'GdaxEthBtcMessage',
}

CHANNELS = {
    'BTC-USD': {'type': 'subscribe', 'product_ids': ['BTC-USD']},
    'ETH-USD': {'type': 'subscribe', 'product_ids': ['ETH-USD']},
    'ETH-BTC': {'type': 'subscribe', 'product_ids': ['ETH-BTC']}
}

MSG_COLS_TBL = ['sequence', 'time', 'received_time', 'type', 'price', 'size', 'order_id',  'side', 'order_type',
                'funds', 'remaining_size', 'reason', 'trade_id', 'maker_order_id', 'taker_order_id', 'new_size',
                'old_size', 'new_funds', 'old_funds']
