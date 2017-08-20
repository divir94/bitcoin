BTC_PRODUCT_ID = 'BTC-USD'
ETH_PRODUCT_ID = 'ETH-USD'

WS_URL = 'wss://ws-feed.gdax.com'
SNAPSHOT_TBLS = {
    'BTC-USD': 'GdaxBtcSnapshot',
    'ETH-USD': 'GdaxEthSnapshot'
}
MSG_TBLS = {
    'BTC-USD': 'GdaxBtcMessage',
    'ETH-USD': 'GdaxEthMessage'
}
CHANNELS = {
    'BTC-USD': {'type': 'subscribe', 'product_ids': [BTC_PRODUCT_ID]},
    'ETH-USD': {'type': 'subscribe', 'product_ids': [ETH_PRODUCT_ID]}
}
MSG_COLS_TBL = ['sequence', 'time', 'received_time', 'type', 'price', 'size', 'order_id',  'side', 'order_type',
                'funds', 'remaining_size', 'reason', 'trade_id', 'maker_order_id', 'taker_order_id', 'new_size',
                'old_size', 'new_funds', 'old_funds']

