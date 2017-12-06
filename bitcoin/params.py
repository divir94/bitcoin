import numpy as np


DEFAULT_EXCHANGE = 'GDAX'
DEFAULT_PRODUCT = 'BTC-USD'

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
        'BTC-USD': {'event': 'pusher:subscribe', 'data': {'channel': 'live_orders'}},
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

MSG_DTYPE = {
    'GDAX':
        {
            'sequence': np.int_,
            'time': np.datetime64,
            'received_time': np.datetime64,
            'type': np.object_,
            'price': np.float_,
            'size': np.float_,
            'order_id': np.object_,
            'side': np.object_,
            'order_type': np.object_,
            'funds': np.float_,
            'remaining_size': np.float_,
            'reason': np.object_,
            'trade_id': np.float_,
            'maker_order_id': np.object_,
            'taker_order_id': np.object_,
            'new_size': np.float_,
            'old_size': np.float_,
            'new_funds': np.float_,
            'old_funds': np.float_,
            'product_id': np.object_,
            'client_oid': np.object_,
        }
}

DATE_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'
