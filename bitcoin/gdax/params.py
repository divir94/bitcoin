BTC_PRODUCT_ID = 'BTC-USD'
WS_URL = 'wss://ws-feed.gdax.com'
CHANNEL = {'type': 'subscribe', 'product_ids': [BTC_PRODUCT_ID]}


# mapping of db table name to field names to store
MSG_DB_MAPPING = {
    'received': {
        'table': 'GdaxReceived',
        'fields': ['time', 'received_time', 'sequence', 'order_id', 'price', 'size', 'side', 'funds', 'order_type'],
    },
    'open': {
        'table': 'GdaxOpen',
        'fields': ['time', 'received_time', 'sequence', 'order_id', 'price', 'remaining_size', 'side'],
    },
    'done': {
        'table': 'GdaxDone',
        'fields': ['time', 'received_time', 'sequence', 'order_id', 'price', 'remaining_size', 'side', 'reason'],
    },
    'match': {
        'table': 'GdaxMatch',
        'fields': ['time', 'received_time', 'sequence', 'trade_id', 'maker_order_id', 'taker_order_id', 'price', 'size',
                   'side'],
    },
    'change': {
        'table': 'GdaxChange',
        'fields': ['time', 'received_time', 'sequence', 'order_id', 'price', 'new_size', 'old_size', 'side',
                   'new_funds', 'old_funds'],
    },
}

BOOK_TBL = 'GdaxBookLevel3'
