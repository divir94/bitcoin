from collections import namedtuple

SUPPORTED_ORDER_TYPES = ['LimitOrder', 'cancel']
SUPPORTED_ORDER_ACTIONS = ['fill', 'place', 'cancel']
SUPPORTED_ORDER_SIDES = ['buy', 'sell']

OrderType = namedtuple('OrderType', ['LIMIT', 'CANCEL'])(*SUPPORTED_ORDER_TYPES)
OrderAction = namedtuple('OrderAction', ['FILL', 'PLACE', 'CANCEL'])(*SUPPORTED_ORDER_ACTIONS)
OrderSide = namedtuple('OrderSide', ['BUY', 'SELL'])(*SUPPORTED_ORDER_SIDES)

CancelOrder = namedtuple('CancelOrder', ['id'])
LimitOrder = namedtuple('LimitOrder', ['side', 'quote', 'base', 'price', 'size'])
OutstandingOrder = namedtuple('OutstandingOrder', ['id', 'side', 'quote', 'base', 'price', 'size', 'order_time'])
