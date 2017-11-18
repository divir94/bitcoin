from collections import namedtuple

SUPPORTED_ORDER_TYPES = ['LimitOrder', 'CancelOrder']
SUPPORTED_ORDER_ACTIONS = ['fill', 'place', 'cancel']
SUPPORTED_ORDER_SIDES = ['buy', 'sell']

OrderType = namedtuple('OrderType', ['LIMIT', 'CANCEL'])(*SUPPORTED_ORDER_TYPES)
OrderAction = namedtuple('OrderAction', ['FILL', 'PLACE', 'CANCEL'])(*SUPPORTED_ORDER_ACTIONS)
OrderSide = namedtuple('OrderSide', ['BUY', 'SELL'])(*SUPPORTED_ORDER_SIDES)

CancelOrder = namedtuple('CancelOrder', ['id'])
LimitOrder = namedtuple('LimitOrder', ['id', 'side', 'price', 'size'])
CurrentOrder = namedtuple('CurrentOrder', ['id', 'side', 'price', 'size', 'order_time'])
