from collections import namedtuple

CancelOrder = namedtuple('CancelOrder', ['id'])
LimitOrder = namedtuple('LimitOrder', ['side', 'quote', 'base', 'price', 'size'])
# OutstandingOrder.time_string is assumed to come straight from the gdax api so that
# lexicographical ordering is appropriate
OutstandingOrder = namedtuple('OutstandingOrder', ['id', 'side', 'quote', 'base', 'price', 'size', 'time_string'])
