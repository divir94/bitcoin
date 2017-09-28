from collections import namedtuple

CancelOrder = namedtuple('CancelOrder', ['id'])
LimitOrder = namedtuple('LimitOrder', ['price', 'size', 'side', 'base', 'quote'])
# OutstandingOrder.time_string is assumed to come straight from the gdax api so that
# lexicographical ordering is appropriate
OutstandingOrder = namedtuple('OutstandingOrder',
                              [
                                  'id',
                                  'quote',
                                  'base',
                                  'side',
                                  'price',
                                  'size',
                                  'time_string'
                              ])
