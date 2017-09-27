import bitcoin.util as util


class Order(util.BaseObject):
    def __init__(self, order_id, quote, base, side, price, size, time, order_type='limit', cancel=False):
        self.id = order_id
        self.quote = quote
        self.base = base
        self.side = side
        self.price = price
        self.size = size
        self.time = time
        self.type = order_type
        self.cancel = cancel
