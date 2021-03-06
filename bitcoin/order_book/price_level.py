import bitcoin.util as util


class PriceLevel(util.BaseObject):
    def __init__(self, price, orders):
        """
        Contains sll the bid or ask orders for a corresponding price.
        The add, remove and update methods return (price, new_size, order_id).

        Parameters
        ----------
        price: float
        orders: dict[order_id, size]
        """
        self.price = price
        self.orders = orders
        self.size = sum(self.orders.values())

    def add(self, size, order_id):
        # add size and order_id
        self.size += size
        self.orders[order_id] = size
        return self.price, size, order_id

    def update(self, order_id, new_size):
        assert order_id in self.orders
        # subtract the size change
        old_size = self.orders[order_id]
        self.size += (new_size - old_size)
        # update order
        self.orders[order_id] = new_size
        # remove order if needed
        if new_size == 0:
            del self.orders[order_id]
        return self.price, old_size, order_id

    def to_set(self):
        return {(self.price, size, order_id)
                for order_id, size in self.orders.iteritems()}
