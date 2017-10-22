import bitcoin.order_book.order_book as ob


class BtOrderBook(ob.OrderBook):
    """
    Processes Bitstamp messages to maintain an order book.
    """
    def __init__(self, sequence, bids=None, asks=None, time_str=None):
        super(BtOrderBook, self).__init__(sequence=sequence, bids=bids, asks=asks, time_str=time_str)
        self.exchange = 'GDAX'

    def process_message(self, msg, book=None):
        book = book or self
        event = msg['event']
        data = msg['data']

        if event == 'order_created':
            self.create_order(data, book)

        elif event == 'order_deleted':
            self.delete_order(data, book, delete=True)

        elif event == 'order_changed':
            self.change_order(data, book, delete=False)
        else:
            pass

        book.sequence = data.get('id')
        book.time_str = data.get('datetime')

    @staticmethod
    def create_order(data, book):
        side = 'buy' if data['order_type'] == 0 else 'sell'
        price = data['price']
        size = data['amount']
        order_id = data['id']
        book.add(side, price, size, order_id)

    @staticmethod
    def update_order(data, book, delete):
        order_id = data['id']
        size = data['amount']
        price = data['price']
        new_size = 0 if delete else size
        old_price, old_size, order_id = book.update(order_id, new_size)
        assert price == old_price, '{} == {}'.format(price, old_price)
        assert size == old_size, '{} == {}'.format(size, old_size)
