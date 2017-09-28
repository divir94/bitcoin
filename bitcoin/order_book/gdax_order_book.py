import bitcoin.order_book.order_book as ob
import bitcoin.util as util


class GdaxOrderBook(ob.OrderBook):
    def __init__(self, sequence, bids=None, asks=None):
        super(GdaxOrderBook, self).__init__(sequence=sequence, bids=bids, asks=asks)
        # dict[order id, time str]. timestamp is used in backtester to match orders
        self.order_to_time = {}

    def process_message(self, msg, book=None):
        book = book or self
        sequence = msg['sequence']
        msg = util.to_decimal(msg)

        if sequence <= book.sequence:
            return
        assert sequence == book.sequence + 1, '{} != {} + 1'.format(sequence, book.sequence)

        _type = msg['type']
        if _type == 'open':
            self.open_order(msg, book)
            self.order_to_time[msg['order_id']] = msg['time']

        elif _type == 'done':
            self.done_order(msg, book)
            self.order_to_time.pop(msg['order_id'], None)

        elif _type == 'match':
            self.match_order(msg, book)

        elif _type == 'change':
            self.change_order(msg, book)
        elif _type == 'received' or _type == 'heartbeat':
            pass
        else:
            assert _type == 'error'

        book.sequence = int(msg['sequence'])
        book.time_str = msg['time']

    @staticmethod
    def open_order(msg, book):
        """
        The order is now open on the order book. This message will only be sent for orders which are not fully filled
        immediately. remaining_size will indicate how much of the order is unfilled and going on the book.

        Parameters
        ----------
        msg: dict
         e.g.
         {
            "type": "open",
            "time": "2014-11-07T08:19:27.028459Z",
            "product_id": "BTC-USD",
            "sequence": 10,
            "order_id": "d50ec984-77a8-460a-b958-66f114b0de9b",
            "price": "200.2",
            "remaining_size": "1.00",
            "side": "sell"
        }
        book: ob.OrderBook
        """
        side = msg['side']
        price = msg['price']
        size = msg['remaining_size']
        order_id = msg['order_id']
        book.add(side, price, size, order_id)

    @staticmethod
    def done_order(msg, book):
        """
        The order is no longer on the order book. Sent for all orders for which there was a received message.
        This message can result from an order being canceled or filled. There will be no more messages for this
        order_id after a done message. remaining_size indicates how much of the order went unfilled; this will be 0
        for filled orders.

        market orders will not have a remaining_size or price field as they are never on the open order book at a given
        price.

        Notes
        -----
        A done message will be sent for received orders which are fully filled or canceled due to self-trade prevention.
        There will be no open message for such orders. done messages for orders which are not on the book should be
        ignored when maintaining a real-time order book.

        Parameters
        ----------
        msg: dict
        e.g.
        {
            "type": "done",
            "time": "2014-11-07T08:19:27.028459Z",
            "product_id": "BTC-USD",
            "sequence": 10,
            "price": "200.2",
            "order_id": "d50ec984-77a8-460a-b958-66f114b0de9b",
            "reason": "filled", // canceled
            "side": "sell",
            "remaining_size": "0.2"
        }
        book: ob.OrderBook
        """
        order_id = msg['order_id']
        if order_id not in book.orders:
            return
        book.update(order_id, 0)

    @staticmethod
    def match_order(msg, book):
        """
        A trade occurred between two orders. The aggressor or taker order is the one executing immediately after being
        received and the maker order is a resting order on the book. The side field indicates the maker order side.
        If the side is sell this indicates the maker was a sell order and the match is considered an up-tick.
        A buy side match is a down-tick.

        Parameters
        ----------
        msg: dict
        e.g.
        {
            "type": "match",
            "trade_id": 10,
            "sequence": 50,
            "maker_order_id": "ac928c66-ca53-498f-9c13-a110027a60e8",
            "taker_order_id": "132fb6ae-456b-4654-b4e0-d681ac05cea1",
            "time": "2014-11-07T08:19:27.028459Z",
            "product_id": "BTC-USD",
            "size": "5.23512",
            "price": "400.23",
            "side": "sell"
        }
        book: ob.OrderBook
        """
        price = msg['price']
        trade_size = msg['size']
        order_id = msg['maker_order_id']

        # get original order size
        old_price, old_size, order_id = book.get(order_id)
        assert price == old_price
        assert trade_size <= old_size

        # update order to new size
        new_size = old_size - trade_size
        book.update(order_id, new_size)

    @staticmethod
    def change_order(msg, book):
        """
        An order has changed. This is the result of self-trade prevention adjusting the order size or available funds.
        Orders can only decrease in size or funds. change messages are sent anytime an order changes in size;
        this includes resting orders (open) as well as received but not yet open. change messages are also sent when a
        new market order goes through self trade prevention and the funds for the market order have changed.

        Notes
        -----
        change messages for received but not yet open orders can be ignored when building a real-time order book.
        The side field of a change message and price can be used as indicators for whether the change message is
        relevant if building from a level 2 book.

        Any change message where the price is null indicates that the change message is for a market order.
        Change messages for limit orders will always have a price specified.

        Parameters
        ----------
        msg: dict
        e.g.
        {
            "type": "change",
            "time": "2014-11-07T08:19:27.028459Z",
            "sequence": 80,
            "order_id": "ac928c66-ca53-498f-9c13-a110027a60e8",
            "product_id": "BTC-USD",
            "new_size": "5.23512",
            "old_size": "12.234412",
            "price": "400.23",
            "side": "sell"
        }
        book: ob.OrderBook
        """
        price = msg.get('price')
        new_size = msg.get('new_size')
        order_id = msg['order_id']

        if order_id not in book.orders or not price or not new_size:
            return

        result = book.update(order_id, new_size)
        assert price == result[0]
