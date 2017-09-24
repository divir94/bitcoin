import bitcoin.storage.api as st


class Backtester(object):
    def __init__(self, exchange, product_id):
        self.exchange = exchange
        self.product_id = product_id
        self.outstanding_orders = {}
        self.trades = {}
        self.balance = {}

    def execute_orders(self, orders):
        # update outstanding orders and balances
        for order in orders:
            new_balance = self.update_balance(order)
            self.balance = new_balance
            self.outstanding_orders[order.id] = order

    @staticmethod
    def update_balance(order, balance):
        new_balance = balance.copy()
        if order.type == 'limit':
            if order.side == 'buy':
                value = order.price * order.size
                current_balance = balance[order.base]
                field = order.base
            elif order.side == 'sell':
                value = order.size
                field = order.currency
                current_balance = balance[order.currency]
            else:
                raise ValueError

            assert current_balance >= value, 'Insufficient balance {} for order {}'.format(current_balance, order)
            new_balance[field] -= value
        else:
            raise ValueError('Invalid order type {}'.format(order.type))
        return new_balance

    def update_orders(self, msg):
        pass

    def run(self, strategy, start, end):
        book = st.get_book(self.exchange, self.product_id, timestamp=start)
        msgs = st.get_messages(self.exchange, self.product_id, start=start, end=end)

        for msg in msgs:
            new_orders = strategy.rebalance(message=msg,
                                            book=book,
                                            outstanding_orders=self.outstanding_orders,
                                            balance=self.balance)
            self.execute_orders(new_orders)
            self.update_orders(msg)
            book.process_message(msg)
