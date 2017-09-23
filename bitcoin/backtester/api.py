import bitcoin.storage.api as st


class Backtester(object):
    def __init__(self, exchange, product_id):
        self.exchange = exchange
        self.product_id = product_id
        self.outstanding_orders = {}
        self.trades = {}
        self.balances = {}

    def execute_orders(self):
        # update outstanding orders and balances
        pass

    def update_orders(self, msg):
        pass

    def run(self, strategy, start, end):
        book = st.get_book(self.exchange, self.product_id, timestamp=start)
        msgs = st.get_messages(self.exchange, self.product_id, start=start, end=end)

        for msg in msgs:
            new_orders = strategy.rebalance(message=msg,
                                            book=book,
                                            outstanding_orders=self.outstanding_orders,
                                            balance=self.balances)
            book.process_message(msg)
