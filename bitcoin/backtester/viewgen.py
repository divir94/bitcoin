import pandas as pd


class ViewGen(object):
    """
    ViewGen takes a strategy and computers a view (number) i.e. desired exposure at every point in time.
    ViewGen also generates a tear sheet comprising of tables and charts to analyze the view.
    """
    def __init__(self, strategy):
        """

        Parameters
        ----------
        strategy: Strategy object
            implements rebalance method
        """
        self.strategy = strategy
        self._context = Context()
        self.result = self._context.result

    def run(self, data, end=None):
        """
        Generate the view by calling strategy's rebalance method and updates `self.result` with the view and
        other variables from the strategy.

        Parameters
        ----------
        data: object
            has book and messages as fields
        end: str
            used to end the viewgen early

        Returns
        -------
        self.result
        """
        book = data.book
        messages = data.messages

        for msg in messages:
            if end and book.time_str > end:
                return

            # received messages have no impact
            if msg['type'] == 'received':
                continue

            self.strategy.rebalance(self._context, book)
            book.process_message(msg)

    def create_tear_sheet(self):
        pass


class Context(object):
    """
    Records local variables from strategy at every rebalance.
    """
    def __init__(self):
        self._recorded_vars = []

    @property
    def result(self):
        return pd.DataFrame(self._recorded_vars)

    def record(self, **kwargs):
        self._recorded_vars.append(**kwargs)
