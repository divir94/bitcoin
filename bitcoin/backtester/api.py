import pandas as pd

import bitcoin.logs.logger as lc
import bitcoin.storage.util as sutil

import bitcoin.backtester.viewgen as vg
import bitcoin.backtester.tradegen as tg


logger = lc.config_logger('backtester', level='DEBUG', file_handler=False)


class BackTester(object):
    def __init__(self, strategy):
        self.viewgen = vg.ViewGen(strategy)
        self.tradegen = tg.TradeGen()

    def run(self, data, end=None, run_tradegen=False):
        """
        Run viewgen and tradegen. Viewgen outputs how much and at what price to buy for and tradegen implements
        the view by placing limit orders.

        Parameters
        ----------
        data: namedtuple
            has book and messages as fields
        end: pd.datetime or str
            used to end the backtester early
        run_tradegen: bool

        Returns
        -------
        self.result
        """
        book = sutil.df_to_book(data.book)
        messages = data.messages

        for msg in messages:
            if end and book.timestamp > end:
                return

            # received messages have no impact
            if msg['type'] == 'received':
                continue

            # viewgen and tradegen
            view = self.viewgen.run(book=book)

            if run_tradegen:
                self.tradegen.run(view=view, message=msg, book=book)

            # update book
            book.process_message(msg)
