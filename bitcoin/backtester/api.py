import pandas as pd
from sortedcontainers import SortedList

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
        book = None
        messages = data.messages
        if not data.messages:
            logger.error('No messages found!')
            return

        for msg in messages:
            # if msg['sequence'] == 4339585732:
            #     import pdb
            #     pdb.set_trace()

            # get book
            if not book or (msg['sequence'] > (book.sequence + 1)):
                book = self.get_next_book(sequence=msg['sequence'],
                                          timestamp=msg['time'],
                                          books_df=data.books)

            if end and book.timestamp > end:
                return

            # received messages have no impact
            if msg['type'] == 'received':
                book.sequence = msg['sequence']
                continue

            # viewgen and tradegen
            view = self.viewgen.run(book=book)

            if run_tradegen:
                self.tradegen.run(view=view, message=msg, book=book)

            # update book
            book.process_message(msg)

    @staticmethod
    def get_next_book(sequence, timestamp, books_df):
        """
        Get next book after the sequence if available.

        Parameters
        ----------
        sequence: int
        timestamp: datetime
        books_df: DataFrame
            multiple books

        Returns
        -------
        GdaxOrderBook or None
        """
        if books_df.empty:
            logger.error('No books available.')
            return

        # get index of next sequence
        seq_list = SortedList(books_df['sequence'].unique())
        next_seq_idx = seq_list.bisect_left(sequence)

        if next_seq_idx >= len(seq_list):
            logger.error('No book found after {} at {}. Available books: {}'.format(sequence, timestamp, seq_list))
            return

        # next book
        next_seq = seq_list[next_seq_idx]
        book_df = books_df[books_df['sequence'] == next_seq]
        book = sutil.df_to_book(book_df)
        logger.info('At {}. Got book {} at {}'.format(sequence, book.sequence, book.timestamp))
        return book
