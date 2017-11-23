import pprint
import os
import sys
import logging
from datetime import datetime
from decimal import Decimal

import bitcoin.params as pms


logger = logging.getLogger('util')


class BaseObject(object):
    def __repr__(self):
        return pprint.pformat(self.__dict__)

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not self.__eq__(other)


def get_project_root():
    return os.path.dirname(os.path.abspath(__file__))


def time_to_str(timestamp):
    """
    Convert datetime to GDAX time format string for SQL queries.

    Parameters
    ----------
    timestamp: pd.datetime

    Returns
    -------
    str
    """
    if not timestamp or not isinstance(timestamp, datetime):
        return timestamp
    # add quotes to use in sql
    time_str = timestamp.strftime(pms.DATE_FORMAT)
    return time_str


def df_as_type(df, types):
    """
    Convert df columns to dtypes. Modifies input df.

    Parameters
    ----------
    df: pd.DataFrame
    types: dict
        column name to types

    Returns
    -------
    pd.DataFrame
    """
    missing_types = set(types.keys()).difference(set(df.columns))
    assert not missing_types, 'Missing dtypes: {}'.format(missing_types)
    for col in df.columns:
        _type = types[col]
        df[col] = df[col].astype(_type)
    return df


def parse_message(msg, exchange):
    """
    Convert message to appropriate dtypes.

    Parameters
    ----------
    msg: dict
    exchange: str

    Returns
    -------
    dict
    """
    dtypes = pms.MSG_DTYPE[exchange]
    result = {k: dtypes[k](v) for k, v in msg.iteritems() if v}
    return result


def time_elapsed(last_time, tdelta):
    """
    Has it been more than `tdelta` since `last_time` in UTC?
    """
    return (datetime.utcnow() - last_time).seconds >= tdelta.seconds


def is_close(a, b, abs_tol=1e-9):
    return abs(a-b) <= abs_tol


def is_less(a, b, abs_tol=1e-9):
    return a <= (b + abs_tol)


def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return

    logger.error('Uncaught exception', exc_info=(exc_type, exc_value, exc_traceback))
