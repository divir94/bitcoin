import pprint
import os
import sys
import logging
import functools
from datetime import datetime

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


def parse_message(msg):
    """
    Convert message to appropriate dtypes.

    Parameters
    ----------
    msg: dict

    Returns
    -------
    dict
    """
    dtypes = pms.MSG_DTYPE[pms.DEFAULT_EXCHANGE]
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


def memoize(obj):
    cache = obj.cache = {}

    @functools.wraps(obj)
    def memoizer(*args, **kwargs):
        key = str(args) + str(kwargs)
        if key not in cache:
            cache[key] = obj(*args, **kwargs)
        return cache[key]
    return memoizer
