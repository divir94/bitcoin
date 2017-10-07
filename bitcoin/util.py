import pprint
import os
import sys
import logging
from datetime import datetime
import pytz

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


def to_numeric(msg, numeric_fields):
    result = {}
    for k, v in msg.iteritems():
        if v:
            result[k] = float(v) if k in numeric_fields else v
    return result


def time_elapsed(last_time, tdelta):
    """
    Has it been more than `tdelta` since `last_time` in UTC?
    """
    return (datetime.utcnow() - last_time).seconds >= tdelta.seconds


def df_to_dict(df):
    return [v.dropna().to_dict() for k, v in df.iterrows()]


def is_close(a, b, abs_tol=1e-9):
    return abs(a-b) <= abs_tol


def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return

    logger.error('Uncaught exception', exc_info=(exc_type, exc_value, exc_traceback))
