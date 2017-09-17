import pprint
import os
from decimal import Decimal, InvalidOperation
from datetime import datetime


class BaseObject(object):
    def __repr__(self):
        return pprint.pformat(self.__dict__)

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not self.__eq__(other)


def get_project_root():
    return os.path.dirname(os.path.abspath(__file__))


def to_decimal(msg):
    result = {}
    for k, v in msg.iteritems():
        if v:
            try:
                result[k] = Decimal(v)
            except InvalidOperation:
                result[k] = v
    return result


def time_elapsed(last_time, tdelta):
    """
    Has it been more than `tdelta` since `last_time` in UTC?
    """
    return (datetime.utcnow() - last_time).seconds >= tdelta.seconds


def df_to_dict(df):
    return (to_decimal(v.dropna().to_dict()) for k, v in df.iterrows())
