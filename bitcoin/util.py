import pprint
import os
import sys
import logging
from cdecimal import Decimal, InvalidOperation
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
    return [v.dropna().to_dict() for k, v in df.iterrows()]


def gdax_time_parser(time_string):
    """
    Super light weight parser for gdax time strings.
    Gdax time strings are of the form 2017-09-26T04:40:51.596000Z
    """
    return datetime(
        year=int(time_string[:4]),
        month=int(time_string[5:7]),
        day=int(time_string[8:10]),
        hour=int(time_string[11:13]),
        minute=int(time_string[14:16]),
        second=int(time_string[17:19]),
        microsecond=int(time_string[20:26]),
        tzinfo=pytz.utc
    )


def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return

    logger.error('Uncaught exception', exc_info=(exc_type, exc_value, exc_traceback))
