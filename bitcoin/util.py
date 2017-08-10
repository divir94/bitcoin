import pprint
import logging
import os
from decimal import Decimal, InvalidOperation


class BaseObject(object):
    def __repr__(self):
        return pprint.pformat(self.__dict__)

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not self.__eq__(other)


def get_project_root():
    return os.path.dirname(os.path.abspath(__file__))


def to_decimal(value):
    try:
        num = Decimal(value)
        return num
    except InvalidOperation:
        return value
