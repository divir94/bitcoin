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


def get_logger(name, fname=None, level=None, formatting=None):
    fname = fname or '{}.log'.format(name)
    level = level or logging.DEBUG
    formatting = formatting or '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    # format
    logger = logging.getLogger(name)
    logger.setLevel(level)
    formatter = logging.Formatter(formatting)

    # to file
    root_path = get_project_root()
    fh = logging.FileHandler('{}/logs/{}'.format(root_path, fname), 'w')
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    # to stream
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


def to_decimal(value):
    try:
        num = Decimal(value)
        return num
    except InvalidOperation:
        return value
