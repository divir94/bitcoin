import pprint
import logging
import os


class JsonObject(object):
    """
    Access class attributes as dictionary
    """
    def __getitem__(self, attr):
        return self.__dict__[attr]

    def __repr__(self):
        return '\n' + pprint.pformat(self.__dict__)


def get_project_root():
    return os.path.dirname(os.path.abspath(__file__))


def get_logger(name, fname, level, formatting=None):
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


def to_float(value):
    try:
        num = float(value)
        return num
    except ValueError:
        return value
