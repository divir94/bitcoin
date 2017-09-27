import os
import logging.config
from datetime import date

import bitcoin.util as util


def config_logger(dirname, level='INFO', fsuffix=None, file_handler=True):
    formatters = {
        'standard': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        }
    }

    stream_handler = {
        'class': 'logging.StreamHandler',
        'formatter': 'standard',
    }

    config = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': formatters,
        'handlers': {
            'streamHandler': stream_handler,
        },
        'loggers': {
            '': {
                'handlers': ['streamHandler'],
                'level': level,
            },
        },
    }

    if file_handler:
        fname = _get_fname(dirname, fsuffix)
        fhandler = {
            'class': 'logging.FileHandler',
            'formatter': 'standard',
            'filename': fname,
            'mode': 'w',
        }
        config['handlers']['fileHandler'] = fhandler
        config['loggers']['']['handlers'] = ['fileHandler', 'streamHandler']

    logging.config.dictConfig(config)
    logger = logging.getLogger(dirname)
    return logger


def _get_fname(dirname, fsuffix):
    root = util.get_project_root()
    today = date.today()
    directory = '{}/logs/{}'.format(root, dirname)
    # create dir
    if not os.path.exists(directory):
        os.makedirs(directory)
    # get fname
    if fsuffix:
        fname = '{}/{}_{}.log'.format(directory, today, fsuffix)
    else:
        fname = '{}/{}.log'.format(directory, today)
    return fname
