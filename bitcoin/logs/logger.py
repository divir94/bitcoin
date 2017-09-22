import os
import logging.config
from datetime import date

import bitcoin.util as util


def config_logger(dirname, level='INFO', fsuffix=None):
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

    config = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'standard': {
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            }
        },
        'handlers': {
            'fileHandler': {
                'class': 'logging.FileHandler',
                'formatter': 'standard',
                'filename': fname,
                'mode': 'w',
            },
            'streamHandler': {
                'class': 'logging.StreamHandler',
                'formatter': 'standard',
            },
        },
        'loggers': {
            '': {
                'handlers': ['fileHandler', 'streamHandler'],
                'level': level,
            },
        },
    }
    logging.config.dictConfig(config)
    logger = logging.getLogger(dirname)
    return logger
