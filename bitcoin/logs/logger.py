import os
import logging.config
from datetime import date

import bitcoin.util as util


def config_logger(dirname):
    root = util.get_project_root()
    today = date.today()
    directory = '{}/logs/{}'.format(root, dirname)
    if not os.path.exists(directory):
        os.makedirs(directory)
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
                'filename': '{}/{}.log'.format(directory, today),
                'mode': 'w',
            },
            'streamHandler': {
                'class': 'logging.StreamHandler',
                'formatter': 'standard',
            }
        },
        'loggers': {
            '': {
                'handlers': ['fileHandler', 'streamHandler'],
                'level': 'INFO',
            }
        },
    }
    logging.config.dictConfig(config)
    logger = logging.getLogger(dirname)
    return logger
