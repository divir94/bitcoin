import logging.config

import bitcoin.util as util


def config_logger(fname):
    root = util.get_project_root()
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
                'filename': '{}/logs/{}.log'.format(root, fname),
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
    logger = logging.getLogger(fname)
    return logger
