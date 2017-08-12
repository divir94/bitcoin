import logging.config

import bitcoin.util as util


ROOT = util.get_project_root()
FILENAME = '{}/logs/websocket.log'.format(ROOT)
LOGGING_CONF = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        },
        'ob': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(sequence)s - %(message)s'
        }
    },
    'handlers': {
        'fileHandler': {
            'class': 'logging.FileHandler',
            'formatter': 'standard',
            'filename': FILENAME,
            'mode': 'w',
        },
        'streamHandler': {
            'class': 'logging.StreamHandler',
            'formatter': 'standard',
        },
        'obFileHandler': {
            'class': 'logging.FileHandler',
            'formatter': 'ob',
            'filename': FILENAME,
            'mode': 'w',
        },
        'obStreamHandler': {
            'class': 'logging.StreamHandler',
            'formatter': 'ob',
        }
    },
    'loggers': {
        'core_websocket': {
            'handlers': ['fileHandler', 'streamHandler'],
            'level': 'INFO',
        },
        'gdax_websocket': {
            'handlers': ['obFileHandler', 'obStreamHandler'],
            'level': 'INFO',
        }
    },
}
LOGGER = logging.config.dictConfig(LOGGING_CONF)
