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
        }
    },
    'loggers': {
        '': {
            'handlers': ['fileHandler', 'streamHandler'],
            'level': 'INFO',
        }
    },
}
LOGGER = logging.config.dictConfig(LOGGING_CONF)
