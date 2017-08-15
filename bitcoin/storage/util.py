import json
import logging
from sqlalchemy import create_engine

import bitcoin.logs.logger
import bitcoin.util as util

logger = logging.getLogger('db_util')


def get_sqlalchemy_engine():
    logger.info('Getting DB engine instance')
    root_path = util.get_project_root()
    creds = json.load(open('{}/credentials/mysql.json'.format(root_path), 'rb'))
    engine_name = 'mysql://{user}:{pwd}@{host}/{db}'.format(**creds)
    engine = create_engine(engine_name, pool_recycle=1800, echo=False)
    return engine

ENGINE = get_sqlalchemy_engine()


def store_dfs(df_dict):
    logger.info('~' * 30)
    for tbl_name, df in df_dict.iteritems():
        try:
            df.to_sql(name=tbl_name, con=ENGINE, if_exists='append', index=False)
            logger.info('Stored {} rows in {}'.format(len(df), tbl_name))
        except Exception as e:
            logger.error('Failed to store in {}'.format(tbl_name))
            logger.error(e)
            df.to_csv('{}.csv'.format(tbl_name), index=False)
    logger.info('~' * 30)
    return
