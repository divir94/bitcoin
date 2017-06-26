import MySQLdb
import json


def get_mysql_conn():
    creds = json.load(open('../mysql_creds.json', 'rb'))
    conn = MySQLdb.connect(host=creds['host'],
                           user=creds['user'],
                           passwd=creds['pwd'],
                           db=creds['db'])
    return conn


def get_db(tbl_name):
    conn = get_mysql_conn()
    df = pd.read_sql('select top 100 * from {}'.format(tbl_name), con=conn)
    return df
