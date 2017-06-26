import websocket
import thread
import json
import MySQLdb
import pandas as pd

from pprint import pprint
from time import time


class GDAXStore(object):
    def __init__(self, commit_freq=60, close_time=None, debug=False):
        self.host= 'gdax.ch7tzkxzans9.us-east-1.rds.amazonaws.com'
        self.user= 'divir94'
        self.password= 'eternity16!'
        self.db= 'gdax'
        self.gdax_feed = 'wss://ws-feed.gdax.com'
        
        self.commit_frequency = commit_freq # in secs
        self.close_time = close_time
        self.debug = debug
        self.num_rows = 0
        
    def get_mysql_conn(self):
        conn = MySQLdb.connect(host=self.host,
                               user=self.user,
                               passwd=self.password,
                               db=self.db)
        return conn
    
    def get_websocket(self):
        websocket.enableTrace(True)
        ws = websocket.WebSocketApp(self.gdax_feed,
                                    on_open=self.on_open,
                                    on_message=self.on_message,
                                    on_error=self.on_error,
                                    on_close=self.on_close)
        return ws

    def on_message(self, ws, message):
        resp = json.loads(message)
        if self.debug:
            print 'Message arrived'
        self.execute_sql(resp)
        
        time_since_last_commit = time() - self.last_commit_time
        time_since_start = time() - self.start_time

        if time_since_last_commit > self.commit_frequency:
            self.commit()
            
        if self.close_time and time_since_start > self.close_time:
            ws.close()

    def on_error(self, ws, error):
        print 'Message error:\n{}'.format(error)
        try:
            self.on_close(ws)
        except:
            self.run()

    def on_close(self, ws):
        self.conn.close()
        print '### Closing db connection and websocket ###'

    def on_open(self, ws):
        def run(*args):
            print 'Running websocket'
            subscribe_request = {
                'type': 'subscribe',
                'product_ids': ['BTC-USD']
            }
            ws.send(json.dumps(subscribe_request))
            print 'Thread terminating...'

        thread.start_new_thread(run, ())


    def execute_sql(self, message):
        columns = ', '.join(message.keys())
        values = ', '.join(map(lambda x: '"{}"'.format(x), message.values()))
        sql = """INSERT INTO gdax.OrderBookUpdates ({}) VALUES ({})""".format(columns, values)
        if self.debug:
            print sql

        try:
            self.cursor.execute(sql)
            self.num_rows += 1
        except Exception as e:
            print 'Rolling back:\n{}'.format(e)
            print self.conn.rollback()
    
    def commit(self):
        self.conn.commit()
        print 'committed {} rows'.format(self.num_rows)
        self.num_rows = 0
        self.last_commit_time = time()

    def get_db(self):
        df = pd.read_sql('select top 100 * from gdax.OrderBookUpdates;', con=self.conn)    
        return df

    def run(self):
        self.start_time = time()
        self.last_commit_time = self.start_time

        self.conn = self.get_mysql_conn()
        self.cursor = self.conn.cursor()
        
        ws = self.get_websocket()
        ws.run_forever()


if __name__ == '__main__':
    store = GDAXStore()
    store.run()