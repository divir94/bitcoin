import subprocess
import os
import psutil
import time
import multiprocessing
import sys
from datetime import datetime, timedelta

import bitcoin.logs.logger as lc
import bitcoin.util as util


sys.excepthook = util.handle_exception
logger = lc.config_logger('scripts')
PROCESSES = [
    {
        'name': 'Gdax Storage: BTC-USD',
        'cmdline': ['python', '-m', 'bitcoin.storage.gdax_msgs', 'BTC-USD'],
        'pid': None
    },
    {
        'name': 'Gdax Storage: ETH-USD',
        'cmdline': ['python', '-m', 'bitcoin.storage.gdax_msgs', 'ETH-USD'],
        'pid': None
    },
    {
        'name': 'Gdax Storage: ETH-BTC',
        'cmdline': ['python', '-m', 'bitcoin.storage.gdax_msgs', 'ETH-BTC'],
        'pid': None
    },
    {
        'name': 'Snapshot Storage',
        'cmdline': ['python', '-m', 'bitcoin.storage.snapshots'],
        'pid': None
    },
]


class ProcessManager(object):
    def __init__(self, processes, check_freq=60):
        self.processes = processes
        self.pool = multiprocessing.Pool(len(self.processes))
        self.check_freq = check_freq
        self.last_time = datetime.utcnow()

    def start_process(self, idx):
        process = self.processes[idx]
        # start
        f, args = subprocess.Popen, process['cmdline']
        p = self.pool.apply_async(f, args=(args,)).get()
        # store pid
        self.processes[idx]['pid'] = p.pid
        logger.info('Starting process: {}'.format(process))

    def restart_stopped_processes(self):
        # log check
        time_elapsed = util.time_elapsed(self.last_time, timedelta(seconds=self.check_freq))
        if time_elapsed:
            logger.info('Checking processes')
            self.last_time = datetime.utcnow()

        for i, proc in enumerate(self.processes):
            pid = proc['pid']
            if pid:
                # check if running
                p = psutil.Process(pid)
                if p.status() == 'zombie' or not p.is_running():
                    # kill and restart
                    p.kill()
                    logger.error('Stopped process: {}'.format(proc))
                    self.start_process(i)
            else:
                # first time start
                self.start_process(i)

    def kill_processes(self):
        for proc in self.processes:
            pid = proc['pid']
            if pid:
                p = psutil.Process(proc['pid'])
                p.kill()
                logger.info('Killing process: {}'.format(proc))

    def run(self):
        logger.info('Current process: {}'.format(os.getpid()))
        while True:
            try:
                self.restart_stopped_processes()
            except Exception as e:
                logger.exception('Failed to check processes:\n{}'.format(e))
            time.sleep(self.check_freq)


if __name__ == '__main__':
    pm = ProcessManager(PROCESSES)
    try:
        pm.run()
    except KeyboardInterrupt:
        logger.info('Keyboard interrupted')
        pm.kill_processes()
        sys.exit(0)
