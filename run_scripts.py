import subprocess
import os
import psutil
import time
import multiprocessing
import sys
from datetime import datetime, timedelta

import bitcoin.logs.logger as lc
import bitcoin.util as util


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
    def __init__(self, processes, log_freq=60):
        self.processes = processes
        self.pool = multiprocessing.Pool(len(self.processes))
        self.log_freq = timedelta(seconds=log_freq)
        self.last_time = datetime.utcnow()

    def get_python_processes(self):
        result = []
        for pid in psutil.pids():
            p = psutil.Process(pid)
            if 'python' in p.name():
                result.append(p)
        return result

    def restart_stopped_processes(self):
        time_elapsed = util.time_elapsed(self.last_time, self.log_freq)
        if time_elapsed:
            logger.info('Checking processes')
            self.last_time = datetime.utcnow()

        python_processes = self.get_python_processes()
        logger.info([(p.pid, p.status(), p.is_running()) for p in python_processes])
        cmds = [p.cmdline() for p in python_processes if p.status() != 'zombie']
        for i, proc in enumerate(self.processes):
            running = proc['cmdline'] in cmds
            if not running:
                # restart in a new process
                pid = proc['pid']
                if pid:
                    p = psutil.Process(pid)
                    p.kill()
                    logger.error('Stopped process: {}'.format(proc))
                f, args = subprocess.Popen, proc['cmdline']
                p = self.pool.apply_async(f, args=(args,)).get()
                # store pid
                proc['pid'] = p.pid
                self.processes[i] = proc
                logger.info('Starting process: {}'.format(proc))

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
            self.restart_stopped_processes()
            time.sleep(1)


if __name__ == '__main__':
    try:
        pm = ProcessManager(PROCESSES)
        pm.run()
    except KeyboardInterrupt:
        logger.info('Keyboard interrupted')
        pm.kill_processes()
        sys.exit(0)
