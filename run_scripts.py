import subprocess
import os
import psutil
import time
import multiprocessing
import sys

import bitcoin.logs.logger as lc


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
POOL = multiprocessing.Pool(len(PROCESSES))


def get_python_processes():
    result = []
    for pid in psutil.pids():
        p = psutil.Process(pid)
        if 'python' in p.name():
            result.append(p)
    return result


def restart_stopped_processes():
    python_processes = get_python_processes()
    cmds = [p.cmdline() for p in python_processes]
    for i, proc in enumerate(PROCESSES):
        running = proc['cmdline'] in cmds
        if not running:
            # restart in a new process
            if proc['pid']:
                logger.error('Stopped process: {}'.format(proc))
            f, args = subprocess.Popen, proc['cmdline']
            p = POOL.apply_async(f, args=(args,)).get()
            # store pid
            proc['pid'] = p.pid
            PROCESSES[i] = proc
            logger.info('Starting process: {}'.format(proc))


def kill_processes():
    for proc in PROCESSES:
        pid = proc['pid']
        if pid:
            p = psutil.Process(proc['pid'])
            p.kill()
            logger.info('Killing process: {}'.format(proc))


def main():
    logger.info('Current process: {}'.format(os.getpid()))
    while True:
        restart_stopped_processes()
        time.sleep(1)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logger.info('Keyboard interrupted')
        kill_processes()
        sys.exit(0)
