import argparse
import os

from worker.worker_client import WorkerClient
from worker.worker import Worker
from commons.utils import existing_dir

__version__ = '0.1.0'
__author__ = u'Zoraida Hidalgo'


def get_parser():
    """
    Creates a new argument parser.
    """
    parser = argparse.ArgumentParser('Word Count Worker')
    parser.add_argument('--driver', '-d', help='Driver URL: http://hostname:5000', )
    return parser


def main(args=None):

    parser = get_parser()
    args = parser.parse_args(args)
    pid = os.getpid()

    worker_client = WorkerClient(args.driver, pid)
    # TODO check if the server is reachable

    worker = Worker(worker_client)
    worker.run()


if __name__ == '__main__':
    main()