from flask import Flask
import threading
import os

from driver.api import driver_server
from driver.driver import Driver
from commons.utils import existing_dir


import argparse

__version__ = '0.1.0'
__author__ = u'Zoraida Hidalgo'


def get_parser():
    """
    Creates a new argument parser.
    """
    parser = argparse.ArgumentParser('Word Count Driver')
    parser.add_argument('--port', '-p', help='Driver PORT: 5000', type=int, default=5000)
    parser.add_argument('--mappers', '-m', type=int, help='Number of mapper tasks', required=True)
    parser.add_argument('--reducers', '-r', type=int, help='Number of reducer tasks', required=True)
    parser.add_argument('--i_path', '-ip', help='Input path directory', required=True)
    parser.add_argument('--m_path', '-mp', help='Intermediate path directory', required=True)
    parser.add_argument('--o_path', '-op', help='Output path directory', required=True)

    return parser


def run_driver_server(app, driver, port):
    driver_server.configure(app, driver)
    driver_server.start(app, port=port)


def stop_driver_server(driver, port):
    driver.all_workers_notified()
    driver_server.stop(port=port)


def main(args=None):
    parser = get_parser()
    args = parser.parse_args(args)

    existing_dir(args.i_path)
    existing_dir(args.m_path)
    existing_dir(args.o_path)

    input_files = [os.path.join(args.i_path, f) for f in os.listdir(args.i_path)]

    app = Flask(__name__)
    driver = Driver(args.mappers, args.reducers, input_files, args.m_path, args.o_path)

    server_thread = threading.Thread(target=run_driver_server, args=[app, driver, args.port])
    server_thread.start()

    stopping_server_thread = threading.Thread(target=stop_driver_server, args=[driver, args.port])
    stopping_server_thread.start()

    stopping_server_thread.join()

    print(f'Active Threads: {threading.active_count()}')


if __name__ == '__main__':
    main()