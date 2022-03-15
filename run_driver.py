from flask import Flask
from api.driver_server import init_driver_server
from core.driver import Driver
import threading
import os

import argparse

__version__ = '0.1.0'
__author__ = u'Zoraida Hidalgo'


def existing_dir(path):
    if not os.path.isdir(path):
        raise argparse.ArgumentTypeError("existing_dir:{0} is not a valid path".format(path))


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
    init_driver_server(app, driver)
    app.run(host="localhost", port=port)


def main(args=None):
    parser = get_parser()
    args = parser.parse_args(args)

    existing_dir(args.i_path)
    existing_dir(args.m_path)
    existing_dir(args.o_path)

    input_files = [os.path.join(args.i_path, f) for f in os.listdir(args.i_path)]

    app = Flask(__name__)
    driver = Driver(args.mappers, args.reducers, input_files, args.m_path, args.o_path)
    run_driver_server(app, driver, args.port)
    # TODO thread that in background checks if all workers have been notified of all tasks being completed to exit


if __name__ == '__main__':
    main()