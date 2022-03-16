from _pytest.fixtures import fixture
from flask import Flask
import json
import random
import shutil
import os
from driver.driver import Driver
from driver.api import driver_server
from run_driver import run_driver_server, stop_driver_server
from worker.worker import Worker
from worker.worker_client import WorkerClient
from commons.task import JobStatus
import threading
import tempfile


class TestJob:
    port = 5000
    driver_url = "http://localhost:{}".format(port)
    i_path = ['data/input/input1.txt', 'data/input/input2.txt', 'data/input/input3.txt', 'data/input/input4.txt',
              'data/input/input5.txt', 'data/input/input6.txt', 'data/input/input7.txt', 'data/input/input8.txt']

    n_mappers = 4
    n_reducers = 2

    def get_worker(self):
        worker_pid = random.randint(0, 32768)
        worker_client = WorkerClient(self.driver_url, worker_pid)
        worker = Worker(worker_client)
        return worker_pid, worker

    def get_driver(self, m_path, o_path):
        return Driver(self.n_mappers, self.n_reducers, self.i_path, m_path, o_path)

    def test_run_job_single_worker(self):
        worker_pid, worker = self.get_worker()
        worker_thread = threading.Thread(target=lambda w: w.run(), args=[worker])
        worker_thread.start()

        print('Number of threads: {}'.format(threading.active_count()) )# main and  worker

        with tempfile.TemporaryDirectory() as m_path:
            with tempfile.TemporaryDirectory() as o_path:
                print("Intermediate dir {}".format(m_path))
                print("Output dir {}".format(o_path))

                driver = self.get_driver(m_path, o_path)
                app = Flask(__name__)
                driver_thread = threading.Thread(target=run_driver_server, args=[app, driver, self.port])
                driver_thread.start()

                stopping_server_thread = threading.Thread(target=stop_driver_server, args=[driver, self.port])
                stopping_server_thread.start()

                worker_thread.join()
                stopping_server_thread.join()
                driver_thread.join()

                assert set(os.listdir(m_path)) == set(['mr-0-0', 'mr-0-1', 'mr-1-0', 'mr-1-1', 'mr-2-0', 'mr-2-1', 'mr-3-0', 'mr-3-1',])
                assert set(os.listdir(o_path)) == set(['out-0', 'out-1'])

    def test_job_multiple_workers(self):

        worker_pid, worker = self.get_worker()
        worker_thread1 = threading.Thread(target=lambda w: w.run(), args=[worker])
        worker_thread1.start()

        worker_pid, worker = self.get_worker()
        worker_thread2 = threading.Thread(target=lambda w: w.run(), args=[worker])
        worker_thread2.start()

        worker_pid, worker = self.get_worker()
        worker_thread3 = threading.Thread(target=lambda w: w.run(), args=[worker])
        worker_thread3.start()

        print('Number of threads: {}'.format(threading.active_count()) )# main and  worker

        with tempfile.TemporaryDirectory() as m_path:
            with tempfile.TemporaryDirectory() as o_path:
                print("Intermediate dir {}".format(m_path))
                print("Output dir {}".format(o_path))

                driver = self.get_driver(m_path, o_path)
                app = Flask(__name__)
                driver_thread = threading.Thread(target=run_driver_server, args=[app, driver, self.port])
                driver_thread.start()

                assert threading.active_count() >= 2 # main, driver and maybe still worker

                stopping_driver_thread = threading.Thread(target=stop_driver_server, args=[driver, self.port])
                stopping_driver_thread.start()

                worker_thread1.join()
                worker_thread2.join()
                worker_thread3.join()
                stopping_driver_thread.join()
                driver_thread.join()

                assert set(os.listdir(m_path)) == set(['mr-0-0', 'mr-0-1', 'mr-1-0', 'mr-1-1', 'mr-2-0', 'mr-2-1', 'mr-3-0', 'mr-3-1',])
                assert set(os.listdir(o_path)) == set(['out-0', 'out-1'])


if __name__ == '__main__':
    test_job = TestJob()
    test_job.run_job_multiple_workers()
