from core.task import Mapper, HashReducer, TaskStatus, JobStatus
from mock import patch
from core.driver import Driver
import utils


class TestDriver:

    @patch('utils.get_uuid')
    def test__init__(self, mock_uuid):
        mock_uuid.return_value = '1234567890'
        job_uuid = utils.get_uuid()

        n_mappers = 2
        n_reducers = 1
        i_path = ['file1', 'file2', 'file3', 'file4', 'file5']
        m_path = '/intermediate/path'
        o_path = '/output/path'

        driver = Driver(n_mappers, n_reducers, i_path, m_path, o_path)

        assert driver.n_mappers == n_mappers
        assert driver.n_reducers == n_reducers
        assert driver.i_path == i_path
        assert driver.m_path == m_path
        assert driver.o_path == o_path

        mapper0 = Mapper(job_uuid, JobStatus.running, 0, ['file1', 'file3', 'file5'], m_path, TaskStatus.ready, n_reducers)
        mapper1 = Mapper(job_uuid, JobStatus.running, 1, ['file2', 'file4'], m_path, TaskStatus.ready, n_reducers)
        assert driver.ready_queue.pop().__dict__  == mapper1.__dict__
        assert driver.ready_queue.pop().__dict__ == mapper0.__dict__

        reducer0 = HashReducer(job_uuid, JobStatus.running,0, m_path, o_path, TaskStatus.blocked)
        assert driver.running_queue.__len__() == 0

        assert driver.task_dict['mapper:0'].__dict__ == mapper0.__dict__
        assert driver.task_dict['mapper:1'].__dict__ == mapper1.__dict__
        assert driver.task_dict['reducer:0'].__dict__ == reducer0.__dict__

    @patch('utils.get_uuid')
    def test_driver_success_job(self, mock_uuid):
        mock_uuid.return_value = '1234567890'
        job_uuid = utils.get_uuid()

        n_mappers = 2
        n_reducers = 1
        i_path = ['file1', 'file2', 'file3', 'file4', 'file5', 'file6']
        m_path = '/intermediate/path'
        o_path = '/output/path'

        process_pid1 = 29980
        process_pid2 = 29981

        driver = Driver(n_mappers, n_reducers, i_path, m_path, o_path)

        mapper0 = Mapper(job_uuid, JobStatus.running, 0, ['file1', 'file3', 'file5'], m_path, TaskStatus.running, n_reducers)
        mapper1 = Mapper(job_uuid, JobStatus.running, 1, ['file2', 'file4', 'file6'], m_path, TaskStatus.running, n_reducers)
        reducer0 = HashReducer(job_uuid, JobStatus.running,0, m_path, o_path, TaskStatus.running)

        task = driver.run_ready_task(process_pid1) # mapper1 starts
        assert task.__dict__ == mapper1.__dict__

        task = driver.run_ready_task(process_pid2) # mapper0 starts
        assert task.__dict__ == mapper0.__dict__

        assert driver.finish_running_task(process_pid1, mapper1.id, mapper1.type.value) is True # mapper1 finishes

        task = driver.run_ready_task(process_pid1) # mapper0 still running so reducer0 is blocked
        assert task.job_uuid == job_uuid
        assert task.job_status == JobStatus.running
        assert task.id is None
        assert task.type is None
        assert task.i_path is None
        assert task.o_path is None

        assert driver.finish_running_task(process_pid2, mapper0.id, mapper0.type.value) is True # mapper0 finishes

        task = driver.run_ready_task(process_pid2) # reducer0 starts
        assert task.__dict__ == reducer0.__dict__

        task = driver.run_ready_task(process_pid1) # reducer0 still running but no more pending tasks
        assert task.job_uuid == job_uuid
        assert task.job_status == JobStatus.running
        assert task.id is None
        assert task.type is None
        assert task.i_path is None
        assert task.o_path is None

        assert driver.finish_running_task(process_pid2, reducer0.id, reducer0.type.value) is True # reducer0 finishes

        task = driver.run_ready_task(process_pid1)
        assert task.job_uuid == job_uuid
        assert task.job_status == JobStatus.finished
        assert task.id is None
        assert task.type is None
        assert task.i_path is None
        assert task.o_path is None

        task = driver.run_ready_task(process_pid2)
        assert task.job_uuid == job_uuid
        assert task.job_status == JobStatus.finished
        assert task.id is None
        assert task.type is None
        assert task.i_path is None
        assert task.o_path is None

        assert driver.are_tasks_finished() is True
        assert driver.running_queue.__len__() == 0
        assert driver.ready_queue.__len__() == 0

    @patch('utils.get_uuid')
    def test_run_ready_task_already_notified_worker(self, mock_uuid):
        mock_uuid.return_value = '1234567890'
        job_uuid = utils.get_uuid()

        n_mappers = 1
        n_reducers = 1
        i_path = ['file1', 'file2', 'file3', 'file4', 'file5', 'file6']
        m_path = '/intermediate/path'
        o_path = '/output/path'

        process_pid = 29980

        driver = Driver(n_mappers, n_reducers, i_path, m_path, o_path)

        mapper0 = Mapper(job_uuid, JobStatus.running, 0, ['file1', 'file3', 'file5'], m_path, TaskStatus.running, n_reducers)
        reducer0 = HashReducer(job_uuid, JobStatus.running,0, m_path, o_path, TaskStatus.running)

        driver.run_ready_task(process_pid)
        assert driver.finish_running_task(process_pid, mapper0.id, mapper0.type.value) is True
        driver.run_ready_task(process_pid)
        assert driver.finish_running_task(process_pid, reducer0.id, reducer0.type.value) is True
        task = driver.run_ready_task(process_pid)
        assert task.job_uuid == job_uuid
        assert task.job_status == JobStatus.finished
        assert task.id is None
        assert task.type is None
        assert task.i_path is None
        assert task.o_path is None

        task = driver.run_ready_task(process_pid)
        assert task is None

    @patch('utils.get_uuid')
    def test_finish_running_task_fails_on_blocked_task(self, mock_uuid):
        mock_uuid.return_value = '1234567890'
        job_uuid = utils.get_uuid()

        n_mappers = 1
        n_reducers = 1
        i_path = ['file1', 'file2', 'file3', 'file4', 'file5', 'file6']
        m_path = '/intermediate/path'
        o_path = '/output/path'

        process_pid = 29980

        driver = Driver(n_mappers, n_reducers, i_path, m_path, o_path)

        reducer0 = HashReducer(job_uuid, JobStatus.running, 0, m_path, o_path, TaskStatus.running)

        driver.run_ready_task(process_pid)
        assert driver.finish_running_task(process_pid, reducer0.id, reducer0.type.value) is False

