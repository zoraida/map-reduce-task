from commons.task import Mapper, HashReducer, JobStatus, TaskStatus
import os
import filecmp
import shutil


class TestMapper:
    job_uuid = 'job_uuid'
    job_status = JobStatus.running
    task_id = 0
    i_path = ['data/input/input1.txt', 'data/input/input2.txt']
    expected_o_path = 'data/intermediate'

    def test_mapper_success(self, tmpdir):
        o_path = tmpdir.mkdir('intermediate')
        mapper = Mapper(job_uuid=self.job_uuid, job_status=self.job_status, id=self.task_id, i_path=self.i_path, o_path=o_path, status=TaskStatus.running, n_buckets=2)
        mapper.run()

        mr00 = os.path.join(o_path, 'mr-0-0')
        expected_mr00 = os.path.join(self.expected_o_path, 'mr-0-0')
        assert filecmp.cmp(mr00, expected_mr00)

        mr01 = os.path.join(o_path, 'mr-0-1')
        expected_mr01 = os.path.join(self.expected_o_path, 'mr-0-1')
        assert filecmp.cmp(mr01, expected_mr01)

        shutil.rmtree(str(o_path))


class TestReducer:
    job_uuid = 'job_uuid'
    job_status = JobStatus.running
    task_id = 0
    i_path = 'data/intermediate'
    expected_o_path = 'data/output'

    def test_reducer_success(self, tmpdir):
        o_path = tmpdir.mkdir('intermediate')
        reducer = HashReducer(job_uuid=self.job_uuid, job_status=self.job_status, id=self.task_id, i_path=self.i_path, o_path=o_path, status=TaskStatus.running, n_mappers=1)
        reducer.run()

        out0 = os.path.join(o_path, 'out-0')
        expected_out0 = os.path.join(self.expected_o_path, 'out-0')
        assert filecmp.cmp(out0, expected_out0)

        shutil.rmtree(str(o_path))

