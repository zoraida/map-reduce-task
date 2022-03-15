import mock
from api.worker_client import WorkerClient
from core.worker import Worker


class TestWorker:

    port = 5010
    host = "http://localhost:{}".format(port)
    worker_pid = 12345

    def mock_run_task(self):
        return {
            "i_path": [
                "file2"
            ],
            "id": 1,
            "job_status": 1,
            "job_uuid": "af2254b5-4e08-480f-8ae5-addba4014584",
            "n_buckets": 1,
            "o_path": "/path/intermediate",
            "status": 3,
            "type": "mapper"
            }

    def test_run_successful(self):

        with mock.patch.object(WorkerClient, 'run_task', new=self.mock_run_task):
            worker_client = WorkerClient(self.host, self.worker_pid)
            worker = Worker(worker_client)
            worker.run()