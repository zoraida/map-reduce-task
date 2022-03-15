import requests
import json
from api.worker_client import WorkerClient
import requests_mock


class TestWorkerClient():
    port = 5010
    host = "http://localhost:{}".format(port)
    worker_pid = 12345

    def test_run_task_successful(self, requests_mock):
        expected_task_json = {
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

        requests_mock.put('{}/task'.format(self.host), json=expected_task_json)
        worker_client = WorkerClient(self.host, self.worker_pid)
        task_json =  worker_client.run_task()

        assert json.loads(task_json) == expected_task_json

    def test_run_task_fails(self, requests_mock):

        requests_mock.put('{}/task'.format(self.host), status_code=500)
        worker_client = WorkerClient(self.host, self.worker_pid)
        task_json =  worker_client.run_task()

        assert task_json is None

    def test_finish_task_successful(self, requests_mock):
        request_body_json = {
          "status": 4,
        }
        task_id = 0
        task_type = 'reducer'

        requests_mock.post('{}/task/{}/{}/status'.format(self.host, task_type, task_id))
        worker_client = WorkerClient(self.host, self.worker_pid)
        assert  worker_client.finish_task(task_id, task_type) is True

    def test_finish_task_fails(self, requests_mock):
        task_id = 0
        task_type = 'reducer'

        requests_mock.post('{}/task/{}/{}/status'.format(self.host, task_type, task_id), status_code=500)
        worker_client = WorkerClient(self.host, self.worker_pid)
        assert  worker_client.finish_task(task_id, task_type) is False
