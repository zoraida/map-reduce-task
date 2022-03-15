from flask import Flask
import json
import pytest
from api import driver_server
from core.driver import Driver
from core.task import JobStatus


@pytest.fixture()
def driver():
    n_mappers = 2
    n_reducers = 1
    i_path = ['file1', 'file2']
    m_path = '/middle/dir/path'
    o_path = '/output/dir/path'
    driver = Driver(n_mappers, n_reducers, i_path, m_path, o_path)

    yield driver


class TestDriverServer:

    def test_get_task_status_success(self, driver):
        app = Flask(__name__)
        driver_server.configure(app, driver)
        client = app.test_client()
        url = '/task/mapper/0/status'
        mock_response_data_dict = {
            "status": 2
        }

        mock_request_headers = {
            'worker-pid': '123'
        }

        response = client.get(url, headers=mock_request_headers)
        assert response.status_code == 200
        data = json.loads(response.data.decode('utf-8'))
        assert mock_response_data_dict == data


    def test_get_task_status_missing_header(self, driver):
        app = Flask(__name__)
        driver_server.configure(app, driver)
        client = app.test_client()
        url = '/task/pepe/5/status'

        response = client.get(url)
        assert response.status_code == 400


    def test_get_task_status_not_found(self, driver):
        app = Flask(__name__)
        driver_server.configure(app, driver)
        client = app.test_client()
        url = '/task/pepe/5/status'

        mock_request_headers = {
            'worker-pid': '123'
        }

        response = client.get(url, headers=mock_request_headers)
        assert response.status_code == 404


    def test_driver_server_success_job(self, driver):
        app = Flask(__name__)
        driver_server.configure(app, driver)
        client = app.test_client()
        run_task_url = '/task'
        finish_mapper0_task_url = '/task/mapper/0/status'
        finish_mapper1_task_url = '/task/mapper/1/status'
        finish_reducer0_task_url = '/task/reducer/0/status'

        mock_request_headers_pid1 = {'worker-pid': 29980, 'Content-Type': 'application/json'}
        mock_request_headers_pid2 = {'worker-pid': 29981, 'Content-Type': 'application/json'}

        mock_request_body_finish_task = {'status': 4}

        response = client.put(run_task_url, headers=mock_request_headers_pid1) #run task mapper1
        assert response.status_code == 200
        response = client.put(run_task_url, headers=mock_request_headers_pid2) #run task mapper0
        assert response.status_code == 200

        response = client.post(finish_mapper1_task_url, data=json.dumps(mock_request_body_finish_task), headers=mock_request_headers_pid1) # finish task mapper0
        assert response.status_code == 200

        response = client.put(run_task_url, headers=mock_request_headers_pid1)  # run reduce0 but mapper0 still running so reduce0 is blocked
        assert response.status_code == 200
        data = json.loads(response.data.decode('utf-8'))
        assert 'job_uuid' in data is not None
        assert data['job_status'] == JobStatus.running
        assert 'id' not in data
        assert 'type' not in data
        assert 'i_path' not in data
        assert 'o_path' not in data

        response = client.post(finish_mapper0_task_url, data=json.dumps(mock_request_body_finish_task), headers=mock_request_headers_pid2) # finish task mapper0
        assert response.status_code == 200

        response = client.put(run_task_url, headers=mock_request_headers_pid2)  # run reduce0 which is no longer blocked
        assert response.status_code == 200
        data = json.loads(response.data.decode('utf-8'))
        assert 'job_uuid' in data is not None
        assert data['job_status'] == JobStatus.running
        assert data['id'] == 0
        assert data['type'] == 'reducer'
        assert data['i_path'] == '/middle/dir/path'
        assert data['o_path'] == '/output/dir/path'

        response = client.put(run_task_url, headers=mock_request_headers_pid1)  # reducer0 still running but no more pending tasks
        assert response.status_code == 200
        data = json.loads(response.data.decode('utf-8'))
        assert 'job_uuid' in data is not None
        assert data['job_status'] == JobStatus.running
        assert 'id' not in data
        assert 'type' not in data
        assert 'i_path' not in data
        assert 'o_path' not in data

        response = client.post(finish_reducer0_task_url, data=json.dumps(mock_request_body_finish_task), headers=mock_request_headers_pid2)  # finish task mapper0
        assert response.status_code == 200 # reducer0 finishes

        response = client.put(run_task_url, headers=mock_request_headers_pid1)  # reducer0 still running but no more pending tasks
        assert response.status_code == 200
        data = json.loads(response.data.decode('utf-8'))
        assert 'job_uuid' in data is not None
        assert data['job_status'] == JobStatus.finished
        assert 'id' not in data
        assert 'type' not in data
        assert 'i_path' not in data
        assert 'o_path' not in data

        response = client.put(run_task_url, headers=mock_request_headers_pid2)  # reducer0 still running but no more pending tasks
        assert response.status_code == 200
        data = json.loads(response.data.decode('utf-8'))
        assert 'job_uuid' in data is not None
        assert data['job_status'] == JobStatus.finished
        assert 'id' not in data
        assert 'type' not in data
        assert 'i_path' not in data
        assert 'o_path' not in data




