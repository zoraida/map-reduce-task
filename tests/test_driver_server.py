from flask import Flask
import json
import ast

from api.driver_server import init_driver_server
from core.driver import Driver


n_mappers = 4
n_reducers = 2
i_path = '/input/dir/path'
m_path = '/middle/dir/path'
o_path = '/output/dir/path'

driver = Driver(n_mappers, n_reducers, i_path, m_path, o_path)


def test_run_task():
    app = Flask(__name__)
    init_driver_server(app, driver)
    client = app.test_client()
    url = '/task/run'
    mock_data_dict = {
        'id': 4,
        'i_path': ['file1', 'file2'],
        'n_buckets': 2,
        'type': 'mapper'
    }

    response = client.post(url)
    data_dict = ast.literal_eval(response.get_data().decode("UTF-8"))
    assert data_dict == mock_data_dict
    assert response.status_code == 200


def test_finish_task():
    app = Flask(__name__)
    init_driver_server(app, driver)
    client = app.test_client()
    url = '/task/finish'
    mock_request_headers = {}
    mock_request_data = {
        'id': 4
    }

    response = client.post(url, data=json.dumps(mock_request_data), headers=mock_request_headers, content_type='application/json')
    assert response.status_code == 200


def test_heartbeat_task():
    app = Flask(__name__)
    init_driver_server(app, driver)
    client = app.test_client()
    url = '/task/heartbeat'
    mock_request_headers = {}
    mock_request_data = {
        'id': 4
    }

    response = client.post(url, data=json.dumps(mock_request_data), headers=mock_request_headers, content_type='application/json')
    assert response.status_code == 200



