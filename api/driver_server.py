from flask import request, jsonify, make_response
import json
import requests
from core.driver import Driver
from core.task import Mapper, TaskStatus


def configure(app, driver):

    @app.route('/', methods=['GET'])
    def index():
        return 'Here the Driver :) Try with other end point!!'

    @app.route('/task', methods=['PUT'])
    def run_task():
        response = 'Bad Request', 400
        # TODO Any other more elegant way of identification
        worker_pid = request.headers.get('worker-pid')

        if worker_pid is not None:
            task = driver.run_ready_task(worker_pid)
            if task is not None:
                if task.id is None:
                    response = make_response(jsonify({'job_uuid': task.job_uuid, 'job_status': task.job_status}), 200)
                else:
                    response = make_response(jsonify(task.__dict__), 200)
                response.headers["Content-Type"] = "application/json"

        return response

    @app.route('/task/<string:type>/<string:id>/status', methods=['GET', 'POST'])
    def task_status(type, id):
        response = 'Bad Request', 400

        # TODO Any other more elegant way of identification
        worker_pid = request.headers.get('worker-pid')

        if worker_pid is not None:
            if validate_request(worker_pid, [type, id]):
                if request.method == 'GET':
                    task = driver.get_task(id, type)
                    if task is None:
                        response = 'Not Found', 404
                    else:
                        response = make_response(jsonify({'status': task.status.value}), 200)
                        response.headers["Content-Type"] = "application/json"

                else:
                    if request.data is not None:
                        json_task = None
                        try:
                            data = request.data
                            json_task = json.loads(data.decode('utf-8'))
                        except json.JSONDecodeError as e:
                            print("ERROR: Error while decoding request body: {}".format(str(e)))
                            pass
                        else:
                            if 'status' in json_task:
                                status = json_task['status']
                                if status == TaskStatus.finished:
                                    if driver.finish_running_task(worker_pid, id, type):
                                        response = 'Ok', 200

        return response

    @app.route('/shutdown', methods=['GET'])
    def shutdown():
        shutdown_func = request.environ.get('werkzeug.server.shutdown')
        if shutdown_func is None:
            raise RuntimeError('Not running werkzeug')
        print('INFO: Shutting down the driver ...')
        shutdown_func()


def start(app, host='0.0.0.0', port=5000, threaded=False):
    app.run(host=host, threaded=threaded, port=port)


def stop(host='0.0.0.0', port=5000):
    requests.get('http://{}:{}/shutdown'.format(host, port))


def validate_request(worker_pid, required_args):
    # TODO check all restrictions and return a response body with details.
    success_validation = True
    if worker_pid is None:
        success_validation = False
        print('ERROR: worker-pid header is required')
    for arg in required_args:
        if arg is None:
            print('ERROR: {} parameter on the URL is required'.format(arg))
            success_validation = False
            break
    return success_validation

