from flask import request, jsonify
import json
from core import driver
from core.task import Mapper, HashReducer


def init_driver_server(app, driver):

    @app.route('/task', methods=['GET'])
    def get_task():
        return driver.get_task(request.args('id'))

    @app.route('/task/run', methods=['POST'])
    def run_task():
        task = driver.run_ready_task()

        if isinstance(task, Mapper):
            json_task = jsonify(id=task.id,type='mapper',i_path=task.i_path, n_buckets=task.n_buckets)
        else:
            json_task = jsonify(id=task.id, type='reducer', i_path=task.i_path)
        return json_task

    @app.route('/task/finish', methods=['POST'])
    def finish_task():
        json_task = request.json
        id = json_task['id']
        driver.finish_running_task(id)
        return 'Ok', 200

    @app.route('/task/heartbeat', methods=['POST'])
    def heartbeat_task():
        json_task = request.json
        id = json_task['id']
        driver.update_heart_beat(id)
        return 'Ok', 200