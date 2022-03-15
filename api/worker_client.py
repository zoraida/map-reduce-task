from api.client import Client
import json


class WorkerClient(Client):
    def __init__(self, host, pid):
        self.host = host
        self.pid = pid

    def run_task(self):
        task = None
        url = self.host + '/task'
        headers = {'worker-pid': str(self.pid)}
        status_code, body = self.send(url=url, method='PUT', headers=headers)
        if status_code == 200:
            try:
                task = json.loads(body)
            except  json.JSONDecodeError as e:
                print("ERROR: JSONDecodeError while decoding task: {}".format(e))
                pass

        return task

    def finish_task(self, id, type):
        url = '{}/task/{}/{}/status'.format(self.host, type, id)
        headers = {'worker-pid': str(self.pid)}
        status_code, _ = self.send(url=url, method='POST', headers=headers, data={'status': 4})
        if status_code == 200:
            return True
        else:
            return False



