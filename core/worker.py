import json
from task import Task, Mapper, HashReducer


class Worker(object):
    def __init__(self, json_task):
        '''

        :param json_task:
        '''

        if 'n_buckets' in json_task:
            self.task = json.loads(json_task, object_hook=lambda d: Mapper(**d))
            print(self.task.id, self.task.i_path, self.task.o_path, self.task.n_buckets)
        else:
            self.task = json.loads(json_task, object_hook=lambda d: HashReducer(**d))
            print(self.task.id, self.task.i_path, self.task.o_path, )

    def run(self):
        '''

        :return:
        '''
        self.task.run()
