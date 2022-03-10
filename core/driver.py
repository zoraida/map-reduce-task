from core.task import Mapper, HashReducer, Status
from collections import deque
from utils import current_time_in_sec


class Driver(object):
    def __init__(self, n_mappers, n_reducers, i_path, m_path, o_path):
        '''

        :param json_task:
        '''
        self.n_mappers = n_mappers
        self.n_reducers = n_reducers
        self.i_path = i_path
        self.m_path = m_path
        self.o_path = o_path

        mapper_list = [Mapper(m, self.__get_input_files(m), m_path, Status.ready, n_reducers)for m in range(1, n_mappers + 1)]
        reducer_list = [HashReducer(r, m_path, o_path, Status.blocked) for r in range(1, n_reducers + 1)]
        self.task_dict = dict()
        for t in mapper_list + reducer_list:
            self.task_dict.setdefault(t.id, t)

        self.ready_queue = deque(mapper_list)
        self.bloqued_queue = deque(reducer_list)
        self.running_queue = deque([])

    def __get_input_files(self, mapper_id):
        '''
        Based on mapper_id and n_reducers it returns the array with the files assigned to this mapper
        :param mapper_id:
        :return:
        '''
        return ['file1', 'file2']

    def run_ready_task(self):
        task = self.ready_queue.pop()
        self.task_dict[task.id].status = Status.running
        self.running_queue.append(task)
        return task

    def finish_running_task(self, id):
        self.task_dict[id].status = Status.finished
        self.running_queue.remove(self.task_dict[id])

    def update_heart_beat(self, id):
        self.task_dict[id].heart_beat = current_time_in_sec()

    def get_task(self, id):
        return self.task_dict[id]

