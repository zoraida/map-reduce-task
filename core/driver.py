from core.task import Mapper, HashReducer, TaskStatus, JobStatus, TaskType, Task
from collections import deque
from threading import Lock
import sys
import utils


class Driver(object):

    def __init__(self, n_mappers, n_reducers, i_path, m_path, o_path):
        """

        :param n_mappers:
        :param n_reducers:
        :param i_path: [file1 ... fileN]
        :param m_path: intermediate path directory
        :param o_path: output path directory
        """

        self.n_mappers = n_mappers
        self.n_reducers = n_reducers
        self.i_path = i_path
        self.m_path = m_path
        self.o_path = o_path

        self.mapper_files = dict()
        self.mapper_files = dict([(m, []) for m in range(0, n_mappers)])
        self.__init_mapper_files()

        self.job_uuid = utils.get_uuid()
        self.pending = n_mappers + n_reducers
        mapper_list = [Mapper(self.job_uuid, JobStatus.running, m, self.mapper_files[m], m_path, TaskStatus.ready, n_reducers) for m in
                       range(0, n_mappers)]
        reducer_list = [HashReducer(self.job_uuid, JobStatus.running, r, m_path, o_path, TaskStatus.blocked, n_mappers) for r in range(0, n_reducers)]
        #
        self.task_dict = dict()
        for t in mapper_list + reducer_list:
            internal_id = self.__get_internal_id(t.id, t.type.value)
            self.task_dict.setdefault(internal_id, t)

        self.ready_queue = deque(mapper_list)
        self.running_queue = deque([])

        self.notified_workers = dict()

        self.lock = Lock()

    def __get_internal_id(self, id, type):
        #TODO static method or functio outside the class but into the same module
        return type + ':' + str(id)

    def __init_mapper_files(self):
        '''
        Based on mapper_id and n_reducers it initialize a dict with (mapper_id, array_of_assigned_files)
        :return:
        '''

        for idx, f in enumerate(self.i_path):
            self.mapper_files[idx % self.n_mappers].append(f)

    def are_map_tasks_finished(self):
        finished = True
        for m in self.task_dict.values():
            if (m.type == TaskType.mapper) and (m.status != TaskStatus.finished):
                finished = False
                break
        return finished

    def are_reduce_tasks_blocked(self):
        blocked = False
        for t in self.task_dict.values():
            if (t.type == TaskType.reducer) and (t.status == TaskStatus.blocked):
                blocked = True
                break
        return blocked

    def are_tasks_finished(self):
        finished = True
        for m in self.task_dict.values():
            if m.status != TaskStatus.finished:
                finished = False
                break
        return finished

    def reduce_tasks_to_ready_queue(self):
        for t in self.task_dict.values():
            if t.type == TaskType.reducer:
                self.ready_queue.append(t)

    def run_ready_task(self, worker_pid):
        """
          When a worker request a task(PUT /task request):
            - If there is a READY task then it is given to the worker and moved to the RUNNING queue.
            - Only when all the map tasks have finished reduce tasks start: if all map tasks have finished and reduce
            tasks are into the BLOCKED queue then they are moved to the READY queue.
            - If there is no READY task but there are both, RUNNING and BLOCKING tasks, then the worker is invited to
            try later.
            - If all the tasks are in FINISHED state, then the worker is notified thus it will exit successfully.
            If all the workers have been already notified then the driver will exit successfully too.

        :return:  task.Task or None if this worker cannot ask for new tasks.
        """
        task = None

        if worker_pid not in self.notified_workers or self.notified_workers[worker_pid] is False:

            if self.are_map_tasks_finished() and self.are_reduce_tasks_blocked():
                self.reduce_tasks_to_ready_queue()

            if self.ready_queue.__len__() > 0:
                task = self.ready_queue.pop()
                self.running_queue.append(task)
                self.task_dict[self.__get_internal_id(task.id, task.type.value)].status = TaskStatus.running
                self.notified_workers[worker_pid] = False

            elif self.are_tasks_finished():
                task = Task(self.job_uuid, JobStatus.finished)
                self.notified_workers[worker_pid] = True

            else:
                # TODO is a more better way of representing responses
                task = Task(self.job_uuid, JobStatus.running)

        return task

    def finish_running_task(self, worker_pid, id, type):
        """
        :param id: id of the task
        :param type: type of the task
        :return: True if the task is finished successfully, False otherwise.
        The task is removed from the RUNNING queue and its state is changed to FINISHED.
        """
        result = None
        task = self.task_dict[self.__get_internal_id(id, type)]
        if task in self.running_queue:
            self.running_queue.remove(self.task_dict[self.__get_internal_id(id, type)])
            self.task_dict[self.__get_internal_id(id, type)].status = TaskStatus.finished
            result = True
        else:
            print('ERROR: Task with id {} and type {} does not in RUNNING state'.format(id, type))
            result = False

        return result

    def get_task(self, id, type):
        task=None
        internal_id = self.__get_internal_id(id, type)
        if internal_id in self.task_dict:
            task = self.task_dict[internal_id]
        else:
            print("ERROR: There is not a valid task with id {} and type {}".format(id, type))
        return task
