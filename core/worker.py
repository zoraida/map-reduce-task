import json
from task import Task, Mapper, HashReducer, JobStatus


class Worker(object):
    def __init__(self, worker_client):
        '''

        :param json_task:
        '''
        self.worker_client = worker_client

    def run(self):
        '''

        :return:
        '''
        while True:
            #TODO check result
            task_dict = self.worker_client.run_task()

            if 'id' in task_dict:
                if 'n_buckets' in task_dict:
                    task = json.loads(task_dict, object_hook=lambda d: Mapper(**d))
                    print(task.id, task.i_path, task.o_path, task.n_buckets)
                else:
                    task = json.loads(task_dict, object_hook=lambda d: HashReducer(**d))
                    print(task.id, task.i_path, task.o_path, )
                # TODO check result
                task.run()
                self.worker_client.finish_task(task.id)
            elif 'job_status' in task_dict and task_dict['job_status'] == JobStatus.running:
                pass
            elif 'job_status' in task_dict and task_dict['job_status'] == JobStatus.finished:
                break
            else:
                exit(1)

        print("Shutting down Worker ...")