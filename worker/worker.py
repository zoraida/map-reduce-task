import time
from commons.task import Mapper, HashReducer, JobStatus, TaskType


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

            if task_dict is not None:
                if 'id' in task_dict: # task has been returned
                    print("INFO: Task [{}-{}] has been assigned. Taks details: {}".format(task_dict['type'], task_dict['id'], task_dict))
                    if 'type' in task_dict and task_dict['type'] == TaskType.mapper:
                        task = Mapper(task_dict['job_uuid'], task_dict['job_status'], task_dict['id'], task_dict['i_path'], task_dict['o_path'], task_dict['status'], task_dict['n_buckets'])
                    else:
                        task = HashReducer(task_dict['job_uuid'], task_dict['job_status'], task_dict['id'], task_dict['i_path'], task_dict['o_path'], task_dict['status'], task_dict['n_mappers'])
                    # TODO check result
                    task.run()
                    self.worker_client.finish_task(task.id, task.type)
                elif 'job_status' in task_dict and task_dict['job_status'] == JobStatus.running:
                    print('INFO: No tasks available at the moment. Retrying ...')
                    pass
                elif 'job_status' in task_dict and task_dict['job_status'] == JobStatus.finished:
                    print('INFO: All tasks have been run. The job is complete.')
                    break
                else:
                    print('FATAL: Unexpected value returned by the driver: {}. Exiting ...'.format(task))
                    exit(1)
            else:
                print('WARNING: Error connecting with the driver. Trying again in a few seconds... ')
                time.sleep(5)

        print("INFO: Shutting down Worker ...")