# importing enum for enumerations
import enum
from utils import current_time_in_sec


class JobStatus(enum.IntEnum):
    running = 1
    finished = 2


class TaskStatus(enum.IntEnum):
    blocked = 1
    ready = 2
    running = 3
    finished = 4


class TaskType(str, enum.Enum):
    mapper = 'mapper'
    reducer = 'reducer'


class Task(object):
    def __init__(self, job_uuid, job_status, id=None, type=None, i_path=None, o_path=None, status=None):
        self.job_uuid = job_uuid
        self.job_status = job_status
        self.id = id
        self.type = type
        self.i_path = i_path
        self.o_path = o_path
        self.status = status

    def run(self):
        '''

        :return:
        '''


class Mapper(Task):

    def __init__(self, job_uuid, job_status, id, i_path, o_path, status, n_buckets):
        self.n_buckets = n_buckets
        Task.__init__(self, job_uuid, job_status, id, TaskType.mapper, i_path, o_path, status)

    def run(self):
        '''
        Reads line by line from one or more files and for each line splits by word
        and emits "word 1" in an output file, one word per line
        '''
        for file_name in self.i_path:
            i_file = open(file_name, 'r')
            FO = open(file_name.replace('txt', 'out'), 'w')
            with open(fp, 'r') as f:
            for line in FI:
                FO.write(line)

            FI.close()
            FO.close()


class HashReducer(Task):

    def __init__(self, job_uuid, job_status, id, i_path, o_path, status):
        Task.__init__(self, job_uuid, job_status, id, TaskType.reducer, i_path, o_path, status)

    def run(self):
        '''
        Reads line by line from a single file and uses a hash to store counting of word occurrences.
        Writes to an output file the words counting
        :return:
        '''
        print('Running Reduce task with id {}'.format(self.id))
