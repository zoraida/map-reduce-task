# importing enum for enumerations
import enum
from utils import current_time_in_sec


# creating enumerations using class
class Status(enum.Enum):
    blocked = 1
    ready = 2
    running = 3
    finished = 4


class Task(object):
    def __init__(self, id, i_path, o_path, status):
        self.id = id
        self.i_path = i_path
        self.o_path = o_path
        self.status = status
        self.heart_beat = current_time_in_sec()

    def run(self):
        '''

        :return:
        '''


class Mapper(Task):

    def __init__(self, id, i_path, o_path, status, n_buckets):
        self.n_buckets = n_buckets
        Task.__init__(self,id, i_path, o_path, status)

    def run(self):
        '''
        Reads line by line from one or more files and for each line splits by word
        and emits "word 1" in an output file, one word per line
        '''
        print('Running Map task with id {}'.format(self.id))


class HashReducer(Task):

    def __init__(self, id, i_path, o_path, status):
        Task.__init__(self, id, i_path, o_path, status)

    def run(self):
        '''
        Reads line by line from a single file and uses a hash to store counting of word occurrences.
        Writes to an output file the words counting
        :return:
        '''
        print('Running Reduce task with id {}'.format(self.id))
