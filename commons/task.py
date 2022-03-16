import enum
import os

from commons.utils import existing_dir


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

    def generate_output_file_names(self):
        output_files = []
        for b in range(0, self.n_buckets):
            output_files.append(os.path.join(self.o_path, 'mr-{}-{}'.format(self.id, b)))
        return output_files

    def open_output_files(self):
        output_files_names = self.generate_output_file_names()
        streams_dict = dict()
        for idx, stream in enumerate(output_files_names):
            streams_dict[idx] = open(stream, 'w')
        return streams_dict

    def map(self):
        out_streams_dict = self.open_output_files()

        for i_file in self.i_path:
            with open(i_file, 'r') as i_stream:
                for line in i_stream:
                    words = [w.strip().lower() for w in line.split()]
                    for w in words:
                        idx = ord(w[0]) % self.n_buckets
                        out_streams_dict[idx].write('{} {}\n'.format(w, 1))

        for o_stream in out_streams_dict.values():
            o_stream.close()

    def run(self):
        '''
        Reads line by line from one or more files and for each line splits by word
        and emits "word 1" in an output file, one word per line.
        The output file is determined by hashing the ascii value of the first character of the word (module the size of buckest)
        '''
        self.map()


class HashReducer(Task):

    def __init__(self, job_uuid, job_status, id, i_path, o_path, status, n_mappers):
        self.n_mappers = n_mappers

        Task.__init__(self, job_uuid, job_status, id, TaskType.reducer, i_path, o_path, status)

    def generate_input_file_names(self):
        input_files = []
        for m in range(0, self.n_mappers):
            input_files.append(os.path.join(self.i_path, 'mr-{}-{}'.format(m, self.id)))
        return input_files

    def reduce(self):
        input_files_names = self.generate_input_file_names()
        word_count_dict = dict()
        for i_file in input_files_names:
            with open(i_file, 'r') as i_stream:
                for line in i_stream:
                    word, count = line.split()
                    if word in word_count_dict:
                        word_count_dict[word] += int(count)
                    else:
                        word_count_dict[word] = int(count)

        output_file_name = os.path.join(self.o_path, 'out-{}'.format(self.id))
        with open(output_file_name, 'w') as o_stream:
            for word, count in word_count_dict.items():
                o_stream.write('{} {}\n'.format(word, count))

    def run(self):
        '''
        Reads line by line from a single file and uses a hash to store counting of word occurrences.
        Writes to an output file the words counting
        :return:
        '''
        self.reduce()
