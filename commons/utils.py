import uuid
import os
from argparse import ArgumentTypeError


def existing_dir(path):
    if not os.path.isdir(path):
        raise ArgumentTypeError("existing_dir:{0} is not a valid path".format(path))


def get_uuid():
    return uuid.uuid4()
