# using time module
import time
import uuid


def current_time_in_sec ():
    return time.time()


def get_uuid():
    return uuid.uuid4()