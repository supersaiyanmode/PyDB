from PyDB.exceptions import PyDBInternalError
from PyDB.utils import int_to_bytes, bytes_to_int, bytes_to_ints
from PyDB.utils import string_to_bytes, bytes_to_string


class SafeReader(object):
    def __init__(self, io):
        self.io = io

    def next_int(self, size=4):
        return bytes_to_int(self.io.read(size))

    def next_string(self, size_hint=1024, len_bytes=4):
        size = self.next_int(size=len_bytes)
        if 0 <= size <= size_hint:
            res = bytes_to_string(self.io.read(size))
            if len(res) != size:
                raise PyDBInternalError("String read mismatch. string: {}, size: {}".format(res, size))
            return res
        raise PyDBInternalError("Unable to read because of wrong size of string: {}.".format(size))

