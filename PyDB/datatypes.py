import struct

from PyDB.exceptions import PyDBTypeError
from PyDB.exceptions import PyDBTypeConstraintError

class GenericType(object):
    def __init__(self, primary_key=False, unique=False):
        self.primary_key = primary_key
        self.unique = unique

class IntegerType(GenericType):
    def __init__(self, default=0, **kwargs):
        super().__init__(**kwargs)

    def check_value(self, val):
        return isinstance(val, int)

    def get_type(self):
        return int

    def encode(self, val):
        return struct.pack(">I", val)

    def decode(self, val):
        return struct.unpack(">I", val)

    def get_binary_size(self):
        return 4


class StringType(GenericType):
    def __init__(self, size, default="", **kwargs):
        super().__init__(**kwargs)
        self.max_length = size

    def check_value(self, val):
        if len(val) >= self.max_length:
            raise PyDBTypeConstraintError("String size exceeds {}".format(
                self.max_length))
        return isinstance(val, str)

    def get_type(self, val):
        return str

    def encode(self, val):
        return str.encode(val)

    def decode(self, val):
        return bytearray.decode(val)

    def get_binary_size(self):
        return self.max_length

