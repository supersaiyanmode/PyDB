from bisect import bisect_left

from PyDB.exceptions import PyDBUniqueKeyViolation
from PyDB.utils import SafeReader, int_to_bytes, bytes_to_int

class SmallUniqueKeyStore(object):
    SIZE_HEADER = 4

    def __init__(self, io, data_type, initialize=False):
        self.io = io
        self.data_type = data_type

        if initialize:
            self.count = 0
            self.write_header()
        else:
            self.read_header()

    def read_header(self):
        self.io.seek(0)
        self.count = bytes_to_int(self.io.read(self.SIZE_HEADER))

    def write_header(self):
        self.io.seek(0)
        self.io.write(int_to_bytes(self.count, self.SIZE_HEADER))

    def insert(self, item):
        data = self.get_data()
        pos = bisect_left(data, item)

        if pos < len(data):
            if data[pos] == item:
                raise PyDBUniqueKeyViolation(item)

        data.insert(pos, item)

        self.io.seek(0)

        self.count = len(data)
        self.write_header()
        for obj in data:
            self.io.write(self.data_type.encode(obj))

    def get_data(self):
        data = []
        self.read_header()
        for _ in range(self.count):
            obj = self.data_type.decode(self.io.iterdata())
            data.append(obj)
        return data

