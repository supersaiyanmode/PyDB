from PyDB.exceptions import PyDBInternalError

def int_to_bytes(val, size=4):
    return val.to_bytes(size, byteorder='big', signed=True)

def bytes_to_int(val):
    return int.from_bytes(val, byteorder='big', signed=True)

def string_to_bytes(val, encoding='ascii'):
    return str.encode(val, encoding)

def bytes_to_string(barr, encoding='ascii'):
    return bytes.decode(barr, encoding)

def bytes_to_ints(val):
    return [bytes_to_int(val[x:x+4]) for x in range(0, len(val), 4)]

def bytes_to_gen(barr):
    return (bytes([x]) for x in barr)

def gen_to_bytes(barr):
    return b''.join(barr)

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

