import sys
from functools import reduce

def custom_import(class_name):
    if "." not in class_name:
        raise NotImplementedError("Table-Classes must be in packages.")
    parts = class_name.split(".")
    return reduce(getattr, parts[1:], sys.modules[parts[0]])


def get_qualified_name(cls):
    return cls.__module__ + "." + cls.__name__

def int_to_bytes(val, size=4):
    return val.to_bytes(size, byteorder='big', signed=True)

def bytes_to_int(val):
    return int.from_bytes(val, byteorder='big', signed=True)

def bytes_to_ints(val):
    return [bytes_to_int(val[x:x+4]) for x in range(0, len(val), 4)]

def byte_chunker(iterator, chunk_size=1):
    try:
        while True:
            res = b''
            for _ in range(chunk_size):
                res += next(iterator)
            yield res
    except StopIteration:
        pass

def bytes_to_gen(barr):
    return (bytes([x]) for x in barr)
