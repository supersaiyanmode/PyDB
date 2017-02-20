import sys
from functools import reduce

def custom_import(class_name):
    if "." not in class_name:
        raise NotImplementedError("Table-Classes must be in packages.")
    parts = class_name.split(".")
    return reduce(getattr, parts[1:], sys.modules[parts[0]])


def get_qualified_name(cls):
    return cls.__module__ + "." + cls.__name__
